//
// booking-scraper.js — the generic, recipe-driven event scraper.
//
// Replaces one hand-written file per website. Every site is a row in
// mktg_booking_source carrying a `recipe`:
//
//   mode          api    → hit a JSON endpoint the site's own front-end uses
//                 html   → fetch the page and read it (incl. embedded JSON)
//                 browser→ needs a rendered DOM; the Mac app posts the HTML
//   api           { url, method, headers, body, json_path }
//   list_selector CSS container for one event (narrows what the AI reads)
//   field_hints   { artist, date, price, flyer, url, name } — CSS hints
//   detail        { follow_links, link_selector, limit }
//   pricing_mode  none | starting | all_tiers
//   instructions  plain English from the authoring chat
//
// The split that makes this robust: SELECTORS locate the content,
// the LLM reads the messy human text (French dates, "w/ support", tier
// tables) and returns clean fields. If selectors rot, the whole-page text
// still gets read, so a site redesign degrades instead of breaking.
//

const axios = require('axios');
const cheerio = require('cheerio');
const crypto = require('crypto');
const { extractEvents } = require('./booking-ai');

const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
  + '(KHTML, like Gecko) Chrome/126.0 Safari/537.36';

// Kept deliberately tight: every char here is an input token on every
// nightly run across every site. 40K chars ≈ 10K tokens ≈ $0.03/scrape.
const MAX_CONTENT = 40_000;

// ==================== FETCH ====================

async function fetchText(url, { headers = {}, method = 'GET', body = null } = {}) {
  const res = await axios({
    url,
    method,
    data: body || undefined,
    headers: {
      'User-Agent': UA,
      'Accept-Language': 'en-CA,en;q=0.9,fr;q=0.8',
      ...headers,
    },
    timeout: 45_000,
    maxRedirects: 5,
    validateStatus: s => s >= 200 && s < 400,
    responseType: 'text',
    transformResponse: [d => d],
  });
  return typeof res.data === 'string' ? res.data : JSON.stringify(res.data);
}

// ==================== HTML → READABLE CONTENT ====================

/**
 * Follows a path into a parsed payload. Tolerates the JSONPath-ish syntax
 * models like to write: a leading "$.", numeric indexes, and "[*]" wildcards
 * (which flatten, so "a[*].b" collects every b).
 */
function pluck(obj, path) {
  if (!path) return obj;
  const clean = String(path).replace(/^\$\.?/, '');
  if (!clean) return obj;

  let current = [obj];
  for (const segment of clean.split('.')) {
    if (!segment) continue;
    const match = segment.match(/^([^\[\]]*)((?:\[[^\]]*\])*)$/);
    const key = match ? match[1] : segment;
    const brackets = (match?.[2] || '').match(/\[[^\]]*\]/g) || [];

    if (key) current = current.map(v => (v == null ? v : v[key]));
    for (const bracket of brackets) {
      const inner = bracket.slice(1, -1);
      if (inner === '*' || inner === '') {
        current = current.flatMap(v => (Array.isArray(v) ? v : v == null ? [] : [v]));
      } else {
        const idx = Number(inner.replace(/['"]/g, ''));
        current = Number.isFinite(idx)
          ? current.map(v => (Array.isArray(v) ? v[idx] : undefined))
          : current.map(v => (v == null ? v : v[inner.replace(/['"]/g, '')]));
      }
    }
    current = current.filter(v => v !== undefined);
  }
  return current.length === 1 ? current[0] : current;
}

/**
 * Structured data hiding in the HTML. This is where most modern event sites
 * actually keep their data — reading it beats scraping rendered text, and it
 * means "JS-heavy" sites often work with a plain fetch.
 */
function embeddedJSON($) {
  const out = [];

  // schema.org — the jackpot when present
  $('script[type="application/ld+json"]').each((_, el) => {
    const raw = $(el).contents().text().trim();
    if (!raw) return;
    try {
      const parsed = JSON.parse(raw);
      const items = Array.isArray(parsed) ? parsed : [parsed];
      for (const item of items) {
        const graph = item['@graph'];
        for (const node of (Array.isArray(graph) ? graph : [item])) {
          const type = String(node['@type'] || '');
          if (/event|musicevent|festival/i.test(type)) out.push(node);
        }
      }
    } catch { /* malformed block — skip */ }
  });

  // Framework payloads
  for (const id of ['__NEXT_DATA__', '__NUXT_DATA__']) {
    const raw = $(`script#${id}`).contents().text().trim();
    if (raw) {
      try { out.push({ __framework: id, data: JSON.parse(raw) }); } catch { /* skip */ }
    }
  }

  return out;
}

/**
 * schema.org Events parsed WITHOUT a model. Most ticketing/venue sites
 * publish complete ld+json — name, startDate, location, offers — so this
 * path costs nothing and is more accurate than any extraction. The LLM is
 * only needed when this comes up short.
 */
function structuredEvents(nodes, base) {
  const out = [];
  for (const node of nodes) {
    if (node.__framework) continue;
    const start = node.startDate || node.startTime;
    if (!start) continue;
    const date = String(start).slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) continue;

    const performers = []
      .concat(node.performer || node.performers || [])
      .map(p => (typeof p === 'string' ? p : p?.name))
      .filter(Boolean);

    const place = node.location || {};
    const address = place.address || {};
    const offers = [].concat(node.offers || []).filter(o => o && typeof o === 'object');
    const prices = offers.map(o => Number(o.price)).filter(n => Number.isFinite(n) && n > 0);

    const image = Array.isArray(node.image) ? node.image[0] : node.image;

    out.push({
      artist_name: performers[0] || node.name || null,
      artists: performers.length ? performers : (node.name ? [node.name] : []),
      event_name: node.name || null,
      event_date: date,
      venue: typeof place === 'string' ? place : (place.name || null),
      location: address.streetAddress || null,
      city: address.addressLocality || null,
      country: address.addressCountry?.name || address.addressCountry || null,
      url: node.url ? absolute(node.url, base) : null,
      flyer_url: image ? absolute(typeof image === 'string' ? image : image.url, base) : null,
      price_min: prices.length ? Math.min(...prices) : null,
      price_max: prices.length ? Math.max(...prices) : null,
      currency: offers[0]?.priceCurrency || null,
      price_tiers: offers.length > 1
        ? offers.map(o => ({
            name: o.name || null,
            price: Number(o.price) || null,
            currency: o.priceCurrency || null,
            sold_out: /soldout/i.test(String(o.availability || '')),
          }))
        : null,
      __source: 'schema.org',
    });
  }
  return out;
}

/** Flattens one element into a compact line the model can read cheaply. */
function elementDigest($, el, base) {
  const $el = $(el);
  const text = $el.text().replace(/\s+/g, ' ').trim();
  if (!text) return null;

  const links = [];
  $el.find('a[href]').slice(0, 3).each((_, a) => {
    const href = $(a).attr('href');
    if (href && !href.startsWith('#')) links.push(absolute(href, base));
  });
  const images = [];
  $el.find('img').slice(0, 2).each((_, img) => {
    const src = $(img).attr('src') || $(img).attr('data-src') || $(img).attr('data-lazy-src');
    if (src) images.push(absolute(src, base));
  });
  // Dates often live in attributes rather than text
  const attrs = [];
  for (const name of ['datetime', 'data-date', 'content', 'data-start']) {
    $el.find(`[${name}]`).slice(0, 3).each((_, node) => {
      const v = $(node).attr(name);
      if (v) attrs.push(`${name}=${v}`);
    });
  }

  return [
    text.slice(0, 700),
    links.length ? `LINKS: ${links.join(' ')}` : '',
    images.length ? `IMAGES: ${images.join(' ')}` : '',
    attrs.length ? `ATTRS: ${attrs.join(' ')}` : '',
  ].filter(Boolean).join('\n');
}

function absolute(href, base) {
  try { return new URL(href, base).toString(); } catch { return href; }
}

/** Whole-page fallback text with the chrome stripped out. */
function pageText($) {
  $('script, style, noscript, svg, header nav, footer').remove();
  return $('body').text().replace(/[ \t]+/g, ' ').replace(/\n{3,}/g, '\n\n').trim();
}

/**
 * Turns a page into the content block handed to the extractor: embedded JSON
 * first (most reliable), then the per-event blocks, then plain text.
 */
function buildContent(html, { url, listSelector, log = null }) {
  const $ = cheerio.load(html);
  const parts = [];

  const json = embeddedJSON($);
  const structured = structuredEvents(json, url);

  if (json.length) {
    // Framework payloads are mostly routing/state noise; only the ld+json
    // Event nodes are worth paying for.
    const worthSending = json.filter(n => !n.__framework);
    const dump = JSON.stringify(worthSending.length ? worthSending : json)
      .slice(0, Math.floor(MAX_CONTENT * 0.55));
    parts.push(`=== EMBEDDED STRUCTURED DATA (most reliable) ===\n${dump}`);
  }

  // A selector is only trusted if it actually matches this page. A stale or
  // hallucinated one is ignored rather than silently starving the extractor.
  if (listSelector) {
    let matches = [];
    try { matches = $(listSelector).toArray(); } catch { matches = []; }
    if (matches.length) {
      const blocks = [];
      matches.slice(0, 120).forEach((el, i) => {
        const digest = elementDigest($, el, url);
        if (digest) blocks.push(`--- ITEM ${i + 1} ---\n${digest}`);
      });
      if (blocks.length) {
        parts.push(`=== ITEMS MATCHING "${listSelector}" ===\n${blocks.join('\n')}`);
      }
    } else if (log) {
      log(`  [booking] selector "${listSelector}" matched nothing — falling back to the whole page`);
    }
  }

  // Always give the extractor the page text when the structured sources came
  // up short, so a broken selector degrades instead of returning nothing.
  if (parts.join('').length < 2_000) {
    parts.push(`=== PAGE TEXT ===\n${pageText($)}`);
  }

  const content = parts.join('\n\n').slice(0, MAX_CONTENT);
  const hash = crypto.createHash('sha256').update(content).digest('hex').slice(0, 32);
  return { content, structured, hash };
}

// ==================== RECIPE EXECUTION ====================

/**
 * Pulls the content for a source and hands it to the AI extractor.
 * `renderedHTML` lets the Mac app supply a JS-rendered DOM for browser-mode
 * sites (its WKWebView can load anything; the server can't).
 */
async function collectEvents(source, { renderedHTML = null, force = false, log = console.log } = {}) {
  const recipe = source.recipe || {};
  const mode = renderedHTML ? 'html' : (recipe.mode || 'html');

  let content, structured = [], hash = null;
  if (mode === 'api' && recipe.api?.url) {
    const raw = await fetchText(recipe.api.url, {
      method: recipe.api.method || 'GET',
      headers: recipe.api.headers || {},
      body: recipe.api.body || null,
    });
    let payload = null;
    try { payload = JSON.parse(raw); } catch { /* not JSON */ }

    if (payload === null) {
      // The recipe called it an API but the endpoint served a page. Rather
      // than fail the sync, read it as HTML — self-healing beats correct.
      log(`  [booking] ${source.name}: "api" endpoint returned non-JSON — reading it as HTML instead`);
      ({ content, structured, hash } = buildContent(raw, { url: recipe.api.url, listSelector: recipe.list_selector, log }));
    } else {
      const picked = recipe.api.json_path ? pluck(payload, recipe.api.json_path) : payload;
      const usable = (picked == null || (Array.isArray(picked) && picked.length === 0)) ? payload : picked;
      content = `=== API RESPONSE (${recipe.api.url}) ===\n`
        + JSON.stringify(usable).slice(0, MAX_CONTENT);
      hash = crypto.createHash('sha256').update(content).digest('hex').slice(0, 32);
    }
  } else {
    const html = renderedHTML || await fetchText(source.url);
    ({ content, structured, hash } = buildContent(html, { url: source.url, listSelector: recipe.list_selector, log }));
  }

  if (!content || content.length < 40) {
    throw new Error('Nothing readable at that URL — the page may need a rendered browser (browser mode).');
  }

  // Nothing changed since the last successful run → no model call at all.
  if (!force && hash && hash === source.content_hash) {
    log(`  [booking] ${source.name}: unchanged since last sync — skipped (no AI call)`);
    return { events: [], unchanged: true, hash };
  }

  // FREE PATH: the site published complete schema.org events, so there is
  // nothing for a model to work out.
  if (structured.length >= 3) {
    log(`  [booking] ${source.name}: ${structured.length} events from schema.org — no AI needed`);
    return { events: structured, hash };
  }

  log(`  [booking] ${source.name}: ${content.length} chars via ${mode} → AI`);
  let events = await extractEvents({ content, source, recipe });
  // Structured rows are more trustworthy than parsed ones; keep both.
  if (structured.length) {
    const seen = new Set(events.map(e => `${e.event_date}|${(e.artist_name||'').toLowerCase()}`));
    events = [...structured.filter(e => !seen.has(`${e.event_date}|${(e.artist_name||'').toLowerCase()}`)), ...events];
  }

  // Optional second pass: open each event's own page for prices/lineups the
  // listing page doesn't carry.
  if (recipe.detail?.follow_links && events.length) {
    const limit = Math.min(recipe.detail.limit || 12, events.length);
    for (let i = 0; i < limit; i++) {
      const target = events[i]?.url;
      if (!target) continue;
      try {
        const detailHtml = await fetchText(absolute(target, source.url));
        const { content: detailContent } = buildContent(detailHtml, { url: target, listSelector: recipe.detail.list_selector });
        const [enriched] = await extractEvents({
          content: detailContent, source, recipe,
          hint: `This is the DETAIL page for "${events[i].artist_name || events[i].event_name}". Return exactly one event, filling in prices and the full lineup.`,
        });
        if (enriched) events[i] = { ...events[i], ...stripEmpty(enriched) };
      } catch (e) {
        log(`  [booking] detail fetch failed for ${target}: ${e.message}`);
      }
    }
  }

  return { events, hash };
}

function stripEmpty(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v !== null && v !== undefined && v !== '' && !(Array.isArray(v) && v.length === 0)) out[k] = v;
  }
  return out;
}

// ==================== NORMALIZE + PERSIST ====================

function normalizeDate(value) {
  if (!value) return null;
  const s = String(value).trim();
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return iso[0];
  const parsed = new Date(s);
  if (!isNaN(parsed.getTime())) return parsed.toISOString().slice(0, 10);
  return null;
}

function toNumber(value) {
  if (value == null) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const cleaned = String(value).replace(/[^\d.,]/g, '').replace(',', '.');
  const n = parseFloat(cleaned);
  return Number.isFinite(n) ? n : null;
}

function fingerprint(source, event) {
  const key = [
    source.name || '',
    event.event_date || '',
    event.artist_name || event.event_name || event.url || '',
  ].join('|').toLowerCase().replace(/\s+/g, ' ').trim();
  return key;
}

function projectRow(source, event) {
  const date = normalizeDate(event.event_date);
  const artists = Array.isArray(event.artists) ? event.artists.filter(Boolean).map(String) : [];
  const primary = event.artist_name || artists[0] || null;
  const tiers = Array.isArray(event.price_tiers) ? event.price_tiers : null;
  const tierPrices = (tiers || []).map(t => toNumber(t.price)).filter(n => n != null);

  const row = {
    source_id:   source.id,
    source_name: source.name,
    artist_name: primary,
    artists:     artists.length ? artists : (primary ? [primary] : []),
    event_name:  event.event_name || null,
    event_date:  date,
    // The source's manual venue/city/country win — that's the whole point of
    // setting them by hand; the page's own values only fill the gaps.
    venue:    source.venue    || event.venue    || null,
    location: source.location || event.location || null,
    city:     source.city     || event.city     || null,
    country:  source.country  || event.country  || null,
    url:       event.url ? absolute(event.url, source.url) : null,
    flyer_url: event.flyer_url ? absolute(event.flyer_url, source.url) : null,
    price_min: toNumber(event.price_min) ?? (tierPrices.length ? Math.min(...tierPrices) : null),
    price_max: toNumber(event.price_max) ?? (tierPrices.length ? Math.max(...tierPrices) : null),
    currency:  event.currency || (tiers?.[0]?.currency ?? null),
    price_tiers: tiers,
    raw: event,
    last_seen_at: new Date().toISOString(),
    updated_at:   new Date().toISOString(),
  };
  row.fingerprint = fingerprint(source, row);
  return row;
}

/** Scrapes one source and upserts what it finds. */
async function scrapeSource(supabase, source, { renderedHTML = null, dryRun = false, log = console.log } = {}) {
  const started = Date.now();
  try {
    if (!dryRun) {
      await supabase.from('mktg_booking_source')
        .update({ last_status: 'running' }).eq('id', source.id);
    }

    const collected = await collectEvents(source, { renderedHTML, force: dryRun, log });
    const rows = (collected.events || [])
      .map(e => projectRow(source, e))
      .filter(r => r.event_date && (r.artist_name || r.event_name));

    // Same event twice in one page (e.g. listed under two tabs)
    const unique = [...new Map(rows.map(r => [r.fingerprint, r])).values()];

    if (dryRun) {
      return { ok: true, count: unique.length, events: unique, dryRun: true };
    }

    if (collected.unchanged) {
      await supabase.from('mktg_booking_source').update({
        last_sync_at: new Date().toISOString(),
        last_status: 'ok',
        last_error: null,
        updated_at: new Date().toISOString(),
      }).eq('id', source.id);
      return { ok: true, count: 0, unchanged: true };
    }

    if (unique.length) {
      const { error } = await supabase.from('mktg_booking')
        .upsert(unique, { onConflict: 'fingerprint' });
      if (error) throw new Error(`mktg_booking upsert: ${error.message}`);
    }

    await supabase.from('mktg_booking_source').update({
      last_sync_at: new Date().toISOString(),
      last_status: 'ok',
      last_error: null,
      last_count: unique.length,
      content_hash: collected.hash || null,
      updated_at: new Date().toISOString(),
    }).eq('id', source.id);

    log(`  [booking] ${source.name}: ${unique.length} events in ${Math.round((Date.now() - started) / 1000)}s`);
    return { ok: true, count: unique.length, events: unique };
  } catch (err) {
    const message = err.response?.data?.error?.message || err.message;
    if (!dryRun) {
      await supabase.from('mktg_booking_source').update({
        last_sync_at: new Date().toISOString(),
        last_status: 'error',
        last_error: message.slice(0, 500),
        updated_at: new Date().toISOString(),
      }).eq('id', source.id);
    }
    log(`  [booking] ${source.name} FAILED: ${message}`);
    return { ok: false, error: message, count: 0 };
  }
}

/** Every enabled source, one at a time (politeness + rate safety). */
async function scrapeAll(supabase, { log = console.log } = {}) {
  const { data: sources, error } = await supabase
    .from('mktg_booking_source')
    .select('*')
    .eq('enabled', true)
    .order('name');
  if (error) throw new Error(`sources read: ${error.message}`);

  const results = [];
  for (const source of sources || []) {
    // browser-mode sites can only be run from the Mac app's WKWebView
    if ((source.recipe?.mode || 'html') === 'browser') {
      log(`  [booking] ${source.name}: skipped (browser mode — sync it from the app)`);
      results.push({ source: source.name, skipped: true });
      continue;
    }
    results.push({ source: source.name, ...(await scrapeSource(supabase, source, { log })) });
  }
  return results;
}

module.exports = { scrapeSource, scrapeAll, collectEvents, buildContent, fetchText };
