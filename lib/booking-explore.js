//
// booking-explore.js — walk a website and come back with its events.
//
// The old flow proposed selectors and left the operator to test them. This
// one does the work: it looks at the site, finds the pages where events
// actually live (a promoter almost always splits them — "Événements",
// "Archive", "Past events"), runs each one for real, and hands back the
// events it found, split into what is still to come and what has already
// happened. What the operator confirms is a result, not a guess.
//

const cheerio = require('cheerio');
const { llmComplete } = require('./ig-ai');
const { parseJSON } = require('./booking-ai');
const { collectEvents, projectRow, fetchText, absolute } = require('./booking-scraper');
const { pageSignature, recallPatterns } = require('./booking-patterns');

// Words a promoter uses for "here are the events", in both languages.
const SECTION_WORDS = /\b(events?|shows?|gigs?|lineup|programm|agenda|calendar|calendrier|billetterie|tickets?|upcoming|current|next|a[- ]venir|à[- ]venir|prochains?|past|previous|archive|archives|pass[ée]s?|history|gallery|galerie)\b/i;

/**
 * Every page on this site that might be a listing. Nav first — that is where
 * the "Upcoming / Past / Archive" split almost always lives.
 */
function discoverSections($, baseURL) {
  let origin = '';
  try { origin = new URL(baseURL).origin; } catch { /* not a url */ }

  const seen = new Map();
  const consider = (href, text, source) => {
    if (!href || href.startsWith('#') || /^(mailto|tel|javascript):/i.test(href)) return;
    const abs = absolute(href, baseURL);
    if (!abs || (origin && !abs.startsWith(origin))) return;   // same site only
    const label = (text || '').replace(/\s+/g, ' ').trim().slice(0, 60);
    if (!label) return;
    const key = abs.split('#')[0];
    if (seen.has(key)) return;
    if (!SECTION_WORDS.test(label) && !SECTION_WORDS.test(key)) return;
    seen.set(key, { url: key, label, found_in: source });
  };

  $('nav a[href], header a[href], [role=navigation] a[href]').each((_, el) => {
    consider($(el).attr('href'), $(el).text(), 'navigation');
  });
  $('a[href]').each((_, el) => {
    if (seen.size > 40) return;
    consider($(el).attr('href'), $(el).text(), 'page');
  });

  return [...seen.values()].slice(0, 20);
}

const PLAN_SYSTEM = `You are looking at ONE page of an event/venue/promoter website, plus every link on it that might lead to more events. Decide which pages to actually read, and what each one holds.

Promoters almost always split their events up — upcoming shows in one place, past shows in an archive, sometimes a gallery of nothing but flyer artwork. Each of those is a separate pass.

Reply with ONLY a JSON object:
{
  "reply": string,          // 2-4 sentences, plain English: what this site is and how it organises its events
  "sections": [
    {
      "name": string,       // human: "Upcoming shows", "Past events (archive)"
      "url": string,        // the page to read — the page you were shown, or one of the links
      "expects": "upcoming" | "past" | "mixed",
      "read_flyers": boolean,   // the listing is artwork and the date exists only inside the image
      "follow_links": boolean,  // prices/lineup need each event's own page opened
      "list_selector": string|null,
      "why": string         // one sentence
    }
  ],
  "suggested": { "venue": string|null, "location": string|null, "city": string|null, "country": string|null, "name": string|null }
}

Rules:
- At most 4 sections. One per genuinely different listing, never one per event.
- ALWAYS include the page you were shown if it has events on it.
- Only name a URL that appears in the links you were given, or the page itself. Never invent one.
- read_flyers true ONLY when the listing is artwork with no date in the text — it costs vision tokens. A grid of posters under a "Past events" heading is exactly this case.
- list_selector must match EVERY card in that group. Leave off framework-generated classes (w-dyn-item, sqs-block, elementor-*) — they sit on only some cards and hide the rest. When unsure, null: reading the whole page loses nothing.
- ONE PAGE OFTEN HOLDS BOTH. A promoter's events page commonly lists the next few shows in text and then a long grid of past artwork below. That is TWO sections sharing one URL — a text pass for upcoming and a flyer pass for past — and you should propose both. Repeating a URL across two sections is expected, not a mistake.
- A page named "archive" or "gallery" is not always events: it is often crowd photos from past nights. Only treat it as a listing if you can see event names or dates.`;

/** Runs one section for real and normalises what comes back. */
async function runSection(source, section, log) {
  // Tell the extractor what this page is FOR. A promoter's upcoming page
  // usually carries the whole past archive underneath it, and without this
  // the model returns a scattering of both.
  const focus = section.expects === 'upcoming'
    ? 'This page is the site\'s UPCOMING listing. Return only events that are still to come. Ignore any "past events" / "archive" section further down the same page.'
    : section.expects === 'past'
      ? 'This page is the site\'s PAST events archive. Return the past events. They are usually many — return every one you can read.'
      : '';

  const route = {
    name: section.name,
    url: section.url,
    read_flyers: section.read_flyers,
    follow_links: section.follow_links,
    list_selector: section.list_selector,
    instructions: [section.instructions, focus].filter(Boolean).join(' '),
    // Analysis is a preview, not a sync — keep vision costs sane.
    flyer_limit: 20,
    detail: section.follow_links ? { follow_links: true, limit: 6 } : null,
  };

  const collected = await collectEvents(
    { ...source, recipe: { ...source.recipe, routes: [route] } },
    { force: true, log });

  const rows = (collected.events || [])
    .map(e => projectRow(source, e))
    .filter(r => r.event_date && (r.artist_name || r.event_name))
    .map(r => ({ ...r, __section: section.id }));

  return { rows };
}

/** How many real images sit on a page — the tell for an artwork-only listing. */
async function looksLikeArtwork(url) {
  try {
    const $ = cheerio.load(await fetchText(url));
    const images = $('img').toArray().filter(el => {
      const src = $(el).attr('src') || $(el).attr('data-src') || '';
      return src && !/sprite|logo|icon|favicon|pixel|avatar/i.test(src);
    });
    return images.length >= 8 ? images.length : 0;
  } catch {
    return 0;
  }
}

/**
 * Reads the site and returns the events it actually found, split live/past,
 * with the detections that produced them.
 */
async function exploreSite(supabase, { url, html = null, log = console.log }) {
  const startHTML = html && html.length > 500 ? html : await fetchText(url);
  const $ = cheerio.load(startHTML);
  const signature = pageSignature(startHTML, url);
  const patterns = await recallPatterns(supabase, signature);
  const links = discoverSections($, url);

  log(`  [explore] ${url}: ${signature.platform}, ${links.length} candidate section link(s)`);

  const plan = await llmComplete(PLAN_SYSTEM, [{
    role: 'user',
    content: JSON.stringify({
      url,
      built_with: signature.platform,
      traits: signature.traits,
      links_that_might_lead_to_events: links,
      page_text: $('body').text().replace(/\s+/g, ' ').trim().slice(0, 6000),
      images_on_this_page: $('img').toArray().filter(el => {
        const src = $(el).attr('src') || $(el).attr('data-src') || '';
        return src && !/sprite|logo|icon|favicon|pixel|avatar/i.test(src);
      }).length,
      markup_sample: startHTML.slice(0, 14_000),
      sections_that_worked_on_sites_built_the_same_way: patterns.map(p => ({
        platform: p.platform, routes: p.routes, produced: p.event_count,
      })),
    }).slice(0, 60_000),
  }], { maxTokens: 3000, timeout: 180_000 });

  const parsed = parseJSON(plan, null);
  const sections = (parsed?.sections || [])
    .slice(0, 4)
    .map((section, i) => ({
      id: `s${i + 1}`,
      name: section.name || `Section ${i + 1}`,
      url: section.url || url,
      expects: section.expects || 'mixed',
      read_flyers: section.read_flyers === true,
      follow_links: section.follow_links === true,
      list_selector: section.list_selector || null,
      instructions: section.why || '',
    }));

  if (!sections.length) {
    return {
      reply: parsed?.reply || 'I could not find an events listing on that page — try the site\'s own events page.',
      sections: [], live: [], past: [], signature, suggested: parsed?.suggested || {},
    };
  }

  // Now actually run them. This is the difference: what comes back is events,
  // not a proposal to be tested later.
  const source = {
    id: '', name: signature.host || url, url,
    recipe: { capture_prices: true },
  };

  const results = [];
  for (const planned of sections) {
    let section = planned;
    try {
      let outcome = await runSection(source, section, log);

      // The planner had to guess whether a page is artwork-only from a
      // different page, which it cannot know. So don't rely on the guess:
      // if the text pass came back empty and the page is mostly images,
      // read it with vision instead. Self-correcting beats predicting.
      if (!outcome.rows.length && !section.read_flyers) {
        const flyerish = await looksLikeArtwork(section.url);
        if (flyerish) {
          log(`  [explore] ${section.name}: nothing in the text and `
            + `${flyerish} images on the page — rereading it with vision`);
          section = { ...section, read_flyers: true };
          outcome = await runSection(source, section, log);
        }
      }

      results.push({ ...section, count: outcome.rows.length, rows: outcome.rows });
      log(`  [explore] ${section.name}: ${outcome.rows.length} events`);
    } catch (err) {
      results.push({ ...section, count: 0, rows: [], error: err.message });
      log(`  [explore] ${section.name} failed: ${err.message}`);
    }
  }

  // Nothing past anywhere, yet the listing page is wall-to-wall artwork: the
  // past shows are in the images and no section was pointed at them.
  const today0 = new Date().toISOString().slice(0, 10);
  const foundPast = results.some(r => r.rows.some(row => (row.event_date || '') < today0));
  if (!foundPast) {
    const best = results.filter(r => !r.read_flyers).sort((a, b) => b.count - a.count)[0];
    const artwork = best ? await looksLikeArtwork(best.url) : 0;
    if (best && artwork >= 8) {
      log(`  [explore] no past events found and ${artwork} images on ${best.url} — reading them as flyers`);
      const section = {
        id: `s${results.length + 1}`,
        name: 'Past events (flyers)',
        url: best.url,
        expects: 'past',
        read_flyers: true,
        follow_links: false,
        list_selector: null,
        instructions: 'The past shows on this page are artwork only — the date is inside the image.',
      };
      try {
        const outcome = await runSection(source, section, log);
        results.push({ ...section, count: outcome.rows.length, rows: outcome.rows });
        log(`  [explore] ${section.name}: ${outcome.rows.length} events`);
      } catch (err) {
        log(`  [explore] flyer sweep failed: ${err.message}`);
      }
    }
  }

  // Split by date, not by which page they came from — an "upcoming" page that
  // still lists last month's show should say so.
  const today = new Date().toISOString().slice(0, 10);
  const all = results.flatMap(r => r.rows);
  const seen = new Set();
  const unique = all.filter(r => !seen.has(r.fingerprint) && seen.add(r.fingerprint));

  return {
    reply: parsed?.reply || '',
    suggested: parsed?.suggested || {},
    signature,
    learned_from: patterns.length,
    sections: results.map(({ rows, ...rest }) => rest),
    live: unique.filter(r => (r.event_date || '') >= today),
    past: unique.filter(r => (r.event_date || '') < today),
  };
}

module.exports = { exploreSite, discoverSections };
