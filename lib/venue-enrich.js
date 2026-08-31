//
// venue-enrich.js — top up a venue from its own website.
//
// The structured sources cover most of the watchlist, but not all of it:
// This Is House, Off Piknic and La Voute are on no API at all, and RA lists
// only two BeachClub dates. For those rooms this reads their own site.
//
// Deliberately the LAST resort and deliberately per-venue. It is the only
// path that costs model tokens, so it runs for venues that opt in by pasting
// a URL — never as a blanket crawl. Within that, it still tries the free
// routes first: schema.org if the page publishes it, plain text next, and
// vision over the artwork only when a page is posters with no dates in it.
//

const { buildContent, fetchText } = require('./booking-scraper');
const { extractEvents, extractFromFlyers } = require('./booking-ai');
const { normalizeArtistName } = require('./name-case');
const cheerio = require('cheerio');

/** Real artwork on the page, with whatever text sits beside it. */
function harvestArtwork(html, url, limit = 40) {
  const $ = cheerio.load(html);
  const out = [];
  const seen = new Set();
  for (const el of $('img').toArray()) {
    const $img = $(el);
    const src = $img.attr('src') || $img.attr('data-src') || $img.attr('data-lazy-src');
    if (!src) continue;
    let abs = src;
    try { abs = new URL(src, url).href; } catch { /* keep as-is */ }
    if (seen.has(abs) || /sprite|logo|icon|favicon|pixel|avatar|placeholder/i.test(abs)) continue;
    seen.add(abs);
    const container = $img.closest('a, li, article, div');
    const link = $img.closest('a[href]').attr('href') || container.find('a[href]').first().attr('href');
    let absLink = null;
    try { absLink = link ? new URL(link, url).href : null; } catch { /* not a url */ }
    out.push({
      url: abs,
      link: absLink,
      alt: ($img.attr('alt') || '').trim(),
      nearby: container.text().replace(/\s+/g, ' ').trim().slice(0, 160),
    });
    if (out.length >= limit) break;
  }
  return out;
}

/**
 * Reads one venue's ticketing/events page.
 *
 * @param {object} venue   a mktg_tracked_venue row with ticket_page_url set
 * @param {string} [renderedHTML]  DOM from the Mac app, for sites that refuse
 *                                 plain server requests (Tixr, notably)
 */
async function enrichVenue(venue, { renderedHTML = null, readFlyers = null, log = console.log } = {}) {
  const url = venue.ticket_page_url;
  if (!url) return { ok: false, error: 'no ticketing page set', events: [] };

  let html = renderedHTML;
  if (!html || html.length < 500) {
    try {
      html = await fetchText(url);
    } catch (err) {
      // A ticketing platform refusing the server is the single most common
      // outcome here, and it has a specific remedy — say so rather than
      // returning a bare status code.
      const blocked = /\b(401|403|429)\b/.test(err.message);
      return {
        ok: false, events: [],
        error: blocked
          ? `${new URL(url).hostname} refuses server requests. Open it in the app and analyse it there.`
          : err.message.slice(0, 200),
        needsBrowser: blocked,
      };
    }
  }

  const { content, structured } = buildContent(html, { url, log: () => {} });

  // FREE: the site publishes schema.org events, so there is nothing to infer.
  if (structured.length >= 2 && readFlyers !== true) {
    log(`  [enrich] ${venue.name}: ${structured.length} events from schema.org — no model call`);
    return { ok: true, events: structured.map(e => shape(e, venue)), strategy: 'schema.org' };
  }

  // Artwork-only listings: the date exists nowhere but inside the image.
  const artwork = harvestArtwork(html, url);
  const wantsVision = readFlyers === true
    || (readFlyers === null && artwork.length >= 8 && content.length < 3000);

  if (wantsVision && artwork.length) {
    log(`  [enrich] ${venue.name}: reading ${artwork.length} images with vision`);
    const events = [];
    for (let i = 0; i < artwork.length; i += 10) {
      const batch = artwork.slice(i, i + 10);
      const note = batch.map((f, j) => `${i + j + 1}. ${f.alt || f.nearby}`).join('\n');
      events.push(...await extractFromFlyers({
        images: batch,
        source: { name: venue.name, url, venue: venue.display_name || venue.name, city: venue.city },
        route: { instructions: venue.ticket_page_note || '' },
        contextNote: note,
      }));
    }
    return { ok: true, events: events.map(e => shape(e, venue)), strategy: 'artwork' };
  }

  log(`  [enrich] ${venue.name}: ${content.length} chars of text — reading it`);
  const events = await extractEvents({
    content,
    source: { name: venue.name, url, venue: venue.display_name || venue.name,
              city: venue.city, country: venue.country },
    recipe: { capture_prices: true, instructions: venue.ticket_page_note || '' },
  });
  return { ok: true, events: events.map(e => shape(e, venue)), strategy: 'text' };
}

/** Whatever a page gave us → the shape every other connector produces. */
function shape(event, venue) {
  const artists = (Array.isArray(event.artists) ? event.artists : [])
    .filter(Boolean).map(String).map(normalizeArtistName);
  const primary = normalizeArtistName(event.artist_name) || artists[0] || null;
  const tiers = Array.isArray(event.price_tiers)
    ? event.price_tiers.filter(t => Number(t?.price) > 0)
    : null;
  const prices = (tiers || []).map(t => Number(t.price));

  return {
    source: 'website',
    source_event_id: event.url || `${venue.id}:${event.event_date}:${primary || event.event_name || ''}`,
    title: event.event_name || null,
    artists: artists.length ? artists : (primary ? [primary] : []),
    artist_name: primary,
    lineup_text: null,
    event_date: event.event_date ? String(event.event_date).slice(0, 10) : null,
    start_time: null, end_time: null,
    // The operator's own name for the room always wins over the page's.
    venue_name: venue.display_name || venue.name,
    venue_source_id: null,
    venue_url: venue.ticket_page_url || null,
    city: venue.city || null,
    country: venue.country || null,
    flyer_url: event.flyer_url || null,
    event_url: event.url || venue.ticket_page_url || null,
    promoters: [], genres: [], minimum_age: null,
    is_ticketed: !!(tiers && tiers.length),
    door_price_text: null, door_price_min: null,
    price_tiers: tiers && tiers.length ? tiers : null,
    price_min: prices.length ? Math.min(...prices) : null,
    price_max: prices.length ? Math.max(...prices) : null,
    currency: tiers?.[0]?.currency || null,
    price_source: tiers && tiers.length ? 'website' : null,
  };
}

module.exports = { enrichVenue, harvestArtwork };
