//
// ra.js — Resident Advisor as the discovery layer.
//
// RA's own front-end talks to a GraphQL endpoint that answers server-side
// requests, and — unusually — leaves introspection on. That matters more than
// it sounds: it means the schema is discoverable rather than guessed, so this
// client is written against what RA actually exposes today, and a schema drift
// shows up as a named GraphQL error instead of silently empty results.
//
// Why this replaces DOM scraping outright:
//   - Upcoming vs past is a DATE RANGE ON THE QUERY, not a guess about which
//     part of a page an event sat in. That was the single biggest source of
//     wrong data in the old scraper.
//   - Artists, venue, date and flyer arrive already structured. No model call,
//     so no per-page cost and nothing to re-learn when a site is redesigned.
//   - ra.co's HTML 403s server-side; the GraphQL endpoint does not. The old
//     approach could never have worked from Render at all.
//
// It is an internal API, not a published one, so this client stays polite:
// one request at a time, a real browser User-Agent, a small page size, and
// callers are expected to poll daily rather than on demand.
//

const axios = require('axios');

const ENDPOINT = 'https://ra.co/graphql';

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
    + '(KHTML, like Gecko) Chrome/126.0 Safari/537.36',
  'Content-Type': 'application/json',
  'Referer': 'https://ra.co/events/ca/montreal',
  'Origin': 'https://ra.co',
  'Accept': '*/*',
  'Accept-Language': 'en-CA,en;q=0.9,fr;q=0.8',
};

/** One GraphQL call, with a single retry for transient network trouble. */
async function raQuery(query, variables = {}, { operationName = null, timeout = 30_000 } = {}) {
  let lastError;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const { data, status } = await axios.post(
        ENDPOINT,
        { operationName, query, variables },
        { headers: HEADERS, timeout, validateStatus: () => true }
      );
      if (status !== 200) throw new Error(`RA GraphQL HTTP ${status}`);
      if (data?.errors?.length) {
        // A named GraphQL error means the schema moved under us — that is
        // worth surfacing loudly rather than returning an empty list.
        throw new Error(`RA GraphQL: ${data.errors.map(e => e.message).join('; ').slice(0, 300)}`);
      }
      return data?.data || {};
    } catch (err) {
      lastError = err;
      const transient = /ETIMEDOUT|ECONNRESET|ECONNABORTED|socket hang up|timeout|HTTP 5\d\d/i
        .test(err.message || '');
      if (!transient || attempt === 1) throw err;
      await new Promise(r => setTimeout(r, 3000));
    }
  }
  throw lastError;
}

// ==================== AREAS ====================

const AREA_QUERY = `query AREA($urlName: String, $country: String) {
  area(areaUrlName: $urlName, countryUrlCode: $country) {
    id name urlName country { id name urlCode }
  }
}`;

/**
 * Montreal is area 40 — but resolve it by name rather than hardcoding, so a
 * re-numbering on RA's side surfaces as a lookup failure instead of silently
 * scraping some other city.
 */
async function resolveArea(urlName = 'montreal', countryUrlCode = 'ca') {
  const { area } = await raQuery(AREA_QUERY, { urlName, country: countryUrlCode });
  if (!area?.id) throw new Error(`RA has no area "${urlName}" in "${countryUrlCode}"`);
  return { id: Number(area.id), name: area.name, urlName: area.urlName };
}

// ==================== EVENT LISTINGS ====================

const LISTINGS_QUERY = `query GET_EVENT_LISTINGS($filters: FilterInputDtoInput, $pageSize: Int, $page: Int) {
  eventListings(filters: $filters, pageSize: $pageSize, page: $page) {
    data {
      id
      listingDate
      event {
        id date startTime endTime title contentUrl isTicketed cost minimumAge lineup
        images { id filename type }
        venue { id name contentUrl area { id name } }
        artists { id name }
        promoters { id name }
        genres { name }
      }
    }
    totalResults
  }
}`;

/**
 * Every event in an area between two dates. The date window IS the
 * upcoming/past distinction — ask for the range you want rather than trying
 * to work out which half of a page you are looking at.
 *
 * @param {number} areaId     RA numeric area id (Montreal = 40)
 * @param {string} from       inclusive "YYYY-MM-DD"
 * @param {string} to         inclusive "YYYY-MM-DD"
 */
async function listEvents({ areaId, from, to, pageSize = 100, maxPages = 60, log = console.log }) {
  const byId = new Map();
  let total = null;

  for (let page = 1; page <= maxPages; page++) {
    const filters = { areas: { eq: areaId }, listingDate: { gte: from, lte: to } };
    const data = await raQuery(LISTINGS_QUERY, { filters, pageSize, page },
      { operationName: 'GET_EVENT_LISTINGS' });

    const listings = data?.eventListings;
    // A bad filter combination returns data:null with no `errors` key. Reading
    // .data.length on that throws; treat it as the end of the road instead.
    if (!listings || !Array.isArray(listings.data)) break;

    if (total === null) {
      total = listings.totalResults ?? 0;
      // RA answers -1 past the ~10k deep-pagination ceiling AND for some
      // broken filter combinations. Either way the count is not a count.
      if (total < 0) {
        log('  [ra] totalResults came back negative — narrow the date window; '
          + 'paginating until the pages run dry instead');
        total = null;
      } else {
        log(`  [ra] area ${areaId} ${from}..${to}: ${total} listings`);
      }
    }

    const rows = listings.data.map(d => d?.event).filter(e => e?.id);
    // eventListings returns one row per listing DAY, not per event: a
    // multi-day festival comes back once per day it runs (the Jazz Festival
    // appears 10 times). Deduplicate on event id or the same night is
    // processed, normalised and written ten times over.
    for (const event of rows) {
      if (!byId.has(event.id)) byId.set(event.id, event);
    }

    if (!listings.data.length) break;
    if (total !== null && (page * pageSize) >= total) break;

    // Deliberately serial and unhurried: this is somebody's internal API.
    await new Promise(r => setTimeout(r, 400));
  }

  return { events: [...byId.values()], total: total ?? byId.size, listings: total };
}

/**
 * Long windows in year-sized bites. RA stops counting honestly past roughly
 * 10,000 deep-paginated results, so a multi-year backfill asked for in one
 * request would silently truncate.
 */
async function listEventsChunked({ areaId, from, to, chunkDays = 365, log = console.log, ...rest }) {
  const start = new Date(from), finish = new Date(to);
  const byId = new Map();
  let cursor = start;

  while (cursor <= finish) {
    const next = new Date(Math.min(cursor.getTime() + chunkDays * 86_400_000, finish.getTime()));
    const { events } = await listEvents({
      areaId, from: cursor.toISOString().slice(0, 10), to: next.toISOString().slice(0, 10),
      log, ...rest,
    });
    for (const e of events) byId.set(e.id, e);
    if (next.getTime() >= finish.getTime()) break;
    cursor = new Date(next.getTime() + 86_400_000);
  }
  return { events: [...byId.values()], total: byId.size };
}

// ==================== VENUES ====================

const VENUES_QUERY = `query VENUES($areaId: ID, $limit: Int, $orderBy: OrderByType!) {
  venues(areaId: $areaId, limit: $limit, orderBy: $orderBy) {
    id name contentUrl area { id name }
  }
}`;

/**
 * CAUTION: `venues(limit: N)` returns an EMPTY array at HTTP 200 with no
 * errors key once N >= 21 — it looks like "this area has no venues" rather
 * than "your limit is too high". The watchlist is therefore resolved from the
 * venues attached to real events (see event-sync), not from this query; it is
 * kept only for small lookups.
 */
async function listVenues({ areaId, limit = 20, orderBy = 'ALPHABETICAL' }) {
  const data = await raQuery(VENUES_QUERY, { areaId: String(areaId), limit, orderBy });
  return data?.venues || [];
}

// ==================== MAPPING ====================

/** RA content URLs are site-relative; images.ra.co ones are already absolute. */
function absoluteRA(url) {
  if (!url) return null;
  if (/^https?:\/\//i.test(url)) return url;
  return `https://ra.co${url.startsWith('/') ? '' : '/'}${url}`;
}

/**
 * The flyer. `flyerFront` is dead on RA's current schema — it comes back empty
 * on every event — but `images[]` carries the same artwork tagged FLYERFRONT,
 * and it can be selected in the LISTING query, so flyers cost no extra
 * requests. Verified 92/100 on a live Montreal page.
 */
function flyerFrom(event) {
  const images = event?.images || [];
  const front = images.find(i => String(i?.type || '').toUpperCase() === 'FLYERFRONT');
  return (front || images[0])?.filename || absoluteRA(event?.flyerFront) || null;
}

/**
 * RA's `lineup` is richer than its `artists` array: artists with an RA profile
 * are wrapped in <artist id="…">…</artist>, and everyone else — support acts,
 * local openers, b2b partners — appears as plain text that `artists` drops
 * entirely. Read both and merge.
 */
function lineupNames(lineup) {
  if (!lineup) return [];
  const text = String(lineup)
    // Turn the tags into boundaries FIRST. Adjacent linked artists carry no
    // punctuation between them, so stripping the tags outright welds two
    // names into one ("Akanbi zi!").
    .replace(/<artist[^>]*>/gi, '\n')
    .replace(/<\/artist>/gi, '\n')
    .replace(/<[^>]+>/g, '\n');

  return text
    .split(/\r?\n|\s*[\/|+•·]\s*|\s*,\s*|\s+b2b\s+|\s+vs\.?\s+|\s+w\/\s*/i)
    .map(part => part
      .replace(/\s+/g, ' ')
      .replace(/^(?:\+|&|with|and|feat\.?|ft\.?|presents?|pres\.?)\s+/i, '')
      .replace(/[\s\-–—:]+$/, '')
      .trim())
    .filter(part => part.length > 1 && part.length < 60
      && !/^(?:more|tba|tbc|special guests?|guests?|support|residents?|and|&|w)$/i.test(part));
}

/** RA states door price as free text: "10", "0", "", "10-15", "Free before 11". */
function parseCost(cost) {
  if (cost == null) return { text: null, min: null };
  const text = String(cost).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
  if (!text) return { text: null, min: null };
  const numbers = (text.match(/\d+(?:[.,]\d{2})?/g) || [])
    .map(n => parseFloat(n.replace(',', '.')))
    .filter(n => Number.isFinite(n) && n > 0);
  return { text, min: numbers.length ? Math.min(...numbers) : null };
}

/**
 * One RA event → our canonical shape. Deliberately does NOT decide whether an
 * event is past: that is a function of the date at read time, never a stored
 * flag that goes stale the moment midnight passes.
 */
function mapEvent(event, { areaName = null } = {}) {
  if (!event?.id) return null;

  const linked = (event.artists || []).map(a => a?.name).filter(Boolean);
  const fromLineup = lineupNames(event.lineup);
  // Linked artists first (they are canonical spellings), then anyone the
  // lineup text mentions that the array missed.
  const seen = new Set(linked.map(n => n.toLowerCase()));
  const artists = [...linked, ...fromLineup.filter(n => !seen.has(n.toLowerCase()) && seen.add(n.toLowerCase()))];

  const cost = parseCost(event.cost);
  const date = String(event.date || event.startTime || '').slice(0, 10) || null;

  return {
    source: 'ra',
    source_event_id: String(event.id),
    title: event.title || null,
    artists,
    artist_name: artists[0] || null,
    lineup_text: event.lineup ? String(event.lineup).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim() : null,
    event_date: date,
    start_time: event.startTime || null,
    end_time: event.endTime || null,
    venue_name: event.venue?.name || null,
    venue_source_id: event.venue?.id ? String(event.venue.id) : null,
    venue_url: absoluteRA(event.venue?.contentUrl),
    city: event.venue?.area?.name || areaName || null,
    country: 'Canada',
    flyer_url: flyerFrom(event),
    event_url: absoluteRA(event.contentUrl),
    promoters: (event.promoters || []).map(p => p?.name).filter(Boolean),
    genres: (event.genres || []).map(g => g?.name).filter(Boolean),
    minimum_age: Number.isFinite(event.minimumAge) ? event.minimumAge : null,
    is_ticketed: event.isTicketed === true,
    // RA's door price — coarse and only present about half the time, but free.
    // Real tiers come from the ticketing platform (see the pricing layer).
    door_price_text: cost.text,
    door_price_min: cost.min,
  };
}

module.exports = {
  raQuery, resolveArea, listEvents, listEventsChunked, listVenues, mapEvent,
  lineupNames, parseCost, flyerFrom,
  ENDPOINT, LISTINGS_QUERY, AREA_QUERY, VENUES_QUERY,
};
