//
// evenko.js — evenko as the Tier 3 discovery source.
//
// evenko.ca is a Next.js app whose venue pages ship an EMPTY "Upcoming shows"
// container: the listing is filled in the browser, so there is nothing in the
// HTML to scrape and a DOM scraper returns zero rows forever. The data behind
// that container comes from Contentful's public Delivery API, and evenko's own
// bundle carries the space id and read token in clear text because that is how
// a Contentful front-end is meant to work — the token is read-only and
// published, not a secret that leaked.
//
// WHY THERE IS AN OFF SWITCH (EVENKO_ENABLED)
//   Two things make this different from RA, and both are judgement calls
//   rather than technical limits, so they get a switch instead of a comment:
//     - evenko's robots.txt disallows /api/, /search and the filtered listing
//       query strings. This client touches none of those — it talks to
//       graphql.contentful.com, a different host with its own permissive
//       robots — but the spirit of that file is "do not automate our
//       listings", and reasonable people would read it either way.
//     - The credentials come from their JS bundle. Published, but not offered.
//   The operator approved this connector, so the default is ON. Setting
//   EVENKO_ENABLED=false turns it off at the single choke point (cfQuery)
//   without touching any caller, so switching it back off is one env var and
//   a redeploy, not a code change.
//
// TWO THINGS THAT WILL BITE A FUTURE READER
//   1. showCollection is NOT Montreal-scoped. It is evenko's whole national
//      feed — a 120-day window carries Halifax, Moncton, Shelburne VT and
//      about 60 Quebec towns. Filtering is this module's job, not the API's.
//   2. Contentful caps query COMPLEXITY at 11000 and charges by
//      limit x nested-collection-limits. A query that fans out into
//      venue.linkedFrom costs 48400 and is rejected outright. The selection
//      set below is deliberately flat for that reason: widen it and page
//      sizes have to come down to compensate.
//
// PRICE: there is none. Verified across the live 120-day feed — no price
// field on Show, Event or Venue, and not one "$<digits>" anywhere in the
// payload. The only monetary lead is the ticket URL inside
// additionalInformation, and it points off-site to a dozen different
// ticketing platforms (ticketmaster, universe, eventim, tuxedobillet,
// ticketpro, lepointdevente...). Price for evenko events is a separate tier
// that resolves those URLs; it is not obtainable here.
//

const axios = require('axios');
const cheerio = require('cheerio');
const { normalizeArtistName } = require('./name-case');

const SPACE_DEFAULT = '3yxl57nu0yl4';
const TOKEN_DEFAULT = '3idgqsCLLCm3FH0SV6M9BZTdDDyJn_dVZAX_8uHYsTU';

// Mutable because refreshTokens can replace them at runtime. Env wins over the
// hardcoded pair so a rotation can be fixed by config before it is fixed by code.
let space = process.env.EVENKO_SPACE || SPACE_DEFAULT;
let token = process.env.EVENKO_TOKEN || TOKEN_DEFAULT;

const SITE = 'https://evenko.ca';

function endpointFor(s) {
  return `https://graphql.contentful.com/content/v1/spaces/${s}/environments/master`;
}

const HEADERS = () => ({
  'Authorization': `Bearer ${token}`,
  'Content-Type': 'application/json',
  'Accept': 'application/json',
});

// ==================== GATE ====================

/** Off only when explicitly set to the string "false"; anything else is on. */
function enabled() {
  return String(process.env.EVENKO_ENABLED ?? 'true').toLowerCase() !== 'false';
}

// ==================== TRANSPORT ====================

// A rotated token would otherwise re-scrape the bundle on every single call.
let refreshed = false;

// Contentful names its own failures, and only two of them are repairable from
// here. Recognising them by code rather than by HTTP status matters: a dead
// token answers 401, but a renamed space answers 400 — the same status as a
// typo'd field name and a query over the complexity cap, neither of which a
// credential refresh could possibly fix.
const CREDENTIAL_CODES = new Set(['ACCESS_TOKEN_INVALID', 'UNKNOWN_SPACE']);

/**
 * Whatever Contentful said about the failure, in preference to a bare status
 * code. "HTTP 400" sends the next operator to the network tab; "the maximum
 * allowed complexity for a query is 11000 but it was 101000" sends them to the
 * selection set.
 */
function cfError(data, status) {
  const messages = (data?.errors || []).map(e => e?.message).filter(Boolean);
  const err = new Error(messages.length
    ? `evenko Contentful: ${messages.join('; ').slice(0, 400)}`
    : `evenko Contentful HTTP ${status}`);
  // Carried separately so the retry test does not depend on the status being
  // legible in the message — a 502 that also returns an errors array reads as
  // its message now, and a string match would stop calling it transient.
  err.status = status;
  return err;
}

/**
 * One Contentful call. Throws on an `errors` array rather than returning it,
 * because every error this API produces is structural — a renamed field, a
 * dead token, a query over the complexity cap — and none of them should be
 * allowed to look like "this window has no shows".
 */
async function cfQuery(query, variables = {}, { timeout = 30_000, allowRefresh = true } = {}) {
  if (!enabled()) {
    throw new Error('evenko connector disabled (EVENKO_ENABLED=false); unset it to re-enable');
  }

  let lastError;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const { data, status } = await axios.post(
        endpointFor(space), { query, variables },
        { headers: HEADERS(), timeout, validateStatus: () => true }
      );

      // A rotated token or a renamed space. Try to re-read the pair from the
      // live bundle once, then give the call one more go before failing the
      // run. 404 stays in the test as a belt-and-braces status check; the
      // codes are what actually fire.
      const codes = (data?.errors || []).map(e => e?.extensions?.contentful?.code).filter(Boolean);
      const credentialFailure = status === 401 || status === 404
        || codes.some(c => CREDENTIAL_CODES.has(c));

      if (credentialFailure && allowRefresh && !refreshed) {
        refreshed = true;
        await refreshTokens();
        return cfQuery(query, variables, { timeout, allowRefresh: false });
      }
      if (status !== 200 || data?.errors?.length) throw cfError(data, status);
      return data?.data || {};
    } catch (err) {
      lastError = err;
      const transient = err.status >= 500
        || /ETIMEDOUT|ECONNRESET|ECONNABORTED|socket hang up|timeout|HTTP 5\d\d/i
          .test(err.message || '');
      if (!transient || attempt === 1) throw err;
      await new Promise(r => setTimeout(r, 3000));
    }
  }
  throw lastError;
}

// ==================== CREDENTIAL REFRESH ====================

// Contentful delivery tokens are 43 base64url characters; space ids are 12
// lower-case alphanumerics. Both are string literals sitting inside the same
// minified function as the endpoint template.
const TOKEN_LITERAL = /["']([A-Za-z0-9_-]{43})["']/g;
const SPACE_LITERAL = /["']([a-z0-9]{12})["']/g;

async function fetchText(url, timeout = 20_000) {
  const { data, status } = await axios.get(url, {
    timeout, responseType: 'text', validateStatus: () => true,
    headers: {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
        + '(KHTML, like Gecko) Chrome/126.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,*/*',
      'Accept-Language': 'en-CA,en;q=0.9,fr;q=0.8',
    },
  });
  if (status !== 200) throw new Error(`GET ${url} -> HTTP ${status}`);
  return String(data);
}

async function ask(candidateSpace, candidateToken, query) {
  const { data, status } = await axios.post(
    endpointFor(candidateSpace), { query },
    {
      headers: { Authorization: `Bearer ${candidateToken}`, 'Content-Type': 'application/json' },
      timeout: 15_000, validateStatus: () => true,
    }
  );
  return { status, data };
}

/**
 * Is this pair the published-content pair?
 *
 * Two things have to be true, and the second one is not optional. evenko's
 * bundle holds three tokens in one ternary: delivery, PREVIEW, and a second
 * brand's space. The preview token answers exactly the same showCollection
 * query on the same endpoint — an earlier version of this function accepted
 * it, which would have quietly started syncing unpublished draft shows.
 *
 * `preview: true` is the discriminator, and it is Contentful's own rather
 * than a guess about where a literal sits in minified code: a delivery token
 * fails authentication against it, a preview token does not.
 */
async function probe(candidateSpace, candidateToken) {
  const live = await ask(candidateSpace, candidateToken, '{ showCollection(limit: 1) { total } }');
  if (live.status !== 200 || live.data?.errors?.length) return false;
  if (!Number.isFinite(live.data?.data?.showCollection?.total)) return false;

  const draft = await ask(candidateSpace, candidateToken,
    '{ showCollection(limit: 1, preview: true) { total } }');
  return Boolean(draft.data?.errors?.length);
}

/**
 * Re-extract the space and token from the live site when the hardcoded pair
 * stops working.
 *
 * The chunk filenames are content-hashed and change on every deploy, so the
 * only durable anchor is the CURRENT html: read its <script src> list, then
 * look for the endpoint template inside each file. Hardcoding a chunk URL
 * would buy a fix that expires at their next deploy.
 *
 * Candidates are confirmed by calling the API, never by their position in the
 * minified ternary — the preview token and the 5 Salles token are string
 * literals of exactly the same shape sitting three characters away.
 */
async function refreshTokens({ url = `${SITE}/en`, log = console.log } = {}) {
  const html = await fetchText(url);
  const $ = cheerio.load(html);

  const srcs = [];
  $('script[src]').each((_, el) => {
    const src = $(el).attr('src');
    if (!src || !/\.js(\?|$)/i.test(src)) return;
    const abs = new URL(src, url).toString();
    if (!srcs.includes(abs)) srcs.push(abs);
  });
  if (!srcs.length) throw new Error('evenko refresh: no script tags on ' + url);

  for (const src of srcs) {
    let js;
    try { js = await fetchText(src); } catch { continue; }
    if (!js.includes('graphql.contentful.com')) {
      await new Promise(r => setTimeout(r, 150));
      continue;
    }

    // Only the neighbourhood of the endpoint template, so an unrelated 43-char
    // literal elsewhere in a 70KB chunk cannot become a candidate.
    for (const m of js.matchAll(/graphql\.contentful\.com/g)) {
      const window_ = js.slice(Math.max(0, m.index - 500), m.index + 1500);
      const spaces = [...new Set([...window_.matchAll(SPACE_LITERAL)].map(x => x[1]))];
      const tokens = [...new Set([...window_.matchAll(TOKEN_LITERAL)].map(x => x[1]))];

      for (const s of spaces) {
        for (const t of tokens) {
          if (await probe(s, t)) {
            space = s;
            token = t;
            log(`  [evenko] credentials refreshed from ${src}: space ${s}`);
            return { space: s, token: t, endpoint: endpointFor(s), chunk: src };
          }
        }
      }
    }
    await new Promise(r => setTimeout(r, 150));
  }
  throw new Error('evenko refresh: no working space/token pair found in the current bundle');
}

// ==================== CITY NORMALISATION ====================

/**
 * City strings in this space are hand-entered and inconsistent — accents come
 * and go ("Montréal" / "Montreal"), and leading and trailing spaces survive
 * into the field. Comparing them raw silently drops real Montreal venues, so
 * every comparison and every value written out goes through here first.
 */
function cityKey(value) {
  return String(value ?? '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

/**
 * The city as written to the database. Folded to ASCII on purpose: RA reports
 * "Montreal" and evenko reports "Montréal", and if both spellings land in
 * mktg_event.city the same city groups as two.
 */
function cityDisplay(value) {
  const clean = String(value ?? '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  return clean || null;
}

// ==================== SHOWS ====================

// Flat by necessity: see the complexity note in the header. Every nested
// collection here multiplies against the page limit.
const SHOW_FIELDS = `
    sys { id }
    code title slug showTime doorTime admissionAge free
    eventTag presentedBy additionalInformation
    venue { sys { id } name slug city province country }
    category { title }
    styleCollection(limit: 4) { items { title } }
    headlinersCollection(limit: 8) { items { name } }
    supportArtistsCollection(limit: 8) { items { name } }
    visual { url }`;

const SHOWS_QUERY = `query SHOWS($from: DateTime!, $to: DateTime!, $limit: Int!, $skip: Int!) {
  showCollection(
    limit: $limit
    skip: $skip
    order: show_time_ASC
    where: { show_time_gte: $from, show_time_lte: $to, private_not: true }
  ) {
    total
    items {${SHOW_FIELDS}
    }
  }
}`;

/** Accepts "YYYY-MM-DD", a Date, or a full ISO string. */
function isoStart(value) {
  if (value instanceof Date) return value.toISOString();
  const s = String(value);
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? `${s}T00:00:00.000Z` : new Date(s).toISOString();
}

function isoEnd(value) {
  if (value instanceof Date) return value.toISOString();
  const s = String(value);
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? `${s}T23:59:59.999Z` : new Date(s).toISOString();
}

/**
 * evenko shows in a date window, filtered down to the cities we care about.
 *
 * @param {string}   from    inclusive "YYYY-MM-DD"
 * @param {string}   to      inclusive "YYYY-MM-DD"
 * @param {string[]} cities  matched through cityKey; pass null for the whole feed
 * @param {number}   limit   page size — 200 measured well under the 11000 cap
 */
async function listShows({ from, to, cities = ['Montreal'], limit = 200, maxPages = 40,
                           log = console.log } = {}) {
  const wanted = cities?.length ? new Set(cities.map(cityKey)) : null;
  const byId = new Map();
  let total = null;
  let scanned = 0;

  for (let page = 0; page < maxPages; page++) {
    const data = await cfQuery(SHOWS_QUERY, {
      from: isoStart(from), to: isoEnd(to), limit, skip: page * limit,
    });

    const collection = data?.showCollection;
    // On the first page this is a schema change wearing the costume of a quiet
    // night: every caller downstream reads zero shows as a real answer and
    // writes nothing, so the break has to be an exception instead. Later pages
    // can stop quietly — whatever came before them is still good data.
    if (!collection || !Array.isArray(collection.items)) {
      if (page === 0) throw new Error('evenko: no showCollection in the response; the schema moved');
      break;
    }

    if (total === null) {
      total = collection.total ?? 0;
      log(`  [evenko] ${from}..${to}: ${total} shows nationally`
        + (wanted ? `, filtering to ${[...wanted].join(', ')}` : ', unfiltered'));
    }

    for (const show of collection.items) {
      scanned++;
      if (!show?.sys?.id) continue;
      if (wanted && !wanted.has(cityKey(show.venue?.city))) continue;
      // skip-based paging over a collection that is being published into can
      // hand back the same entry on two pages; sys.id is the only key that is
      // guaranteed unique per entry (`code` is a business code, not a key).
      if (!byId.has(show.sys.id)) byId.set(show.sys.id, show);
    }

    if (!collection.items.length) break;
    if ((page + 1) * limit >= total) break;

    // Serial and unhurried. This is a free public CDN, not our quota.
    await new Promise(r => setTimeout(r, 250));
  }

  // Silently partial is the same failure as silently empty, one page later:
  // the run succeeds, the count looks plausible, and the tail of the window is
  // simply missing. Say so.
  if (total !== null && scanned < total && scanned >= maxPages * limit) {
    log(`  [evenko] WARNING: page cap reached — scanned ${scanned} of ${total} feed rows; raise maxPages`);
  }

  const shows = [...byId.values()];
  log(`  [evenko] ${shows.length} kept of ${scanned} scanned`);
  return { shows, total: shows.length, scanned, feedTotal: total ?? scanned };
}

// ==================== VENUES ====================

const VENUES_QUERY = `query VENUES($limit: Int!, $skip: Int!) {
  venueCollection(limit: $limit, skip: $skip, order: name_ASC, where: { archive_not: true }) {
    total
    items {
      sys { id }
      name slug city province country address code website admission ownedByEvenko
    }
  }
}`;

/**
 * Every venue evenko lists, not just the seven it owns — the feed is full of
 * third-party rooms it sells tickets for. Returns city both raw and
 * normalised so a caller can match on one and display the other.
 */
async function listVenues({ cities = null, limit = 200, log = console.log } = {}) {
  const wanted = cities?.length ? new Set(cities.map(cityKey)) : null;
  const out = [];
  let total = null;
  let scanned = 0;

  const maxPages = 20;
  for (let page = 0; page < maxPages; page++) {
    const data = await cfQuery(VENUES_QUERY, { limit, skip: page * limit });
    const collection = data?.venueCollection;
    if (!collection || !Array.isArray(collection.items)) {
      if (page === 0) throw new Error('evenko: no venueCollection in the response; the schema moved');
      break;
    }
    if (total === null) total = collection.total ?? 0;
    scanned += collection.items.length;

    for (const v of collection.items) {
      if (!v?.sys?.id) continue;
      if (wanted && !wanted.has(cityKey(v.city))) continue;
      out.push({
        source_venue_id: v.sys.id,
        name: (v.name || '').trim() || null,
        slug: v.slug || null,
        code: v.code || null,
        city: cityDisplay(v.city),
        city_raw: v.city ?? null,
        province: v.province || null,
        country: v.country || 'Canada',
        address: v.address || null,
        website: v.website || null,
        admission: v.admission || null,
        owned_by_evenko: v.ownedByEvenko === true,
        venue_url: v.slug ? `${SITE}/en/venues/${v.slug}` : null,
      });
    }

    if (!collection.items.length || (page + 1) * limit >= total) break;
    await new Promise(r => setTimeout(r, 250));
  }

  if (total !== null && scanned < total && scanned >= maxPages * limit) {
    log(`  [evenko] WARNING: page cap reached — scanned ${scanned} of ${total} venues`);
  }
  log(`  [evenko] ${out.length} venues${wanted ? ` in ${[...wanted].join(', ')}` : ''} of ${total} listed`);
  return out;
}

// ==================== MAPPING ====================

/** "everyone" and "none" are policies, not ages; only a number is an age. */
function parseAdmissionAge(value) {
  const n = parseInt(String(value ?? ''), 10);
  return Number.isFinite(n) && n > 0 && n < 100 ? n : null;
}

/**
 * evenko publishes artists as linked entries rather than a lineup string, so
 * the headliner/support split is already correct and only the CASING needs
 * fixing — their CMS has both "RUSH" and "Ilana Glazer".
 */
function showArtists(show) {
  const raw = [
    ...(show?.headlinersCollection?.items || []),
    ...(show?.supportArtistsCollection?.items || []),
  ].map(a => (a?.name || '').trim()).filter(Boolean);

  const seen = new Set();
  const out = [];
  for (const name of raw) {
    const clean = normalizeArtistName(name);
    const key = clean.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(clean);
  }
  return out;
}

/**
 * Two genre fields, both partly filled. `eventTag` is the specific one
 * ("Hip-Hop,Rap") and `style` is the broad one ("Musique"), so the specific
 * tags lead.
 */
function showGenres(show) {
  const tags = String(show?.eventTag || '').split(',').map(t => t.trim()).filter(Boolean);
  const styles = (show?.styleCollection?.items || [])
    .map(s => (s?.title || '').trim()).filter(Boolean);

  const seen = new Set();
  return [...tags, ...styles].filter(g => {
    const key = g.toLowerCase();
    return seen.has(key) ? false : seen.add(key);
  });
}

// "Présenté par le Festival International de Jazz de Montréal", "Agence
// Spectra présente", "evenko et POP Montréal présentent". The presenter is
// stored as a sentence, so the sentence has to come off before the name is
// worth anything to a join.
const PRESENTER_PREFIX = /^(?:présent[ée]s?\s+par|presented\s+by|présente|presents)\s+/i;
const PRESENTER_SUFFIX = /\s+(?:présenten?t|presents?|présent[ée]s?)\s*$/i;
const LEADING_ARTICLE = /^(?:les|le|la|l')\s+/i;

/**
 * Deliberately does NOT split a co-presentation ("evenko et POP Montréal")
 * into two names: " et " and " & " turn up inside single organisation names
 * often enough that splitting would invent promoters that do not exist.
 */
function parsePresenter(value) {
  const clean = String(value ?? '').replace(/\s+/g, ' ').trim()
    .replace(PRESENTER_PREFIX, '')
    .replace(PRESENTER_SUFFIX, '')
    .replace(LEADING_ARTICLE, '')
    .trim();
  return clean ? normalizeArtistName(clean) : null;
}

/**
 * The public page URL, built the way evenko's own router builds it:
 * /{lang}/{events}/{venue.slug}/{show.slug}?code={show.code}. The code query
 * param is not decoration — a venue can run the same slug twice.
 */
function showUrl(show) {
  const venueSlug = show?.venue?.slug;
  const slug = show?.slug;
  if (!venueSlug || !slug) return null;
  const code = show?.code ? `?code=${encodeURIComponent(show.code)}` : '';
  return `${SITE}/en/events/${venueSlug}/${slug}${code}`;
}

/**
 * One evenko show → the same canonical shape lib/ra.js emits, so both sources
 * feed bindAndWrite unchanged.
 *
 * door_price_text and door_price_min are always null and that is not an
 * oversight: Contentful carries no price for these events at all. See the
 * header.
 */
function mapShow(show) {
  if (!show?.sys?.id) return null;

  const artists = showArtists(show);
  const venue = show.venue || {};
  // showTime carries its own -04:00/-05:00 offset, so slicing the date off the
  // string gives the local calendar night. Converting to UTC first would move
  // every evening show after 8pm onto the following day.
  const date = show.showTime ? String(show.showTime).slice(0, 10) : null;

  return {
    source: 'evenko',
    // The Contentful entry id, not `code`. The entry id is the actual primary
    // key; `code` is an Akeneo product code and is not guaranteed unique.
    source_event_id: String(show.sys.id),
    title: (show.title || '').trim() || null,
    artists,
    artist_name: artists[0] || null,
    lineup_text: artists.length ? artists.join(', ') : null,
    event_date: date,
    start_time: show.showTime || null,
    // Nothing on the schema states when a show ends. closingDate exists but
    // marks the last date of a multi-date run, so using it here would report
    // a September show as ending in December.
    end_time: null,
    venue_name: (venue.name || '').trim() || null,
    venue_source_id: venue.sys?.id ? String(venue.sys.id) : null,
    venue_url: venue.slug ? `${SITE}/en/venues/${venue.slug}` : null,
    city: cityDisplay(venue.city),
    country: venue.country || 'Canada',
    flyer_url: show.visual?.url || null,
    event_url: showUrl(show),
    promoters: [parsePresenter(show.presentedBy)].filter(Boolean),
    genres: showGenres(show),
    minimum_age: parseAdmissionAge(show.admissionAge),
    // `free` is nullable and in practice never set, so absence means ticketed.
    is_ticketed: show.free !== true,
    door_price_text: null,
    door_price_min: null,
  };
}

module.exports = {
  cfQuery, listShows, listVenues, mapShow, refreshTokens,
};

// Getters, not constants: refreshTokens can replace the pair at runtime, and a
// canary asserting a stale literal would pass while the module talked to a
// different space.
Object.defineProperties(module.exports, {
  ENDPOINT: { enumerable: true, get: () => endpointFor(space) },
  SPACE: { enumerable: true, get: () => space },
  TOKEN: { enumerable: true, get: () => token },
});
