//
// event-sync.js — one real-world event, one row, whatever found it.
//
// Discovery and pricing come from different tiers (RA knows an event exists;
// the ticketing platform knows what it costs), so the merge key has to be
// something both can agree on without either owning it. That key is
// artists + date + venue, normalised — not a source id, which would split the
// same night into two rows the moment a second tier saw it.
//

const crypto = require('crypto');
const ra = require('./ra');
const evenko = require('./evenko');
const { enrichVenue } = require('./venue-enrich');
const { normalizeArtistName } = require('./name-case');

// ==================== MERGE KEY ====================

const strip = s => String(s || '')
  .toLowerCase()
  .normalize('NFD').replace(/[̀-ͯ]/g, '')   // café → cafe
  .replace(/[^a-z0-9]+/g, ' ')
  .trim();

/**
 * artists + date + venue. Artists are sorted so a lineup listed in a different
 * order on another source still lands on the same row, and only the first two
 * are used — support acts differ between sources far more often than
 * headliners do.
 */
function fingerprint({ artists = [], artist_name, event_date, venue_name, title }) {
  const names = (artists.length ? artists : [artist_name].filter(Boolean))
    .map(strip).filter(Boolean).sort().slice(0, 2);
  // No artist at all (a club night, a festival day) → fall back to the title,
  // which is the only stable thing such an event has.
  const who = names.length ? names.join('+') : strip(title);
  return [strip(venue_name), String(event_date || '').slice(0, 10), who].join('|');
}

function contentHash(row) {
  return crypto.createHash('sha256')
    .update(JSON.stringify([row.title, row.artists, row.event_date, row.venue_name,
                            row.flyer_url, row.price_min, row.price_max]))
    .digest('hex').slice(0, 32);
}

// ==================== PROJECTION ====================

/**
 * A mapped source event → a mktg_event row.
 *
 * `venue` is the tracked venue it resolved to. Its display name wins over the
 * source's: RA calls it "Piknic Électronik / Parc Jean Drapeau" and we call it
 * Piknic Electronik. The exception is a promoter match — (Avec) Courage! books
 * across a dozen rooms, and overwriting the room with the promoter's name
 * would throw away the most useful thing about those events.
 */
function toRow(mapped, { venueId = null, venue = null, viaPromoter = false } = {}) {
  const artists = (mapped.artists || []).map(normalizeArtistName).filter(Boolean);
  const row = {
    fingerprint:      fingerprint(mapped),
    title:            mapped.title || null,
    artists,
    artist_name:      normalizeArtistName(mapped.artist_name) || artists[0] || null,
    lineup_text:      mapped.lineup_text || null,
    event_date:       mapped.event_date,
    start_time:       mapped.start_time || null,
    end_time:         mapped.end_time || null,
    venue_name:       (!viaPromoter && venue?.display_name) || mapped.venue_name || null,
    venue_id:         venueId,
    city:             mapped.city || null,
    country:          mapped.country || null,
    flyer_url:        mapped.flyer_url || null,
    discovery_source: mapped.source,
    discovery_url:    mapped.event_url || null,
    source_event_id:  mapped.source_event_id || null,
    door_price_text:  mapped.door_price_text || null,
    genres:           mapped.genres || [],
    promoters:        mapped.promoters || [],
    minimum_age:      mapped.minimum_age ?? null,
    is_ticketed:      mapped.is_ticketed ?? null,
    last_validated:   new Date().toISOString(),
    updated_at:       new Date().toISOString(),
  };
  row.content_hash = contentHash(row);
  return row;
}

// ==================== VENUE RESOLUTION ====================

/**
 * Map RA venue ids onto the watchlist. Matching on the RA id is exact; the
 * name fallback exists only for venues added by hand before their id is known.
 */
async function venueIndex(supabase) {
  const { data, error } = await supabase
    .from('mktg_tracked_venue')
    .select('id, name, display_name, enabled, ra_venue_id, evenko_slug, tixr_group_slug, '
          + 'ra_area_id, ra_promoter, internal_venues, platform, color, promoter_id, '
          + 'ticket_page_url, ticket_page_note, city, country');
  if (error) throw new Error(`tracked venues: ${error.message}`);

  const bySlug = new Map();
  const byName = new Map();
  const byPromoter = new Map();
  for (const v of data || []) {
    // One tracked "venue" is really a promoter who books across rooms —
    // (Avec) Courage! has 168 events at a dozen different addresses.
    if (v.ra_promoter) byPromoter.set(strip(v.ra_promoter), v);
    // One venue, an id per platform — SAT is on RA and evenko at once.
    if (v.ra_venue_id)     bySlug.set(`ra:${v.ra_venue_id}`, v);
    if (v.evenko_slug)     bySlug.set(`evenko:${v.evenko_slug}`, v);
    if (v.tixr_group_slug) bySlug.set(`tixr:${v.tixr_group_slug}`, v);
    byName.set(strip(v.name), v);
  }
  return { bySlug, byName, byPromoter, all: data || [] };
}

function resolveVenue(index, mapped) {
  const bySlug = mapped.venue_source_id
    ? index.bySlug.get(`${mapped.source}:${mapped.venue_source_id}`)
    : null;
  if (bySlug) return { venue: bySlug, viaPromoter: false };
  const byName = index.byName.get(strip(mapped.venue_name));
  if (byName) return { venue: byName, viaPromoter: false };
  for (const promoter of mapped.promoters || []) {
    const hit = index.byPromoter.get(strip(promoter));
    if (hit) return { venue: hit, viaPromoter: true };
  }
  return { venue: null, viaPromoter: false };
}

// ==================== RA SYNC ====================

/**
 * Pull an area's events for a date window and upsert them.
 *
 * `watchlistOnly` is the difference between tracking a scene and crawling a
 * platform: with it on, an event is only stored if its venue is one we track.
 * The plan is explicit that this is a watchlist, not a blanket crawl.
 */
async function syncRA(supabase, {
  areaUrlName = 'montreal', countryUrlCode = 'ca',
  from, to, watchlistOnly = true, dryRun = false, log = console.log,
} = {}) {
  const started = Date.now();
  const area = await ra.resolveArea(areaUrlName, countryUrlCode);
  const index = await venueIndex(supabase);

  // Not every tracked venue sits in Montreal's area — BeachClub is in
  // Pointe-Calumet (area 250). Pull each area the watchlist actually needs.
  const areas = [...new Set([
    area.id,
    ...index.all.map(v => v.ra_area_id).filter(id => Number.isFinite(id)),
  ])];

  const events = [];
  let total = 0;
  for (const areaId of areas) {
    const result = await ra.listEvents({ areaId, from, to, log });
    events.push(...result.events);
    total += result.total;
  }
  const mapped = events.map(e => ra.mapEvent(e, { areaName: area.name })).filter(Boolean);

  const candidates = [];
  let skipped = 0;
  for (const m of mapped) {
    if (!m.event_date) { skipped++; continue; }
    const { venue, viaPromoter } = resolveVenue(index, m);
    if (watchlistOnly && !venue) { skipped++; continue; }
    if (venue && venue.enabled === false) { skipped++; continue; }
    candidates.push({ mapped: m, row: toRow(m, { venueId: venue?.id || null, venue, viaPromoter }) });
  }

  log(`  [sync:ra] ${area.name} ${from}..${to}: ${total} on RA → `
    + `${candidates.length} in the watchlist, ${skipped} outside it`);

  if (dryRun) {
    return { ok: true, area: area.name, total, count: candidates.length, skipped,
             events: candidates.map(c => c.row) };
  }

  const stats = await bindAndWrite(supabase, 'ra', candidates, { log });
  log(`  [sync:ra] ${stats.inserted} new · ${stats.updated} updated · `
    + `${stats.merged} merged into an event another source already knew, `
    + `in ${Math.round((Date.now() - started) / 1000)}s`);

  return { ok: true, area: area.name, total, skipped, count: candidates.length, ...stats };
}

/**
 * Writes a source's events, resolving identity through the observation table.
 *
 * The order matters and is the whole point:
 *   1. Have we seen THIS source's id before? Then it is that event, full stop —
 *      no matter how the artist or venue name has been normalised since.
 *   2. Otherwise, does the fingerprint match an event another source already
 *      found? Then bind to it: one night, one row, two sources.
 *   3. Otherwise it is new.
 *
 * Step 1 is what makes identity survive a change to the normalisation rules.
 * Keying on the fingerprint alone — as the first draft did — meant improving
 * name handling would silently duplicate the entire table.
 */
/**
 * Fingerprints are long strings and PostgREST puts an `in()` list in the URL,
 * so a big batch produces a URL long enough to be dropped mid-flight. Keep the
 * batches small and retry once — a transient failure here would otherwise look
 * like "no existing event", which duplicates the whole table.
 */
async function chunkedIn(supabase, table, select, column, values, { batch = 80, extra = null } = {}) {
  const out = [];
  for (let i = 0; i < values.length; i += batch) {
    const slice = values.slice(i, i + batch);
    let lastError;
    for (let attempt = 0; attempt < 3; attempt++) {
      let query = supabase.from(table).select(select);
      if (extra) query = extra(query);
      const { data, error } = await query.in(column, slice);
      if (!error) { out.push(...(data || [])); lastError = null; break; }
      lastError = error;
      await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
    }
    if (lastError) throw new Error(`${table} lookup: ${lastError.message}`);
  }
  return out;
}

async function bindAndWrite(supabase, source, candidates, { log = console.log } = {}) {
  if (!candidates.length) return { inserted: 0, updated: 0, merged: 0 };

  const sourceIds = candidates.map(c => c.mapped.source_event_id).filter(Boolean);
  const bound = new Map();
  const seen = await chunkedIn(supabase, 'mktg_event_observation',
    'event_id, source_event_id', 'source_event_id', sourceIds,
    { batch: 150, extra: q => q.eq('source', source) });
  for (const o of seen) bound.set(o.source_event_id, o.event_id);

  // Only the unbound ones need a fingerprint lookup.
  const unboundPrints = candidates
    .filter(c => !bound.has(c.mapped.source_event_id))
    .map(c => c.row.fingerprint);
  const byPrint = new Map();
  const hits = await chunkedIn(supabase, 'mktg_event', 'id, fingerprint',
    'fingerprint', unboundPrints, { batch: 60 });
  for (const e of hits) if (!byPrint.has(e.fingerprint)) byPrint.set(e.fingerprint, e.id);

  // Two listings can resolve to the same event — a two-room night, or an
  // event another source already contributed. Collapse before writing:
  // a single statement cannot touch the same row twice, and an insert batch
  // cannot carry the same fingerprint twice either.
  const updateById = new Map();
  const insertByPrint = new Map();
  let merged = 0;
  for (const c of candidates) {
    const existing = bound.get(c.mapped.source_event_id) || byPrint.get(c.row.fingerprint);
    if (existing) {
      if (!bound.has(c.mapped.source_event_id) && !updateById.has(existing)) merged++;
      updateById.set(existing, { id: existing, ...c.row });
    } else if (!insertByPrint.has(c.row.fingerprint)) {
      insertByPrint.set(c.row.fingerprint, c);
    }
  }
  const updates = [...updateById.values()];
  const inserts = [...insertByPrint.values()];

  for (let i = 0; i < updates.length; i += 200) {
    const batch = updates.slice(i, i + 200);
    const { error } = await supabase.from('mktg_event').upsert(batch, { onConflict: 'id' });
    if (error) throw new Error(`mktg_event update: ${error.message}`);
  }

  const insertedIds = [];
  for (let i = 0; i < inserts.length; i += 200) {
    const batch = inserts.slice(i, i + 200).map(c => c.row);
    const { data, error } = await supabase.from('mktg_event').insert(batch).select('id, fingerprint');
    if (error) throw new Error(`mktg_event insert: ${error.message}`);
    insertedIds.push(...(data || []));
  }

  // Bind every source id we just wrote, so the next run takes the fast path
  // and can never lose the row to a renormalisation.
  const idByPrint = new Map(insertedIds.map(r => [r.fingerprint, r.id]));
  const observations = [];
  for (const c of candidates) {
    const eventId = bound.get(c.mapped.source_event_id)
      || byPrint.get(c.row.fingerprint)
      || idByPrint.get(c.row.fingerprint);
    if (!eventId || !c.mapped.source_event_id) continue;
    observations.push({
      event_id: eventId,
      source,
      source_event_id: c.mapped.source_event_id,
      source_url: c.mapped.event_url || null,
      payload: c.mapped,
      last_seen_at: new Date().toISOString(),
    });
  }
  for (let i = 0; i < observations.length; i += 200) {
    const { error } = await supabase.from('mktg_event_observation')
      .upsert(observations.slice(i, i + 200), { onConflict: 'source,source_event_id' });
    if (error) throw new Error(`observations write: ${error.message}`);
  }

  return { inserted: insertedIds.length, updated: updates.length, merged };
}

// ==================== OUR OWN ROOMS ====================

/**
 * New City Gas and Bazart come from our own events table and nowhere else.
 * A third party's listing of our own show is never more correct than our own
 * record of it, so these venues are deliberately excluded from every other
 * connector rather than merged with them.
 */
async function syncInternal(supabase, { from, to, dryRun = false, log = console.log } = {}) {
  const started = Date.now();
  const index = await venueIndex(supabase);
  const internal = index.all.filter(v => v.platform === 'internal' && v.enabled !== false);
  if (!internal.length) return { ok: true, count: 0, total: 0 };

  const wanted = new Map();
  for (const venue of internal) {
    for (const name of venue.internal_venues || [venue.name]) wanted.set(strip(name), venue);
  }

  // Deliberately NOT windowed. Every other connector is asking someone else's
  // API and a date range is how you stay polite; this is our own table, the
  // whole of it is a few hundred rows, and clipping it to the sync window was
  // silently hiding four years of our own history.
  const { data: rows, error } = await supabase
    .from('events')
    .select('event_id, event_name, event_date, event_venue, event_artist, event_flyer')
    .gte('event_id', 10000)
    .order('event_date', { ascending: false })
    .limit(5000);
  if (error) throw new Error(`events read: ${error.message}`);

  const candidates = [];
  let skipped = 0;
  for (const row of rows || []) {
    // A null event_venue is a template, a reservation or a test — never a show.
    const venue = wanted.get(strip(row.event_venue));
    if (!venue || !row.event_date || !row.event_name) { skipped++; continue; }

    const artists = String(row.event_artist || '')
      .split(/,|&|\bb2b\b|\bwith\b|\+/i)
      .map(a => a.trim())
      .filter(a => a.length > 1);

    const mapped = {
      source: 'internal',
      source_event_id: String(row.event_id),
      title: row.event_name,
      artists: artists.length ? artists : [row.event_name],
      artist_name: artists[0] || row.event_name,
      lineup_text: row.event_artist || null,
      event_date: String(row.event_date).slice(0, 10),
      start_time: null, end_time: null,
      venue_name: venue.display_name || venue.name,
      venue_source_id: null, venue_url: null,
      city: venue.city || 'Montreal', country: venue.country || 'Canada',
      flyer_url: row.event_flyer || null,
      event_url: null,
      promoters: [], genres: [], minimum_age: null,
      is_ticketed: true,
      door_price_text: null, door_price_min: null,
    };
    candidates.push({ mapped, row: toRow(mapped, { venueId: venue.id, venue }) });
  }

  log(`  [sync:internal] all time: ${candidates.length} of our own events `
    + `(${skipped} rows without a tracked venue)`);

  if (dryRun) return { ok: true, total: rows?.length || 0, count: candidates.length, skipped,
                       events: candidates.map(c => c.row) };

  const stats = await bindAndWrite(supabase, 'internal', candidates, { log });
  log(`  [sync:internal] ${stats.inserted} new · ${stats.updated} updated `
    + `in ${Math.round((Date.now() - started) / 1000)}s`);
  return { ok: true, total: rows?.length || 0, skipped, count: candidates.length, ...stats };
}

// ==================== WEBSITE TOP-UP ====================

/**
 * Reads the ticketing pages of venues that opted in, and merges what it finds.
 *
 * This is the only connector that spends model tokens, so it only ever runs
 * for venues where somebody pasted a URL — a room is enrolled by hand, never
 * by default. Events it finds bind through the same fingerprint as everything
 * else, so a night RA already knows about gets topped up rather than doubled.
 */
async function enrichVenues(supabase, { venueId = null, dryRun = false, renderedHTML = null,
                                        readFlyers = null, log = console.log } = {}) {
  const index = await venueIndex(supabase);
  const targets = index.all.filter(v =>
    v.ticket_page_url && v.enabled !== false && (!venueId || v.id === venueId));

  if (!targets.length) return { ok: true, count: 0, venues: 0, results: [] };

  const results = [];
  const candidates = [];
  for (const venue of targets) {
    const outcome = await enrichVenue(venue, { renderedHTML, readFlyers, log });
    const usable = (outcome.events || []).filter(e => e.event_date && (e.artist_name || e.title));
    results.push({
      venue: venue.display_name || venue.name,
      ok: outcome.ok, strategy: outcome.strategy || null,
      found: usable.length, error: outcome.error || null,
      needsBrowser: outcome.needsBrowser || false,
    });
    for (const mapped of usable) {
      candidates.push({ mapped, row: toRow(mapped, { venueId: venue.id, venue }) });
    }
    log(`  [enrich] ${venue.display_name || venue.name}: ${usable.length} events`
      + (outcome.error ? ` — ${outcome.error}` : ''));
  }

  if (dryRun) {
    return { ok: true, venues: targets.length, count: candidates.length,
             results, events: candidates.map(c => c.row) };
  }

  const stats = candidates.length
    ? await bindAndWrite(supabase, 'website', candidates, { log })
    : { inserted: 0, updated: 0, merged: 0 };

  return { ok: true, venues: targets.length, count: candidates.length, results, ...stats };
}

// ==================== PRICING ====================

/**
 * Second pass over the events RA says are ticketed, filling in the ladder.
 *
 * Deliberately a separate pass rather than part of discovery: it costs one
 * request per ticketed event (about a quarter of them), where discovery costs
 * two for the whole city. Priced events are skipped on later runs unless the
 * price is missing or the event is close enough that tiers are still moving.
 */
async function priceRA(supabase, { from, to, limit = 200, force = false, log = console.log } = {}) {
  let query = supabase
    .from('mktg_event')
    .select('id, source_event_id, event_date, price_min, price_source, artist_name')
    .eq('discovery_source', 'ra')
    .eq('is_ticketed', true)
    .gte('event_date', from)
    .lte('event_date', to)
    .order('event_date', { ascending: true })
    .limit(limit);
  if (!force) query = query.is('price_min', null);

  const { data: rows, error } = await query;
  if (error) throw new Error(`price candidates: ${error.message}`);
  if (!rows?.length) return { ok: true, priced: 0, checked: 0 };

  let priced = 0, checked = 0;
  for (const row of rows) {
    if (!row.source_event_id) continue;
    checked++;
    try {
      const ladder = await ra.fetchTickets(row.source_event_id);
      if (!ladder) continue;
      const { error: writeError } = await supabase.from('mktg_event')
        .update({ ...ladder, updated_at: new Date().toISOString() })
        .eq('id', row.id);
      if (writeError) throw new Error(writeError.message);
      priced++;
    } catch (err) {
      log(`  [price:ra] ${row.artist_name || row.id}: ${err.message.slice(0, 120)}`);
    }
    await new Promise(r => setTimeout(r, 350));
  }

  log(`  [price:ra] ${priced} of ${checked} ticketed events got a real ladder`);
  return { ok: true, priced, checked };
}

// ==================== EVENKO SYNC ====================

/**
 * evenko runs its own rooms and its own ticketing, so it is the authority on
 * the venues it operates — and it covers rooms RA barely sees (MTELUS,
 * Théâtre Beanfield, Centre Bell). It carries no price at all, which is worth
 * knowing rather than discovering later: 100% flyers and ticket links, 0%
 * prices.
 */
async function syncEvenko(supabase, { from, to, watchlistOnly = true, dryRun = false, log = console.log } = {}) {
  const started = Date.now();
  const index = await venueIndex(supabase);

  const result = await evenko.listShows({ from, to, log });
  const shows = Array.isArray(result) ? result : (result?.shows || []);
  const mapped = shows.map(s => evenko.mapShow(s)).filter(m => m?.event_date);

  const candidates = [];
  let skipped = 0;
  for (const m of mapped) {
    const { venue, viaPromoter } = resolveVenue(index, m);
    if (watchlistOnly && !venue) { skipped++; continue; }
    if (venue && venue.enabled === false) { skipped++; continue; }
    candidates.push({ mapped: m, row: toRow(m, { venueId: venue?.id || null, venue, viaPromoter }) });
  }

  log(`  [sync:evenko] ${from}..${to}: ${mapped.length} Montreal shows → `
    + `${candidates.length} in the watchlist, ${skipped} outside it`);

  if (dryRun) {
    return { ok: true, total: mapped.length, count: candidates.length, skipped,
             events: candidates.map(c => c.row) };
  }

  const stats = await bindAndWrite(supabase, 'evenko', candidates, { log });
  log(`  [sync:evenko] ${stats.inserted} new · ${stats.updated} updated · `
    + `${stats.merged} merged with an event RA already had, `
    + `in ${Math.round((Date.now() - started) / 1000)}s`);

  return { ok: true, total: mapped.length, skipped, count: candidates.length, ...stats };
}

/** Windows the plan calls for: forward-looking daily, backfill on demand. */
function dateWindow(days, { from = new Date() } = {}) {
  const start = new Date(from);
  const end = new Date(from.getTime() + days * 86_400_000);
  const iso = d => d.toISOString().slice(0, 10);
  return days >= 0 ? { from: iso(start), to: iso(end) } : { from: iso(end), to: iso(start) };
}

module.exports = { syncRA, syncEvenko, syncInternal, enrichVenues, priceRA, bindAndWrite, fingerprint, toRow, venueIndex, resolveVenue,
                   dateWindow, contentHash };
