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

/** A mapped source event → a mktg_event row. */
function toRow(mapped, { venueId = null } = {}) {
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
    venue_name:       mapped.venue_name || null,
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
    .select('id, name, enabled, ra_venue_id, evenko_slug, tixr_group_slug');
  if (error) throw new Error(`tracked venues: ${error.message}`);

  const bySlug = new Map();
  const byName = new Map();
  for (const v of data || []) {
    // One venue, an id per platform — SAT is on RA and evenko at once.
    if (v.ra_venue_id)     bySlug.set(`ra:${v.ra_venue_id}`, v);
    if (v.evenko_slug)     bySlug.set(`evenko:${v.evenko_slug}`, v);
    if (v.tixr_group_slug) bySlug.set(`tixr:${v.tixr_group_slug}`, v);
    byName.set(strip(v.name), v);
  }
  return { bySlug, byName, all: data || [] };
}

function resolveVenue(index, mapped) {
  const bySlug = mapped.venue_source_id
    ? index.bySlug.get(`${mapped.source}:${mapped.venue_source_id}`)
    : null;
  return bySlug || index.byName.get(strip(mapped.venue_name)) || null;
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

  const { events, total } = await ra.listEvents({ areaId: area.id, from, to, log });
  const mapped = events.map(e => ra.mapEvent(e, { areaName: area.name })).filter(Boolean);

  const candidates = [];
  let skipped = 0;
  for (const m of mapped) {
    if (!m.event_date) { skipped++; continue; }
    const venue = resolveVenue(index, m);
    if (watchlistOnly && !venue) { skipped++; continue; }
    if (venue && venue.enabled === false) { skipped++; continue; }
    candidates.push({ mapped: m, row: toRow(m, { venueId: venue?.id || null }) });
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
    const venue = resolveVenue(index, m);
    if (watchlistOnly && !venue) { skipped++; continue; }
    if (venue && venue.enabled === false) { skipped++; continue; }
    candidates.push({ mapped: m, row: toRow(m, { venueId: venue?.id || null }) });
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

module.exports = { syncRA, syncEvenko, priceRA, bindAndWrite, fingerprint, toRow, venueIndex, resolveVenue,
                   dateWindow, contentHash };
