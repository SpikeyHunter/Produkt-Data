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
    .select('id, name, platform, source_slug, enabled');
  if (error) throw new Error(`tracked venues: ${error.message}`);

  const bySlug = new Map();
  const byName = new Map();
  for (const v of data || []) {
    if (v.source_slug) bySlug.set(`${v.platform}:${v.source_slug}`, v);
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

  const rows = [];
  let skipped = 0;
  for (const m of mapped) {
    if (!m.event_date) { skipped++; continue; }
    const venue = resolveVenue(index, m);
    if (watchlistOnly && !venue) { skipped++; continue; }
    if (venue && venue.enabled === false) { skipped++; continue; }
    rows.push(toRow(m, { venueId: venue?.id || null }));
  }

  // The same night can appear twice in one pull (a two-room event listed per
  // room); collapse before writing so the upsert has one row per key.
  const unique = [...new Map(rows.map(r => [r.fingerprint, r])).values()];

  log(`  [sync:ra] ${area.name} ${from}..${to}: ${total} on RA → `
    + `${unique.length} kept, ${skipped} outside the watchlist`);

  if (dryRun) {
    return { ok: true, area: area.name, total, count: unique.length, skipped, events: unique };
  }

  for (let i = 0; i < unique.length; i += 200) {
    const batch = unique.slice(i, i + 200);
    const { error } = await supabase.from('mktg_event')
      .upsert(batch, { onConflict: 'fingerprint' });
    if (error) throw new Error(`mktg_event upsert: ${error.message}`);
  }

  log(`  [sync:ra] ${unique.length} events in ${Math.round((Date.now() - started) / 1000)}s`);
  return { ok: true, area: area.name, total, count: unique.length, skipped };
}

/** Windows the plan calls for: forward-looking daily, backfill on demand. */
function dateWindow(days, { from = new Date() } = {}) {
  const start = new Date(from);
  const end = new Date(from.getTime() + days * 86_400_000);
  const iso = d => d.toISOString().slice(0, 10);
  return days >= 0 ? { from: iso(start), to: iso(end) } : { from: iso(end), to: iso(start) };
}

module.exports = { syncRA, fingerprint, toRow, venueIndex, resolveVenue, dateWindow, contentHash };
