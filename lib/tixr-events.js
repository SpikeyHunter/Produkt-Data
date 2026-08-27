//
// tixr-events.js — event detail straight from the Studio API.
//
// Two things the events table doesn't carry:
//   · the canonical public URL (slugged, e.g. .../andruss-paskal-daze-moonart-187478)
//   · ticket tier pricing (GA / VIP price ladders)
//
// Used by the "New City Gas" internal booking source so our own shows show
// the same price ranges we scrape from everyone else.
//

const axios = require('axios');
const crypto = require('crypto');

const { TIXR_GROUP_ID, TIXR_CPK, TIXR_SECRET_KEY } = process.env;
const API = 'https://studio.tixr.com';

/** Real ticket categories — everything else is tables, comps, guest list. */
const PRICED_CATEGORIES = new Set(['GA', 'VIP']);

/** Names that exist in Studio but aren't things a fan can buy online. */
const NON_PUBLIC = /\b(comp|billet physique|physical|guest ?list|staff|artist)\b/i;

function signedURL(basePath) {
  const params = { cpk: TIXR_CPK, t: Date.now() };
  const sorted = Object.keys(params).sort()
    .map(k => `${k}=${encodeURIComponent(params[k])}`).join('&');
  const hash = crypto.createHmac('sha256', TIXR_SECRET_KEY)
    .update(`${basePath}?${sorted}`).digest('hex');
  const query = Object.keys(params)
    .map(k => `${k}=${encodeURIComponent(params[k])}`).join('&');
  return `${API}${basePath}?${query}&hash=${hash}`;
}

/**
 * Canonical URL + ticket tiers for one event.
 * Returns null when Tixr doesn't know the event (custom/internal rows).
 */
async function fetchEventDetail(eventId) {
  const url = signedURL(`/v1/groups/${TIXR_GROUP_ID}/events/${eventId}`);
  const { data } = await axios.get(url, { timeout: 20_000 });
  const event = Array.isArray(data) ? data[0] : data;
  if (!event || !event.id) return null;

  const tiers = [];
  for (const sale of event.sales || []) {
    if (!PRICED_CATEGORIES.has(sale.category)) continue;
    if (NON_PUBLIC.test(sale.name || '')) continue;

    for (const tier of sale.tiers || []) {
      const price = Number(tier.price);
      // Zero-price rows are comps/holds, not a public price point.
      if (!Number.isFinite(price) || price <= 0) continue;
      if (NON_PUBLIC.test(tier.name || '')) continue;
      tiers.push({
        name: tier.name || sale.name,
        category: sale.category,
        price,
        currency: 'CAD',
        // ONLY an explicit status means sold out. `active` just marks which
        // rung of the ladder is currently selling — on a past event every
        // tier is inactive, which was striking the whole list through.
        sold_out: String(tier.status || '').toUpperCase() === 'SOLD_OUT',
        on_sale: tier.active === true,
      });
    }
  }

  // Cheapest first, so the ladder reads the way a buyer sees it
  tiers.sort((a, b) => a.price - b.price);
  const prices = tiers.map(t => t.price);

  return {
    url: event.url || event.short_url || null,
    flyer_url: event.flyer_url || event.mobile_image_url || null,
    tiers: tiers.length ? tiers : null,
    price_min: prices.length ? Math.min(...prices) : null,
    price_max: prices.length ? Math.max(...prices) : null,
    currency: prices.length ? 'CAD' : null,
  };
}

/** Run fn over items with at most `limit` in flight. */
async function mapLimit(items, limit, fn) {
  const results = new Array(items.length);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const i = next++;
      results[i] = await fn(items[i], i);
    }
  }));
  return results;
}

/**
 * Details for many events, politely. Failures resolve to null so one dead
 * event id can't sink a sync.
 */
async function fetchEventDetails(eventIds, { concurrency = 5, log = console.log } = {}) {
  const out = new Map();
  await mapLimit(eventIds, concurrency, async (eventId) => {
    try {
      const detail = await fetchEventDetail(eventId);
      if (detail) out.set(eventId, detail);
    } catch (err) {
      log(`  [tixr] event ${eventId} detail failed: ${err.response?.status || err.message}`);
    }
  });
  return out;
}

module.exports = { fetchEventDetail, fetchEventDetails };
