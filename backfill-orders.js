// ============================================================================
// BACKFILL — full order history for chosen events, from the Tixr Studio API.
//
// Usage:
//   node backfill-orders.js 197887 198478 198646   <- explicit event ids
//   node backfill-orders.js --requested            <- every event flagged
//                                                     backfill_requested = true
//                                                     in tixr_sync_state
//
// Idempotent: re-running produces zero duplicates (everything is upserted on
// its natural key). Uses the same projection module as the webhook server, so
// backfilled and webhook'd orders are identical in shape.
//
// Traps handled (from the build plan):
//   · start_date is set explicitly to 2010-01-01 — the API defaults to TODAY.
//   · Money sanity: the raw money fields of the first order are printed so a
//     dollars-vs-cents mistake is visible before anything is trusted.
// ============================================================================

const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const crypto = require('crypto');
require('dotenv').config();

const {
  transformOrderForDB,
  transformOrderItems,
  transformTickets,
  transformUserFromOrder,
} = require('./lib/order-projection');

// --- CONFIGURATION ---
const { SUPABASE_URL, SUPABASE_KEY, TIXR_GROUP_ID, TIXR_CPK, TIXR_SECRET_KEY } = process.env;

if (!SUPABASE_URL || !SUPABASE_KEY || !TIXR_CPK || !TIXR_SECRET_KEY || !TIXR_GROUP_ID) {
  console.error('❌ Missing required environment variables. Check your .env file.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const TIXR_API_BASE_URL = 'https://studio.tixr.com';
const PAGE_SIZE = 100;
const PAGE_SLEEP_MS = 200;
const DB_BATCH_SIZE = 500;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ==================== TIXR API ====================

function buildHash(basePath, paramsObj) {
  const paramsSorted = Object.keys(paramsObj).sort().map(k => `${k}=${encodeURIComponent(paramsObj[k])}`).join('&');
  const hashString = `${basePath}?${paramsSorted}`;
  return crypto.createHmac('sha256', TIXR_SECRET_KEY).update(hashString).digest('hex');
}

async function fetchOrdersPage(eventId, pageNumber) {
  const basePath = `/v1/groups/${TIXR_GROUP_ID}/events/${eventId}/orders`;
  const params = {
    cpk: TIXR_CPK,
    t: Date.now(),
    page_number: pageNumber,
    page_size: PAGE_SIZE,
    start_date: '2010-01-01',   // explicit — API default is TODAY (trap #7)
  };
  const paramsString = Object.keys(params).sort().map(k => `${k}=${encodeURIComponent(params[k])}`).join('&');
  const hash = buildHash(basePath, params);
  const url = `${TIXR_API_BASE_URL}${basePath}?${paramsString}&hash=${hash}`;
  const { data } = await axios.get(url, { timeout: 30000 });
  return Array.isArray(data) ? data : [];
}

// ==================== DB HELPERS ====================

async function upsertBatch(table, rows, onConflict) {
  for (let i = 0; i < rows.length; i += DB_BATCH_SIZE) {
    const batch = rows.slice(i, i + DB_BATCH_SIZE);
    const { error } = await supabase.from(table).upsert(batch, { onConflict });
    if (error) throw new Error(`${table} upsert failed: ${error.message}`);
  }
}

/** Dedupe rows on a key, keeping the LAST occurrence (a key twice in one
 *  upsert request is a Postgres error). */
function dedupeBy(rows, keyFn) {
  const map = new Map();
  for (const row of rows) map.set(keyFn(row), row);
  return Array.from(map.values());
}

// ==================== PER-EVENT BACKFILL ====================

async function backfillEvent(eventId, { moneySampleShown }) {
  console.log(`\n📥 Backfilling event ${eventId}...`);

  const stats = { orders: 0, items: 0, tickets: 0, byStatus: {}, grossComplete: 0, netComplete: 0, geoFilled: 0 };
  const userLatestOrder = new Map();   // user_id -> order with newest purchase_date
  let maxPurchaseDate = 0;
  let page = 1;

  while (true) {
    const orders = await fetchOrdersPage(eventId, page);
    if (orders.length === 0) break;

    // Money-trap check: show the raw money fields of the very first order seen.
    if (!moneySampleShown.done) {
      const o = orders[0];
      console.log('  🔎 MONEY SANITY CHECK — raw API values of first order:');
      console.log(`     order ${o.order_id}: gross_sales=${o.gross_sales} net=${o.net} total=${o.total} taxes=${o.taxes} fees=${o.fees} currency=${o.currency}`);
      console.log('     ^ these must read as DOLLARS (e.g. 45.00, not 4500).');
      moneySampleShown.done = true;
    }

    // Transform the whole page (no webhookBody -> webhook fields left untouched)
    const orderRows  = dedupeBy(orders.map(o => transformOrderForDB(o, null)), r => r.order_id);
    const itemRows   = dedupeBy(orders.flatMap(transformOrderItems), r => `${r.order_id}:${r.sale_id}`);
    const ticketRows = dedupeBy(orders.flatMap(transformTickets), r => r.serial_number);

    // 1. Orders
    await upsertBatch('events_orders', orderRows, 'order_id');

    // 2. Items — clean slate for this page's orders, then insert fresh
    const orderIds = orderRows.map(r => r.order_id);
    const { error: delErr } = await supabase.from('events_order_items').delete().in('order_id', orderIds);
    if (delErr) throw new Error(`events_order_items delete failed: ${delErr.message}`);
    for (let i = 0; i < itemRows.length; i += DB_BATCH_SIZE) {
      const batch = itemRows.slice(i, i + DB_BATCH_SIZE);
      const { error } = await supabase.from('events_order_items').insert(batch);
      if (error) throw new Error(`events_order_items insert failed: ${error.message}`);
    }

    // 3. Tickets
    await upsertBatch('events_tickets', ticketRows, 'serial_number');

    // Bookkeeping for users / cursor / verify summary
    for (const o of orders) {
      stats.orders++;
      stats.byStatus[o.status] = (stats.byStatus[o.status] || 0) + 1;
      if (o.status === 'COMPLETE') {
        stats.grossComplete += o.gross_sales || 0;
        stats.netComplete   += o.net || 0;
      }
      if (o.geo_info && (o.geo_info.city || o.geo_info.country_code)) stats.geoFilled++;
      if (o.purchase_date && o.purchase_date > maxPurchaseDate) maxPurchaseDate = o.purchase_date;
      if (o.user_id) {
        const key = String(o.user_id);
        const prev = userLatestOrder.get(key);
        if (!prev || (o.purchase_date || 0) > (prev.purchase_date || 0)) userLatestOrder.set(key, o);
      }
    }
    stats.items += itemRows.length;
    stats.tickets += ticketRows.length;

    process.stdout.write(`  📄 Page ${page}: ${orders.length} orders (running total ${stats.orders})\r`);
    if (orders.length < PAGE_SIZE) break;
    page++;
    await sleep(PAGE_SLEEP_MS);
  }

  // 4. Users — merge event_ids with what's already stored, then batch upsert
  const userIds = Array.from(userLatestOrder.keys());
  const existingEventIds = new Map();
  for (let i = 0; i < userIds.length; i += DB_BATCH_SIZE) {
    const chunk = userIds.slice(i, i + DB_BATCH_SIZE);
    const { data, error } = await supabase.from('events_users').select('user_id, event_ids').in('user_id', chunk);
    if (error) throw new Error(`events_users read failed: ${error.message}`);
    for (const u of (data || [])) existingEventIds.set(String(u.user_id), u.event_ids || []);
  }
  const userRows = userIds.map(uid => {
    const order = userLatestOrder.get(uid);
    const merged = Array.from(new Set([...(existingEventIds.get(uid) || []), order.event_id]));
    return transformUserFromOrder(order, merged);
  });
  await upsertBatch('events_users', userRows, 'user_id');

  // 5. Sync state — cursor + backfill done, flag cleared
  const { error: stateErr } = await supabase.from('tixr_sync_state').upsert({
    event_id:           eventId,
    backfill_requested: false,
    backfill_done_at:   new Date().toISOString(),
    orders_cursor:      maxPurchaseDate ? new Date(maxPurchaseDate).toISOString() : null,
    last_synced_at:     new Date().toISOString(),
    error_count:        0,
    last_error:         null,
    updated_at:         new Date().toISOString(),
  }, { onConflict: 'event_id' });
  if (stateErr) throw new Error(`tixr_sync_state upsert failed: ${stateErr.message}`);

  // Verify summary — cross-check these against the Tixr Studio dashboard
  console.log(`\n  ✅ Event ${eventId} done.`);
  console.log(`     Orders: ${stats.orders} | Items: ${stats.items} | Tickets: ${stats.tickets} | Users touched: ${userRows.length}`);
  console.log(`     By status: ${JSON.stringify(stats.byStatus)}`);
  console.log(`     COMPLETE money: gross $${stats.grossComplete.toFixed(2)} | net $${stats.netComplete.toFixed(2)}`);
  console.log(`     geo_info filled: ${stats.geoFilled}/${stats.orders}`);
}

// ==================== MAIN ====================

async function main() {
  const args = process.argv.slice(2);
  let eventIds = [];

  if (args[0] === '--requested') {
    const { data, error } = await supabase.from('tixr_sync_state').select('event_id').eq('backfill_requested', true);
    if (error) { console.error('❌ Failed to read tixr_sync_state:', error.message); process.exit(1); }
    eventIds = (data || []).map(r => r.event_id);
    if (eventIds.length === 0) { console.log('✅ No events flagged backfill_requested. Nothing to do.'); return; }
  } else {
    eventIds = args.map(Number).filter(n => Number.isInteger(n) && n > 0);
    if (eventIds.length === 0) {
      console.log('Usage:');
      console.log('  node backfill-orders.js <event_id> [event_id ...]');
      console.log('  node backfill-orders.js --requested');
      process.exit(1);
    }
  }

  console.log(`🚀 Backfill starting for ${eventIds.length} event(s): ${eventIds.join(', ')}`);
  const startTime = Date.now();
  const moneySampleShown = { done: false };
  const failed = [];

  for (const eventId of eventIds) {
    try {
      await backfillEvent(eventId, { moneySampleShown });
    } catch (err) {
      console.error(`\n  ❌ Event ${eventId} failed: ${err.message}`);
      failed.push(eventId);
      await supabase.from('tixr_sync_state').upsert({
        event_id:   eventId,
        last_error: err.message,
        updated_at: new Date().toISOString(),
      }, { onConflict: 'event_id' });
    }
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n✨ Backfill finished in ${duration}s. ${eventIds.length - failed.length}/${eventIds.length} events OK.`);
  if (failed.length > 0) {
    console.log(`⚠️  Failed events (safe to just re-run for these): ${failed.join(', ')}`);
    process.exit(1);
  }
}

main().catch(err => {
  console.error('\n❌ A fatal error occurred:', err);
  process.exit(1);
});
