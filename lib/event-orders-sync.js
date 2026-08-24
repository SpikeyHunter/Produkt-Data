// ============================================================================
// Per-event order sync: fetches ALL orders for one event from the Tixr Studio
// API (paginated) and upserts them into events_orders / events_order_items /
// events_tickets / events_users, then stamps tixr_sync_state.
//
// Idempotent — safe to run any number of times. A full fetch per event is the
// bulletproof choice: it catches new purchases, refunds/cancellations on OLD
// orders (the Studio API can only filter by purchase date, so an incremental
// fetch would miss those), and the zero-orders case, all in one code path.
//
// Used by: backfill-orders.js (manual CLI) and webhook-server.js (15-min
// reconciliation sweep of LIVE events).
// ============================================================================

const axios = require('axios');
const crypto = require('crypto');

const {
  transformOrderForDB,
  transformOrderItems,
  transformTickets,
  transformUserFromOrder,
} = require('./order-projection');

const { TIXR_GROUP_ID, TIXR_CPK, TIXR_SECRET_KEY } = process.env;

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

async function upsertBatch(supabase, table, rows, onConflict) {
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

/** Record a sync failure on tixr_sync_state without throwing further. */
async function recordSyncError(supabase, eventId, message) {
  try {
    const { data } = await supabase.from('tixr_sync_state').select('error_count').eq('event_id', eventId).maybeSingle();
    await supabase.from('tixr_sync_state').upsert({
      event_id:    eventId,
      error_count: (data?.error_count || 0) + 1,
      last_error:  message,
      updated_at:  new Date().toISOString(),
    }, { onConflict: 'event_id' });
  } catch (e) {
    console.error(`  ⚠️ Could not record sync error for event ${eventId}: ${e.message}`);
  }
}

// ==================== THE SYNC ====================

/**
 * Full order sync for one event. Returns stats:
 * { orders, items, tickets, users, byStatus, grossComplete, netComplete,
 *   geoFilled, firstOrderRaw }
 */
async function syncEventOrders(supabase, eventId, { onPage } = {}) {
  const stats = {
    orders: 0, items: 0, tickets: 0, users: 0,
    byStatus: {}, grossComplete: 0, netComplete: 0, geoFilled: 0,
    firstOrderRaw: null,
  };
  const userLatestOrder = new Map();   // user_id -> order with newest purchase_date
  let maxPurchaseDate = 0;
  let page = 1;

  while (true) {
    const orders = await fetchOrdersPage(eventId, page);
    if (orders.length === 0) break;

    if (!stats.firstOrderRaw) {
      const o = orders[0];
      stats.firstOrderRaw = {
        order_id: o.order_id, gross_sales: o.gross_sales, net: o.net,
        total: o.total, taxes: o.taxes, fees: o.fees, currency: o.currency,
      };
    }

    // Transform the whole page (no webhookBody -> webhook fields left untouched)
    const orderRows  = dedupeBy(orders.map(o => transformOrderForDB(o, null)), r => r.order_id);
    const itemRows   = dedupeBy(orders.flatMap(transformOrderItems), r => `${r.order_id}:${r.sale_id}`);
    const ticketRows = dedupeBy(orders.flatMap(transformTickets), r => r.serial_number);

    // 1. Orders
    await upsertBatch(supabase, 'events_orders', orderRows, 'order_id');

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
    await upsertBatch(supabase, 'events_tickets', ticketRows, 'serial_number');

    // Bookkeeping
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

    if (onPage) onPage(page, orders.length, stats.orders);
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
  await upsertBatch(supabase, 'events_users', userRows, 'user_id');
  stats.users = userRows.length;

  // 5. Sync state — cursor + backfill done, manual flag cleared
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

  return stats;
}

module.exports = { syncEventOrders, recordSyncError };
