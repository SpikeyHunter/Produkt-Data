const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const crypto = require('crypto');
require('dotenv').config();

console.log('🚀 Starting Tixr Order Sync Script (Final Version)...');

// --- CONFIGURATION ---
const {
  SUPABASE_URL,
  SUPABASE_KEY,
  TIXR_GROUP_ID,
  TIXR_CPK,
  TIXR_SECRET_KEY
} = process.env;

const MAX_CONCURRENT_EVENTS = 10; 
const ORDER_FETCH_PAGE_SIZE = 100;
const DB_UPSERT_BATCH_SIZE = 500;

if (!SUPABASE_URL || !SUPABASE_KEY || !TIXR_CPK || !TIXR_SECRET_KEY || !TIXR_GROUP_ID) {
  console.error('❌ Missing required environment variables. Check your .env file.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const TIXR_API_BASE_URL = 'https://studio.tixr.com';

// ==================== UTILITIES ====================

class Limiter {
  constructor(maxConcurrent) { this.maxConcurrent = maxConcurrent; this.current = 0; this.queue = []; }
  async execute(fn) {
    while (this.current >= this.maxConcurrent) await new Promise(resolve => this.queue.push(resolve));
    this.current++;
    try { return await fn(); } 
    finally { this.current--; const next = this.queue.shift(); if (next) next(); }
  }
}

class ProgressBar {
    constructor(total) { this.total = total; this.current = 0; this.startTime = Date.now(); }
    tick() {
        this.current++;
        const elapsed = (Date.now() - this.startTime) / 1000;
        const avgTime = elapsed / this.current;
        const remaining = this.total - this.current;
        const eta = Math.round(remaining * avgTime);
        const percent = ((this.current / this.total) * 100).toFixed(1);
        const formatTime = s => s < 60 ? `${Math.floor(s)}s` : `${Math.floor(s / 60)}m ${Math.floor(s % 60)}s`;
        process.stdout.write(`  Syncing Events: ${this.current}/${this.total} [${percent}%] | Elapsed: ${formatTime(elapsed)} | ETA: ${formatTime(eta)}\r`);
    }
}

const eventLimiter = new Limiter(MAX_CONCURRENT_EVENTS);

function capitalize(str) {
    if (typeof str !== 'string' || !str) return str;
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
}

// ==================== TIXR API FUNCTIONS ====================

function buildHash(basePath, paramsObj) {
  const paramsSorted = Object.keys(paramsObj).sort().map(k => `${k}=${encodeURIComponent(paramsObj[k])}`).join('&');
  const hashString = `${basePath}?${paramsSorted}`;
  return crypto.createHmac('sha256', TIXR_SECRET_KEY).update(hashString).digest('hex');
}

async function fetchAllOrdersForEvent(eventId) {
    const allOrders = []; let pageNumber = 1; let hasMorePages = true;
    while(hasMorePages) {
        const basePath = `/v1/groups/${TIXR_GROUP_ID}/events/${eventId}/orders`;
        const params = { cpk: TIXR_CPK, t: Date.now(), page_number: pageNumber, page_size: ORDER_FETCH_PAGE_SIZE, start_date: '2010-01-01' };
        
        const paramsString = Object.keys(params).sort().map(k=>`${k}=${encodeURIComponent(params[k])}`).join('&');
        const hash = buildHash(basePath, params);

        const url = `${TIXR_API_BASE_URL}${basePath}?${paramsString}&hash=${hash}`;
        try {
            const { data } = await axios.get(url, { timeout: 20000 });
            if (Array.isArray(data) && data.length > 0) {
                allOrders.push(...data);
                if (data.length < ORDER_FETCH_PAGE_SIZE) hasMorePages = false; else pageNumber++;
            } else { hasMorePages = false; }
        } catch (error) { console.error(`\n  - Error fetching orders for event ${eventId}: ${error.message}`); return []; }
    }
    return allOrders;
}

// ==================== DATABASE OPERATIONS ====================

async function saveBatchToDB(table, data, onConflict) {
  if (data.length === 0) return true;
  for (let i = 0; i < data.length; i += DB_UPSERT_BATCH_SIZE) {
    const batch = data.slice(i, i + DB_UPSERT_BATCH_SIZE);
    const { error } = await supabase.from(table).upsert(batch, { onConflict });
    if (error) {
      console.error(`\n  - Error saving batch to ${table}:`, error.message);
      return false;
    }
  }
  return true;
}

// ==================== CORE SYNC LOGIC ====================

async function syncEventData(event) {
  const rawOrders = await fetchAllOrdersForEvent(event.event_id);
  
  const transformedOrders = rawOrders.flatMap(order => 
    (order.sale_items || []).map(item => ({
      order_id: order.order_id, event_id: order.event_id, order_sale_id: item.sale_id,
      order_status: order.status, order_tier_id: item.tier_id, order_user_id: order.user_id,
      order_name: `${capitalize(order.first_name) || ''} ${capitalize(order.lastname) || ''}`.trim(),
      order_sales_item_name: item.name, order_category: item.category, order_quantity: item.quantity,
      order_purchase_date: new Date(order.purchase_date).toISOString(), order_gross: order.gross_sales,
      order_net: order.net, order_user_agent: order.user_agent_type, order_card_type: order.card_type,
      order_ref: order.ref_id, order_ref_type: order.ref_type,
      order_serials: item.tickets?.map(t => t.serial_number).join(',') || null,
    }))
  );
  
  await saveBatchToDB('events_orders', transformedOrders, 'order_id, order_sale_id');

  const userOrderMap = new Map();
  for (const order of rawOrders) {
    if (order.user_id) {
        const userIdStr = order.user_id.toString();
        if (!userOrderMap.has(userIdStr) || order.purchase_date > userOrderMap.get(userIdStr).purchase_date) {
            userOrderMap.set(userIdStr, order);
        }
    }
  }

  await supabase.from('events').update({ event_order_updated: new Date().toISOString() }).eq('event_id', event.event_id);
  return userOrderMap;
}

// ==================== MAIN SCRIPT LOGIC ====================

function shouldSyncEvent(event) {
  if (event.event_status === 'LIVE') return true;
  if (event.event_status === 'PAST') {
    if (!event.event_order_updated) return true;
    const eventDate = new Date(event.event_date);
    const cutoffDate = new Date(eventDate);
    cutoffDate.setDate(cutoffDate.getDate() + 1);
    cutoffDate.setHours(4, 0, 0, 0); // ~4am the day after the event
    const lastUpdateDate = new Date(event.event_order_updated);
    return lastUpdateDate < cutoffDate;
  }
  return false;
}

async function runFullSync() {
  const startTime = Date.now();
  console.log('Fetching all events from the database...');
  const { data: allEvents, error } = await supabase.from('events').select('event_id, event_name, event_status, event_date, event_order_updated');
  if (error) throw new Error(`Fatal error fetching events: ${error.message}`);

  console.log(`📋 Found ${allEvents.length} total events. Applying sync logic...`);
  const eventsToProcess = allEvents.filter(shouldSyncEvent);

  if (eventsToProcess.length === 0) {
    console.log('✅ All events are up-to-date. No sync needed.');
    return;
  }
  console.log(`➡️  ${eventsToProcess.length} events require syncing. Starting parallel processing...`);

  const progressBar = new ProgressBar(eventsToProcess.length);
  const allUserMaps = await Promise.all(eventsToProcess.map(event => 
    eventLimiter.execute(async () => {
        const userMap = await syncEventData(event);
        progressBar.tick();
        return userMap;
    })
  ));
  
  console.log('\n\n- All event orders synced. Now consolidating and saving user data...');

  const masterUserOrderMap = new Map();
  for (const userMap of allUserMaps) {
      for (const [userId, order] of userMap.entries()) {
           if (!masterUserOrderMap.has(userId) || order.purchase_date > masterUserOrderMap.get(userId).purchase_date) {
              masterUserOrderMap.set(userId, order);
          }
      }
  }

  const userIdsInSync = Array.from(masterUserOrderMap.keys());
  const { data: existingUsers } = await supabase.from('events_users').select('user_id, event_ids').in('user_id', userIdsInSync);
  const existingUserEventMap = new Map((existingUsers || []).map(u => [u.user_id.toString(), u.event_ids || []]));

  const usersToUpsert = Array.from(masterUserOrderMap.values()).map(order => {
    const userIdStr = order.user_id.toString();
    const geoInfo = order.geo_info;
    
    const allAttendedEventsForUser = allUserMaps.flatMap(userMap => 
        Array.from(userMap.values())
            .filter(o => o.user_id.toString() === userIdStr)
            .map(o => o.event_id)
    ).filter(Boolean);
      
    const existingEvents = existingUserEventMap.get(userIdStr) || [];
    const updatedEvents = Array.from(new Set([...existingEvents, ...allAttendedEventsForUser]));

    // This object ONLY updates the information we get directly from the order.
    // It will NOT overwrite fields managed by enrich-users.js (like age, gender, total_spend).
    return {
        user_id: userIdStr,
        user_first_name: capitalize(order.first_name),
        user_last_name: capitalize(order.lastname),
        user_mail: order.email,
        user_city: geoInfo?.city,
        user_state: geoInfo?.state,
        user_country: geoInfo?.country_code,
        user_postal: geoInfo?.postal_code,
        event_ids: updatedEvents,
        // ** IMPORTANT CHANGE **
        // Update the last purchase date. This is the trigger for the enrichment script.
        user_last_purchase: new Date(order.purchase_date).toISOString(),
      };
  });

  console.log(`- Saving/updating ${usersToUpsert.length} user profiles to the database...`);
  await saveBatchToDB('events_users', usersToUpsert, 'user_id');

  const duration = (Date.now() - startTime) / 1000;
  console.log(`\n✨ Sync complete! ✨`);
  console.log(`- ${eventsToProcess.length} events processed in ${duration.toFixed(1)}s.`);
}

async function main() {
    await runFullSync();
    console.log('\n✅ Script finished!');
}

main().catch(err => {
    console.error("\n❌ A fatal error occurred:", err);
    process.exit(1);
});

