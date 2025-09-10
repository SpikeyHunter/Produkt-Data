const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const crypto = require('crypto');
const http = require('http');
const https = require('https');

// Load environment variables
if (process.env.NODE_ENV !== 'production') {
  try {
    require('dotenv').config();
  } catch (error) {
    console.log('dotenv not available, using system environment variables');
  }
}

// --- CONFIGURATION ---
const {
  SUPABASE_URL,
  SUPABASE_KEY,
  TIXR_GROUP_ID,
  TIXR_CPK,
  TIXR_SECRET_KEY,
} = process.env;

// --- TESTING CONFIGURATION ---
const TEST_EVENT_ID = 150136; // <-- Set to null to run for all 'PAST' events

// --- ULTRA PERFORMANCE CONFIGURATION ---
const API_CONCURRENCY_LIMIT = 200; // Increased from 100. Be careful of rate-limiting!
const DB_PAGE_SIZE = 1000;
const DB_UPDATE_BATCH_SIZE = 100;
const API_TIMEOUT = 5000;
const MAX_RETRIES = 1;

// --- VALIDATION ---
if (!SUPABASE_URL || !SUPABASE_KEY || !TIXR_CPK || !TIXR_SECRET_KEY || !TIXR_GROUP_ID) {
  console.error('❌ Missing required environment variables.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Create axios instance with aggressive connection pooling
const axiosInstance = axios.create({
  timeout: API_TIMEOUT,
  maxRedirects: 0,
  validateStatus: (status) => status < 500,
  httpAgent: new http.Agent({ keepAlive: true, maxSockets: 400 }),
  httpsAgent: new https.Agent({ keepAlive: true, maxSockets: 400 })
});

const baseHashParams = { cpk: TIXR_CPK };
const hashCache = new Map();

function buildHash(basePath, paramsObj) {
  const cacheKey = `${basePath}:${paramsObj.t}`;
  if (hashCache.has(cacheKey)) {
    return hashCache.get(cacheKey);
  }

  const paramsSorted = Object.keys(paramsObj)
    .sort()
    .map(k => `${k}=${encodeURIComponent(paramsObj[k])}`)
    .join('&');
  const hashString = `${basePath}?${paramsSorted}`;
  const hash = crypto
    .createHmac('sha256', TIXR_SECRET_KEY)
    .update(hashString)
    .digest('hex');

  const result = { paramsSorted, hash };
  hashCache.set(cacheKey, result);
  return result;
}

async function getAttendanceState(eventId, serialNumber, retries = MAX_RETRIES) {
  const basePath = `/v1/events/${eventId}/attendance/${serialNumber}`;
  const t = Date.now();
  const params = { ...baseHashParams, t };
  const { paramsSorted, hash } = buildHash(basePath, params);
  const url = `https://studio.tixr.com${basePath}?${paramsSorted}&hash=${hash}`;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const { data, status } = await axiosInstance.get(url);
      if (status === 404) return null;
      return data;
    } catch (error) {
      if (attempt === retries) return null;
      if (attempt > 0) await new Promise(r => setTimeout(r, 50 * attempt));
    }
  }
  return null;
}

// Fetches all orders using correct pagination
async function fetchAllOrdersForEvents(eventIds) {
    console.log(`🗂️  Fetching all orders for ${eventIds.length} event(s)...`);
    let allOrders = [];
    let page = 0;
    while (true) {
        const from = page * DB_PAGE_SIZE;
        const to = from + DB_PAGE_SIZE - 1;
        process.stdout.write(`  - Fetching page ${page + 1}...\r`);

        const { data, error } = await supabase
            .from('events_orders')
            .select('order_id, event_id, order_serials, order_category, order_gross, order_ref_type, order_sales_item_name, order_checkin_state, order_checkin_count, order_checkin_time')
            .in('event_id', eventIds)
            .range(from, to);

        if (error) {
            console.error('');
            throw new Error(`Failed to fetch orders on page ${page}: ${error.message}`);
        }
        if (data && data.length > 0) {
            allOrders.push(...data);
            if (data.length < DB_PAGE_SIZE) break; // Last page
            page++;
        } else {
            break; // No more data
        }
    }
    console.log('');
    console.log(`  - Found ${allOrders.length} total orders in the database.`);
    return allOrders;
}

// Batch update function with a more robust fallback
async function batchUpdateOrders(updates) {
    console.log(`\n💾 Updating ${updates.length} orders in database...`);
    const { error } = await supabase.rpc('update_attendance_batch', {
        p_order_ids: updates.map(u => u.order_id),
        p_checkin_states: updates.map(u => u.order_checkin_state),
        p_checkin_counts: updates.map(u => u.order_checkin_count),
        p_checkin_times: updates.map(u => u.order_checkin_time)
    });
    
    if (error) {
        console.log(`  - RPC failed (${error.message}). Using fallback update method...`);
        for (let i = 0; i < updates.length; i += DB_UPDATE_BATCH_SIZE) {
            const batch = updates.slice(i, i + DB_UPDATE_BATCH_SIZE);
            process.stdout.write(`    - Updating batch ${i / DB_UPDATE_BATCH_SIZE + 1}...\r`);
            const updatePromises = batch.map(update => 
                supabase
                    .from('events_orders')
                    .update({
                        order_checkin_state: update.order_checkin_state,
                        order_checkin_count: update.order_checkin_count,
                        order_checkin_time: update.order_checkin_time
                    })
                    .eq('order_id', update.order_id)
            );
            await Promise.all(updatePromises);
        }
        console.log('');
    }
    console.log('  ✅ Database updates complete.');
}

// --- MAIN SYNC LOGIC ---
async function syncAttendance() {
  console.log('🚀 Starting Tixr Attendance Sync (ULTRA-FAST Edition)...');
  const startTime = Date.now();

  try {
    const eventIdsToProcess = TEST_EVENT_ID ? [TEST_EVENT_ID] : await getPastEventIds();
    if (!eventIdsToProcess || eventIdsToProcess.length === 0) {
      console.log('✅ No events to sync. Exiting.');
      return;
    }

    const orders = await fetchAllOrdersForEvents(eventIdsToProcess);
    if (orders.length === 0) return;

    console.log('⚙️  Processing serial numbers...');
    const { serialTasks, totalSerials } = processOrders(orders);
    console.log(`  - Found ${totalSerials} total serials (${serialTasks.length} unique) to check.`);

    console.log(`📡 Calling Tixr API (${API_CONCURRENCY_LIMIT} concurrent)...`);
    const resultsMap = await fetchAllAttendance(serialTasks);
    
    console.log('🧮 Aggregating attendance data...');
    const { updatesToApply, finalRecap } = aggregateResults(orders, resultsMap);
    
    if (updatesToApply.length > 0) {
      await batchUpdateOrders(updatesToApply);
      const uniqueEventIds = [...new Set(updatesToApply.map(u => u.event_id))];
      console.log(`  ✍️  Updating timestamp for ${uniqueEventIds.length} event(s)...`);
      await supabase.from('events').update({ event_attendance_updated: new Date().toISOString() }).in('event_id', uniqueEventIds);
    } else {
      console.log('\n  ✅ All orders are already up-to-date.');
    }
    printRecap(finalRecap);
  } catch (error) {
    console.error('\n❌ Fatal error:', error.message || error);
    process.exit(1);
  } finally {
    const elapsed = (Date.now() - startTime) / 1000;
    console.log(`\n✅ Sync finished in ${elapsed.toFixed(2)} seconds (${(elapsed/60).toFixed(2)} minutes)`);
  }
}

// Helper functions for main logic
async function getPastEventIds() {
    console.log("🔍 Finding all 'PAST' events...");
    const { data, error } = await supabase.from('events').select('event_id').eq('event_status', 'PAST');
    if (error) throw new Error(`Error fetching past events: ${error.message}`);
    return data.map(e => e.event_id);
}

function processOrders(orders) {
    const serialTasksMap = new Map();
    let totalSerials = 0;
    for (const order of orders) {
        if (!order.order_serials) continue;
        const serials = order.order_serials.split(',');
        for (const serial of serials) {
            const trimmedSerial = serial.trim();
            if (!trimmedSerial) continue;
            totalSerials++;
            if (!serialTasksMap.has(trimmedSerial)) {
                serialTasksMap.set(trimmedSerial, { eventId: order.event_id, serial: trimmedSerial });
            }
        }
    }
    return { serialTasks: Array.from(serialTasksMap.values()), totalSerials };
}

async function fetchAllAttendance(tasks) {
    const results = [];
    for (let i = 0; i < tasks.length; i += API_CONCURRENCY_LIMIT) {
        const batch = tasks.slice(i, i + API_CONCURRENCY_LIMIT);
        const promises = batch.map(task => getAttendanceState(task.eventId, task.serial));
        results.push(...(await Promise.all(promises)));
        const percent = Math.round(((i + batch.length) / tasks.length) * 100);
        process.stdout.write(`  - Progress: ${percent}% (${i + batch.length}/${tasks.length}) \r`);
    }
    console.log('');
    return new Map(results.filter(Boolean).map(r => [r.serial_number, r]));
}

function aggregateResults(orders, resultsMap) {
    const updatesToApply = [];
    // ADDED 'Other' category to ensure all tickets are counted
    const recap = { 'GA': 0, 'VIP': 0, 'COMP GA': 0, 'COMP VIP': 0, 'TABLE': 0, 'OUTLET': 0, 'FREE GA': 0, 'FREE VIP': 0, 'Other': 0 };

    for (const order of orders) {
        if (!order.order_serials) continue;
        const serials = order.order_serials.split(',').map(s => s.trim()).filter(Boolean);
        const orderResults = serials.map(s => resultsMap.get(s)).filter(Boolean);
        if (orderResults.length === 0) continue;

        const newStates = orderResults.map(r => r.state || 'UNKNOWN').join(',');
        const newCheckinCount = orderResults.reduce((sum, r) => sum + (r.ins || 0), 0);
        
        let newFirstCheckinTime = null;
        const validDates = orderResults.map(r => r.first_check_in_date ? new Date(r.first_check_in_date).getTime() : null).filter(Boolean);
        if (validDates.length > 0) {
            newFirstCheckinTime = new Date(Math.min(...validDates)).toISOString();
        }
        
        const existingTime = order.order_checkin_time ? new Date(order.order_checkin_time).toISOString() : null;
        if (order.order_checkin_state !== newStates || order.order_checkin_count !== newCheckinCount || existingTime !== newFirstCheckinTime) {
            updatesToApply.push({
                order_id: order.order_id, event_id: order.event_id, order_checkin_state: newStates,
                order_checkin_count: newCheckinCount, order_checkin_time: newFirstCheckinTime,
            });
        }
        
        orderResults.forEach(result => {
            if (result.state !== 'CHECKED_IN') return;
            const { order_category: cat, order_gross: gross, order_ref_type: refType, order_sales_item_name: itemName = '' } = order;
            
            // MODIFIED logic to include a catch-all 'Other' category
            if (cat === 'GA') {
                recap[gross == 0 ? ((refType === 'BACKSTAGE' && itemName.toUpperCase().includes('COMP')) ? 'COMP GA' : 'FREE GA') : 'GA']++;
            } else if (cat === 'VIP') {
                recap[gross == 0 ? ((refType === 'BACKSTAGE' && itemName.toUpperCase().includes('COMP')) ? 'COMP VIP' : 'FREE VIP') : 'VIP']++;
            } else if (cat === 'TABLE_SERVICE' || cat === 'TABLE') {
                recap['TABLE']++;
            } else if (cat === 'OUTLET') {
                recap['OUTLET']++;
            } else {
                recap['Other']++; // Catch-all for any other category
            }
        });
    }
    return { updatesToApply, finalRecap: recap };
}

function printRecap(recap) {
    console.log('\n📊 --- Attendance Sync Recap ---');
    console.log(`GA Checked In:                ${recap['GA']}`);
    console.log(`VIP Checked In:               ${recap['VIP']}`);
    console.log(`Comp GA Checked In:           ${recap['COMP GA']}`);
    console.log(`Comp VIP Checked In:          ${recap['COMP VIP']}`);
    console.log(`Table Checked In:             ${recap['TABLE']}`);
    console.log(`Coatcheck/Outlet Checked In:  ${recap['OUTLET']}`);
    console.log(`Other Checked In:             ${recap['Other']}`); // ADDED Other category to output
    console.log(`Free GA Checked In:           ${recap['FREE GA']}`);
    console.log(`Free VIP Checked In:          ${recap['FREE VIP']}`);
    console.log('---------------------------------');
}

// --- EXECUTION ---
syncAttendance();