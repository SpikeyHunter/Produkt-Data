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
// Set specific event IDs to test, or leave empty to sync all PAST events
const TEST_EVENT_IDS = [155673]; // Example: [150136, 150137]
const DRY_RUN = true; // Set to true to see what would change without updating DB

// --- PERFORMANCE CONFIGURATION ---
const API_CONCURRENCY_LIMIT = 50; // Reduced for safety with transaction endpoint
const DB_UPDATE_BATCH_SIZE = 100;
const API_TIMEOUT = 10000;
const MAX_RETRIES = 2;

// --- VALIDATION ---
if (!SUPABASE_URL || !SUPABASE_KEY || !TIXR_CPK || !TIXR_SECRET_KEY || !TIXR_GROUP_ID) {
  console.error('❌ Missing required environment variables.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Create axios instance with connection pooling
const axiosInstance = axios.create({
  timeout: API_TIMEOUT,
  maxRedirects: 0,
  validateStatus: (status) => status < 500,
  httpAgent: new http.Agent({ keepAlive: true, maxSockets: 200 }),
  httpsAgent: new https.Agent({ keepAlive: true, maxSockets: 200 })
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

// NEW: Fetch attendance transaction history for a serial
async function getAttendanceTransactions(eventId, serialNumber, retries = MAX_RETRIES) {
  const basePath = `/v1/events/${eventId}/attendance/${serialNumber}/transactions`;
  const t = Date.now();
  const params = { ...baseHashParams, t };
  const { paramsSorted, hash } = buildHash(basePath, params);
  const url = `https://studio.tixr.com${basePath}?${paramsSorted}&hash=${hash}`;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const { data, status } = await axiosInstance.get(url);
      if (status === 404) return [];
      if (!Array.isArray(data)) return [];
      return data;
    } catch (error) {
      if (attempt === retries) return [];
      if (attempt > 0) await new Promise(r => setTimeout(r, 100 * attempt));
    }
  }
  return [];
}

// Process transactions to determine current state and count
function processTransactions(transactions) {
  if (!transactions || transactions.length === 0) {
    return {
      state: 'IN_HAND',
      checkInCount: 0,
      firstCheckInTime: null
    };
  }

  // Sort by date ascending
  const sorted = [...transactions].sort((a, b) => (a.date || 0) - (b.date || 0));
  
  let currentState = 'IN_HAND';
  let checkInCount = 0;
  let firstCheckInTime = null;

  for (const transaction of sorted) {
    const action = transaction.action;
    
    if (action === 'CHECKED_IN' || action === 'REENTERED') {
      currentState = 'CHECKED_IN';
      checkInCount++;
      if (!firstCheckInTime) {
        firstCheckInTime = transaction.date;
      }
    } else if (action === 'CHECKED_OUT') {
      currentState = 'CHECKED_OUT';
    } else if (action === 'VOID') {
      currentState = 'VOID';
    }
  }

  return {
    state: currentState,
    checkInCount,
    firstCheckInTime: firstCheckInTime ? new Date(firstCheckInTime).toISOString() : null
  };
}

// Fetch all orders using correct pagination
async function fetchAllOrdersForEvents(eventIds) {
  console.log(`🗂️  Fetching all orders for ${eventIds.length} event(s)...`);
  let allOrders = [];
  let page = 0;
  const PAGE_SIZE = 1000;
  
  while (true) {
    const from = page * PAGE_SIZE;
    const to = from + PAGE_SIZE - 1;
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
      if (data.length < PAGE_SIZE) break;
      page++;
    } else {
      break;
    }
  }
  console.log('');
  console.log(`  - Found ${allOrders.length} total orders in the database.`);
  return allOrders;
}

// Batch update function
async function batchUpdateOrders(updates) {
  if (DRY_RUN) {
    console.log(`\n🔍 DRY RUN: Would update ${updates.length} orders`);
    console.log('Sample updates:', updates.slice(0, 3));
    return;
  }

  console.log(`\n💾 Updating ${updates.length} orders in database...`);
  
  for (let i = 0; i < updates.length; i += DB_UPDATE_BATCH_SIZE) {
    const batch = updates.slice(i, i + DB_UPDATE_BATCH_SIZE);
    process.stdout.write(`  - Updating batch ${Math.floor(i/DB_UPDATE_BATCH_SIZE) + 1}/${Math.ceil(updates.length/DB_UPDATE_BATCH_SIZE)}...\r`);
    
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
  console.log('  ✅ Database updates complete.');
}

// --- MAIN SYNC LOGIC ---
async function syncAttendance() {
  console.log('🚀 Starting Tixr Attendance Sync V2...');
  if (DRY_RUN) console.log('⚠️  DRY RUN MODE - No database changes will be made\n');
  if (TEST_EVENT_IDS.length > 0) console.log(`🧪 TEST MODE - Only syncing events: ${TEST_EVENT_IDS.join(', ')}\n`);
  
  const startTime = Date.now();

  try {
    const eventIdsToProcess = TEST_EVENT_IDS.length > 0 ? TEST_EVENT_IDS : await getPastEventIds();
    
    if (!eventIdsToProcess || eventIdsToProcess.length === 0) {
      console.log('✅ No events to sync. Exiting.');
      return;
    }

    console.log(`📋 Processing ${eventIdsToProcess.length} event(s)`);
    
    const orders = await fetchAllOrdersForEvents(eventIdsToProcess);
    if (orders.length === 0) {
      console.log('No orders found for these events.');
      return;
    }

    console.log('⚙️  Processing serial numbers...');
    const { serialTasks, totalSerials } = processOrders(orders);
    console.log(`  - Found ${totalSerials} total serials (${serialTasks.length} unique) to check.`);

    console.log(`📡 Calling Tixr API (${API_CONCURRENCY_LIMIT} concurrent)...`);
    const transactionsMap = await fetchAllTransactions(serialTasks);
    
    console.log('🧮 Aggregating attendance data...');
    const { updatesToApply, finalRecap } = aggregateResults(orders, transactionsMap);
    
    if (updatesToApply.length > 0) {
      await batchUpdateOrders(updatesToApply);
      
      if (!DRY_RUN) {
        const uniqueEventIds = [...new Set(updatesToApply.map(u => u.event_id))];
        console.log(`  ✏️  Updating timestamp for ${uniqueEventIds.length} event(s)...`);
        await supabase
          .from('events')
          .update({ event_attendance_updated: new Date().toISOString() })
          .in('event_id', uniqueEventIds);
      }
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

// Helper functions
async function getPastEventIds() {
  console.log("📅 Finding all 'PAST' events...");
  const { data, error } = await supabase
    .from('events')
    .select('event_id')
    .eq('event_status', 'PAST');
  
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
        serialTasksMap.set(trimmedSerial, {
          eventId: order.event_id,
          serial: trimmedSerial
        });
      }
    }
  }
  
  return {
    serialTasks: Array.from(serialTasksMap.values()),
    totalSerials
  };
}

async function fetchAllTransactions(tasks) {
  const results = new Map();
  
  for (let i = 0; i < tasks.length; i += API_CONCURRENCY_LIMIT) {
    const batch = tasks.slice(i, i + API_CONCURRENCY_LIMIT);
    const promises = batch.map(task => 
      getAttendanceTransactions(task.eventId, task.serial)
    );
    
    const batchResults = await Promise.all(promises);
    
    batch.forEach((task, index) => {
      results.set(task.serial, batchResults[index]);
    });
    
    const percent = Math.round(((i + batch.length) / tasks.length) * 100);
    process.stdout.write(`  - Progress: ${percent}% (${i + batch.length}/${tasks.length}) \r`);
  }
  
  console.log('');
  return results;
}

function aggregateResults(orders, transactionsMap) {
  const updatesToApply = [];
  const recap = {
    'GA': 0, 'VIP': 0, 'COMP GA': 0, 'COMP VIP': 0,
    'TABLE': 0, 'OUTLET': 0, 
    'Billet Physique - GA': 0,
    'Billet Physique - VIP': 0,
    'Billet Physique - GL': 0,
    'Billet Physique - Table Prepaid': 0,
    'Billet Physique - Pay at the Door': 0,
    'Coatcheck': 0,
    'Other': 0
  };

  for (const order of orders) {
    if (!order.order_serials) continue;
    
    const serials = order.order_serials.split(',').map(s => s.trim()).filter(Boolean);
    const serialResults = [];
    
    for (const serial of serials) {
      const transactions = transactionsMap.get(serial) || [];
      const result = processTransactions(transactions);
      serialResults.push(result);
    }
    
    if (serialResults.length === 0) continue;

    // Build comma-separated states
    const newStates = serialResults.map(r => r.state).join(',');
    
    // Sum total check-ins across all serials
    const newCheckinCount = serialResults.reduce((sum, r) => sum + r.checkInCount, 0);
    
    // Find earliest check-in time
    let newFirstCheckinTime = null;
    const validDates = serialResults
      .map(r => r.firstCheckInTime)
      .filter(Boolean)
      .map(d => new Date(d).getTime());
    
    if (validDates.length > 0) {
      newFirstCheckinTime = new Date(Math.min(...validDates)).toISOString();
    }
    
    // Compare with existing values
    const existingTime = order.order_checkin_time ? new Date(order.order_checkin_time).toISOString() : null;
    
    if (order.order_checkin_state !== newStates || 
        order.order_checkin_count !== newCheckinCount || 
        existingTime !== newFirstCheckinTime) {
      updatesToApply.push({
        order_id: order.order_id,
        event_id: order.event_id,
        order_checkin_state: newStates,
        order_checkin_count: newCheckinCount,
        order_checkin_time: newFirstCheckinTime,
      });
    }
    
    // Count for recap - count ALL check-ins (including re-entries for unlimited tickets)
    serialResults.forEach(result => {
      if (result.state !== 'CHECKED_IN') return;
      
      const { order_category: cat, order_gross: gross, order_ref_type: refType, order_sales_item_name: itemName = '' } = order;
      
      // Count each checked-in ticket once (regardless of re-entries)
      // This matches Tixr's "Unique Check-ins" metric
      const count = 1;
      
      const itemNameUpper = itemName.toUpperCase();
      
      // Priority 1: Check for Coatcheck/Vestiaire FIRST (before other checks)
      // This prevents double-counting with OUTLET category
      if (itemNameUpper.includes('VESTIAIRE') || 
          itemNameUpper.includes('VESTIARE') || 
          itemNameUpper.includes('COATCHECK') || 
          itemNameUpper.includes('COAT CHECK') ||
          itemNameUpper.includes('PREPAID COAT CHECK')) {
        recap['Coatcheck'] += count;
        return; // STOP HERE - don't check other categories
      }
      
      // Priority 2: Check for Billet Physique tickets BEFORE COMP check
      // (because Billet Physique also uses BACKSTAGE ref_type)
      if (itemNameUpper.includes('BILLET PHYSIQUE')) {
        // More specific matching for Billet Physique variants
        if (itemNameUpper.includes('TABLE') && itemNameUpper.includes('PREPAID')) {
          recap['Billet Physique - Table Prepaid'] += count;
        } else if (itemNameUpper.includes('PAY AT THE DOOR')) {
          recap['Billet Physique - Pay at the Door'] += count;
        } else if (itemNameUpper.includes('- GL')) {
          recap['Billet Physique - GL'] += count;
        } else if (itemNameUpper.includes('- VIP')) {
          recap['Billet Physique - VIP'] += count;
        } else if (itemNameUpper.includes('- GA')) {
          recap['Billet Physique - GA'] += count;
        } else {
          // Fallback for unnamed Billet Physique tickets
          recap['Other'] += count;
        }
        return; // STOP HERE - don't check other categories
      }
      
      // Priority 3: Check for COMP tickets (by name only, not ref_type)
      // Only check name prefix to avoid catching non-COMP BACKSTAGE tickets
      const isCompByName = itemNameUpper.startsWith('COMP -') || 
                          itemNameUpper.startsWith('COMP-') ||
                          itemNameUpper.includes('COMPLIMENTARY');
      
      if (isCompByName) {
        // Determine if it's GA or VIP comp
        if (itemNameUpper.includes('VIP') || cat === 'VIP') {
          recap['COMP VIP'] += count;
        } else {
          recap['COMP GA'] += count;
        }
        return; // STOP HERE - don't check other categories
      }
      
      // Priority 4: Regular ticket categories (only if not caught above)
      if (cat === 'GA') {
        recap['GA'] += count;
      } else if (cat === 'VIP') {
        recap['VIP'] += count;
      } else if (cat === 'TABLE_SERVICE' || cat === 'TABLE') {
        recap['TABLE'] += count;
      } else if (cat === 'OUTLET') {
        // Only count as OUTLET if it wasn't already counted as Coatcheck
        recap['OUTLET'] += count;
      } else {
        recap['Other'] += count;
      }
    });
  }
  
  return { updatesToApply, finalRecap: recap };
}

function printRecap(recap) {
  console.log('\n📊 --- Attendance Sync Recap ---');
  console.log(`GA Checked In:                              ${recap['GA']}`);
  console.log(`VIP Checked In:                             ${recap['VIP']}`);
  console.log(`Comp GA Checked In:                         ${recap['COMP GA']}`);
  console.log(`Comp VIP Checked In:                        ${recap['COMP VIP']}`);
  console.log(`Table Checked In:                           ${recap['TABLE']}`);
  console.log(`Outlet Checked In:                          ${recap['OUTLET']}`);
  console.log(`Coatcheck Checked In:                       ${recap['Coatcheck']}`);
  console.log(`Billet Physique - GA Checked In:            ${recap['Billet Physique - GA']}`);
  console.log(`Billet Physique - VIP Checked In:           ${recap['Billet Physique - VIP']}`);
  console.log(`Billet Physique - GL Checked In:            ${recap['Billet Physique - GL']}`);
  console.log(`Billet Physique - Table Prepaid Checked In: ${recap['Billet Physique - Table Prepaid']}`);
  console.log(`Billet Physique - Pay at Door Checked In:   ${recap['Billet Physique - Pay at the Door']}`);
  console.log(`Other Checked In:                           ${recap['Other']}`);
  console.log('-----------------------------------------------');
  const total = Object.values(recap).reduce((sum, val) => sum + val, 0);
  console.log(`TOTAL Checked In:                           ${total}`);
}

// --- EXECUTION ---
syncAttendance();