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
const TEST_EVENT_IDS = [155673];
const DRY_RUN = true;

// --- PERFORMANCE CONFIGURATION ---
const API_CONCURRENCY_LIMIT = 50;
const API_TIMEOUT = 10000;
const MAX_RETRIES = 2;

// --- VALIDATION ---
if (!SUPABASE_URL || !SUPABASE_KEY || !TIXR_CPK || !TIXR_SECRET_KEY || !TIXR_GROUP_ID) {
  console.error('❌ Missing required environment variables.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Create axios instance
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

// Fetch attendance state for a serial
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
      if (status === 200 && data) return data;
    } catch (error) {
      if (attempt === retries) return null;
      if (attempt > 0) await new Promise(r => setTimeout(r, 100 * attempt));
    }
  }
  return null;
}

// Fetch all orders for an event from database
async function fetchAllOrdersForEvent(eventId) {
  console.log(`🗂️  Fetching all orders for event ${eventId} from database...`);
  let allOrders = [];
  let page = 0;
  const PAGE_SIZE = 1000;
  
  while (true) {
    const from = page * PAGE_SIZE;
    const to = from + PAGE_SIZE - 1;
    process.stdout.write(`  - Fetching page ${page + 1}...\r`);

    const { data, error } = await supabase
      .from('events_orders')
      .select('order_id, event_id, order_serials, order_category, order_ref_type, order_sales_item_name, order_checkin_state, order_checkin_count, order_checkin_time')
      .eq('event_id', eventId)
      .range(from, to);

    if (error) {
      console.error('');
      throw new Error(`Failed to fetch orders: ${error.message}`);
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

// Process orders to extract serials
function extractSerials(orders) {
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

// Fetch all attendance states from Tixr API
async function fetchAllAttendanceStates(tasks) {
  const results = new Map();
  
  console.log(`📡 Fetching attendance states from Tixr API (${API_CONCURRENCY_LIMIT} concurrent)...`);
  
  for (let i = 0; i < tasks.length; i += API_CONCURRENCY_LIMIT) {
    const batch = tasks.slice(i, i + API_CONCURRENCY_LIMIT);
    const promises = batch.map(task => 
      getAttendanceState(task.eventId, task.serial)
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

// Aggregate results and categorize
function aggregateResults(orders, attendanceMap) {
  const updatesToApply = [];
  const recap = {
    'GA': 0,
    'VIP': 0,
    'COMP GA': 0,
    'COMP VIP': 0,
    'TABLE': 0,
    'OUTLET': 0,
    'Billet Physique - GA': 0,
    'Billet Physique - VIP': 0,
    'Billet Physique - GL': 0,
    'Billet Physique - Table Prepaid': 0,
    'Billet Physique - Pay at the Door': 0,
    'Coatcheck': 0,
    'Prévente GA': 0,
    'Prévente VIP': 0,
    'Other': 0
  };

  for (const order of orders) {
    if (!order.order_serials) continue;
    
    const serials = order.order_serials.split(',').map(s => s.trim()).filter(Boolean);
    const serialStates = [];
    let totalCheckIns = 0;
    let earliestCheckIn = null;
    
    for (const serial of serials) {
      const attendance = attendanceMap.get(serial);
      if (attendance) {
        serialStates.push(attendance.state || 'IN_HAND');
        totalCheckIns += (attendance.ins || 0);
        
        if (attendance.first_check_in_date) {
          const checkInTime = attendance.first_check_in_date;
          if (!earliestCheckIn || checkInTime < earliestCheckIn) {
            earliestCheckIn = checkInTime;
          }
        }
      } else {
        serialStates.push('IN_HAND');
      }
    }
    
    const newStates = serialStates.join(',');
    const newCheckinTime = earliestCheckIn ? new Date(earliestCheckIn).toISOString() : null;
    const existingTime = order.order_checkin_time ? new Date(order.order_checkin_time).toISOString() : null;
    
    if (order.order_checkin_state !== newStates || 
        order.order_checkin_count !== totalCheckIns || 
        existingTime !== newCheckinTime) {
      updatesToApply.push({
        order_id: order.order_id,
        event_id: order.event_id,
        order_checkin_state: newStates,
        order_checkin_count: totalCheckIns,
        order_checkin_time: newCheckinTime,
      });
    }
    
    // Count for recap - count UNIQUE checked-in tickets (not re-entries)
    serials.forEach(serial => {
      const attendance = attendanceMap.get(serial);
      if (!attendance || attendance.state !== 'CHECKED_IN') return;
      
      const { order_category: cat, order_ref_type: refType, order_sales_item_name: itemName = '' } = order;
      
      // Count each checked-in ticket once (unique check-ins)
      const count = 1;
      
      const itemNameUpper = itemName.toUpperCase();
      
      // Priority 1: Coatcheck/Vestiaire
      if (itemNameUpper.includes('VESTIAIRE') || 
          itemNameUpper.includes('VESTIARE') || 
          itemNameUpper.includes('COATCHECK') || 
          itemNameUpper.includes('COAT CHECK') ||
          itemNameUpper.includes('PREPAID COAT CHECK')) {
        recap['Coatcheck'] += count;
        return;
      }
      
      // Priority 2: Billet Physique
      if (itemNameUpper.includes('BILLET PHYSIQUE')) {
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
          recap['Other'] += count;
        }
        return;
      }
      
      // Priority 3: Prévente tickets
      if (itemNameUpper.includes('PRÉVENTE') || 
          itemNameUpper.includes('PREVENTE')) {
        if (itemNameUpper.includes('VIP') || cat === 'VIP') {
          recap['Prévente VIP'] += count;
        } else {
          recap['Prévente GA'] += count;
        }
        return;
      }
      
      // Priority 4: COMP tickets (by name only)
      if (itemNameUpper.startsWith('COMP -') || 
          itemNameUpper.startsWith('COMP-') ||
          itemNameUpper.includes('COMPLIMENTARY')) {
        if (itemNameUpper.includes('VIP') || cat === 'VIP') {
          recap['COMP VIP'] += count;
        } else {
          recap['COMP GA'] += count;
        }
        return;
      }
      
      // Priority 5: Regular categories
      if (cat === 'GA' || itemNameUpper.includes('ADMISSION GÉNÉRALE') || itemNameUpper.includes('ADMISSION GENERALE')) {
        recap['GA'] += count;
      } else if (cat === 'VIP' || itemNameUpper.includes('VIP/LINE-BYPASS')) {
        recap['VIP'] += count;
      } else if (cat === 'TABLE_SERVICE' || cat === 'TABLE') {
        recap['TABLE'] += count;
      } else if (cat === 'OUTLET') {
        recap['OUTLET'] += count;
      } else {
        recap['Other'] += count;
      }
    });
  }
  
  return { updatesToApply, finalRecap: recap };
}

// Update database
async function batchUpdateOrders(updates) {
  if (DRY_RUN) {
    console.log(`\n🔍 DRY RUN: Would update ${updates.length} orders`);
    console.log('Sample updates:', updates.slice(0, 3));
    return;
  }

  console.log(`\n💾 Updating ${updates.length} orders in database...`);
  const BATCH_SIZE = 100;
  
  for (let i = 0; i < updates.length; i += BATCH_SIZE) {
    const batch = updates.slice(i, i + BATCH_SIZE);
    process.stdout.write(`  - Updating batch ${Math.floor(i/BATCH_SIZE) + 1}/${Math.ceil(updates.length/BATCH_SIZE)}...\r`);
    
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

function printRecap(recap) {
  console.log('\n📊 --- Attendance Sync Recap ---');
  console.log(`GA Checked In:                              ${recap['GA']}`);
  console.log(`VIP Checked In:                             ${recap['VIP']}`);
  console.log(`Comp GA Checked In:                         ${recap['COMP GA']}`);
  console.log(`Comp VIP Checked In:                        ${recap['COMP VIP']}`);
  console.log(`Prévente GA Checked In:                     ${recap['Prévente GA']}`);
  console.log(`Prévente VIP Checked In:                    ${recap['Prévente VIP']}`);
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

// --- MAIN SYNC LOGIC ---
async function syncAttendance() {
  console.log('🚀 Starting Tixr Attendance Sync (Direct from Tixr API)...');
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
    
    for (const eventId of eventIdsToProcess) {
      console.log(`\n${'='.repeat(60)}`);
      console.log(`📅 Event ID: ${eventId}`);
      console.log('='.repeat(60));
      
      const orders = await fetchAllOrdersForEvent(eventId);
      if (orders.length === 0) {
        console.log('  ⚠️  No orders found for this event');
        continue;
      }

      console.log('⚙️  Processing serial numbers...');
      const { serialTasks, totalSerials } = extractSerials(orders);
      console.log(`  - Found ${totalSerials} total serials (${serialTasks.length} unique) to check.`);

      const attendanceMap = await fetchAllAttendanceStates(serialTasks);
      
      console.log('🧮 Aggregating attendance data...');
      const { updatesToApply, finalRecap } = aggregateResults(orders, attendanceMap);
      
      if (updatesToApply.length > 0) {
        await batchUpdateOrders(updatesToApply);
        
        if (!DRY_RUN) {
          console.log(`  ✏️  Updating timestamp for event ${eventId}...`);
          await supabase
            .from('events')
            .update({ event_attendance_updated: new Date().toISOString() })
            .eq('event_id', eventId);
        }
      } else {
        console.log('\n  ✅ All orders are already up-to-date.');
      }
      
      printRecap(finalRecap);
    }
    
  } catch (error) {
    console.error('\n❌ Fatal error:', error.message || error);
    console.error(error.stack);
    process.exit(1);
  } finally {
    const elapsed = (Date.now() - startTime) / 1000;
    console.log(`\n✅ Sync finished in ${elapsed.toFixed(2)} seconds (${(elapsed/60).toFixed(2)} minutes)`);
  }
}

async function getPastEventIds() {
  console.log("📅 Finding all 'PAST' events...");
  const { data, error } = await supabase
    .from('events')
    .select('event_id')
    .eq('event_status', 'PAST');
  
  if (error) throw new Error(`Error fetching past events: ${error.message}`);
  return data.map(e => e.event_id);
}

// --- EXECUTION ---
syncAttendance();