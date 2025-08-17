const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const crypto = require('crypto');

// Load environment variables from .env file
require('dotenv').config();

console.log('🚀 Starting Tixr Orders Sync (Optimized Version)...');

// --- CONFIGURATION ---
const {
  SUPABASE_URL,
  SUPABASE_KEY,
  TIXR_GROUP_ID,
  TIXR_CPK,
  TIXR_SECRET_KEY
} = process.env;

// Batch sizes for efficiency and performance
const EVENT_BATCH_SIZE = 5; // Process 5 events in parallel at a time
const ORDER_FETCH_PAGE_SIZE = 100; // Max allowed by Tixr API
const ORDER_DB_BATCH_SIZE = 500; // How many order rows to write to Supabase at once

// --- VALIDATION ---
if (!SUPABASE_URL || !SUPABASE_KEY || !TIXR_CPK || !TIXR_SECRET_KEY || !TIXR_GROUP_ID) {
  console.error('❌ Missing required environment variables. Check your .env file.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const TIXR_API_BASE_URL = 'https://studio.tixr.com';

// ==================== TIXR API FUNCTIONS ====================

/**
 * Builds the required HMAC-SHA256 hash for Tixr API requests.
 */
function buildHash(basePath, paramsObj) {
  const paramsSorted = Object.keys(paramsObj)
    .sort()
    .map(k => `${k}=${encodeURIComponent(paramsObj[k])}`)
    .join('&');
  const hashString = `${basePath}?${paramsSorted}`;
  return {
    paramsSorted,
    hash: crypto.createHmac('sha256', TIXR_SECRET_KEY).update(hashString).digest('hex')
  };
}

/**
 * Fetches all completed orders for a specific event, handling pagination and retries.
 */
async function fetchAllOrdersForEvent(eventId) {
  const allOrders = [];
  let pageNumber = 1;
  let hasMorePages = true;
  let retryCount = 0;
  const maxRetries = 3;

  while (hasMorePages) {
    const basePath = `/v1/groups/${TIXR_GROUP_ID}/events/${eventId}/orders`;
    const t = Date.now();
    const params = {
      cpk: TIXR_CPK,
      t,
      page_number: pageNumber,
      page_size: ORDER_FETCH_PAGE_SIZE,
      status: 'COMPLETE' // Crucial optimization: only fetch completed orders
    };
    const { paramsSorted, hash } = buildHash(basePath, params);
    const url = `${TIXR_API_BASE_URL}${basePath}?${paramsSorted}&hash=${hash}`;

    try {
      const { data } = await axios.get(url, { timeout: 20000 });

      if (Array.isArray(data) && data.length > 0) {
        allOrders.push(...data);
        console.log(`    Page ${pageNumber}: ${data.length} orders (Total: ${allOrders.length})`);

        if (data.length < ORDER_FETCH_PAGE_SIZE) {
          hasMorePages = false;
        } else {
          pageNumber++;
          await new Promise(resolve => setTimeout(resolve, 250)); // Rate limiting
        }
      } else {
        hasMorePages = false; // No more orders found
      }
      retryCount = 0; // Reset retries on success
    } catch (error) {
      retryCount++;
      if (retryCount >= maxRetries) {
        console.error(`    ❌ Failed to fetch page ${pageNumber} for event ${eventId} after ${maxRetries} retries. Skipping event.`);
        throw error; // Propagate the error to stop processing this event
      }
      console.warn(`    ⚠️ Retry ${retryCount}/${maxRetries} for page ${pageNumber} due to error: ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, 2000 * retryCount)); // Exponential backoff
    }
  }
  return allOrders;
}


// ==================== DATA TRANSFORMATION ====================

/**
 * Transforms raw Tixr order data into the format for the 'events_orders' table.
 */
function transformOrdersForDB(tixrOrders) {
  const transformedRows = [];

  for (const order of tixrOrders) {
    if (!order.sale_items || order.sale_items.length === 0) continue;

    for (const item of order.sale_items) {
      const serials = item.tickets ? item.tickets.map(ticket => ticket.serial_number).join(',') : null;

      const row = {
        // Use the original Tixr order_id. The database's composite primary key 
        // on (order_id, order_sale_id) will handle uniqueness.
        order_id: order.order_id,
        event_id: order.event_id,
        order_sale_id: item.sale_id,
        order_tier_id: item.tier_id,
        order_category: item.category,
        order_quantity: item.quantity,
        order_sales_item_name: item.name,
        order_serials: serials,
        order_name: `${order.first_name || ''} ${order.lastname || ''}`.trim(),
        order_gross: order.gross_sales,
        order_net: order.net,
        order_purchase_date: new Date(order.purchase_date).toISOString(),
        order_user_id: order.user_id,
        order_user_agent: order.user_agent_type,
        order_card_type: order.card_type,
        order_ref: order.ref_id,
        order_ref_type: order.ref_type,
      };
      transformedRows.push(row);
    }
  }
  return transformedRows;
}


// ==================== DATABASE OPERATIONS ====================

/**
 * Saves a batch of transformed orders to Supabase using upsert.
 */
async function saveOrdersBatch(orders) {
  if (orders.length === 0) return { success: true, count: 0 };

  try {
    const { error } = await supabase
      .from('events_orders')
      .upsert(orders, {
        // This must match the composite primary key in your database.
        onConflict: 'order_id, order_sale_id',
        ignoreDuplicates: false
      });

    if (error) throw error;
    return { success: true, count: orders.length };
  } catch (error) {
    console.error('  ❌ Error in saveOrdersBatch:', error.message);
    return { success: false, error: error.message };
  }
}

/**
 * Updates the 'event_order_updated' timestamp for a given event.
 */
async function updateEventOrderTimestamp(eventId) {
  const { error } = await supabase
    .from('events')
    .update({ event_order_updated: new Date().toISOString() })
    .eq('event_id', eventId);

  if (error) {
    console.error(`  ❌ Error updating timestamp for event ${eventId}:`, error.message);
  }
}


// ==================== MAIN SYNC LOGIC (WITH OPTIMIZATIONS) ====================

/**
 * Checks if a specific event needs to be synced based on its status and last update time.
 * @param {object} event - The event object from the database.
 * @returns {boolean} - True if the event needs to be synced.
 */
function shouldSyncEvent(event) {
  // Always sync if the event is LIVE, as new tickets can be sold.
  if (event.event_status === 'LIVE') {
    return true;
  }

  // For PAST events, perform the check against the 4 AM cutoff.
  if (event.event_status === 'PAST') {
    // If it has never been synced, it needs to be synced.
    if (!event.event_order_updated) {
      return true;
    }

    // Define the cutoff time: 4 AM on the day after the event.
    const eventDate = new Date(event.event_date);
    const cutoffDate = new Date(eventDate);
    cutoffDate.setDate(cutoffDate.getDate() + 1); // Move to the next day
    cutoffDate.setHours(4, 0, 0, 0); // Set time to 4:00:00.000

    const lastUpdateDate = new Date(event.event_order_updated);

    // If the last update was BEFORE the 4 AM cutoff, a final sync is needed.
    return lastUpdateDate < cutoffDate;
  }

  // Default to false for any other statuses.
  return false;
}

/**
 * Orchestrates the entire sync process for a single event.
 */
async function syncOrdersForEvent(event) {
  const { event_id, event_name } = event;
  const startTime = Date.now();
  console.log(`\n  📦 Processing Event ${event_id}: ${event_name}`);

  try {
    const rawOrders = await fetchAllOrdersForEvent(event_id);
    console.log(`    ✅ Fetched ${rawOrders.length} total orders for event ${event_id}`);

    if (rawOrders.length === 0) {
      await updateEventOrderTimestamp(event_id);
      console.log(`    - No orders to sync. Timestamp updated.`);
      return { success: true, orderCount: 0 };
    }

    const transformedOrders = transformOrdersForDB(rawOrders);
    console.log(`    🔄 Transformed into ${transformedOrders.length} database rows.`);

    let savedCount = 0;
    for (let i = 0; i < transformedOrders.length; i += ORDER_DB_BATCH_SIZE) {
      const batch = transformedOrders.slice(i, i + ORDER_DB_BATCH_SIZE);
      const result = await saveOrdersBatch(batch);
      if (result.success) {
        savedCount += result.count;
        console.log(`    💾 Saved batch ${Math.floor(i / ORDER_DB_BATCH_SIZE) + 1} (${savedCount}/${transformedOrders.length})`);
      } else {
        throw new Error(`Failed to save a batch: ${result.error}`);
      }
    }

    await updateEventOrderTimestamp(event_id);

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`    ✅ Event ${event_id} complete in ${duration}s.`);
    return { success: true, orderCount: savedCount };

  } catch (error) {
    console.error(`  ❌ An error occurred while processing event ${event_id}.`);
    return { success: false, error: error.message };
  }
}

/**
 * Fetches events from the database and processes their orders in batches.
 */
async function syncAllEventOrders(filter = null) {
  const startTime = Date.now();
  console.log('\n╔══════════════════════════════════════╗');
  console.log('║     TIXR ORDERS SYNC - FULL RUN     ║');
  console.log('╚══════════════════════════════════════╝\n');

  try {
    let query = supabase.from('events').select('event_id, event_name, event_status, event_date, event_order_updated');
    if (filter === 'live') {
      query = query.eq('event_status', 'LIVE');
      console.log('🎯 Filtering for LIVE events only.\n');
    }

    const { data: allEvents, error } = await query;
    if (error) throw error;
    if (!allEvents || allEvents.length === 0) {
      console.log('No events found to process.');
      return;
    }

    console.log(`📋 Found ${allEvents.length} total events. Applying sync logic to see which need updates...`);
    const eventsToProcess = allEvents.filter(shouldSyncEvent);

    if (eventsToProcess.length === 0) {
      console.log('✅ All events are up-to-date. No sync needed.');
      return;
    }
    console.log(`➡️  ${eventsToProcess.length} events require syncing. Processing in batches of ${EVENT_BATCH_SIZE}.\n`);

    const results = { successful: 0, failed: 0, totalOrders: 0, errors: [] };

    for (let i = 0; i < eventsToProcess.length; i += EVENT_BATCH_SIZE) {
      const batch = eventsToProcess.slice(i, i + EVENT_BATCH_SIZE);
      const batchNum = Math.floor(i / EVENT_BATCH_SIZE) + 1;
      console.log(`--- Starting Batch ${batchNum} (Events ${i + 1}-${i + batch.length}) ---`);

      const batchPromises = batch.map(event => syncOrdersForEvent(event));
      const batchResults = await Promise.all(batchPromises);

      for (const result of batchResults) {
        if (result.success) {
          results.successful++;
          results.totalOrders += result.orderCount;
        } else {
          results.failed++;
          results.errors.push(result.error);
        }
      }
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log('\n╔══════════════════════════════════════╗');
    console.log('║           SYNC COMPLETE              ║');
    console.log('╚══════════════════════════════════════╝');
    console.log(`✅ Successful: ${results.successful} events`);
    console.log(`❌ Failed:     ${results.failed} events`);
    console.log(`📦 Total orders synced: ${results.totalOrders}`);
    console.log(`⏱️  Total time: ${duration}s`);

  } catch (error) {
    console.error('❌ A fatal error occurred:', error);
    process.exit(1);
  }
}


// ==================== MAIN EXECUTION ====================

/**
 * Parses command-line arguments and runs the appropriate sync function.
 */
async function main() {
  const command = process.argv[2];
  const eventId = process.argv[3];
  
  const eventIdFromCommand = parseInt(command, 10);

  const handleSingleEvent = async (id) => {
    const { data: event, error } = await supabase
      .from('events')
      .select('event_id, event_name, event_status, event_date, event_order_updated')
      .eq('event_id', id)
      .single();
    
    if (error || !event) {
        console.error(`❌ Could not find event with ID ${id} in your database.`);
        process.exit(1);
    }

    if (shouldSyncEvent(event)) {
      console.log(`🎯 Event ${id} needs an update. Starting sync...`);
      await syncOrdersForEvent(event);
    } else {
      console.log(`✅ Event ${id} is already up-to-date. No sync needed.`);
    }
  };

  if (!isNaN(eventIdFromCommand)) {
    await handleSingleEvent(eventIdFromCommand);
  } else {
    switch (command) {
      case 'full':
        await syncAllEventOrders();
        break;
      case 'live':
        await syncAllEventOrders('live');
        break;
      case 'event':
        if (!eventId) {
          console.error('Please provide an event ID: `node sync-orders.js event 12345`');
          process.exit(1);
        }
        await handleSingleEvent(parseInt(eventId, 10));
        break;
      default:
        console.log('Usage:');
        console.log('  node sync-orders.js full          - Sync all events that need updates');
        console.log('  node sync-orders.js live          - Sync orders for LIVE events');
        console.log('  node sync-orders.js event [ID]    - Sync a specific event if it needs an update');
        console.log('  node sync-orders.js [ID]          - Shortcut to sync a specific event ID');
        process.exit(1);
    }
  }
  
  console.log('\n✅ Script finished!');
  process.exit(0);
}

main();