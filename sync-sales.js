// sync-sales.js
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

// --- CONFIGURATION ---
const { SUPABASE_URL, SUPABASE_KEY } = process.env;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('❌ Missing required environment variables: SUPABASE_URL and SUPABASE_KEY.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

/**
 * A helper function to sum the 'order_quantity' from an array of orders.
 * @param {Array} orders - The array of order objects to sum.
 * @returns {number} - The total quantity.
 */
const sumOrderQuantity = (orders) => {
  return orders.reduce((sum, order) => sum + (order.order_quantity || 0), 0);
};


/**
 * Main function to process events and update their sales data in batches.
 */
async function syncEventSales() {
  console.log('🚀 Starting event sales sync process...');

  try {
    // 1. Fetch all events and existing sales records in parallel
    // (ADDED: is_custom to the select list)
    const [
      { data: allEvents, error: eventsError },
      { data: existingSales, error: salesError }
    ] = await Promise.all([
      supabase.from('events').select('event_id, event_status, is_custom'), 
      supabase.from('events_sales').select('event_id')
    ]);

    if (eventsError) throw new Error(`Fatal error fetching events: ${eventsError.message}`);
    if (salesError) throw new Error(`Fatal error fetching existing sales: ${salesError.message}`);
    
    const existingSaleIds = new Set(existingSales.map(s => s.event_id));

    // 2. Determine which events actually need to be processed
    const eventsToProcess = allEvents.filter(event => {
      // 🛡️ IMMEDIATELY SKIP CUSTOM EVENTS
      if (event.is_custom === true) return false; 

      if (event.event_status === 'LIVE') return true;
      if (event.event_status === 'PAST') return !existingSaleIds.has(event.event_id);
      return false;
    });

    if (eventsToProcess.length === 0) {
      console.log('✅ All events are up-to-date. No sync needed.');
      return;
    }

    console.log(`📋 Found ${eventsToProcess.length} events to process. Fetching all order data...`);

    // 3. Create a batch of promises to fetch orders and calculate sales for each event
    const calculationPromises = eventsToProcess.map(async (event) => {
      let allOrders = [];
      let page = 0;
      const pageSize = 1000;
      let hasMore = true;

      while(hasMore) {
        // **NEW:** Added retry logic for fetching data
        let orders = null;
        let ordersError = null;
        const maxRetries = 3;
        let attempt = 0;

        while (attempt < maxRetries) {
            const { data, error } = await supabase
              .from('events_orders')
              .select('order_category, order_net, order_ref_type, order_gross, order_quantity, order_sales_item_name, order_status')
              .eq('event_id', event.event_id)
              .range(page * pageSize, (page + 1) * pageSize - 1);
            
            if (error) {
                ordersError = error;
                attempt++;
                console.warn(`  - ⚠️ Attempt ${attempt}/${maxRetries} failed for event ${event.event_id} (page ${page}). Retrying in 2s...`);
                await new Promise(res => setTimeout(res, 2000)); // Wait 2 seconds before retrying
            } else {
                orders = data;
                ordersError = null;
                break; // Success, exit the retry loop
            }
        }

        if (ordersError) {
          console.error(`  - ❌ Failed to fetch orders for event ${event.event_id} after ${maxRetries} attempts. Skipping event. Error: ${ordersError.message}`);
          return null;
        }

        if (orders && orders.length > 0) {
          allOrders.push(...orders);
          page++;
          if(orders.length < pageSize) hasMore = false;
        } else {
          hasMore = false;
        }
      }

      const completeOrders = allOrders.filter(o => o.order_status === 'COMPLETE');

      return {
        event_id: event.event_id,
        sales_total_ga: sumOrderQuantity(completeOrders.filter(o => o.order_category === 'GA' && o.order_gross > 0)),
        sales_total_vip: sumOrderQuantity(completeOrders.filter(o => o.order_category === 'VIP' && o.order_gross > 0)),
        sales_total_coatcheck: sumOrderQuantity(completeOrders.filter(o => o.order_category === 'OUTLET' && o.order_gross > 0)),
        sales_total_tables: sumOrderQuantity(completeOrders.filter(o => (o.order_category === 'TABLE_SERVICE' || o.order_category === 'TABLE') && o.order_gross > 0)),
        sales_total_comp_ga: sumOrderQuantity(completeOrders.filter(o => o.order_category === 'GA' && o.order_gross === 0 && o.order_ref_type === 'BACKSTAGE' && o.order_sales_item_name && o.order_sales_item_name.toUpperCase().includes('COMP'))),
        sales_total_comp_vip: sumOrderQuantity(completeOrders.filter(o => o.order_category === 'VIP' && o.order_gross === 0 && o.order_ref_type === 'BACKSTAGE' && o.order_sales_item_name && o.order_sales_item_name.toUpperCase().includes('COMP'))),
        sales_total_free_ga: sumOrderQuantity(completeOrders.filter(o => o.order_category === 'GA' && o.order_gross === 0 && o.order_ref_type !== 'BACKSTAGE')),
        sales_total_free_vip: sumOrderQuantity(completeOrders.filter(o => o.order_category === 'VIP' && o.order_gross === 0 && o.order_ref_type !== 'BACKSTAGE')),
        sales_gross: completeOrders.reduce((sum, o) => sum + (o.order_gross || 0), 0),
        sales_net: completeOrders.reduce((sum, o) => sum + (o.order_net || 0), 0),
      };
    });

    const salesDataToUpsert = (await Promise.all(calculationPromises))
        .filter(data => data !== null);

    if (salesDataToUpsert.length === 0) {
        console.log('⚠️ No sales data could be calculated, check for errors above.');
        return;
    }

    console.log(`💾 Saving ${salesDataToUpsert.length} calculated sales records to 'events_sales'...`);

    const { error: upsertError } = await supabase
      .from('events_sales')
      .upsert(salesDataToUpsert, { onConflict: 'event_id' });

    if (upsertError) {
      throw new Error(`Batch upsert to events_sales failed: ${upsertError.message}`);
    }

    const processedEventIds = salesDataToUpsert.map(data => data.event_id);
    console.log(`✍️  Updating 'event_sales_updated' timestamp for ${processedEventIds.length} events...`);
    
    const { error: updateTimestampError } = await supabase
      .from('events')
      .update({ event_sales_updated: new Date().toISOString() })
      .in('event_id', processedEventIds);

    if (updateTimestampError) {
        throw new Error(`Failed to update timestamps in events table: ${updateTimestampError.message}`);
    }

    console.log(`\n✨ Successfully synced sales data for ${salesDataToUpsert.length} events!`);

  } catch (err) {
    console.error("\n❌ A fatal error occurred during the sync process:", err.message);
    process.exit(1);
  }
}

// Run the main function
syncEventSales();