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

const sumOrderQuantity = (orders) => {
  return orders.reduce((sum, order) => sum + (order.order_quantity || 0), 0);
};

const safeUpper = (str) => (str || '').toUpperCase();

/**
 * Main function to reconcile and backfill sales data from events_orders.
 * DOES NOT TOUCH event_status.
 */
async function syncEventSales() {
  console.log('🚀 Starting event sales reconciliation process...');

  try {
    // 1. Fetch ALL LIVE events, and PAST events that do not yet have a sales record
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

    // 2. Filter events
    const eventsToProcess = allEvents.filter(event => {
      if (event.is_custom === true) return false; 
      // Process if it's LIVE (to catch any missed webhooks), OR if it's missing from events_sales
      if (event.event_status === 'LIVE') return true;
      if (event.event_status === 'PAST') return !existingSaleIds.has(event.event_id);
      return false;
    });

    if (eventsToProcess.length === 0) {
      console.log('✅ All event sales are up-to-date. No reconciliation needed.');
      return;
    }

    console.log(`📋 Found ${eventsToProcess.length} events to reconcile. Fetching order data...`);

    // 3. Batch process DB orders into events_sales
    const calculationPromises = eventsToProcess.map(async (event) => {
      let allOrders = [];
      let page = 0;
      const pageSize = 1000;
      let hasMore = true;

      while(hasMore) {
        // We filter for 'COMPLETE' at the database level for maximum speed, 
        // but we will still enforce it in the JS filters below per requirements.
        const { data: orders, error } = await supabase
            .from('events_orders')
            .select('order_category, order_net, order_ref_type, order_gross, order_quantity, order_sales_item_name, order_status')
            .eq('event_id', event.event_id)
            .eq('order_status', 'COMPLETE')
            .range(page * pageSize, (page + 1) * pageSize - 1);

        if (error) {
          console.error(`  - ❌ Failed to fetch orders for event ${event.event_id}. Skipping. Error: ${error.message}`);
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

      // --- APPLY STRICT CLASSIFICATION RULES ---
      return {
        event_id: event.event_id,
        
        // GA: order_category = GA and order_gross > 0 and order_status = COMPLETE
        sales_total_ga: sumOrderQuantity(allOrders.filter(o => 
          o.order_category === 'GA' && 
          o.order_gross > 0 && 
          o.order_status === 'COMPLETE'
        )),
        
        // VIP: order_category = VIP and order_gross > 0 and order_status = COMPLETE
        sales_total_vip: sumOrderQuantity(allOrders.filter(o => 
          o.order_category === 'VIP' && 
          o.order_gross > 0 && 
          o.order_status === 'COMPLETE'
        )),
        
        // COMP GA: order_category = GA, order_gross = 0, status = COMPLETE, ref = BACKSTAGE
        sales_total_comp_ga: sumOrderQuantity(allOrders.filter(o => 
          o.order_category === 'GA' && 
          o.order_gross === 0 && 
          o.order_status === 'COMPLETE' && 
          safeUpper(o.order_ref_type) === 'BACKSTAGE'
        )),
        
        // COMP VIP: order_category = VIP, order_gross = 0, status = COMPLETE, ref = BACKSTAGE
        sales_total_comp_vip: sumOrderQuantity(allOrders.filter(o => 
          o.order_category === 'VIP' && 
          o.order_gross === 0 && 
          o.order_status === 'COMPLETE' && 
          safeUpper(o.order_ref_type) === 'BACKSTAGE'
        )),
        
        // COATCHECK: order_category = OUTLET, order_gross > 0, status = COMPLETE, name contains Vestiaire/Coat Check
        sales_total_coatcheck: sumOrderQuantity(allOrders.filter(o => {
          const nameUpper = safeUpper(o.order_sales_item_name);
          const isCoatCheckName = nameUpper.includes('VESTIA') || nameUpper.includes('COAT CHECK') || nameUpper.includes('COATCHECK');
          return o.order_category === 'OUTLET' && 
                 o.order_gross > 0 && 
                 o.order_status === 'COMPLETE' && 
                 isCoatCheckName;
        })),

        // FREE GA: order_category = GA, order_gross = 0, status = COMPLETE, ref IS NOT BACKSTAGE
        sales_total_free_ga: sumOrderQuantity(allOrders.filter(o => 
          o.order_category === 'GA' && 
          o.order_gross === 0 && 
          o.order_status === 'COMPLETE' && 
          safeUpper(o.order_ref_type) !== 'BACKSTAGE'
        )),
        
        // FREE VIP: order_category = VIP, order_gross = 0, status = COMPLETE, ref IS NOT BACKSTAGE
        sales_total_free_vip: sumOrderQuantity(allOrders.filter(o => 
          o.order_category === 'VIP' && 
          o.order_gross === 0 && 
          o.order_status === 'COMPLETE' && 
          safeUpper(o.order_ref_type) !== 'BACKSTAGE'
        )),

        // TABLES (Retained from previous setup to prevent DB nulls)
        sales_total_tables: sumOrderQuantity(allOrders.filter(o => 
          (o.order_category === 'TABLE_SERVICE' || o.order_category === 'TABLE') && 
          o.order_gross > 0 && 
          o.order_status === 'COMPLETE'
        )),

        // Financials
        sales_gross: allOrders.reduce((sum, o) => sum + (o.order_gross || 0), 0),
        sales_net: allOrders.reduce((sum, o) => sum + (o.order_net || 0), 0),
      };
    });

    const salesDataToUpsert = (await Promise.all(calculationPromises)).filter(data => data !== null);

    if (salesDataToUpsert.length === 0) {
        console.log('⚠️ No sales data calculated, check for errors above.');
        return;
    }

    console.log(`💾 Saving ${salesDataToUpsert.length} calculated sales records to 'events_sales'...`);

    const { error: upsertError } = await supabase
      .from('events_sales')
      .upsert(salesDataToUpsert, { onConflict: 'event_id' });

    if (upsertError) throw new Error(`Batch upsert failed: ${upsertError.message}`);

    const processedEventIds = salesDataToUpsert.map(data => data.event_id);
    
    // Only update the 'event_sales_updated' timestamp. We do NOT update 'event_status'
    console.log(`✍️  Updating 'event_sales_updated' timestamp for ${processedEventIds.length} events...`);
    const { error: updateTimestampError } = await supabase
      .from('events')
      .update({ event_sales_updated: new Date().toISOString() })
      .in('event_id', processedEventIds);

    if (updateTimestampError) throw new Error(`Failed to update timestamps: ${updateTimestampError.message}`);

    console.log(`\n✨ Successfully reconciled sales data for ${salesDataToUpsert.length} events!`);

  } catch (err) {
    console.error("\n❌ A fatal error occurred during the sync process:", err.message);
    process.exit(1);
  }
}

// Run the main function
syncEventSales();