const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const crypto = require('crypto');
require('dotenv').config();

// --- CONFIGURATION ---
const { SUPABASE_URL, SUPABASE_KEY, TIXR_GROUP_ID, TIXR_CPK, TIXR_SECRET_KEY } = process.env;

if (!SUPABASE_URL || !SUPABASE_KEY || !TIXR_CPK || !TIXR_SECRET_KEY) {
  console.error('❌ Missing .env variables.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const TIXR_API_BASE = `https://studio.tixr.com/v1/groups/${TIXR_GROUP_ID}`;

// =========================================================
//  THE CLASSIFICATION ENGINE
// =========================================================
function getReportingCategory(item, refType, gross) {
  const name = (item.name || "").toUpperCase();
  const category = (item.category || "").toUpperCase();
  const ref = (refType || "").toUpperCase();
  
  // Price Logic
  const isFree = gross === 0;
  const isPaid = gross > 0;
  const isBackstage = ref === "BACKSTAGE";

  // 1. COATCHECK (Overrides all)
  if (category === 'OUTLET' && (name.includes('VESTIAIRE') || name.includes('COAT CHECK'))) {
    return "COATCHECK";
  }

  // 2. TRANSFERS (Overrides all)
  if (name.includes("TRANSFERT") || name.includes("TRANSFÉRÉ") || name.includes("TRANSFERE") || name.includes("REPORTÉ")) {
    return "TRANSFERRED";
  }

  // 3. PROMOTERS (Overrides all)
  if (name.includes("PROMOTER") || name.includes("PROMOTEUR")) {
    return "PROMOTER";
  }

  // 4. PHYSICAL / DOOR TICKETS (Backstage Only)
  if (isBackstage && isFree) {
    // Tables
    if (category.includes('TABLE') || category.includes('SERVICE')) {
      if ((name.includes('BILLET PHYSIQUE') || name.includes('DOOR TABLE')) && name.includes('PREPAID') && !name.includes('PAY AT THE DOOR')) {
        return "PHYSICAL_TABLE_PREPAID";
      }
      if ((name.includes('BILLET PHYSIQUE') || name.includes('DOOR')) && (name.includes('PAY AT THE DOOR') || name.includes('BUY AT DOOR')) && !name.includes('PREPAID')) {
        return "PHYSICAL_TABLE_DOOR";
      }
    }
    // Guestlist
    if (category.includes('GUEST') && (name.includes('GUESTLIST') || name.includes('GL'))) {
      return "PHYSICAL_GUESTLIST";
    }
    // GA
    if (category === 'GA' && (name.includes('BILLET PHYSIQUE') || name.includes('HARD COPY') || name.includes('DOOR'))) {
      return "DOOR_GA";
    }
    // VIP
    if (category === 'VIP' && (name.includes('BILLET PHYSIQUE') || name.includes('HARD COPY') || name.includes('DOOR'))) {
      return "DOOR_VIP";
    }
  }

  // 5. COMPS (Internal/Backstage)
  if (isFree && (name.includes("COMP") || name.includes("INVITÉ") || name.includes("INVITE") || 
      name.includes("FAVEUR") || name.includes("INVITATION") || name.includes("CONCOURS") || 
      name.includes("GIVEAWAY") || ref === "BACKSTAGE")) {
        if (category === 'VIP') return "COMP_VIP";
        return "COMP_GA";
  }

  // 6. FREE PUBLIC TICKETS (RSVP, $0 Buys Online)
  if (isFree && !isBackstage) {
    if (category === 'VIP') return "FREE_VIP";
    return "FREE_GA"; // Covers GA and GUEST/RSVP
  }

  // 7. TABLES (Paid)
  if (category.includes('TABLE') || category.includes('BOOTH') || name.includes('BANQUETTE') || category.includes('SEATED')) {
    return "TABLES_RSVP";
  }

  // 8. STANDARD PAID
  if (isPaid) {
    if (category === 'VIP' || category === 'PHOTO') return "VIP_PAID";
    return "GA_PAID";
  }

  return "UNCATEGORIZED";
}

// =========================================================
//  SYNC FUNCTION
// =========================================================
async function syncEvent(eventId) {
  console.log(`\n📥 Starting Sync for Event: ${eventId}`);
  let page = 1;
  let hasMore = true;
  let totalOrders = 0;
  let totalTickets = 0;

  while (hasMore) {
    // 1. Build Tixr Request
    const path = `/events/${eventId}/orders`;
    const params = { cpk: TIXR_CPK, t: Date.now(), page_number: page, page_size: 100 };
    const sortedParams = Object.keys(params).sort().map(k => `${k}=${encodeURIComponent(params[k])}`).join('&');
    const hash = crypto.createHmac('sha256', TIXR_SECRET_KEY).update(`/v1/groups/${TIXR_GROUP_ID}${path}?${sortedParams}`).digest('hex');
    const url = `${TIXR_API_BASE}${path}?${sortedParams}&hash=${hash}`;

    try {
      const { data: orders } = await axios.get(url);
      
      if (!orders || orders.length === 0) {
        hasMore = false;
        break;
      }

      const dbOrders = [];
      const dbItems = [];

      // 2. Process Orders
      for (const order of orders) {
        const orderRef = order.ref_type || 'UNKNOWN';
        
        // --- Prepare Order Row ---
        dbOrders.push({
          order_id: order.order_id,
          event_id: order.event_id,
          user_id: order.user_id,
          user_name: `${order.first_name || ''} ${order.lastname || ''}`.trim(),
          user_email: order.email,
          total_gross: order.gross_sales,
          total_net: order.net,
          currency: order.currency,
          order_status: order.status,
          purchase_date: new Date(order.purchase_date).toISOString(),
          platform_source: order.user_agent_type,
          ref_type: orderRef,
          ref_id: order.ref_id, // Promo code often here
          card_type: order.card_type,
          synced_at: new Date().toISOString()
        });

        // --- Prepare Item Rows (Tickets) ---
        if (order.sale_items) {
          for (const item of order.sale_items) {
            const qty = item.quantity || 0;
            // Use tickets array if available, otherwise create dummy array based on qty
            // This handles bundles: "Paquet x4" usually has 4 entries in item.tickets
            const tickets = (item.tickets && item.tickets.length > 0) 
                            ? item.tickets 
                            : Array(qty).fill({ serial_number: null, status: 'IN_HAND' }); 

            // Calculate Unit Price (Avoid double counting bundles)
            const unitPrice = qty > 0 ? (item.total / qty) : 0;

            for (const ticket of tickets) {
              // CLASSIFY
              const category = getReportingCategory(item, orderRef, unitPrice);

              // Handle checkin time if available in ticket object
              // Note: Tixr Order API usually gives ticket status, but exact checkin time might need attendance API.
              // We store what we have.
              let scannedAt = null;
              if (ticket.status === 'CHECKED_IN') {
                 // If Tixr provides a timestamp in future updates, map it here. 
                 // For now, we rely on status.
                 scannedAt = new Date().toISOString(); // Approximate if live sync, otherwise null
              }

              dbItems.push({
                order_id: order.order_id,
                event_id: order.event_id,
                sale_id: item.sale_id,
                tier_id: item.tier_id,
                tixr_name: item.name,
                tixr_category: item.category,
                price_paid: unitPrice,
                reporting_category: category,
                serial_number: ticket.serial_number,
                checkin_status: ticket.status || 'IN_HAND',
                // scanned_at: scannedAt, // Optional: enable if you have precise data
                synced_at: new Date().toISOString()
              });
            }
          }
        }
      }

      // 3. Database Upsert
      // Orders first
      if (dbOrders.length > 0) {
        const { error: orderErr } = await supabase.from('raw_orders').upsert(dbOrders);
        if (orderErr) console.error(`  ❌ Order DB Error: ${orderErr.message}`);
      }

      // Items second (Delete old items for these orders to prevent duplicates during re-sync)
      if (dbItems.length > 0) {
        const orderIds = dbOrders.map(o => o.order_id);
        // Clean slate for these specific orders to ensure bundle explosions are correct
        await supabase.from('raw_order_items').delete().in('order_id', orderIds);
        const { error: itemErr } = await supabase.from('raw_order_items').insert(dbItems);
        if (itemErr) console.error(`  ❌ Item DB Error: ${itemErr.message}`);
      }

      totalOrders += dbOrders.length;
      totalTickets += dbItems.length;
      process.stdout.write(`  - Page ${page}: Synced ${dbOrders.length} orders, ${dbItems.length} tickets.\r`);
      
      // Optimization: If we fetched less than requested, we are done
      if (orders.length < 100) hasMore = false;
      page++;

    } catch (err) {
      console.error(`\n  ❌ API Error on page ${page}:`, err.message);
      hasMore = false;
    }
  }

  console.log(`\n✅ Event ${eventId} Finished. Total: ${totalOrders} orders, ${totalTickets} tickets.`);
}

// =========================================================
//  MAIN ENTRY POINT
// =========================================================
async function main() {
  const eventId = process.argv[2];
  if (!eventId) {
    console.log("Usage: node order-sync.js <EVENT_ID>");
    console.log("Example: node order-sync.js 123456");
    process.exit(1);
  }
  await syncEvent(eventId);
}

main();