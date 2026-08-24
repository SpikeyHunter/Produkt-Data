// ============================================================================
// BACKFILL — full order history for chosen events, from the Tixr Studio API.
//
// Usage:
//   node backfill-orders.js 197887 198478 198646   <- explicit event ids
//   node backfill-orders.js --requested            <- every event flagged
//                                                     backfill_requested = true
//                                                     in tixr_sync_state
//
// Idempotent: re-running produces zero duplicates. The actual sync logic lives
// in lib/event-orders-sync.js, shared with the webhook server's 15-minute
// reconciliation sweep — one code path everywhere.
// ============================================================================

const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const { syncEventOrders, recordSyncError } = require('./lib/event-orders-sync');

const { SUPABASE_URL, SUPABASE_KEY, TIXR_GROUP_ID, TIXR_CPK, TIXR_SECRET_KEY } = process.env;

if (!SUPABASE_URL || !SUPABASE_KEY || !TIXR_CPK || !TIXR_SECRET_KEY || !TIXR_GROUP_ID) {
  console.error('❌ Missing required environment variables. Check your .env file.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

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
  let moneySampleShown = false;
  const failed = [];

  for (const eventId of eventIds) {
    console.log(`\n📥 Backfilling event ${eventId}...`);
    try {
      const stats = await syncEventOrders(supabase, eventId, {
        onPage: (page, count, total) =>
          process.stdout.write(`  📄 Page ${page}: ${count} orders (running total ${total})\r`),
      });

      // Money-trap check: show the raw money fields of the first order seen.
      if (!moneySampleShown && stats.firstOrderRaw) {
        const o = stats.firstOrderRaw;
        console.log('\n  🔎 MONEY SANITY CHECK — raw API values of first order:');
        console.log(`     order ${o.order_id}: gross_sales=${o.gross_sales} net=${o.net} total=${o.total} taxes=${o.taxes} fees=${o.fees} currency=${o.currency}`);
        console.log('     ^ these must read as DOLLARS (e.g. 45.00, not 4500).');
        moneySampleShown = true;
      }

      console.log(`\n  ✅ Event ${eventId} done.`);
      console.log(`     Orders: ${stats.orders} | Items: ${stats.items} | Tickets: ${stats.tickets} | Users touched: ${stats.users}`);
      console.log(`     By status: ${JSON.stringify(stats.byStatus)}`);
      console.log(`     COMPLETE money: gross $${stats.grossComplete.toFixed(2)} | net $${stats.netComplete.toFixed(2)}`);
      console.log(`     geo_info filled: ${stats.geoFilled}/${stats.orders}`);
    } catch (err) {
      console.error(`\n  ❌ Event ${eventId} failed: ${err.message}`);
      failed.push(eventId);
      await recordSyncError(supabase, eventId, err.message);
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
