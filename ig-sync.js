// ============================================================================
// IG SYNC — manual CLI. Runs ONE full cycle (detect + capture + refresh +
// snapshot) and exits. The same cycle runs every 5 min inside webhook-server.
//
// Usage:  node ig-sync.js
// ============================================================================

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const { runIgCycle } = require('./lib/ig-sync');

const { SUPABASE_URL, SUPABASE_KEY } = process.env;
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('❌ Missing SUPABASE_URL / SUPABASE_KEY.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

(async () => {
  console.log('📸 Running one IG sync cycle...');
  const started = Date.now();
  try {
    const result = await runIgCycle(supabase);
    const secs = ((Date.now() - started) / 1000).toFixed(1);
    if (result.skipped) console.log(`🐢 Skipped (rate backoff) after ${secs}s.`);
    else console.log(`✅ Cycle done in ${secs}s — ${result.detected} new item(s), ${result.refreshed} refreshed.`);
  } catch (err) {
    console.error('❌ Cycle failed:', err.message);
    process.exit(1);
  }
})();
