const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const crypto = require('crypto');
require('dotenv').config();

console.log('🚀 Starting Tixr User Enrichment Script (Final Version)...');

// --- CONFIGURATION ---
const {
  SUPABASE_URL,
  SUPABASE_KEY, // Using the service key for admin access
  TIXR_GROUP_ID,
  TIXR_CPK,
  TIXR_SECRET_KEY
} = process.env;

// Performance Tuning
const MAX_CONCURRENT_API_CALLS = 30; // A balanced and fast setting

if (!SUPABASE_URL || !SUPABASE_KEY || !TIXR_CPK || !TIXR_SECRET_KEY || !TIXR_GROUP_ID) {
  console.error('❌ Missing required environment variables. Check your .env file or GitHub Secrets.');
  process.exit(1);
}

// Initialize with the service key to bypass RLS policies
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
    constructor(total, batchNum, totalBatches) { this.total = total; this.current = 0; this.startTime = Date.now(); this.batchInfo = `| Batch ${batchNum}/${totalBatches || '?'}` }
    tick() {
        this.current++;
        const elapsed = (Date.now() - this.startTime) / 1000;
        const avgTime = elapsed / this.current;
        const remaining = this.total - this.current;
        const eta = Math.round(remaining * avgTime);
        const percent = ((this.current / this.total) * 100).toFixed(1);
        const formatTime = s => s < 60 ? `${Math.floor(s)}s` : `${Math.floor(s / 60)}m ${Math.floor(s % 60)}s`;
        process.stdout.write(`  Progress: ${this.current}/${this.total} [${percent}%] | ETA: ${formatTime(eta)} ${this.batchInfo}\r`);
    }
}

const apiLimiter = new Limiter(MAX_CONCURRENT_API_CALLS);

// ==================== TIXR API FUNCTIONS ====================

function buildHash(basePath, paramsObj) {
  const paramsSorted = Object.keys(paramsObj).sort().map(k => `${k}=${encodeURIComponent(paramsObj[k])}`).join('&');
  const hashString = `${basePath}?${paramsSorted}`;
  return crypto.createHmac('sha256', TIXR_SECRET_KEY).update(hashString).digest('hex');
}

async function fetchFanDetails(userId, errorCounter) {
    const basePath = `/v1/groups/${TIXR_GROUP_ID}/fans/${userId}`;
    const params = { cpk: TIXR_CPK, t: Date.now() };
    const hash = buildHash(basePath, params);
    const paramsString = Object.keys(params).map(k=>`${k}=${encodeURIComponent(params[k])}`).join('&');
    const url = `${TIXR_API_BASE_URL}${basePath}?${paramsString}&hash=${hash}`;
    try {
        const { data } = await axios.get(url, { timeout: 20000 });
        return data;
    } catch (error) { 
        errorCounter.count++;
        return null; 
    }
}

// ==================== MAIN SCRIPT LOGIC (Corrected for Pagination) ====================

async function enrichUsers() {
  const overallStartTime = Date.now();
  let totalUsersUpdated = 0;
  let totalErrors = 0;
  let batchNum = 0;
  let keepProcessing = true;
  
  // Work WITH the Supabase limit, not against it.
  const USERS_PER_BATCH = 1000; 

  // Get total count for better progress reporting
  const { count: totalUsersToProcess, error: countError } = await supabase
    .from('events_users')
    .select('user_id', { count: 'exact', head: true })
    .is('last_enriched_at', null); // Query users that have never been enriched
    
  if (countError) {
      console.error('❌ Error counting users:', countError);
      return;
  }
  
  if (totalUsersToProcess === 0) {
      console.log('✅ All users are fully enriched. Nothing to do.');
      return;
  }
  
  const totalBatches = Math.ceil(totalUsersToProcess / USERS_PER_BATCH);
  console.log(`🔥 Total users to process: ${totalUsersToProcess} in ~${totalBatches} batches of ${USERS_PER_BATCH}.`);

  while (keepProcessing) {
    const batchStartTime = Date.now();
    
    // Use range() for true pagination
    const from = batchNum * USERS_PER_BATCH;
    const to = from + USERS_PER_BATCH - 1;

    batchNum++; // Increment batch number for the next loop

    console.log(`\nFetching batch ${batchNum}/${totalBatches} (rows ${from}-${to})...`);

    const { data: usersToEnrich, error } = await supabase
      .from('events_users')
      .select('user_id')
      .is('last_enriched_at', null)
      .range(from, to); // Use range() instead of limit()

    if (error) {
      console.error('❌ Error fetching users to enrich:', error.message);
      return; 
    }

    if (!usersToEnrich || usersToEnrich.length === 0) {
      keepProcessing = false;
      continue;
    }
    
    const userIds = usersToEnrich.map(u => u.user_id);
    console.log(`➡️  Enriching ${userIds.length} users with ${MAX_CONCURRENT_API_CALLS} parallel workers...`);

    const progressBar = new ProgressBar(userIds.length, batchNum, totalBatches);
    const errorCounter = { count: 0 };
    
    const fanPromises = userIds.map(id => 
      apiLimiter.execute(async () => {
          const fanData = await fetchFanDetails(id, errorCounter);
          progressBar.tick();
          return fanData;
      })
    );

    const fanDetails = (await Promise.all(fanPromises)).filter(Boolean);

    console.log(`\n🔄 Found details for ${fanDetails.length} users. Updating database...`);

    const usersToUpsert = fanDetails.map(fan => ({
      user_id: fan.id.toString(),
      user_age: fan.age,
      user_birth_date: fan.birth_date,
      user_opt_in: fan.opt_in,
      user_total_spend: fan.overall_spend,
      user_tickets_purchased: fan.tickets_purchased,
      user_last_purchase: fan.last_purchase ? new Date(fan.last_purchase).toISOString() : null,
      user_gender: fan.gender,
      last_enriched_at: new Date().toISOString(), // Set the timestamp
    }));

    if (usersToUpsert.length > 0) {
        const { error: upsertError } = await supabase.from('events_users').upsert(usersToUpsert, { onConflict: 'user_id' });
        if (upsertError) {
            console.error('❌ Error saving enriched user data:', upsertError.message);
        }
    }
    
    totalUsersUpdated += fanDetails.length;
    totalErrors += errorCounter.count;
    const batchDuration = (Date.now() - batchStartTime) / 1000;
    
    console.log(`\n✨ Batch ${batchNum} complete in ${batchDuration.toFixed(1)}s. ${fanDetails.length} updated, ${errorCounter.count} failed.`);
  }

  const totalDuration = (Date.now() - overallStartTime) / 1000;
  console.log('\n\n🎉🎉 Enrichment Complete! 🎉🎉');
  console.log(`- Total users updated: ${totalUsersUpdated} in ${totalDuration.toFixed(1)}s.`);
  console.log(`- Total fetch errors: ${totalErrors}.`);
}

async function main() {
    await enrichUsers();
    console.log('\n✅ Script finished!');
}

main().catch(err => {
    console.error("\n❌ A fatal error occurred:", err);
    process.exit(1);
});