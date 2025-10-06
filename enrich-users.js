const { createClient } = require("@supabase/supabase-js");
const axios = require("axios");
const crypto = require("crypto");
require("dotenv").config();

console.log("🚀 Starting Tixr User Enrichment Script (Final Version)...");

// --- CONFIGURATION ---
const {
  SUPABASE_URL,
  SUPABASE_KEY,
  TIXR_GROUP_ID,
  TIXR_CPK,
  TIXR_SECRET_KEY,
} = process.env;

const MAX_CONCURRENT_API_CALLS = 60; // Safe, reliable speed
const USERS_PER_BATCH = 1000; // How many users to process in each API-calling batch

if (!SUPABASE_URL || !SUPABASE_KEY || !TIXR_CPK || !TIXR_SECRET_KEY || !TIXR_GROUP_ID) {
  console.error("❌ Missing required environment variables. Check your .env file or GitHub Secrets.");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const TIXR_API_BASE_URL = "https://studio.tixr.com";

// ==================== UTILITIES ====================

class Limiter {
  constructor(maxConcurrent) { this.maxConcurrent = maxConcurrent; this.current = 0; this.queue = []; }
  async execute(fn) {
    while (this.current >= this.maxConcurrent) await new Promise((resolve) => this.queue.push(resolve));
    this.current++;
    try { return await fn(); } 
    finally { this.current--; const next = this.queue.shift(); if (next) next(); }
  }
}

class ProgressBar {
  constructor(total, batchInfo = "") { this.total = total; this.current = 0; this.startTime = Date.now(); this.batchInfo = batchInfo; }
  tick(message = "") {
    this.current++;
    const elapsed = (Date.now() - this.startTime) / 1000;
    const avgTime = elapsed / this.current;
    const remaining = this.total - this.current;
    const eta = Math.round(remaining * avgTime);
    const percent = ((this.current / this.total) * 100).toFixed(1);
    const formatTime = (s) => s < 60 ? `${Math.floor(s)}s` : `${Math.floor(s / 60)}m ${Math.floor(s % 60)}s`;
    process.stdout.write(`  Progress: ${this.current}/${this.total} [${percent}%] | ETA: ${formatTime(eta)} ${this.batchInfo} ${message}\r`);
  }
}

const apiLimiter = new Limiter(MAX_CONCURRENT_API_CALLS);

// ==================== TIXR API FUNCTIONS ====================

function buildHash(basePath, paramsObj) {
  const paramsSorted = Object.keys(paramsObj).sort().map((k) => `${k}=${encodeURIComponent(paramsObj[k])}`).join("&");
  const hashString = `${basePath}?${paramsSorted}`;
  return crypto.createHmac("sha256", TIXR_SECRET_KEY).update(hashString).digest("hex");
}

async function fetchFanDetails(userId) {
  const basePath = `/v1/groups/${TIXR_GROUP_ID}/fans/${userId}`;
  const params = { cpk: TIXR_CPK, t: Date.now() };
  const hash = buildHash(basePath, params);
  const paramsString = Object.keys(params).sort().map((k) => `${k}=${encodeURIComponent(params[k])}`).join("&");
  const url = `${TIXR_API_BASE_URL}${basePath}?${paramsString}&hash=${hash}`;

  try {
    const { data } = await axios.get(url, { timeout: 20000 });
    return data;
  } catch (error) {
    if (error.response && error.response.status === 404) return null;
    console.error(`\n  - API Error for user ${userId}: ${error.response ? `Status ${error.response.status}` : error.message}`);
    return "error";
  }
}

async function fetchOrdersForUser(userId) {
    const allOrders = []; let pageNumber = 1; let hasMorePages = true; const ORDER_PAGE_SIZE = 100;
    while (hasMorePages) {
        const basePath = `/v1/groups/${TIXR_GROUP_ID}/fans/${userId}/orders`;
        const params = { cpk: TIXR_CPK, t: Date.now(), page_number: pageNumber, page_size: ORDER_PAGE_SIZE };
        const hash = buildHash(basePath, params);
        const paramsString = Object.keys(params).sort().map(k => `${k}=${encodeURIComponent(params[k])}`).join('&');
        const url = `${TIXR_API_BASE_URL}${basePath}?${paramsString}&hash=${hash}`;
        try {
            const { data } = await axios.get(url, { timeout: 20000 });
            if (Array.isArray(data) && data.length > 0) {
                allOrders.push(...data);
                if (data.length < ORDER_PAGE_SIZE) hasMorePages = false; else pageNumber++;
            } else { hasMorePages = false; }
        } catch (error) {
            console.error(`\n  - Error fetching orders for user ${userId}: ${error.message}`);
            hasMorePages = false; return null;
        }
    }
    return allOrders;
}

// ==================== CORE PROCESSING LOGIC ====================

async function processSingleUser(userId, progressBar, stats) {
    const fanData = await fetchFanDetails(userId);
    let userDataToUpsert;

    if (fanData && fanData !== "error") {
        progressBar.tick(`(Fan ID: ${userId} ✔️)`);
        stats.enriched++;
        userDataToUpsert = {
            user_id: fanData.id.toString(), user_age: fanData.age, user_birth_date: fanData.birth_date, user_opt_in: fanData.opt_in,
            user_total_spend: fanData.overall_spend, user_tickets_purchased: fanData.tickets_purchased,
            user_last_purchase: fanData.last_purchase ? new Date(fanData.last_purchase).toISOString() : null,
            user_gender: fanData.gender, last_enriched_at: new Date().toISOString(),
        };
    } else if (fanData === null) {
        progressBar.tick(`(Fan ID: ${userId} -> Fallback 🔨)`);
        const orders = await fetchOrdersForUser(userId);
        if (orders && orders.length > 0) {
            stats.calculated++;
            const calculated = orders.reduce((acc, order) => {
                if (order.status !== 'COMPLETE') return acc;
                acc.totalSpend += order.total || 0;
                acc.lastPurchase = Math.max(acc.lastPurchase, order.purchase_date || 0);
                const ticketQuantity = (order.sale_items || []).reduce((ticketAcc, item) => ticketAcc + (item.quantity || 0), 0);
                acc.totalTickets += ticketQuantity;
                return acc;
            }, { totalSpend: 0, totalTickets: 0, lastPurchase: 0 });
            userDataToUpsert = {
                user_id: userId, user_total_spend: calculated.totalSpend, user_tickets_purchased: calculated.totalTickets,
                user_last_purchase: calculated.lastPurchase > 0 ? new Date(calculated.lastPurchase).toISOString() : null,
                last_enriched_at: new Date().toISOString(),
            };
        } else {
            stats.noOrders++;
            userDataToUpsert = { user_id: userId, last_enriched_at: new Date().toISOString() };
        }
    } else {
        stats.errors++;
        progressBar.tick(`(Fan ID: ${userId} ❌ Error)`);
    }

    if (userDataToUpsert) {
        const { error } = await supabase.from('events_users').upsert(userDataToUpsert, { onConflict: 'user_id' });
        if (error) {
            console.error(`\n❌ Error upserting user ${userId}:`, error.message);
            stats.errors++;
        }
    }
}

// ==================== SCRIPT MODES (FULL & TEST) ====================

async function runFullScript() {
    const overallStartTime = Date.now();
    console.log("🔍 Identifying all users to process (this may take a moment)...");

    // --- NEW: Fetch ALL users who need processing ---
    const allUserIdsToProcess = new Set();
    let page = 0;
    let keepFetching = true;

    // Fetch new users (last_enriched_at is NULL)
    while(keepFetching) {
        const { data, error } = await supabase.from("events_users").select("user_id").is("last_enriched_at", null).range(page * 1000, (page + 1) * 1000 - 1);
        if (error) { console.error("❌ Error fetching new users:", error.message); return; }
        if (data.length > 0) {
            data.forEach(u => allUserIdsToProcess.add(u.user_id));
            page++;
        } else {
            keepFetching = false;
        }
    }
    const newUsersCount = allUserIdsToProcess.size;

    // Fetch users with new purchases
    page = 0;
    keepFetching = true;
     while(keepFetching) {
        const { data, error } = await supabase.from("events_users").select("user_id, user_last_purchase, last_enriched_at").not("user_last_purchase", "is", null).not("last_enriched_at", "is", null).range(page * 1000, (page + 1) * 1000 - 1);
        if (error) { console.error("❌ Error fetching potentially updated users:", error.message); return; }
        if (data.length > 0) {
            data.forEach(u => {
                if (new Date(u.user_last_purchase) > new Date(u.last_enriched_at)) {
                    allUserIdsToProcess.add(u.user_id);
                }
            });
            page++;
        } else {
            keepFetching = false;
        }
    }

    const totalUsersToProcess = allUserIdsToProcess.size;
    const updatedUsersCount = totalUsersToProcess - newUsersCount;
    const userIdArray = Array.from(allUserIdsToProcess);

    if (totalUsersToProcess === 0) { console.log("✅ All users are up-to-date. Nothing to do."); return; }

    console.log(`🔥 Found ${newUsersCount} new user(s) and ${updatedUsersCount} user(s) with new activity.`);
    console.log(`🔥 Total unique users to process: ${totalUsersToProcess}.`);

    let totalProcessed = 0;
    const stats = { enriched: 0, calculated: 0, noOrders: 0, errors: 0 };
    let batchNum = 0;
    const totalBatches = Math.ceil(totalUsersToProcess / USERS_PER_BATCH);

    while (totalProcessed < totalUsersToProcess) {
        const from = batchNum * USERS_PER_BATCH;
        const batchIds = userIdArray.slice(from, from + USERS_PER_BATCH);
        batchNum++;

        if (batchIds.length === 0) break;

        console.log(`\n➡️  Processing batch ${batchNum}/${totalBatches} (${batchIds.length} users)...`);
        const progressBar = new ProgressBar(batchIds.length, `| Batch ${batchNum}/${totalBatches}`);
        
        const userPromises = batchIds.map(id => apiLimiter.execute(() => processSingleUser(id, progressBar, stats)));
        await Promise.all(userPromises);
        
        totalProcessed += batchIds.length;
        console.log(`\n✨ Batch complete.`);
    }
    
    const totalDuration = (Date.now() - overallStartTime) / 1000;
    console.log("\n\n🎉🎉 Enrichment Complete! 🎉🎉");
    console.log(`- Total users processed: ${totalProcessed} in ${totalDuration.toFixed(1)}s.`);
    console.log(`- ${stats.enriched} enriched from Fan profiles.`);
    console.log(`- ${stats.calculated} calculated from order history (fallback).`);
    console.log(`- ${stats.noOrders} had no fan profile or order history.`);
    console.log(`- ${stats.errors} failed due to API or database errors.`);
}

async function runTestMode(testIds) {
    console.log(`🔍 Running in TEST MODE for ${testIds.length} user(s)...`);
    const progressBar = new ProgressBar(testIds.length, '| Test Mode');
    const stats = { enriched: 0, calculated: 0, noOrders: 0, errors: 0 };
    
    const userPromises = testIds.map(id => apiLimiter.execute(() => processSingleUser(id, progressBar, stats)));
    await Promise.all(userPromises);
    
    console.log("\n\n✅ Test complete!");
    console.log(`- ${stats.enriched} enriched, ${stats.calculated} calculated, ${stats.errors} errors.`);
}

async function main() {
  const args = process.argv.slice(2);
  
  if (args[0] && args[0].toUpperCase() === 'TEST' && args.length > 1) {
    const testIds = args.slice(1);
    await runTestMode(testIds);
  } else {
    await runFullScript();
  }

  console.log("\n✅ Script finished!");
}

main().catch((err) => {
  console.error("\n❌ A fatal error occurred:", err);
  process.exit(1);
});

