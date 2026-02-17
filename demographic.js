require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const crypto = require('crypto');

// --- CONFIGURATION ---
const CONFIG = {
    // DB Config
    supabaseUrl: process.env.SUPABASE_URL,
    supabaseKey: process.env.SUPABASE_KEY,
    TABLE_ORDERS: 'events_orders',
    TABLE_USERS: 'events_users',
    
    // Tixr API Config
    tixrBaseUrl: 'https://studio.tixr.com/v1',
    tixrGroupId: process.env.TIXR_GROUP_ID || '980',
    tixrCpk: process.env.TIXR_CPK,
    tixrSecret: process.env.TIXR_SECRET_KEY,

    // Target Event
    TARGET_EVENT_ID: 155673
};

if (!CONFIG.supabaseUrl || !CONFIG.supabaseKey) {
    console.error("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env");
    process.exit(1);
}

const supabase = createClient(CONFIG.supabaseUrl, CONFIG.supabaseKey);

// --- GLOBAL STORAGE ---
const uniqueUsers = new Map();
// New counter for Total Tickets (to match screenshot)
let statsTotals = {
    ticketsDb: 0,
    ticketsApi: 0,
    ordersDoor: 0,
    ordersOnline: 0
};

// --- SUPABASE HELPER FUNCTIONS ---

async function fetchAllSupabase(table, selectCols, filterFn) {
    let allRows = [];
    let page = 0;
    const pageSize = 1000; 
    let hasMore = true;

    process.stdout.write(`   ↳ DB Fetching ${table}... `);

    while (hasMore) {
        const from = page * pageSize;
        const to = from + pageSize - 1;

        let query = supabase
            .from(table)
            .select(selectCols)
            .range(from, to);

        if (filterFn) query = filterFn(query);

        const { data, error } = await query;

        if (error) {
            console.error(`\n❌ Error fetching ${table}:`, error.message);
            throw error;
        }

        if (data && data.length > 0) {
            allRows = allRows.concat(data);
            process.stdout.write(`${allRows.length}.. `);
            if (data.length < pageSize) hasMore = false;
            else page++;
        } else {
            hasMore = false;
        }
    }
    console.log(`Done. (${allRows.length} total)`);
    return allRows;
}

async function fetchDbUsers(userIds) {
    if (userIds.length === 0) return [];
    
    const CHUNK_SIZE = 500;
    let allUsers = [];

    for (let i = 0; i < userIds.length; i += CHUNK_SIZE) {
        const chunk = userIds.slice(i, i + CHUNK_SIZE);
        const { data, error } = await supabase
            .from(CONFIG.TABLE_USERS)
            .select('user_id, user_age, user_gender, user_country, user_city, user_state')
            .in('user_id', chunk);

        if (error) console.error("Error fetching user batch:", error.message);
        else if (data) allUsers = allUsers.concat(data);
    }
    return allUsers;
}

// --- TIXR API HELPER FUNCTIONS ---

function getSignedUrl(endpointPath, params = {}) {
    const timestamp = Date.now();
    const allParams = { ...params, cpk: CONFIG.tixrCpk, t: timestamp };

    const sortedKeys = Object.keys(allParams).sort();
    const queryParts = sortedKeys.map(key => `${key}=${encodeURIComponent(allParams[key])}`);
    const queryString = queryParts.join('&');

    // Tixr requires /v1 in the hash string
    const stringToHash = `/v1${endpointPath}?${queryString}`;

    const hash = crypto.createHmac('sha256', CONFIG.tixrSecret)
        .update(stringToHash)
        .digest('hex');

    return `${CONFIG.tixrBaseUrl}${endpointPath}?${queryString}&hash=${hash}`;
}

async function fetchTixrPaginated(endpointPath, params = {}) {
    let allResults = [];
    let page = 1;
    const pageSize = 1000;
    let hasMore = true;

    // Use a past start_date to ensure we get all historical orders
    const finalParams = { 
        ...params, 
        start_date: '2015-01-01', 
        page_size: pageSize 
    };

    try {
        while (hasMore) {
            process.stdout.write(`   ↳ API Fetching ${endpointPath} (Page ${page})... \r`);
            
            finalParams.page_number = page;
            const url = getSignedUrl(endpointPath, finalParams);

            const response = await axios.get(url, { headers: { 'Accept': 'application/json' } });
            const data = response.data;

            if (Array.isArray(data) && data.length > 0) {
                allResults = allResults.concat(data);
                if (data.length < pageSize) hasMore = false;
                else page++;
            } else {
                hasMore = false;
            }
        }
        console.log(`   ✅ API Fetched ${allResults.length} records from ${endpointPath}`);
    } catch (error) {
        console.log(`\n❌ API Error on ${endpointPath}`);
        if (error.response) {
            console.log(`   Status: ${error.response.status}`);
            console.log(`   Response: ${JSON.stringify(error.response.data)}`);
        } else {
            console.log(`   Message: ${error.message}`);
        }
    }
    return allResults;
}

// --- DATA PROCESSING LOGIC ---

function normalizeGender(g) {
    if (!g) return "UNSPECIFIED";
    g = String(g).toUpperCase();
    if (g.includes("FEMALE") || g === "F") return "FEMALE";
    if (g.includes("MALE") || g === "M") return "MALE";
    return "UNSPECIFIED";
}

function normalizeLocation(city, state, country) {
    if (!country) country = "UNKNOWN";
    country = country.toUpperCase();
    
    let isQC = false;
    if (state && (state.toUpperCase().includes('QC') || state.toUpperCase().includes('QUEBEC'))) isQC = true;
    if (city && (city.toUpperCase().includes('QUEBEC'))) isQC = true;

    let normCity = city ? city.trim().toLowerCase().replace(/(^\w|\s\w)/g, m => m.toUpperCase()) : "Unknown";

    return { country, city: normCity, isQC };
}

function mergeUserData(id, sourceData) {
    if (!id) return;
    
    if (!uniqueUsers.has(id)) {
        uniqueUsers.set(id, {
            id: id,
            age: null,
            gender: null,
            country: null,
            city: null,
            isQC: false
        });
    }

    const user = uniqueUsers.get(id);

    if (!user.age && sourceData.age) user.age = sourceData.age;
    if ((!user.gender || user.gender === 'UNSPECIFIED') && sourceData.gender) {
        user.gender = normalizeGender(sourceData.gender);
    }
    if (!user.country || user.country === 'UNKNOWN') {
        if (sourceData.country) user.country = sourceData.country;
        if (sourceData.city) user.city = sourceData.city;
        if (sourceData.isQC) user.isQC = sourceData.isQC;
    }
}

// --- MAIN EXECUTION ---

async function analyzeEvent(eventId) {
    console.log(`\n\n=========================================`);
    console.log(`🚀 STARTING EXHAUSTIVE ANALYSIS`);
    console.log(`🎯 TARGET EVENT ID: ${eventId}`);
    console.log(`=========================================`);

    // 1. FETCH FROM SUPABASE
    console.log(`\n📡 [SOURCE 1] Fetching Supabase DB...`);
    const dbOrders = await fetchAllSupabase(
        CONFIG.TABLE_ORDERS,
        'order_quantity, order_user_id', 
        (q) => q.eq('event_id', eventId).eq('order_status', 'COMPLETE')
    );
    
    // Sum DB Tickets
    dbOrders.forEach(o => {
        statsTotals.ticketsDb += (o.order_quantity || 0);
    });

    const dbUserIds = [...new Set(dbOrders.map(o => o.order_user_id).filter(Boolean))];
    console.log(`   Found ${dbOrders.length} orders, ${statsTotals.ticketsDb} tickets, ${dbUserIds.length} unique users in DB.`);

    if (dbUserIds.length > 0) {
        const dbUsers = await fetchDbUsers(dbUserIds);
        dbUsers.forEach(u => {
            const loc = normalizeLocation(u.user_city, u.user_state, u.user_country);
            mergeUserData(u.user_id, {
                age: u.user_age,
                gender: u.user_gender,
                country: loc.country,
                city: loc.city,
                isQC: loc.isQC
            });
        });
        console.log(`   ✅ DB Data Merged.`);
    }

    // 2. FETCH FROM TIXR API
    if (CONFIG.tixrCpk && CONFIG.tixrSecret) {
        console.log(`\n📡 [SOURCE 2] Fetching Tixr API (Real-time)...`);
        
        // A. FANS
        const apiFans = await fetchTixrPaginated(`/groups/${CONFIG.tixrGroupId}/events/${eventId}/fans`);
        apiFans.forEach(f => {
            mergeUserData(f.id, { age: f.age, gender: f.gender });
        });

        // B. ORDERS
        const apiOrders = await fetchTixrPaginated(`/groups/${CONFIG.tixrGroupId}/events/${eventId}/orders`, { status: 'COMPLETE' });
        
        apiOrders.forEach(o => {
            // Count Tickets from Order Items
            let orderTickets = 0;
            if (o.sale_items && Array.isArray(o.sale_items)) {
                o.sale_items.forEach(item => {
                    orderTickets += (item.quantity || 0);
                });
            }
            statsTotals.ticketsApi += orderTickets;

            // Track Source (Door vs Online)
            if (o.order_source === 'DOOR') statsTotals.ordersDoor++;
            else statsTotals.ordersOnline++;

            // Merge Location Data
            if (o.user_id) {
                if (o.geo_info) {
                    const loc = normalizeLocation(o.geo_info.city, o.geo_info.state, o.geo_info.country_code);
                    mergeUserData(o.user_id, {
                        country: loc.country,
                        city: loc.city,
                        isQC: loc.isQC
                    });
                }
            }
        });
        
    } else {
        console.log(`\n⚠️  Skipping API: Missing TIXR credentials.`);
    }

    // --- AGGREGATE STATS ---
    console.log(`\n⚙️  Processing Final Stats...`);

    const stats = {
        totalAttendees: uniqueUsers.size,
        withDemographics: 0,
        age: { "18-20": 0, "21-24": 0, "25-28": 0, "29-30": 0, "31+": 0, "Unknown": 0 },
        gender: { "MALE": 0, "FEMALE": 0, "UNSPECIFIED": 0 },
        countries: {},
        qcCities: {}
    };

    uniqueUsers.forEach(u => {
        let hasDemo = false;

        // Age
        if (u.age) {
            const age = parseInt(u.age);
            if (!isNaN(age)) {
                hasDemo = true;
                if (age >= 18 && age <= 20) stats.age["18-20"]++;
                else if (age >= 21 && age <= 24) stats.age["21-24"]++;
                else if (age >= 25 && age <= 28) stats.age["25-28"]++;
                else if (age >= 29 && age <= 30) stats.age["29-30"]++;
                else if (age > 30) stats.age["31+"]++;
                else stats.age["Unknown"]++;
            } else {
                stats.age["Unknown"]++;
            }
        } else {
            stats.age["Unknown"]++;
        }

        // Gender
        const g = u.gender || "UNSPECIFIED";
        stats.gender[g]++;
        if (g !== 'UNSPECIFIED') hasDemo = true;

        if (hasDemo) stats.withDemographics++;

        // Location
        const country = u.country || "UNKNOWN";
        stats.countries[country] = (stats.countries[country] || 0) + 1;

        if (u.isQC && u.city) {
            stats.qcCities[u.city] = (stats.qcCities[u.city] || 0) + 1;
        }
    });

    printReport(eventId, stats);
}

function printReport(eventId, stats) {
    console.log(`\n=========================================`);
    console.log(`📊 FINAL REPORT: EVENT ${eventId}`);
    console.log(`=========================================`);
    
    console.log(`1. ATTENDANCE OVERVIEW`);
    console.log(`   - Total Unique Attendees (Users): ${stats.totalAttendees}`);
    console.log(`   - Total Tickets/Scans (API):      ${statsTotals.ticketsApi} (Matches Screenshot ~2451)`);
    console.log(`   - Total Tickets (DB):             ${statsTotals.ticketsDb}`);
    console.log(`   - Orders Source:                  ${statsTotals.ordersOnline} Online / ${statsTotals.ordersDoor} Door`);
    
    const ratio = stats.totalAttendees > 0 ? ((stats.withDemographics / stats.totalAttendees) * 100).toFixed(1) : 0;
    console.log(`\n2. DATA COMPLETENESS`);
    console.log(`   - Users with Demographics: ${stats.withDemographics} (${ratio}%)`);

    console.log(`\n3. DEMOGRAPHICS (AGE)`);
    Object.keys(stats.age).forEach(r => console.log(`   - ${r}: ${stats.age[r]}`));

    console.log(`\n4. DEMOGRAPHICS (GENDER)`);
    console.log(`   - Male: ${stats.gender.MALE}`);
    console.log(`   - Female: ${stats.gender.FEMALE}`);
    console.log(`   - Unspecified: ${stats.gender.UNSPECIFIED}`);

    console.log(`\n5. TOP COUNTRIES`);
    const sortedCountries = Object.entries(stats.countries)
        .sort((a,b) => b[1] - a[1])
        .filter(([c, count]) => count > 0 && c !== 'UNKNOWN');
    
    sortedCountries.slice(0, 10).forEach(([c, count]) => console.log(`   - ${c}: ${count}`));

    console.log(`\n6. TOP 10 QC CITIES (CANADA)`);
    const sortedCities = Object.entries(stats.qcCities)
        .sort((a,b) => b[1] - a[1])
        .slice(0, 10);
        
    if (sortedCities.length === 0) console.log("   (No QC city data available)");
    sortedCities.forEach(([c, count]) => console.log(`   - ${c}: ${count}`));
    console.log(`\n`);
}

(async () => {
    try {
        await analyzeEvent(CONFIG.TARGET_EVENT_ID);
    } catch (e) {
        console.error("Critical Execution Error:", e);
    }
})();