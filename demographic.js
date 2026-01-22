require('dotenv').config();
const axios = require('axios');
const crypto = require('crypto');

// --- CONFIGURATION ---
const CONFIG = {
    // We use the raw base without /v1 here because we construct the full URL manually
    baseUrl: 'https://studio.tixr.com', 
    cpk: process.env.TIXR_CPK,
    groupId: process.env.TIXR_GROUP_ID,
    secretKey: process.env.TIXR_SECRET_KEY,
};

// --- DATA DEFINITIONS ---
const CATEGORIES = {
    "Lumen": [161378, 161253, 161254, 161252],
    "Moët City": [
        133748, 138284, 144014, 149298, 150888, 153204, 
        156561, 156208, 161616, 165547, 168478
    ],
    "Bazart Nuits": [
        140010, 138528, 140813, 127237, 142809, 142827, 
        145452, 143159, 143050, 142999, 142840, 142829, 
        143105, 142832, 143110, 142830, 143735, 143037, 
        144002, 144912, 144905, 142831, 144154, 151682, 
        151683, 154748, 154749, 157655, 155341, 158763, 
        158800, 158778, 161633, 161903, 159612, 163107, 
        158796, 158798, 166623, 163282
    ]
};

// --- API UTILITIES ---

/**
 * Generic fetcher that handles pagination and Manual URL Construction
 * to match the strict hashing requirements of Tixr.
 */
async function fetchAllPages(endpoint, extraParams = {}) {
    let allResults = [];
    let page = 1;
    const pageSize = 100; // Max allowed by API
    let hasMore = true;

    while (hasMore) {
        const timestamp = Date.now();
        
        // 1. Prepare Params
        const params = {
            cpk: CONFIG.cpk,
            t: timestamp,
            page_number: page,
            page_size: pageSize,
            ...extraParams
        };

        // 2. Sort and Encode Params (Exactly like your working example)
        // This ensures the hash matches exactly what is sent over the wire.
        const sortedParams = Object.keys(params)
            .sort()
            .map(k => `${k}=${encodeURIComponent(params[k])}`)
            .join('&');

        // 3. Construct Hash String
        // Format: /v1 + endpoint + ? + sortedParams
        // Note: endpoint passed in is usually "/groups/980/..."
        const stringToHash = `/v1${endpoint}?${sortedParams}`;

        // 4. Generate Hash
        const hash = crypto.createHmac('sha256', CONFIG.secretKey)
            .update(stringToHash)
            .digest('hex');

        // 5. Construct Final URL
        // We append &hash=... manually
        const finalUrl = `${CONFIG.baseUrl}/v1${endpoint}?${sortedParams}&hash=${hash}`;
        
        try {
            // 6. Execute Request
            // We pass the full URL string directly to avoid Axios re-serializing params
            const response = await axios.get(finalUrl);

            const data = response.data;
            if (Array.isArray(data) && data.length > 0) {
                allResults = allResults.concat(data);
                if (data.length < pageSize) hasMore = false;
                else page++;
            } else {
                hasMore = false;
            }
            
            // Short pause
            await new Promise(r => setTimeout(r, 100)); 

        } catch (error) {
            const status = error.response ? error.response.status : 'Unknown';
            const msg = error.response ? error.response.statusText : error.message;
            console.error(`Error fetching ${endpoint} page ${page}: ${status} - ${msg}`);
            // If 400 happens again, printing the URL can help debug
            if (status === 400) console.error(`Failed URL: ${finalUrl}`);
            
            hasMore = false;
        }
    }
    return allResults;
}

// --- CORE LOGIC ---

async function getCategoryDemographics(categoryName, eventIds) {
    console.log(`\n--- PROCESSING CATEGORY: ${categoryName} (${eventIds.length} events) ---`);
    
    const stats = {
        totalAttendance: 0,
        ageRanges: { "18-20": 0, "21-24": 0, "25-28": 0, "29-30": 0, "31+": 0, "Unknown": 0 },
        gender: { "MALE": 0, "FEMALE": 0, "NOT_AVAILABLE": 0 },
        countries: {},
        qcCities: {}
    };

    for (const eventId of eventIds) {
        process.stdout.write(`Processing Event ID ${eventId}... `);

        // 1. GET ORDERS
        // Endpoint: /groups/{group_id}/events/{event_id}/orders
        const orders = await fetchAllPages(`/groups/${CONFIG.groupId}/events/${eventId}/orders`, {
            status: 'COMPLETE'
        });
        
        orders.forEach(order => {
            // A. Attendance
            if (order.sale_items) {
                order.sale_items.forEach(item => {
                    stats.totalAttendance += (item.quantity || 0);
                });
            }

            // B. Location
            let country = 'Unknown';
            let city = null;
            let state = null;

            if (order.geo_info) {
                country = order.geo_info.country_code || 'Unknown';
                city = order.geo_info.city;
                state = order.geo_info.state;
            } else if (order.shipping_address) {
                country = order.shipping_address.country_code || 'Unknown';
                city = order.shipping_address.city;
                state = order.shipping_address.state;
            }

            stats.countries[country] = (stats.countries[country] || 0) + 1;

            const isCanada = ['CA', 'CAN', 'CANADA'].includes(country.toUpperCase());
            const isQC = state && ['QC', 'QUEBEC', 'QUÉBEC'].includes(state.toUpperCase());

            if (isCanada && isQC && city) {
                const normCity = city.trim().toLowerCase().replace(/(^\w|\s\w)/g, m => m.toUpperCase());
                stats.qcCities[normCity] = (stats.qcCities[normCity] || 0) + 1;
            }
        });

        // 2. GET FANS
        // Endpoint: /groups/{group_id}/events/{event_id}/fans
        const fans = await fetchAllPages(`/groups/${CONFIG.groupId}/events/${eventId}/fans`);
        
        fans.forEach(fan => {
            // C. Age
            const age = fan.age;
            if (!age) {
                stats.ageRanges["Unknown"]++;
            } else if (age >= 18 && age <= 20) stats.ageRanges["18-20"]++;
            else if (age >= 21 && age <= 24) stats.ageRanges["21-24"]++;
            else if (age >= 25 && age <= 28) stats.ageRanges["25-28"]++;
            else if (age >= 29 && age <= 30) stats.ageRanges["29-30"]++;
            else if (age > 30) stats.ageRanges["31+"]++;
            else stats.ageRanges["Unknown"]++;

            // D. Gender
            const g = (fan.gender || "").toUpperCase();
            if (g === 'MALE') stats.gender["MALE"]++;
            else if (g === 'FEMALE') stats.gender["FEMALE"]++;
            else stats.gender["NOT_AVAILABLE"]++;
        });

        console.log(`Done. (Orders: ${orders.length}, Fans: ${fans.length})`);
    }

    return stats;
}

function printReport(categoryName, stats) {
    console.log(`\n=========================================`);
    console.log(`REPORT: ${categoryName}`);
    console.log(`=========================================`);
    console.log(`1. Total Attendance (Tickets Sold): ${stats.totalAttendance}`);
    
    console.log(`\n2. Age Ranges:`);
    Object.keys(stats.ageRanges).forEach(r => console.log(`   - ${r}: ${stats.ageRanges[r]}`));

    console.log(`\n3. Gender:`);
    console.log(`   - MALE: ${stats.gender.MALE}`);
    console.log(`   - FEMALE: ${stats.gender.FEMALE}`);
    console.log(`   - NOT AVAILABLE: ${stats.gender.NOT_AVAILABLE}`);

    console.log(`\n4. Top Countries:`);
    const sortedCountries = Object.entries(stats.countries)
        .sort((a,b) => b[1] - a[1])
        .filter(([c, count]) => count > 0);
    
    if (sortedCountries.length === 0) console.log("   (No country data available)");
    sortedCountries.forEach(([c, count]) => console.log(`   - ${c}: ${count}`));

    console.log(`\n5. Top 5 QC Cities (Canada):`);
    const sortedCities = Object.entries(stats.qcCities)
        .sort((a,b) => b[1] - a[1])
        .slice(0, 5);
        
    if (sortedCities.length === 0) console.log("   (No QC city data available)");
    sortedCities.forEach(([c, count]) => console.log(`   - ${c}: ${count}`));
    console.log(`\n`);
}

// --- MAIN EXECUTION ---
(async () => {
    try {
        if (!CONFIG.cpk || !CONFIG.secretKey || !CONFIG.groupId) {
            throw new Error("Missing .env configuration (TIXR_CPK, TIXR_SECRET_KEY, TIXR_GROUP_ID)");
        }

        for (const [name, ids] of Object.entries(CATEGORIES)) {
            const data = await getCategoryDemographics(name, ids);
            printReport(name, data);
        }

    } catch (e) {
        console.error("Critical Execution Error:", e.message);
    }
})();