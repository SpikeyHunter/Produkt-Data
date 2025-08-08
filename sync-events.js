const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const crypto = require('crypto');

console.log('🚀 Starting Tixr Events Sync (FINAL PRODUCTION)...');

// Load environment variables
if (process.env.NODE_ENV !== 'production') {
  try {
    require('dotenv').config();
  } catch (error) {
    console.log('dotenv not available, using system environment variables');
  }
}

// Configuration
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_KEY;
const GROUP_ID = process.env.TIXR_GROUP_ID || '980';
const CPK = process.env.TIXR_CPK;
const SECRET_KEY = process.env.TIXR_SECRET_KEY;

// Validation
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !CPK || !SECRET_KEY) {
  console.error('❌ Missing required environment variables');
  console.error('Required: SUPABASE_URL, SUPABASE_KEY, TIXR_CPK, TIXR_SECRET_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ==================== ARTIST EXTRACTION (Claude's Superior Logic) ====================
const EXCLUDE_LIST = [
  "moet city", "moët city", "le grand prix", "prix", "mutek", "édition",
  "évènement spécial", "room202", "produktworld", "admission", "taraka",
  "bazart", "city gas", "showcase", "special guest", "guests", "invité",
  "guest", "festival", "event", "experience", "produtk", "produkt",
  "soirée", "party", "post-race", "officiel", "after party", "ncg360",
  "visionnement", "montréal", "grand match", "off-piknic", "piknic",
  "ticket", "table", "official", "pass", "réveillon"
];

const INCLUDE_LIST = ["mimouna night", "dome of faith"];

function toTitleCase(name) {
  if (!name) return null;
  const preserveWords = ['DJ', 'MC', 'NYC', 'LA', 'UK', 'USA', 'II', 'III', 'IV'];
  
  return name.split(/\s+/).map((word) => {
    const upperWord = word.toUpperCase();
    if (preserveWords.includes(upperWord)) return upperWord;
    
    if (word.includes('-')) {
      return word.split('-')
        .map(part => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
        .join('-');
    }
    
    return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
  }).join(' ');
}

function extractMainArtist(eventName) {
  if (!eventName || typeof eventName !== "string") return null;
  let name = eventName.trim();
  
  // Priority to INCLUDE_LIST
  if (INCLUDE_LIST.some((w) => name.toLowerCase().includes(w))) {
    return toTitleCase(name.replace(/Takeover$/i, "").replace(/Night$/i, "").trim());
  }
  
  // Clean prefixes
  name = name.replace(/^gp\d+[:\-\s]*/i, "");
  name = name.replace(/^(.+?)\s+(présente|présentent|presents?)\s+.+$/i, "$1");
  
  // Split on artist delimiters
  const delimiters = [", ", " + ", " b2b ", " & ", " x ", " / ", " vs ", " v "];
  for (const delimiter of delimiters) {
    if (name.includes(delimiter)) {
      name = name.split(delimiter)[0];
      break;
    }
  }
  
  // Remove featuring/guests
  name = name.replace(/\s+(et invités|and guests?|avec|feat\.?|featuring|ft\.?|w\/)\s.*$/i, "");
  name = name.replace(/ *[\(\[].*?[\)\]] */g, " ");
  
  // Split on separators
  name = name.split("|")[0].split("-")[0].split(":")[0].split("@")[0].trim();
  
  // Handle possessive
  if (/(\w+)'s\b/i.test(name)) {
    const match = name.match(/^(.+?)'s\b/i);
    if (match) name = match[1];
  }
  
  // Remove event suffixes
  name = name.replace(/\b(tour(née)?|edition|montr[eé]al|takeover|night|experience|showcase|official|after\s?party|post-race)\b.*$/i, "");
  name = name.replace(/\b\d{4}\b/g, "").replace(/\d{5,}/g, "");
  name = name.replace(/[-–—|•:]+$/g, "");
  name = name.replace(/^[^a-z0-9]+|[^a-z0-9]+$/gi, "");
  
  let main = toTitleCase(name.replace(/\s{2,}/g, " "));
  
  if (!main || !main.length) return null;
  
  // Check exclusions
  const lowerMain = main.toLowerCase();
  const excludeRegex = new RegExp(`\\b(${EXCLUDE_LIST.join("|")})\\b`, "i");
  if (excludeRegex.test(lowerMain) && !INCLUDE_LIST.some((w) => lowerMain.includes(w))) {
    return null;
  }
  
  main = main.replace(/\b(\d{2,4}|live|tour|edition|set|experience)\b$/gi, "").trim();
  
  return main || null;
}

function extractArtistFromEvent(tixrEvent) {
  // Priority 1: Lineups
  if (tixrEvent.lineups?.length > 0) {
    for (const lineup of tixrEvent.lineups) {
      if (lineup.acts?.length > 0) {
        const sortedActs = lineup.acts.sort((a, b) => (a.rank || 999) - (b.rank || 999));
        if (sortedActs[0]?.artist?.name) {
          return toTitleCase(sortedActs[0].artist.name.trim());
        }
      }
    }
  }
  
  // Priority 2: Parse from name
  return extractMainArtist(tixrEvent.name);
}

// ==================== STATUS LOGIC (LIVE/PAST ONLY with 4 AM rule) ====================
function computeEventStatus(eventDate) {
  const now = new Date();
  
  // Parse event date and set to start of day in Montreal time
  const eventStart = new Date(eventDate + 'T00:00:00');
  
  // ALWAYS use 4 AM next day as end time (per your requirement)
  const eventEnd = new Date(eventStart);
  eventEnd.setDate(eventEnd.getDate() + 1);
  eventEnd.setHours(4, 0, 0, 0);
  
  // Simple comparison: if current time is past 4 AM next day, it's PAST
  return now > eventEnd ? 'PAST' : 'LIVE';
}

// ==================== DATE CONVERSION ====================
function convertToMontrealDate(utcDateString) {
  if (!utcDateString) return null;
  const utcDate = new Date(utcDateString);
  
  // Use en-CA locale for YYYY-MM-DD format
  return utcDate.toLocaleDateString("en-CA", {
    timeZone: "America/Montreal",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  });
}

// ==================== TIXR API FUNCTIONS ====================
function buildHash(basePath, paramsObj) {
  const paramsSorted = Object.keys(paramsObj)
    .sort()
    .map(k => `${k}=${encodeURIComponent(paramsObj[k])}`)
    .join('&');
  const hashString = `${basePath}?${paramsSorted}`;
  const hash = crypto
    .createHmac('sha256', SECRET_KEY)
    .update(hashString)
    .digest('hex');
  return { paramsSorted, hash };
}

async function fetchAllTixrEvents() {
  console.log(`📥 Fetching all events from Tixr group ${GROUP_ID}...`);
  const allEvents = [];
  let pageNumber = 1;
  let hasMorePages = true;
  
  while (hasMorePages) {
    const basePath = `/v1/groups/${GROUP_ID}/events`;
    const t = Date.now();
    const params = { cpk: CPK, t, page_number: pageNumber, page_size: 100 };
    const { paramsSorted, hash } = buildHash(basePath, params);
    const url = `https://studio.tixr.com${basePath}?${paramsSorted}&hash=${hash}`;
    
    try {
      console.log(`  📄 Fetching page ${pageNumber}...`);
      const { data } = await axios.get(url, { timeout: 15000 });
      
      if (!Array.isArray(data) || data.length === 0) {
        hasMorePages = false;
        break;
      }
      
      allEvents.push(...data);
      console.log(`  ✓ Page ${pageNumber}: ${data.length} events`);
      
      if (data.length < 100) {
        hasMorePages = false;
      } else {
        pageNumber++;
        await new Promise(resolve => setTimeout(resolve, 250)); // Rate limiting
      }
      
    } catch (error) {
      console.error(`  ❌ Error fetching page ${pageNumber}:`, error.message);
      console.log('  🔄 Retrying in 5 seconds...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
  
  console.log(`✅ Total events fetched: ${allEvents.length}`);
  return allEvents;
}

// ==================== TRANSFORM EVENT FOR DATABASE ====================
function transformEventForDB(tixrEvent) {
  const eventDate = convertToMontrealDate(tixrEvent.start_date);
  
  return {
    event_id: parseInt(tixrEvent.id),
    event_name: tixrEvent.name,
    event_date: eventDate, // YYYY-MM-DD format
    event_artist: extractArtistFromEvent(tixrEvent),
    event_status: computeEventStatus(eventDate), // LIVE or PAST only
    event_genre: null,
    event_flyer: tixrEvent.flyer_url || tixrEvent.mobile_image_url || null,
    event_tags: null,
    event_updated: new Date().toISOString(),
    event_order_updated: null,
    event_attendance_updated: null,
    timetable: null,
    timetable_active: true
  };
}

// ==================== MAIN SYNC FUNCTION ====================
async function syncAllEvents() {
  const startTime = Date.now();
  
  try {
    // Test database connection
    console.log('🔌 Testing Supabase connection...');
    const { error: testError } = await supabase.from('events').select('count').limit(1);
    if (testError) {
      console.error('❌ Supabase connection failed:', testError);
      process.exit(1);
    }
    console.log('✅ Supabase connected\n');
    
    // Fetch all events
    const tixrEvents = await fetchAllTixrEvents();
    if (tixrEvents.length === 0) {
      console.log('No events to process. Exiting.');
      return;
    }
    
    // Transform events
    console.log(`\n🔄 Transforming ${tixrEvents.length} events for database...`);
    const eventsToUpsert = tixrEvents.map(transformEventForDB).filter(Boolean);
    
    // Count statuses
    const statusCounts = eventsToUpsert.reduce((acc, event) => {
      acc[event.event_status] = (acc[event.event_status] || 0) + 1;
      return acc;
    }, {});
    
    console.log('📊 Status breakdown:');
    console.log(`   LIVE: ${statusCounts.LIVE || 0}`);
    console.log(`   PAST: ${statusCounts.PAST || 0}`);
    
    // Save to database in batches
    console.log(`\n💾 Saving ${eventsToUpsert.length} events to Supabase...`);
    const batchSize = 100;
    
    for (let i = 0; i < eventsToUpsert.length; i += batchSize) {
      const batch = eventsToUpsert.slice(i, i + batchSize);
      const { error } = await supabase
        .from('events')
        .upsert(batch, { onConflict: 'event_id' });
      
      if (error) {
        console.error(`  ❌ Error saving batch:`, error.message);
      } else {
        console.log(`  ✓ Saved batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(eventsToUpsert.length/batchSize)}`);
      }
    }
    
    const totalTime = (Date.now() - startTime) / 1000;
    console.log(`\n✨ Sync complete in ${totalTime.toFixed(1)}s!`);
    
  } catch (error) {
    console.error('\n❌ Fatal error during sync:', error);
    process.exit(1);
  }
}

// ==================== STATUS UPDATE FUNCTION ====================
async function updateStatuses() {
  console.log('🔄 Updating event statuses...');
  
  try {
    // Get all LIVE events
    const { data: liveEvents, error } = await supabase
      .from('events')
      .select('event_id, event_date')
      .eq('event_status', 'LIVE');
    
    if (error) throw error;
    
    if (!liveEvents || liveEvents.length === 0) {
      console.log('  No LIVE events to check');
      return;
    }
    
    const updates = [];
    
    for (const event of liveEvents) {
      const status = computeEventStatus(event.event_date);
      
      if (status === 'PAST') {
        updates.push({
          event_id: event.event_id,
          event_status: 'PAST',
          event_updated: new Date().toISOString()
        });
      }
    }
    
    if (updates.length > 0) {
      const { error: updateError } = await supabase
        .from('events')
        .upsert(updates, { onConflict: 'event_id' });
      
      if (updateError) {
        console.error('  ❌ Error updating statuses:', updateError);
      } else {
        console.log(`  ✅ Updated ${updates.length} events to PAST`);
      }
    } else {
      console.log('  ✓ All statuses are correct');
    }
    
  } catch (error) {
    console.error('  ❌ Error in status update:', error);
  }
}

// ==================== MAIN EXECUTION ====================
async function main() {
  const command = process.argv[2] || 'sync';
  
  console.log('═══════════════════════════════════════════');
  console.log('     TIXR EVENTS SYNC - FINAL PRODUCTION');
  console.log('═══════════════════════════════════════════\n');
  
  switch (command) {
    case 'sync':
      // Full sync
      await syncAllEvents();
      break;
      
    case 'status':
      // Only update statuses (for hourly cron)
      await updateStatuses();
      break;
      
    default:
      console.log('Usage:');
      console.log('  node sync-events.js sync   - Full sync of all events');
      console.log('  node sync-events.js status - Update statuses only');
      process.exit(1);
  }
  
  console.log('\n✅ Done!');
  process.exit(0);
}

// Run
main();