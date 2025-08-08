const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const crypto = require('crypto');

const app = express();

// Capture raw body for signature verification
app.use(express.json({
  verify: (req, res, buf) => {
    req.rawBody = buf.toString('utf-8');
  }
}));

// Load environment variables
if (process.env.NODE_ENV !== 'production') {
  try {
    require('dotenv').config();
  } catch (error) {
    console.log('Using system environment variables');
  }
}

// Configuration
const PORT = process.env.PORT || 3000;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_KEY;
const GROUP_ID = process.env.TIXR_GROUP_ID || '980';
const CPK = process.env.TIXR_CPK;
const SECRET_KEY = process.env.TIXR_SECRET_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ==================== REUSE EXACT FUNCTIONS FROM SYNC ====================

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
  
  if (INCLUDE_LIST.some((w) => name.toLowerCase().includes(w))) {
    return toTitleCase(name.replace(/Takeover$/i, "").replace(/Night$/i, "").trim());
  }
  
  name = name.replace(/^gp\d+[:\-\s]*/i, "");
  name = name.replace(/^(.+?)\s+(présente|présentent|presents?)\s+.+$/i, "$1");
  
  const delimiters = [", ", " + ", " b2b ", " & ", " x ", " / ", " vs ", " v "];
  for (const delimiter of delimiters) {
    if (name.includes(delimiter)) {
      name = name.split(delimiter)[0];
      break;
    }
  }
  
  name = name.replace(/\s+(et invités|and guests?|avec|feat\.?|featuring|ft\.?|w\/)\s.*$/i, "");
  name = name.replace(/ *[\(\[].*?[\)\]] */g, " ");
  name = name.split("|")[0].split("-")[0].split(":")[0].split("@")[0].trim();
  
  if (/(\w+)'s\b/i.test(name)) {
    const match = name.match(/^(.+?)'s\b/i);
    if (match) name = match[1];
  }
  
  name = name.replace(/\b(tour(née)?|edition|montr[eé]al|takeover|night|experience|showcase|official|after\s?party|post-race)\b.*$/i, "");
  name = name.replace(/\b\d{4}\b/g, "").replace(/\d{5,}/g, "");
  name = name.replace(/[-–—|•:]+$/g, "");
  name = name.replace(/^[^a-z0-9]+|[^a-z0-9]+$/gi, "");
  
  let main = toTitleCase(name.replace(/\s{2,}/g, " "));
  
  if (!main || !main.length) return null;
  
  const lowerMain = main.toLowerCase();
  const excludeRegex = new RegExp(`\\b(${EXCLUDE_LIST.join("|")})\\b`, "i");
  if (excludeRegex.test(lowerMain) && !INCLUDE_LIST.some((w) => lowerMain.includes(w))) {
    return null;
  }
  
  main = main.replace(/\b(\d{2,4}|live|tour|edition|set|experience)\b$/gi, "").trim();
  
  return main || null;
}

function extractArtistFromEvent(tixrEvent) {
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
  return extractMainArtist(tixrEvent.name);
}

function computeEventStatus(eventDate) {
  const now = new Date();
  const eventStart = new Date(eventDate + 'T00:00:00');
  
  // ALWAYS use 4 AM next day as end time
  const eventEnd = new Date(eventStart);
  eventEnd.setDate(eventEnd.getDate() + 1);
  eventEnd.setHours(4, 0, 0, 0);
  
  return now > eventEnd ? 'PAST' : 'LIVE';
}

function convertToMontrealDate(utcDateString) {
  if (!utcDateString) return null;
  const utcDate = new Date(utcDateString);
  
  return utcDate.toLocaleDateString("en-CA", {
    timeZone: "America/Montreal",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  });
}

function transformEventForDB(tixrEvent) {
  const eventDate = convertToMontrealDate(tixrEvent.start_date);
  
  return {
    event_id: parseInt(tixrEvent.id),
    event_name: tixrEvent.name,
    event_date: eventDate,
    event_artist: extractArtistFromEvent(tixrEvent),
    event_status: computeEventStatus(eventDate),
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

// CRITICAL: Re-fetch full event data from Tixr API
async function fetchTixrEventById(eventId) {
  const basePath = `/v1/groups/${GROUP_ID}/events/${eventId}`;
  const t = Date.now();
  const params = { cpk: CPK, t };
  const { paramsSorted, hash } = buildHash(basePath, params);
  const url = `https://studio.tixr.com${basePath}?${paramsSorted}&hash=${hash}`;
  
  try {
    console.log(`  🔍 Re-fetching full data for event ${eventId}...`);
    const { data } = await axios.get(url, { timeout: 10000 });
    
    // Handle both single event and array responses
    if (Array.isArray(data)) {
      return data[0];
    }
    return data;
    
  } catch (error) {
    console.error(`  ❌ Error fetching event ${eventId}:`, error.message);
    throw error;
  }
}

// ==================== WEBHOOK MIDDLEWARE ====================
function verifyWebhookSignature(req, res, next) {
  if (!WEBHOOK_SECRET || WEBHOOK_SECRET === 'your-webhook-secret') {
    console.warn('⚠️  Webhook signature verification skipped (no secret configured)');
    return next();
  }
  
  const signature = req.headers['x-tixr-signature'];
  if (!signature) {
    console.warn('⚠️  No signature header received');
    return next();
  }
  
  const expectedSignature = crypto
    .createHmac('sha256', WEBHOOK_SECRET)
    .update(req.rawBody)
    .digest('hex');
  
  if (signature !== expectedSignature) {
    console.error('❌ Invalid webhook signature');
    return res.status(401).json({ error: 'Invalid signature' });
  }
  
  next();
}

// ==================== WEBHOOK ENDPOINTS ====================
app.post('/webhook/event', verifyWebhookSignature, async (req, res) => {
  const { event_id, action } = req.body;
  
  if (!event_id || !action) {
    return res.status(400).json({ error: 'Missing event_id or action' });
  }
  
  console.log(`\n📥 Webhook received: Action=${action}, EventID=${event_id}`);
  
  try {
    // Handle removal actions
    if (action === 'UNPUBLISH' || action === 'REMOVED') {
      const { error } = await supabase
        .from('events')
        .delete()
        .eq('event_id', event_id);
      
      if (error) throw error;
      
      console.log(`  ✅ Event ${event_id} removed from database`);
      return res.status(200).json({ success: true, message: 'Event removed' });
    }
    
    // For all other actions, re-fetch full event data
    const fullEventData = await fetchTixrEventById(event_id);
    
    if (!fullEventData) {
      console.error(`  ❌ Event ${event_id} not found in Tixr`);
      return res.status(404).json({ error: 'Event not found' });
    }
    
    // Transform and save
    const eventForDB = transformEventForDB(fullEventData);
    
    console.log(`  💾 Saving event: ${eventForDB.event_name}`);
    console.log(`     Artist: ${eventForDB.event_artist || 'None'}`);
    console.log(`     Status: ${eventForDB.event_status}`);
    console.log(`     Date: ${eventForDB.event_date}`);
    
    const { error } = await supabase
      .from('events')
      .upsert(eventForDB, { onConflict: 'event_id' });
    
    if (error) throw error;
    
    console.log(`  ✅ Event ${event_id} successfully synced`);
    res.status(200).json({ 
      success: true, 
      message: 'Event synced',
      event: {
        id: eventForDB.event_id,
        name: eventForDB.event_name,
        status: eventForDB.event_status
      }
    });
    
  } catch (error) {
    console.error(`  ❌ Error processing webhook:`, error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ 
    status: 'healthy',
    service: 'Tixr Webhook Server',
    timestamp: new Date().toISOString()
  });
});

// Root endpoint
app.get('/', (req, res) => {
  res.status(200).json({
    service: 'Tixr Webhook Listener',
    status: 'running',
    endpoints: [
      'POST /webhook/event - Event updates from Tixr',
      'GET /health - Health check'
    ],
    timestamp: new Date().toISOString()
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: 'Endpoint not found' });
});

// ==================== START SERVER ====================
const server = app.listen(PORT, () => {
  console.log('═══════════════════════════════════════════');
  console.log('     TIXR WEBHOOK SERVER - PRODUCTION');
  console.log('═══════════════════════════════════════════');
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📡 Ready to receive webhooks at:`);
  console.log(`   POST /webhook/event`);
  console.log('═══════════════════════════════════════════');
  
  if (!WEBHOOK_SECRET || WEBHOOK_SECRET === 'your-webhook-secret') {
    console.log('\n⚠️  WARNING: Webhook signature verification is disabled');
    console.log('   Set WEBHOOK_SECRET environment variable for security');
  }
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('\nSIGTERM received, shutting down gracefully...');
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});