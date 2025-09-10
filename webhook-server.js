const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const crypto = require('crypto');
require('dotenv').config();

console.log('🚀 Starting Tixr All-in-One Webhook Server...');

// --- CONFIGURATION ---
const PORT = process.env.PORT || 3000;
const {
  SUPABASE_URL,
  SUPABASE_KEY,
  TIXR_GROUP_ID,
  TIXR_CPK,
  TIXR_SECRET_KEY
} = process.env;

if (!SUPABASE_URL || !SUPABASE_KEY || !TIXR_CPK || !TIXR_SECRET_KEY || !TIXR_GROUP_ID) {
  console.error('❌ Missing required environment variables.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const TIXR_API_BASE_URL = 'https://studio.tixr.com';
const app = express();
app.use(express.json());

// ==================== SHARED HELPER FUNCTIONS ====================

function buildHash(basePath, paramsObj) {
  const paramsSorted = Object.keys(paramsObj).sort().map(k => `${k}=${encodeURIComponent(paramsObj[k])}`).join('&');
  const hashString = `${basePath}?${paramsSorted}`;
  return crypto.createHmac('sha256', TIXR_SECRET_KEY).update(hashString).digest('hex');
}

function capitalize(str) {
    if (typeof str !== 'string' || !str) return str;
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
}

// ==================== TIXR API FETCH FUNCTIONS ====================

async function fetchTixrOrderById(orderId) {
    const basePath = `/v1/groups/${TIXR_GROUP_ID}/orders/${orderId}`;
    const params = { cpk: TIXR_CPK, t: Date.now() };
    const hash = buildHash(basePath, params);
    const paramsString = Object.keys(params).map(k=>`${k}=${encodeURIComponent(params[k])}`).join('&');
    const url = `${TIXR_API_BASE_URL}${basePath}?${paramsString}&hash=${hash}`;
    try {
        console.log(`  🔍 Fetching full details for order ${orderId}...`);
        const { data } = await axios.get(url, { timeout: 10000 });
        return data;
    } catch (error) {
        console.error(`  ❌ Error fetching order ${orderId}:`, error.message);
        throw error;
    }
}

async function fetchTixrEventById(eventId) {
    const basePath = `/v1/groups/${TIXR_GROUP_ID}/events/${eventId}`;
    const params = { cpk: TIXR_CPK, t: Date.now() };
    const hash = buildHash(basePath, params);
    const paramsString = Object.keys(params).map(k=>`${k}=${encodeURIComponent(params[k])}`).join('&');
    const url = `${TIXR_API_BASE_URL}${basePath}?${paramsString}&hash=${hash}`;
    try {
        console.log(`  🔍 Fetching full details for event ${eventId}...`);
        const { data } = await axios.get(url, { timeout: 10000 });
        return Array.isArray(data) ? data[0] : data;
    } catch (error) {
        console.error(`  ❌ Error fetching event ${eventId}:`, error.message);
        throw error;
    }
}

// ==================== SECURITY MIDDLEWARE ====================

function checkWebhookSecurity(req, res, next) {
  const clientIp = req.headers['x-forwarded-for']?.split(',')[0] || req.connection.remoteAddress;
  console.log(`\n📥 Received webhook from IP: ${clientIp}`);
  next();
}

// ==================== EVENT PROCESSING LOGIC ====================
// (Brought in from your sync-events.js file for a self-contained server)

function computeEventStatus(eventDate) {
  const now = new Date();
  const eventStart = new Date(eventDate + 'T00:00:00');
  const eventEnd = new Date(eventStart);
  eventEnd.setDate(eventEnd.getDate() + 1);
  eventEnd.setHours(4, 0, 0, 0);
  return now > eventEnd ? 'PAST' : 'LIVE';
}

function convertToMontrealDate(utcDateString) {
  if (!utcDateString) return null;
  const utcDate = new Date(utcDateString);
  return utcDate.toLocaleDateString("en-CA", { timeZone: "America/Montreal", year: "numeric", month: "2-digit", day: "2-digit" });
}

function transformEventForDB(tixrEvent) {
  const eventDate = convertToMontrealDate(tixrEvent.start_date);
  return {
    event_id: parseInt(tixrEvent.id),
    event_name: tixrEvent.name,
    event_date: eventDate,
    event_status: computeEventStatus(eventDate),
    event_flyer: tixrEvent.flyer_url || tixrEvent.mobile_image_url || null,
    event_updated: new Date().toISOString(),
  };
}

// ==================== WEBHOOK ENDPOINTS ====================

app.post('/webhook/event', checkWebhookSecurity, async (req, res) => {
  const { event_id, action } = req.body;
  console.log(`  Processing EVENT webhook: Action=${action || 'UPDATE'}, EventID=${event_id}`);

  if (!event_id) {
    return res.status(200).json({ success: true, message: 'No event_id, ignored' });
  }

  try {
    if (action === 'UNPUBLISH' || action === 'REMOVED') {
      await supabase.from('events').delete().eq('event_id', event_id);
      console.log(`  ✅ Event ${event_id} removed from database.`);
      return res.status(200).json({ success: true, message: 'Event removed' });
    }
    
    const fullEventData = await fetchTixrEventById(event_id);
    if (!fullEventData) {
      return res.status(404).json({ error: 'Event not found in Tixr' });
    }

    const eventForDB = transformEventForDB(fullEventData);
    await supabase.from('events').upsert(eventForDB, { onConflict: 'event_id' });
    
    console.log(`  ✅ Event ${eventForDB.event_name} (ID: ${event_id}) successfully synced.`);
    res.status(200).json({ success: true, message: 'Event synced' });

  } catch (error) {
    console.error(`  ❌ Error processing event webhook:`, error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/webhook/order', checkWebhookSecurity, async (req, res) => {
    const { order_id, transaction_type } = req.body;
    console.log(`  Processing ORDER webhook: Transaction=${transaction_type}, OrderID=${order_id}`);

    if (!order_id) {
        return res.status(200).json({ success: true, message: 'No order_id, ignored' });
    }

    try {
        const fullOrder = await fetchTixrOrderById(order_id);
        if (!fullOrder) {
            return res.status(404).json({ error: 'Order not found in Tixr' });
        }

        const transformedOrders = (fullOrder.sale_items || []).map(item => ({
            order_id: fullOrder.order_id, event_id: fullOrder.event_id, order_sale_id: item.sale_id,
            order_status: fullOrder.status, order_tier_id: item.tier_id, order_user_id: fullOrder.user_id,
            order_name: `${capitalize(fullOrder.first_name) || ''} ${capitalize(fullOrder.lastname) || ''}`.trim(),
            order_sales_item_name: item.name, order_category: item.category, order_quantity: item.quantity,
            order_purchase_date: new Date(fullOrder.purchase_date).toISOString(), order_gross: fullOrder.gross_sales,
            order_net: fullOrder.net, order_user_agent: fullOrder.user_agent_type, order_card_type: fullOrder.card_type,
            order_ref: fullOrder.ref_id, order_ref_type: fullOrder.ref_type,
            order_serials: item.tickets?.map(t => t.serial_number).join(',') || null,
        }));

        await supabase.from('events_orders').upsert(transformedOrders, { onConflict: 'order_id, order_sale_id' });
        console.log(`  💾 Saved ${transformedOrders.length} order items for order ${order_id}.`);

        if (fullOrder.user_id) {
            const { data: existingUser } = await supabase.from('events_users').select('event_ids').eq('user_id', fullOrder.user_id.toString()).single();
            const existingEvents = existingUser?.event_ids || [];
            const updatedEvents = Array.from(new Set([...existingEvents, fullOrder.event_id]));

            const userPayload = {
                user_id: fullOrder.user_id.toString(),
                user_first_name: capitalize(fullOrder.first_name),
                user_last_name: capitalize(fullOrder.lastname),
                user_mail: fullOrder.email,
                user_opt_in: fullOrder.opt_in,
                user_city: fullOrder.geo_info?.city,
                user_state: fullOrder.geo_info?.state,
                user_country: fullOrder.geo_info?.country_code,
                user_postal: fullOrder.geo_info?.postal_code,
                event_ids: updatedEvents,
            };
            await supabase.from('events_users').upsert(userPayload, { onConflict: 'user_id' });
            console.log(`  👥 Synced user profile for ${userPayload.user_first_name} ${userPayload.user_last_name}.`);
        }
        
        res.status(200).json({ success: true, message: 'Order synced' });

    } catch (error) {
        console.error(`  ❌ Error processing order webhook:`, error.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});


// ==================== SERVER BOILERPLATE ====================

app.get('/health', (req, res) => res.status(200).json({ status: 'healthy' }));
app.get('/', (req, res) => res.status(200).json({ service: 'Tixr Webhook Listener is running' }));
app.use((req, res) => res.status(404).json({ error: 'Endpoint not found' }));

const server = app.listen(PORT, () => {
  console.log('═══════════════════════════════════════════');
  console.log('     TIXR ALL-IN-ONE WEBHOOK SERVER');
  console.log(`🚀 Server running on port ${PORT}, ready for webhooks.`);
  console.log('═══════════════════════════════════════════');
});
