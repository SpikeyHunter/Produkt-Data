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

// ================== SHARED HELPER FUNCTIONS ====================

function buildHash(basePath, paramsObj) {
  const paramsSorted = Object.keys(paramsObj).sort().map(k => `${k}=${encodeURIComponent(paramsObj[k])}`).join('&');
  const hashString = `${basePath}?${paramsSorted}`;
  return crypto.createHmac('sha256', TIXR_SECRET_KEY).update(hashString).digest('hex');
}

function capitalize(str) {
    if (typeof str !== 'string' || !str) return str;
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
}

/** Tixr millis timestamp -> ISO string (null-safe). */
function msToIso(ms) {
    if (!ms) return null;
    const d = new Date(ms);
    return isNaN(d.getTime()) ? null : d.toISOString();
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

// ⚠️ Must stay identical to computeEventStatus() in sync-events.js
function computeEventStatus(eventDate) {
  const now = new Date();
  const eventStart = new Date(eventDate + "T00:00:00");
  const eventEnd = new Date(eventStart);
  eventEnd.setDate(eventEnd.getDate() + 1);
  eventEnd.setHours(4, 0, 0, 0);

  return now > eventEnd ? "PAST" : "LIVE";
}

function convertToMontrealDate(utcDateString) {
  if (!utcDateString) return null;
  const utcDate = new Date(utcDateString);
  return utcDate.toLocaleDateString("en-CA", { timeZone: "America/Montreal", year: "numeric", month: "2-digit", day: "2-digit" });
}

function transformEventForDB(tixrEvent, { includeStatus = false } = {}) {
  const eventDate = convertToMontrealDate(tixrEvent.start_date);
  const row = {
    event_id: parseInt(tixrEvent.id),
    event_name: tixrEvent.name,
    event_date: eventDate,
    event_flyer: tixrEvent.flyer_url || tixrEvent.mobile_image_url || null,
    event_updated: new Date().toISOString(),
    // ⚠️ NOTE: event_status is excluded for EXISTING events so we don't overwrite the
    // daily sync's LIVE/PAST logic. It IS set on brand-new inserts (see /webhook/event).
  };

  if (includeStatus) {
    row.event_status = computeEventStatus(eventDate);
  }

  return row;
}

// ==================== ORDER PROJECTION (Studio API -> new tables) ====================
//
// Money comes from the Studio API order, NEVER from the webhook payload
// (webhooks carry no prices). order_source comes from the webhook when present.

function transformOrderForDB(fullOrder, webhookBody) {
    const geo = fullOrder.geo_info || {};
    return {
        order_id:              fullOrder.order_id,
        event_id:              fullOrder.event_id,
        user_id:               fullOrder.user_id != null ? String(fullOrder.user_id) : null,

        status:                fullOrder.status || null,
        order_type:            fullOrder.type || webhookBody?.order_type || null,
        order_source:          webhookBody?.order_source || fullOrder.order_source || null,
        fulfillment_path:      fullOrder.fulfillment_path || webhookBody?.order_fulfillment_path || null,
        purchase_date:         msToIso(fullOrder.purchase_date),
        refund_date:           msToIso(fullOrder.refund_date),
        cancellation_date:     msToIso(fullOrder.cancellation_date),

        currency:              fullOrder.currency || null,
        exchange_rate:         fullOrder.exchange_rate ?? null,
        gross_sales:           fullOrder.gross_sales ?? null,
        net:                   fullOrder.net ?? null,
        total:                 fullOrder.total ?? null,
        taxes:                 fullOrder.taxes ?? null,
        fees:                  fullOrder.fees ?? null,
        credit_card_fees:      fullOrder.credit_card_fees ?? null,
        delivery_fees:         fullOrder.delivery_fees ?? null,
        discount:              fullOrder.discount ?? null,

        first_name:            capitalize(fullOrder.first_name) || null,
        last_name:             capitalize(fullOrder.lastname) || null,
        email:                 fullOrder.email || null,
        opt_in:                fullOrder.opt_in ?? null,
        opt_in_date:           msToIso(fullOrder.opt_in_date),
        geo_city:              geo.city || null,
        geo_state:             geo.state || null,
        geo_country:           geo.country_code || null,
        geo_postal:            geo.postal_code || null,
        geo_lat:               geo.latitude ?? null,
        geo_lng:               geo.longitude ?? null,

        ref_id:                fullOrder.ref_id || null,
        ref_type:              fullOrder.ref_type || null,
        referrer:              fullOrder.referrer || null,
        seller_id:             fullOrder.seller_id || null,
        user_agent_type:       fullOrder.user_agent_type || null,
        card_type:             fullOrder.card_type || null,

        last_transaction_type: webhookBody?.transaction_type || null,
        webhook_updated_at:    new Date().toISOString(),
        api_synced_at:         new Date().toISOString(),
        updated_at:            new Date().toISOString(),
    };
}

function transformOrderItems(fullOrder) {
    return (fullOrder.sale_items || []).map(item => ({
        order_id:     fullOrder.order_id,
        sale_id:      item.sale_id,
        event_id:     fullOrder.event_id,
        tier_id:      item.tier_id ?? null,
        name:         item.name || null,
        category:     item.category || null,
        quantity:     item.quantity || 0,
        net:          item.net ?? null,
        total:        item.total ?? null,
        tax:          item.tax ?? null,
        group_fee:    item.group_fee ?? null,
        delivery_fee: item.delivery_fee ?? null,
        hold_id:      item.hold_id ?? null,
        hold_label:   item.hold_label || null,
    }));
}

function transformTickets(fullOrder) {
    const rows = [];
    for (const item of (fullOrder.sale_items || [])) {
        for (const ticket of (item.tickets || [])) {
            if (!ticket.serial_number) continue;
            rows.push({
                serial_number:     String(ticket.serial_number),
                order_id:          fullOrder.order_id,
                sale_id:           item.sale_id ?? null,
                event_id:          fullOrder.event_id,
                status:            ticket.status || null,
                holder_first_name: capitalize(ticket.first_name) || null,
                holder_last_name:  capitalize(ticket.lastname) || null,
                updated_at:        new Date().toISOString(),
            });
        }
    }
    return rows;
}

/**
 * Fetches the full order from the Studio API and projects it into
 * events_orders / events_order_items / events_tickets / events_users / tixr_sync_state.
 * Used by the order webhook (async) — and reusable by backfill later.
 */
async function processOrder(orderId, webhookBody) {
    const fullOrder = await fetchTixrOrderById(orderId);
    if (!fullOrder || !fullOrder.order_id) {
        throw new Error(`Order ${orderId} not found in Tixr`);
    }

    // 1. Order row (money lives here, once)
    const orderRow = transformOrderForDB(fullOrder, webhookBody);
    const { error: orderErr } = await supabase.from('events_orders').upsert(orderRow, { onConflict: 'order_id' });
    if (orderErr) throw new Error(`events_orders upsert failed: ${orderErr.message}`);

    // 2. Items — clean slate per order so removed/cancelled items don't linger
    const items = transformOrderItems(fullOrder);
    await supabase.from('events_order_items').delete().eq('order_id', fullOrder.order_id);
    if (items.length > 0) {
        const { error: itemErr } = await supabase.from('events_order_items').insert(items);
        if (itemErr) throw new Error(`events_order_items insert failed: ${itemErr.message}`);
    }

    // 3. Tickets (per serial — live door updates land here later)
    const tickets = transformTickets(fullOrder);
    if (tickets.length > 0) {
        const { error: ticketErr } = await supabase.from('events_tickets').upsert(tickets, { onConflict: 'serial_number' });
        if (ticketErr) throw new Error(`events_tickets upsert failed: ${ticketErr.message}`);
    }

    // 4. Keep the enriched user profile fresh (events_users is kept long-term)
    if (fullOrder.user_id) {
        const userIdStr = String(fullOrder.user_id);
        const { data: existingUser } = await supabase.from('events_users').select('event_ids').eq('user_id', userIdStr).maybeSingle();
        const existingEvents = existingUser?.event_ids || [];
        const updatedEvents = Array.from(new Set([...existingEvents, fullOrder.event_id]));

        const geo = fullOrder.geo_info || {};
        const { error: userErr } = await supabase.from('events_users').upsert({
            user_id:           userIdStr,
            user_first_name:   capitalize(fullOrder.first_name),
            user_last_name:    capitalize(fullOrder.lastname),
            user_mail:         fullOrder.email,
            user_opt_in:       fullOrder.opt_in,
            user_city:         geo.city,
            user_state:        geo.state,
            user_country:      geo.country_code,
            user_postal:       geo.postal_code,
            event_ids:         updatedEvents,
            user_last_purchase: msToIso(fullOrder.purchase_date),
        }, { onConflict: 'user_id' });
        if (userErr) console.error(`  ⚠️ events_users upsert failed: ${userErr.message}`);
    }

    // 5. Sync bookkeeping for this event
    const { error: stateErr } = await supabase.from('tixr_sync_state').upsert({
        event_id:        fullOrder.event_id,
        last_webhook_at: new Date().toISOString(),
        updated_at:      new Date().toISOString(),
    }, { onConflict: 'event_id' });
    if (stateErr) console.error(`  ⚠️ tixr_sync_state upsert failed: ${stateErr.message}`);

    console.log(`  💾 Order ${fullOrder.order_id} projected: ${items.length} items, ${tickets.length} tickets.`);
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
      console.log(`  ⏭️  Event ${event_id} was unpublished/removed on Tixr — ignoring, no DB change made.`);
      return res.status(200).json({ success: true, message: 'Event unpublish/removed ignored, DB untouched' });
    }

    const fullEventData = await fetchTixrEventById(event_id);
    if (!fullEventData) {
      return res.status(404).json({ error: 'Event not found in Tixr' });
    }

    // Only set event_status on brand-new inserts — existing rows keep the
    // status managed by the daily sync-events.js job.
    const { data: existingEvent, error: existsError } = await supabase
      .from('events')
      .select('event_id')
      .eq('event_id', parseInt(fullEventData.id))
      .maybeSingle();

    if (existsError) {
      console.error(`  ❌ Failed to check existing event:`, existsError.message);
      return res.status(500).json({ error: 'Internal server error' });
    }

    const isNewEvent = !existingEvent;
    const eventForDB = transformEventForDB(fullEventData, { includeStatus: isNewEvent });
    await supabase.from('events').upsert(eventForDB, { onConflict: 'event_id' });

    if (isNewEvent) {
      console.log(`  🆕 New event inserted with status ${eventForDB.event_status}: ${eventForDB.event_name} (ID: ${event_id})`);
    } else {
      console.log(`  ✅ Event ${eventForDB.event_name} (ID: ${event_id}) successfully synced (status untouched).`);
    }
    res.status(200).json({ success: true, message: 'Event synced' });

  } catch (error) {
    console.error(`  ❌ Error processing event webhook:`, error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/webhook/order', checkWebhookSecurity, async (req, res) => {
    const { order_id, event_id, transaction_type } = req.body || {};
    console.log(`  Processing ORDER webhook: Transaction=${transaction_type}, OrderID=${order_id}`);

    if (!order_id) {
        return res.status(200).json({ success: true, message: 'No order_id, ignored' });
    }

    // 1. Log the raw payload FIRST (append-only). Idempotency: an exact
    //    duplicate delivery hits the unique index and is acknowledged silently.
    const idempotencyKey = crypto.createHash('sha256')
        .update(`${order_id}:${transaction_type || ''}:${JSON.stringify(req.body)}`)
        .digest('hex');

    const { data: logged, error: logErr } = await supabase
        .from('tixr_webhook_events')
        .insert({
            entity:           'order',
            event_id:         event_id ?? null,
            order_id:         String(order_id),
            transaction_type: transaction_type || null,
            idempotency_key:  idempotencyKey,
            payload:          req.body,
        })
        .select('id')
        .single();

    if (logErr) {
        if (logErr.code === '23505') {
            console.log(`  🔁 Duplicate delivery for order ${order_id} — acknowledged, not reprocessed.`);
            return res.status(200).json({ success: true, message: 'Duplicate ignored' });
        }
        console.error(`  ❌ Failed to log raw webhook:`, logErr.message);
        // Still process — losing the raw log is bad, dropping the order is worse.
    }

    // 2. Acknowledge IMMEDIATELY — Tixr timeouts cause retries or drops.
    res.status(200).json({ success: true, message: 'Received' });

    // 3. Project asynchronously (fetch full order from Studio API — money
    //    comes from there, never from the webhook payload).
    const logId = logged?.id;
    setImmediate(async () => {
        try {
            await processOrder(order_id, req.body);
            if (logId) {
                await supabase.from('tixr_webhook_events')
                    .update({ processed_at: new Date().toISOString() })
                    .eq('id', logId);
            }
        } catch (error) {
            console.error(`  ❌ Error processing order ${order_id}:`, error.message);
            if (logId) {
                await supabase.from('tixr_webhook_events')
                    .update({ process_error: error.message })
                    .eq('id', logId);
            }
        }
    });
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
