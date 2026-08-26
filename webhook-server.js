require('dotenv').config();   // must run BEFORE lib requires (they read env at load)
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const crypto = require('crypto');
const { projectOrder } = require('./lib/order-projection');
const { syncEventOrders, recordSyncError } = require('./lib/event-orders-sync');
const { runIgCycle } = require('./lib/ig-sync');

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

// Unguessable path segment for webhook URLs (Tixr has no HMAC on webhooks, so
// the URL is the secret). Derived from TIXR_SECRET_KEY — identical everywhere
// with no extra env var. Override with WEBHOOK_TOKEN if ever needed.
const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN ||
  crypto.createHmac('sha256', TIXR_SECRET_KEY).update('produkt-webhook-path').digest('hex').slice(0, 24);

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

// ==================== SECURITY ====================

function checkWebhookSecurity(req, res, next) {
  const clientIp = req.headers['x-forwarded-for']?.split(',')[0] || req.connection.remoteAddress;
  console.log(`\n📥 Received webhook from IP: ${clientIp}`);
  next();
}

/** Tokened routes only: 404 on a wrong token (looks like a dead URL). */
function tokenGuard(req, res, next) {
  if (req.params.token !== WEBHOOK_TOKEN) {
    console.log('  🚫 Webhook with invalid path token — rejected.');
    return res.status(404).json({ error: 'Endpoint not found' });
  }
  next();
}

/** Payload must belong to our group. Wrong group -> acknowledged + ignored. */
function isWrongGroup(body) {
  return body?.group_id != null && String(body.group_id) !== String(TIXR_GROUP_ID);
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
    // daily sync's LIVE/PAST logic. It IS set on brand-new inserts (see event handler).
  };

  if (includeStatus) {
    row.event_status = computeEventStatus(eventDate);
  }

  return row;
}

// ==================== ORDER PROCESSING ====================
//
// Projection logic (Studio API order -> events_orders / events_order_items /
// events_tickets / events_users) lives in lib/order-projection.js, shared
// with backfill-orders.js so the two paths can never drift apart.

/**
 * Fetches the full order from the Studio API, projects it, and stamps
 * tixr_sync_state. Money comes from the API, never the webhook payload.
 */
async function processOrder(orderId, webhookBody) {
    const fullOrder = await fetchTixrOrderById(orderId);
    if (!fullOrder || !fullOrder.order_id) {
        throw new Error(`Order ${orderId} not found in Tixr`);
    }

    const { items, tickets } = await projectOrder(supabase, fullOrder, webhookBody);

    const { error: stateErr } = await supabase.from('tixr_sync_state').upsert({
        event_id:        fullOrder.event_id,
        last_webhook_at: new Date().toISOString(),
        updated_at:      new Date().toISOString(),
    }, { onConflict: 'event_id' });
    if (stateErr) console.error(`  ⚠️ tixr_sync_state upsert failed: ${stateErr.message}`);

    console.log(`  💾 Order ${fullOrder.order_id} projected: ${items} items, ${tickets} tickets.`);
}

// ==================== RECONCILIATION SWEEP (Phase 3) ====================
//
// Every 15 minutes, in-process (no separate Render service):
//   · every LIVE event gets a full order re-sync from the Studio API
//     (idempotent upserts — catches missed webhooks, refunds/cancellations on
//     old orders, AND the zero-orders case, all in one code path)
//   · plus any event manually flagged backfill_requested in tixr_sync_state
//     (the Mac app's "backfill this past event" switch)
// Cursors/state live in the DB, so redeploys are harmless.

const RECONCILE_INTERVAL_MS = parseInt(process.env.RECONCILE_INTERVAL_MS || '', 10) || 15 * 60 * 1000;
const RECONCILE_ENABLED = process.env.RECONCILE_ENABLED !== 'false';

let sweepRunning = false;

async function reconcileSweep() {
  if (sweepRunning) {
    console.log('⏭️  Previous reconciliation sweep still running — skipping this cycle.');
    return;
  }
  sweepRunning = true;
  const started = Date.now();

  try {
    // LIVE events (real Tixr events only — customs have event_id < 10000 / is_custom)
    const { data: liveEvents, error: liveErr } = await supabase
      .from('events')
      .select('event_id, is_custom')
      .eq('event_status', 'LIVE');
    if (liveErr) throw new Error(`events read failed: ${liveErr.message}`);

    // Manually requested backfills (past events)
    const { data: requested, error: reqErr } = await supabase
      .from('tixr_sync_state')
      .select('event_id')
      .eq('backfill_requested', true);
    if (reqErr) throw new Error(`tixr_sync_state read failed: ${reqErr.message}`);

    const targets = new Set();
    for (const e of (liveEvents || [])) {
      if (e.event_id >= 10000 && e.is_custom !== true) targets.add(e.event_id);
    }
    for (const r of (requested || [])) targets.add(r.event_id);

    if (targets.size === 0) {
      console.log('🔄 Reconciliation sweep: nothing to sync.');
      return;
    }

    console.log(`🔄 Reconciliation sweep: ${targets.size} event(s)...`);
    let ok = 0, failed = 0, totalOrders = 0;

    for (const eventId of targets) {
      try {
        const stats = await syncEventOrders(supabase, eventId);
        totalOrders += stats.orders;
        ok++;
      } catch (err) {
        failed++;
        console.error(`  ❌ Sweep failed for event ${eventId}: ${err.message}`);
        await recordSyncError(supabase, eventId, err.message);
      }
    }

    const secs = ((Date.now() - started) / 1000).toFixed(1);
    console.log(`✅ Sweep done in ${secs}s — ${ok} event(s) OK (${totalOrders} orders confirmed), ${failed} failed.`);
  } catch (err) {
    console.error('❌ Reconciliation sweep error:', err.message);
  } finally {
    sweepRunning = false;
  }
}

if (RECONCILE_ENABLED) {
  setInterval(reconcileSweep, RECONCILE_INTERVAL_MS);
  setTimeout(reconcileSweep, 60 * 1000);   // first sweep 1 min after boot
  console.log(`⏱️  Reconciliation sweep armed: every ${Math.round(RECONCILE_INTERVAL_MS / 60000)} min (first run in 1 min).`);
}

// ==================== INSTAGRAM SYNC (Phases 5+6) ====================
//
// Every 5 minutes, in-process: detect new posts/reels/stories, capture their
// preview media (CDN URLs expire in hours), refresh insights on the decaying
// ladder, final-capture stories at 23h, daily account snapshot.
// Meta has NO webhook for own posts — polling is the only way.

const IG_SYNC_INTERVAL_MS = parseInt(process.env.IG_SYNC_INTERVAL_MS || '', 10) || 5 * 60 * 1000;
const IG_SYNC_ENABLED = process.env.IG_SYNC_ENABLED !== 'false'
  && !!process.env.META_ACCESS_TOKEN && !!process.env.IG_USER_ID;

let igCycleRunning = false;

async function igSyncTick() {
  if (igCycleRunning) {
    console.log('⏭️  Previous IG cycle still running — skipping.');
    return;
  }
  igCycleRunning = true;
  try {
    const result = await runIgCycle(supabase);
    if (!result.skipped && (result.detected > 0 || result.refreshed > 0)) {
      console.log(`📸 IG cycle: ${result.detected} new, ${result.refreshed} refreshed.`);
    }
  } catch (err) {
    console.error('❌ IG cycle error:', err.message);
  } finally {
    igCycleRunning = false;
  }
}

if (IG_SYNC_ENABLED) {
  setInterval(igSyncTick, IG_SYNC_INTERVAL_MS);
  setTimeout(igSyncTick, 90 * 1000);   // first cycle 90s after boot
  console.log(`📸 IG sync armed: every ${Math.round(IG_SYNC_INTERVAL_MS / 60000)} min (first run in 90s).`);
} else {
  console.log('📸 IG sync disabled (missing META_ACCESS_TOKEN/IG_USER_ID or IG_SYNC_ENABLED=false).');
}

// ==================== WEBHOOK HANDLERS ====================

async function handleEventWebhook(req, res) {
  const { event_id, action } = req.body;
  console.log(`  Processing EVENT webhook: Action=${action || 'UPDATE'}, EventID=${event_id}`);

  if (isWrongGroup(req.body)) {
    console.log(`  🚫 Wrong group_id (${req.body.group_id}) — ignored.`);
    return res.status(200).json({ success: true, message: 'Ignored' });
  }
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
}

async function handleOrderWebhook(req, res) {
    const { order_id, event_id, transaction_type } = req.body || {};
    console.log(`  Processing ORDER webhook: Transaction=${transaction_type}, OrderID=${order_id}`);

    if (isWrongGroup(req.body)) {
        console.log(`  🚫 Wrong group_id (${req.body.group_id}) — ignored.`);
        return res.status(200).json({ success: true, message: 'Ignored' });
    }
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
}

async function handleTicketWebhook(req, res) {
    const body = req.body || {};
    const serial = body.serial_id || body.serial_number;
    const action = (body.action || '').toUpperCase();
    console.log(`  Processing TICKET webhook: Action=${action}, Serial=${serial}, EventID=${body.event_id}`);

    if (isWrongGroup(body)) {
        console.log(`  🚫 Wrong group_id (${body.group_id}) — ignored.`);
        return res.status(200).json({ success: true, message: 'Ignored' });
    }
    if (!serial) {
        return res.status(200).json({ success: true, message: 'No serial, ignored' });
    }

    // 1. Raw log first (same pattern as orders)
    const idempotencyKey = crypto.createHash('sha256')
        .update(`ticket:${serial}:${action}:${JSON.stringify(body)}`)
        .digest('hex');

    const { data: logged, error: logErr } = await supabase
        .from('tixr_webhook_events')
        .insert({
            entity:           'ticket',
            event_id:         body.event_id ?? null,
            order_id:         body.order_id != null ? String(body.order_id) : null,
            transaction_type: action || null,
            idempotency_key:  idempotencyKey,
            payload:          body,
        })
        .select('id')
        .single();

    if (logErr) {
        if (logErr.code === '23505') {
            console.log(`  🔁 Duplicate ticket delivery for ${serial} — acknowledged, not reprocessed.`);
            return res.status(200).json({ success: true, message: 'Duplicate ignored' });
        }
        console.error(`  ❌ Failed to log raw ticket webhook:`, logErr.message);
    }

    // 2. Acknowledge immediately
    res.status(200).json({ success: true, message: 'Received' });

    // 3. Update events_tickets asynchronously. checkin_state is webhook-owned;
    //    the sweep owns `status` — the two never clobber each other.
    const logId = logged?.id;
    setImmediate(async () => {
        try {
            const row = {
                serial_number: String(serial),
                checkin_state: action || null,
                updated_at:    new Date().toISOString(),
            };
            // Insert-safety: a scan can arrive before the order sync creates the
            // row, so carry the identifiers the payload gives us.
            if (body.order_id != null) row.order_id = String(body.order_id);
            if (body.event_id != null) row.event_id = body.event_id;
            if (body.first_name) row.holder_first_name = capitalize(body.first_name);
            if (body.last_name)  row.holder_last_name  = capitalize(body.last_name);
            if (body.agent_email) row.checkin_agent   = body.agent_email;
            if (body.device_name) row.checkin_device  = body.device_name;
            if (body.scanner)     row.checkin_scanner = body.scanner;
            if (action === 'CHECKED_IN') row.checkin_time = msToIso(body.date) || new Date().toISOString();

            const { error: upErr } = await supabase.from('events_tickets')
                .upsert(row, { onConflict: 'serial_number' });
            if (upErr) throw new Error(`events_tickets upsert failed: ${upErr.message}`);

            if (body.event_id != null) {
                await supabase.from('tixr_sync_state').upsert({
                    event_id:        body.event_id,
                    last_webhook_at: new Date().toISOString(),
                    updated_at:      new Date().toISOString(),
                }, { onConflict: 'event_id' });
            }

            console.log(`  💾 Ticket ${serial} -> ${action || '(no action)'} recorded.`);
            if (logId) {
                await supabase.from('tixr_webhook_events')
                    .update({ processed_at: new Date().toISOString() })
                    .eq('id', logId);
            }
        } catch (error) {
            console.error(`  ❌ Error processing ticket ${serial}:`, error.message);
            if (logId) {
                await supabase.from('tixr_webhook_events')
                    .update({ process_error: error.message })
                    .eq('id', logId);
            }
        }
    });
}

// ==================== ROUTES ====================

// Secured routes (the token in the path is the secret — configure these URLs
// as the channels in Tixr Studio):
app.post('/webhook/:token/order',  checkWebhookSecurity, tokenGuard, handleOrderWebhook);
app.post('/webhook/:token/ticket', checkWebhookSecurity, tokenGuard, handleTicketWebhook);
app.post('/webhook/:token/event',  checkWebhookSecurity, tokenGuard, handleEventWebhook);

// LEGACY route — the "Event Updates" policy channel may still point here.
// Once its channel is switched to /webhook/<token>/event, DELETE this block.
app.post('/webhook/event', checkWebhookSecurity, (req, res) => {
  console.log('  ⚠️ Legacy /webhook/event hit — switch the Event Updates channel to the tokened URL.');
  return handleEventWebhook(req, res);
});

// ==================== SERVER BOILERPLATE ====================

app.get('/health', (req, res) => res.status(200).json({ status: 'healthy' }));
app.get('/', (req, res) => res.status(200).json({ service: 'Tixr Webhook Listener is running' }));
app.use((req, res) => res.status(404).json({ error: 'Endpoint not found' }));

const server = app.listen(PORT, () => {
  console.log('═══════════════════════════════════════════');
  console.log('     TIXR ALL-IN-ONE WEBHOOK SERVER');
  console.log(`🚀 Server running on port ${PORT}, ready for webhooks.`);
  console.log('🔐 Tokened webhook paths armed (see repo docs for URLs).');
  console.log('═══════════════════════════════════════════');
});
