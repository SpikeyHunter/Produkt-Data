const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const crypto = require('crypto');
require('dotenv').config();

console.log('🚀 Starting Tixr Attendance Webhook Server...');

// --- CONFIGURATION ---
const PORT = process.env.PORT || 3001;
const {
  SUPABASE_URL,
  SUPABASE_KEY,
  TIXR_CPK,
  TIXR_SECRET_KEY
} = process.env;

if (!SUPABASE_URL || !SUPABASE_KEY || !TIXR_CPK || !TIXR_SECRET_KEY) {
  console.error('❌ Missing required environment variables.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const app = express();
app.use(express.json());

// ==================== HELPER FUNCTIONS ====================

function buildHash(basePath, paramsObj) {
  const paramsSorted = Object.keys(paramsObj)
    .sort()
    .map(k => `${k}=${encodeURIComponent(paramsObj[k])}`)
    .join('&');
  const hashString = `${basePath}?${paramsSorted}`;
  return crypto
    .createHmac('sha256', TIXR_SECRET_KEY)
    .update(hashString)
    .digest('hex');
}

// Fetch attendance transaction history for a serial
async function getAttendanceTransactions(eventId, serialNumber) {
  const basePath = `/v1/events/${eventId}/attendance/${serialNumber}/transactions`;
  const t = Date.now();
  const params = { cpk: TIXR_CPK, t };
  
  const paramsSorted = Object.keys(params)
    .sort()
    .map(k => `${k}=${encodeURIComponent(params[k])}`)
    .join('&');
  const hash = buildHash(basePath, params);
  const url = `https://studio.tixr.com${basePath}?${paramsSorted}&hash=${hash}`;

  try {
    const { data, status } = await axios.get(url, { timeout: 10000 });
    if (status === 404) return [];
    if (!Array.isArray(data)) return [];
    return data;
  } catch (error) {
    console.error(`  ⚠️  Error fetching transactions for ${serialNumber}:`, error.message);
    return [];
  }
}

// Process transactions to determine current state and count
function processTransactions(transactions) {
  if (!transactions || transactions.length === 0) {
    return {
      state: 'IN_HAND',
      checkInCount: 0,
      firstCheckInTime: null
    };
  }

  const sorted = [...transactions].sort((a, b) => (a.date || 0) - (b.date || 0));
  
  let currentState = 'IN_HAND';
  let checkInCount = 0;
  let firstCheckInTime = null;

  for (const transaction of sorted) {
    const action = transaction.action;
    
    if (action === 'CHECKED_IN' || action === 'REENTERED') {
      currentState = 'CHECKED_IN';
      checkInCount++;
      if (!firstCheckInTime) {
        firstCheckInTime = transaction.date;
      }
    } else if (action === 'CHECKED_OUT') {
      currentState = 'CHECKED_OUT';
    } else if (action === 'VOID') {
      currentState = 'VOID';
    }
  }

  return {
    state: currentState,
    checkInCount,
    firstCheckInTime: firstCheckInTime ? new Date(firstCheckInTime).toISOString() : null
  };
}

// ==================== WEBHOOK ENDPOINT ====================

app.post('/webhook/ticket', async (req, res) => {
  const clientIp = req.headers['x-forwarded-for']?.split(',')[0] || req.connection.remoteAddress;
  console.log(`\n🔥 Received ticket webhook from IP: ${clientIp}`);
  
  const { event_id, serial_id, action } = req.body;
  
  console.log(`  Event: ${event_id}, Serial: ${serial_id}, Action: ${action}`);

  if (!event_id || !serial_id) {
    console.log('  ⚠️  Missing event_id or serial_id, ignoring');
    return res.status(200).json({ success: true, message: 'Incomplete data, ignored' });
  }

  try {
    // Find all orders containing this serial
    const { data: orders, error: fetchError } = await supabase
      .from('events_orders')
      .select('order_id, event_id, order_serials, order_checkin_state, order_checkin_count, order_checkin_time')
      .eq('event_id', event_id)
      .like('order_serials', `%${serial_id}%`);

    if (fetchError) {
      console.error('  ❌ Database error:', fetchError.message);
      return res.status(500).json({ error: 'Database error' });
    }

    if (!orders || orders.length === 0) {
      console.log('  ℹ️  No orders found with this serial');
      return res.status(200).json({ success: true, message: 'Serial not found in orders' });
    }

    console.log(`  📋 Found ${orders.length} order(s) to update`);

    // Process each order
    for (const order of orders) {
      const serials = order.order_serials.split(',').map(s => s.trim()).filter(Boolean);
      
      // Fetch fresh transaction data for all serials in this order
      const serialResults = await Promise.all(
        serials.map(async (serial) => {
          const transactions = await getAttendanceTransactions(event_id, serial);
          return processTransactions(transactions);
        })
      );

      // Build new state
      const newStates = serialResults.map(r => r.state).join(',');
      const newCheckinCount = serialResults.reduce((sum, r) => sum + r.checkInCount, 0);
      
      let newFirstCheckinTime = null;
      const validDates = serialResults
        .map(r => r.firstCheckInTime)
        .filter(Boolean)
        .map(d => new Date(d).getTime());
      
      if (validDates.length > 0) {
        newFirstCheckinTime = new Date(Math.min(...validDates)).toISOString();
      }

      // Update if changed
      const existingTime = order.order_checkin_time ? new Date(order.order_checkin_time).toISOString() : null;
      
      if (order.order_checkin_state !== newStates || 
          order.order_checkin_count !== newCheckinCount || 
          existingTime !== newFirstCheckinTime) {
        
        const { error: updateError } = await supabase
          .from('events_orders')
          .update({
            order_checkin_state: newStates,
            order_checkin_count: newCheckinCount,
            order_checkin_time: newFirstCheckinTime
          })
          .eq('order_id', order.order_id);

        if (updateError) {
          console.error(`  ❌ Error updating order ${order.order_id}:`, updateError.message);
        } else {
          console.log(`  ✅ Updated order ${order.order_id}: ${newStates} (count: ${newCheckinCount})`);
        }
      } else {
        console.log(`  ℹ️  Order ${order.order_id} already up-to-date`);
      }
    }

    res.status(200).json({ success: true, message: 'Attendance updated' });

  } catch (error) {
    console.error('  ❌ Error processing webhook:', error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ==================== SERVER BOILERPLATE ====================

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy', service: 'attendance-webhook' });
});

app.get('/', (req, res) => {
  res.status(200).json({ 
    service: 'Tixr Attendance Webhook Server',
    endpoints: {
      '/webhook/ticket': 'POST - Handle ticket check-in/out events',
      '/health': 'GET - Health check'
    }
  });
});

app.use((req, res) => {
  res.status(404).json({ error: 'Endpoint not found' });
});

const server = app.listen(PORT, () => {
  console.log('═══════════════════════════════════════════');
  console.log('     TIXR ATTENDANCE WEBHOOK SERVER');
  console.log(`🚀 Server running on port ${PORT}`);
  console.log('═══════════════════════════════════════════');
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM signal received: closing HTTP server');
  server.close(() => {
    console.log('HTTP server closed');
  });
});