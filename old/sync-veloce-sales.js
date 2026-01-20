// sync-veloce-sales.js
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

// --- CONFIGURATION ---
const { SUPABASE_URL, SUPABASE_KEY } = process.env;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('❌ Missing required environment variables: SUPABASE_URL and SUPABASE_KEY.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const BASE_URL = 'https://api.posveloce.com';

async function getSalesSummary() {
  const input = process.argv[2];
  const debug = process.argv[3] === '--debug';
  
  if (!input) {
    console.error('❌ Please provide an event ID or date: node sync-veloce-sales.js 163107');
    console.error('   Or: node sync-veloce-sales.js 2025-11-06');
    console.error('   Add --debug to see all invoices: node sync-veloce-sales.js 2025-11-06 --debug');
    process.exit(1);
  }

  try {
    let dateStart, dateEnd, eventName;

    // Check if input is a date (YYYY-MM-DD format) or event ID
    if (/^\d{4}-\d{2}-\d{2}$/.test(input)) {
      dateStart = new Date(`${input}T00:00:00`);
      dateEnd = new Date(`${input}T23:59:59`);
      eventName = `Sales for ${input}`;
      console.log(`📅 Using date: ${input} (full day)`);
    } else {
      const eventId = input;
      const { data: event, error } = await supabase
        .from('events')
        .select('event_date, event_name')
        .eq('event_id', eventId)
        .single();

      if (error || !event) {
        console.error(`❌ Event ${eventId} not found:`, error?.message);
        process.exit(1);
      }

      const eventDate = event.event_date;
      eventName = event.event_name;
      
      dateStart = new Date(`${eventDate}T21:00:00`);
      dateEnd = new Date(dateStart);
      dateEnd.setDate(dateEnd.getDate() + 1);
      dateEnd.setHours(4, 0, 0, 0);
      
      console.log(`🎉 Found event: ${eventName}`);
    }

    // Authenticate
    const authResponse = await fetch(`${BASE_URL}/users/authenticate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email: 'charles@produkt.ca',
        password: 'Aa95dr33'
      })
    });

    if (!authResponse.ok) {
      throw new Error(`Veloce auth failed: ${authResponse.statusText}`);
    }

    const authData = await authResponse.json();
    const token = authData.token;

    // Fetch wider range
    const searchStart = new Date(dateStart);
    searchStart.setDate(searchStart.getDate() - 1);
    const searchEnd = new Date(dateEnd);
    searchEnd.setDate(searchEnd.getDate() + 1);

    const invoicesResponse = await fetch(
      `${BASE_URL}/invoices?from=${searchStart.toISOString()}&to=${searchEnd.toISOString()}`,
      { headers: { 'Authorization': `Bearer ${token}` } }
    );

    if (!invoicesResponse.ok) {
      throw new Error(`Veloce invoices API failed: ${invoicesResponse.statusText}`);
    }

    const allInvoices = await invoicesResponse.json();

    // Filter by accounting date
    const targetDate = input.match(/^\d{4}-\d{2}-\d{2}$/) ? input : dateStart.toISOString().split('T')[0];
    
    const invoices = allInvoices.filter(invoice => {
      if (invoice.status !== 0 || invoice.isCancelled) return false;
      const accountingDate = invoice.accountingTime.split('T')[0];
      return accountingDate === targetDate;
    });

    // Debug mode - show first 10 invoices with totals
    if (debug) {
      console.log('\n🔍 Debug: First 10 invoices:');
      invoices.slice(0, 10).forEach((inv, i) => {
        console.log(`\nInvoice ${i + 1}:`);
        console.log(`  Invoice #: ${inv.invoiceNumber}`);
        console.log(`  Invoice Time: ${inv.invoiceTime}`);
        console.log(`  Accounting Time: ${inv.accountingTime}`);
        console.log(`  Total: $${inv.total}`);
        console.log(`  SubTotal: $${inv.subTotal}`);
        console.log(`  Discount: $${inv.discount}`);
        console.log(`  Taxes: $${inv.taxesTotalAmount || 0}`);
        console.log(`  Status: ${inv.status} (Cancelled: ${inv.isCancelled})`);
      });
      console.log(`\n... and ${invoices.length - 10} more invoices\n`);
    }

    // The "Net Sales" in Veloce reports is the subTotal (after discounts)
    const netSales = invoices.reduce((sum, invoice) => sum + (invoice.subTotal || 0), 0);
    
    // Gross sales would be subTotal + discount (before discounts applied)
    const totalDiscounts = invoices.reduce((sum, invoice) => sum + (invoice.discount || 0), 0);
    const grossSales = netSales + totalDiscounts;

    console.log('═══════════════════════════════════════');
    console.log(`Event: ${eventName}`);
    console.log(`Gross sales: $${grossSales.toFixed(2)}`);
    console.log(`Discounts: -$${totalDiscounts.toFixed(2)}`);
    console.log(`Net sales: $${netSales.toFixed(2)}`);
    console.log(`Date start: ${dateStart.toLocaleString('en-CA', { timeZone: 'America/Montreal' })}`);
    console.log(`Date end: ${dateEnd.toLocaleString('en-CA', { timeZone: 'America/Montreal' })}`);
    console.log(`Invoices: ${invoices.length} (Checks: ${invoices.length}, Clients: ${invoices.reduce((sum, inv) => sum + inv.customers, 0)})`);
    console.log('═══════════════════════════════════════');

  } catch (err) {
    console.error('\n❌ A fatal error occurred:', err.message);
    process.exit(1);
  }
}

getSalesSummary();