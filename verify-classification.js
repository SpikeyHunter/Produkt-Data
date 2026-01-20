const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const { SUPABASE_URL, SUPABASE_KEY } = process.env;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

function classifyItem(item) {
  // 1. Normalize Inputs Inputs
  const name = (item.order_sales_item_name || "").toUpperCase();
  const category = (item.order_category || "").toUpperCase();
  const refType = (item.order_ref_type || "").toUpperCase(); // The "Source"
  const gross = parseFloat(item.order_gross || 0);
  
  const isFree = gross === 0;
  const isPaid = gross > 0;
  const isBackstage = refType === "BACKSTAGE";

  // --- 1. COAT CHECK (Category Override) ---
  // Rule: Category OUTLET and name contains Vestiaire/Coat Check
  if (category === 'OUTLET' && (name.includes('VESTIAIRE') || name.includes('COAT CHECK'))) {
    return "COATCHECK";
  }

  // --- 2. PHYSICAL & DOOR TICKETS (Backstage Only) ---
  if (isBackstage && isFree) {
    
    // Physical Tables
    if (category.includes('TABLE') || category.includes('SERVICE')) {
      if ((name.includes('BILLET PHYSIQUE') || name.includes('DOOR TABLE')) && name.includes('PREPAID') && !name.includes('PAY AT THE DOOR')) {
        return "PHYSICAL_TABLE_PREPAID";
      }
      if ((name.includes('BILLET PHYSIQUE') || name.includes('DOOR')) && (name.includes('PAY AT THE DOOR') || name.includes('BUY AT DOOR')) && !name.includes('PREPAID')) {
        return "PHYSICAL_TABLE_DOOR";
      }
    }

    // Physical Guestlist
    if (category.includes('GUEST') && (name.includes('GUESTLIST') || name.includes('GL'))) {
      return "PHYSICAL_GUESTLIST";
    }

    // Physical GA
    if (category === 'GA' && (name.includes('BILLET PHYSIQUE') || name.includes('HARD COPY') || name.includes('DOOR'))) {
      return "DOOR_GA";
    }

    // Physical VIP
    if (category === 'VIP' && (name.includes('BILLET PHYSIQUE') || name.includes('HARD COPY') || name.includes('DOOR'))) {
      return "DOOR_VIP";
    }
  }

  // --- 3. COMPS (Backstage Only) ---
  // Distinct from "Free" tickets bought online
  if (isBackstage && isFree) {
    if (category === 'GA' && (name.includes('COMP') || name.includes('INVITÉ') || name.includes('INVITE'))) {
      return "COMP_GA";
    }
    if (category === 'VIP' && (name.includes('COMP') || name.includes('INVITÉ') || name.includes('INVITE'))) {
      return "COMP_VIP";
    }
  }

  // --- 4. FREE TICKETS (Online/Public) ---
  // Bought by client for $0, OR RSVP, NOT Backstage
  if (!isBackstage && isFree) {
    if (category === 'GA' || category === 'GUEST') { // Added GUEST here as RSVP often uses Guest category
      return "FREE_GA";
    }
    if (category === 'VIP') {
      return "FREE_VIP";
    }
  }

  // --- 5. TABLES (Paid / RSVP) ---
  // Covers paid table reservations
  if (category.includes('TABLE') || category.includes('BOOTH') || name.includes('BANQUETTE')) {
    return "TABLES_RSVP";
  }

  // --- 6. STANDARD PAID TICKETS ---
  if (isPaid) {
    if (category === 'VIP') return "VIP_PAID";
    if (category === 'GA') return "GA_PAID";
    
    // Specific handling for your "Photo" / Meet & Greet items
    if (category === 'PHOTO') return "VIP_PAID"; 
  }

  // --- 7. CATCH-ALL FOR UNMATCHED ---
  // Helps us identify what didn't fit the rules above
  return "UNCATEGORIZED";
}

async function runValidation() {
  console.log("🚀 Running Classification Validation...");
  
  const uniqueTypes = {};
  let page = 0;
  const pageSize = 1000;
  let hasMore = true;
  let totalScanned = 0;

  while (hasMore) {
    const { data: orders, error } = await supabase
      .from('events_orders')
      .select('order_sales_item_name, order_category, order_gross, order_ref_type')
      .range(page * pageSize, (page + 1) * pageSize - 1);

    if (error || !orders || orders.length === 0) {
      hasMore = false;
      break;
    }

    orders.forEach(o => {
      const name = (o.order_sales_item_name || "[NO NAME]").trim();
      const category = (o.order_category || "[NO CAT]").trim();
      const ref = (o.order_ref_type || "[NO REF]").trim();
      const gross = parseFloat(o.order_gross || 0);
      const priceStatus = gross > 0 ? "PAID" : "FREE";

      // Group by unique signature
      const key = `${name}|${category}|${priceStatus}|${ref}`;
      
      if (!uniqueTypes[key]) {
        uniqueTypes[key] = {
          name, category, ref, gross, count: 0
        };
      }
      uniqueTypes[key].count++;
    });

    totalScanned += orders.length;
    process.stdout.write(`\r🔍 Scanned: ${totalScanned} items...`);
    page++;
  }

  console.log(`\n✅ Analysis Complete.`);
  
  const summary = {};
  const uncategorizedList = [];

  // Sort by count to show most impactful items first
  const sortedItems = Object.values(uniqueTypes).sort((a, b) => b.count - a.count);

  sortedItems.forEach(item => {
    const mockItem = {
      order_sales_item_name: item.name,
      order_category: item.category,
      order_ref_type: item.ref,
      order_gross: item.gross
    };

    const result = classifyItem(mockItem);
    summary[result] = (summary[result] || 0) + item.count;

    if (result === "UNCATEGORIZED") {
      uncategorizedList.push(item);
    }
  });

  console.log("\n📊 FINAL BREAKDOWN:");
  console.table(summary);

  if (uncategorizedList.length > 0) {
    console.log("\n⚠️  UNCATEGORIZED ITEMS (These did not fit your rules):");
    console.log("=================================================================================================================");
    console.log(`| ${"COUNT".padEnd(6)} | ${"PRICE".padEnd(6)} | ${"CATEGORY".padEnd(15)} | ${"SOURCE".padEnd(15)} | ${"NAME"}`);
    console.log("=================================================================================================================");
    uncategorizedList.forEach(u => {
      const priceStr = u.gross > 0 ? "PAID" : "FREE";
      console.log(`| ${u.count.toString().padEnd(6)} | ${priceStr.padEnd(6)} | ${u.category.substring(0,14).padEnd(15)} | ${u.ref.substring(0,14).padEnd(15)} | ${u.name}`);
    });
    console.log("=================================================================================================================");
  } else {
    console.log("\n✨ 100% MATCH! All items fit into your defined categories.");
  }
}

runValidation();