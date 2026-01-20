import axios from 'axios';

// --- Configuration ---
const BASE_URL = 'https://api.posveloce.com';
const USER_EMAIL = 'charles@produkt.ca';

// ⬇️ PUT YOUR PASSWORD HERE ⬇️
const USER_PASSWORD = 'Aa95dr33'; 

const REPORT_DATE_START = '2025-12-12T00:00:00Z'; 
const REPORT_DATE_END = '2025-12-13T00:00:00Z';   

// ⬇️ DEFINE CATEGORIES TO EXCLUDE HERE (Case insensitive) ⬇️
// Example: If your food category is named "Nourriture" or "Food" or "Kitchen"
const EXCLUDED_CATEGORIES = ['FOOD', 'NOURRITURE', 'KITCHEN', 'REPAS', 'SNACKS'];

// --- Interfaces ---
interface AuthResponse {
  token: string;
}

interface Division {
  name: string;
}

interface ProductDeprecated {
  nameMain: string;
}

interface SalesProduct {
  product: ProductDeprecated;
  division?: Division; // This holds the "Category" name
  salesAmount: number; 
  count: number;       
}

// --- Main Logic ---
async function runReport() {
  try {
    console.log(`\n--- Veloce Sales Report (Excluding Food) ---`);
    console.log(`Target Date: ${REPORT_DATE_START.split('T')[0]}`);
    console.log(`Excluding Categories containing: ${EXCLUDED_CATEGORIES.join(', ')}\n`);

    // 1. Authentication
    console.log('Authenticating...');
    const authRes = await axios.post<AuthResponse>(`${BASE_URL}/users/authenticate`, {
      email: USER_EMAIL,
      password: USER_PASSWORD 
    });
    const token = authRes.data.token;
    console.log('Authentication successful.');

    // 2. Fetching Sales Data
    console.log('Fetching detailed sales data...');
    
    let allSales: SalesProduct[] = [];
    let offset = 0;
    const limit = 250;
    let fetchMore = true;

    while (fetchMore) {
      // We set groupByName: false so we receive the 'division' (Category) info
      const response = await axios.get<SalesProduct[]>(`${BASE_URL}/sales/products`, {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        params: {
          from: REPORT_DATE_START,
          to: REPORT_DATE_END,
          groupByName: false, // Important: Must be false to get Category/Division info
          limit: limit,
          offset: offset,
          useAccountingTime: true
        }
      });

      const data = response.data;
      allSales = allSales.concat(data);

      if (data.length < limit) {
        fetchMore = false;
      } else {
        offset += limit;
      }
    }

    // 3. Process, Filter & Aggregate Data
    // We use a Map to aggregate items by name manually
    const aggregatedItems = new Map<string, { Quantity: number, Revenue: number }>();

    for (const item of allSales) {
      const itemName = item.product?.nameMain || "Unknown Item";
      const categoryName = item.division?.name?.toUpperCase() || "";

      // FILTER: Check if the category matches any excluded keywords
      const isExcluded = EXCLUDED_CATEGORIES.some(excluded => categoryName.includes(excluded));
      
      if (isExcluded) {
        continue; // Skip this item
      }

      // AGGREGATE: Add to existing total or create new entry
      if (aggregatedItems.has(itemName)) {
        const existing = aggregatedItems.get(itemName)!;
        existing.Quantity += item.count;
        existing.Revenue += item.salesAmount;
      } else {
        aggregatedItems.set(itemName, {
          Quantity: item.count,
          Revenue: item.salesAmount
        });
      }
    }

    // Convert Map back to Array for sorting/display
    const formattedReport = Array.from(aggregatedItems.entries()).map(([name, data]) => ({
      Item: name,
      Quantity: data.Quantity,
      Revenue: data.Revenue
    })).sort((a, b) => a.Item.localeCompare(b.Item)); // Sort A-Z

    // 4. Calculate Grand Total
    const grandTotal = formattedReport.reduce((sum, item) => sum + item.Revenue, 0);

    // 5. Output
    console.log(`\n--- Sales Report (Beverage Only) ---`);
    
    const displayTable = formattedReport.map(r => ({
      ...r,
      Revenue: `$${r.Revenue.toFixed(2)}`
    }));
    
    console.table(displayTable);

    console.log(`\n=========================================`);
    console.log(`GRAND TOTAL (Excluding Food): $${grandTotal.toFixed(2)}`);
    console.log(`=========================================\n`);

  } catch (error: any) {
    if (axios.isAxiosError(error)) {
      console.error('API Error:', error.response?.status, error.response?.data);
      if (error.response?.status === 401) {
        console.error('>> Please check your password in the USER_PASSWORD field.');
      }
    } else {
      console.error('An unexpected error occurred:', error.message);
    }
  }
}

runReport();