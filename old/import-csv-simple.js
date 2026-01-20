const fs = require('fs');
const csv = require('csv-parser');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

console.log('📥 Starting CSV Import...\n');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const CSV_FILE = './fans-2025-10-15T20-30-40.325Z.csv';

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const records = [];

console.log('📂 Reading CSV file...');

fs.createReadStream(CSV_FILE)
  .pipe(csv())
  .on('data', (row) => {
    records.push(row);
  })
  .on('end', async () => {
    console.log(`✅ Parsed ${records.length} rows\n`);
    
    console.log('💾 Uploading to Supabase in batches...');
    const BATCH_SIZE = 1000;
    
    for (let i = 0; i < records.length; i += BATCH_SIZE) {
      const batch = records.slice(i, i + BATCH_SIZE);
      
      const { error } = await supabase
        .from('temp_audience_republic')
        .insert(batch);
      
      if (error) {
        console.error(`❌ Error in batch ${Math.floor(i/BATCH_SIZE) + 1}:`, error.message);
        process.exit(1);
      }
      
      console.log(`   ✅ Batch ${Math.floor(i/BATCH_SIZE) + 1}/${Math.ceil(records.length/BATCH_SIZE)} uploaded`);
    }
    
    console.log(`\n✨ Import complete! ${records.length} rows imported.`);
  });