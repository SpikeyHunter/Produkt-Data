#!/usr/bin/env node
/**
 * exportcsv.js
 * --------------------------------------------------------------------------
 * Exports first name, last name and email of every person who placed an order
 * for a Tixr event, into a CSV file.
 *
 * API used: Tixr Studio API  ->  GET /v1/groups/{group_id}/events/{event_id}/orders
 * Auth:     HMAC-SHA256 hash over "<path>?<params sorted alphabetically>"
 *
 * Usage:
 *   node exportcsv.js                 # writes ./tixr_event_192190_buyers.csv
 *   node exportcsv.js --desktop       # writes the CSV to your Desktop
 *   node exportcsv.js --out ./data    # writes the CSV into ./data
 *
 * No external dependencies. Works on Node 14+ (uses built-in https + crypto).
 * --------------------------------------------------------------------------
 */

const crypto = require("crypto");
const https = require("https");
const fs = require("fs");
const path = require("path");
const os = require("os");

// ─────────────────────────────────────────────────────────────────────────
// CONFIG  (env vars override these defaults)
// ─────────────────────────────────────────────────────────────────────────
const TIXR_GROUP_ID = process.env.TIXR_GROUP_ID || "980";
const TIXR_CPK = process.env.TIXR_CPK || "si8rzJCwnGHC5lCPnbqM";
const TIXR_SECRET_KEY = process.env.TIXR_SECRET_KEY || "ii39nQ4ALcqAYEZ3UIyM";

const EVENT_ID = process.env.EVENT_ID || "192190";

const HOST = "studio.tixr.com";
const BASE_PATH = "/v1";
const PAGE_SIZE = 100; // Tixr default page size
const REQUEST_DELAY_MS = 150; // small pause between pages to be polite

// IMPORTANT: the orders endpoint defaults start_date to TODAY, which silently
// hides all historical orders. We pass an explicit wide range to get EVERYTHING
// (paid + free/comp tickets, all the way back). Format: YYYY-MM-DD (UTC).
const START_DATE = process.env.START_DATE || "2000-01-01";
const END_DATE = process.env.END_DATE || "2100-01-01";

// ─────────────────────────────────────────────────────────────────────────
// Build the signed query string for a given path + params.
//
// Per the Studio API docs:
//   - concatenate "<path>?<params>" where params are sorted alphabetically
//   - URL-encode every param value
//   - HMAC-SHA256 the resulting string with your SECRET key (hex output)
//   - append &hash=<hash>
// ─────────────────────────────────────────────────────────────────────────
function buildSignedPath(reqPath, params, secret) {
  const query = Object.keys(params)
    .sort()
    .map((k) => `${k}=${encodeURIComponent(params[k])}`)
    .join("&");

  const stringToHash = `${reqPath}?${query}`;
  const hash = crypto
    .createHmac("sha256", secret)
    .update(stringToHash)
    .digest("hex");

  return `${reqPath}?${query}&hash=${hash}`;
}

// Simple promise wrapper around https.get
function httpGetJson(fullPath) {
  return new Promise((resolve, reject) => {
    const options = { host: HOST, path: fullPath, method: "GET" };
    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          return reject(
            new Error(`HTTP ${res.statusCode}: ${data.slice(0, 500)}`)
          );
        }
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error(`Failed to parse JSON: ${data.slice(0, 500)}`));
        }
      });
    });
    req.on("error", reject);
    req.end();
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Fetch a single page of orders for the event
async function fetchOrdersPage(pageNumber) {
  const reqPath = `${BASE_PATH}/groups/${TIXR_GROUP_ID}/events/${EVENT_ID}/orders`;
  const params = {
    cpk: TIXR_CPK,
    start_date: START_DATE,
    end_date: END_DATE,
    page_number: pageNumber,
    page_size: PAGE_SIZE,
    t: Date.now(), // milliseconds, must be within 5 min of server time
  };
  const signedPath = buildSignedPath(reqPath, params, TIXR_SECRET_KEY);
  const result = await httpGetJson(signedPath);
  return Array.isArray(result) ? result : [];
}

// Escape a single CSV field per RFC 4180
function csvField(value) {
  const s = value == null ? "" : String(value);
  if (/[",\n\r]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

// Resolve the output file path based on CLI flags
function resolveOutputPath() {
  const args = process.argv.slice(2);
  const fileName = `tixr_event_${EVENT_ID}_buyers.csv`;

  if (args.includes("--desktop")) {
    return path.join(os.homedir(), "Desktop", fileName);
  }
  const outIdx = args.indexOf("--out");
  if (outIdx !== -1 && args[outIdx + 1]) {
    const dir = args[outIdx + 1];
    fs.mkdirSync(dir, { recursive: true });
    return path.join(dir, fileName);
  }
  // default: current working directory (the project folder)
  return path.join(process.cwd(), fileName);
}

async function main() {
  console.log(`Exporting buyers for event ${EVENT_ID} (group ${TIXR_GROUP_ID})...`);

  const seenEmails = new Set();
  const rows = []; // { first, last, email }
  let page = 1;
  let totalOrders = 0;

  while (true) {
    let orders;
    try {
      orders = await fetchOrdersPage(page);
    } catch (err) {
      console.error(`\nError fetching page ${page}: ${err.message}`);
      if (page === 1) process.exit(1); // hard fail on first page
      break; // otherwise keep what we have
    }

    if (orders.length === 0) break; // no more pages

    totalOrders += orders.length;

    for (const o of orders) {
      const email = (o.email || "").trim();
      const key = email.toLowerCase();
      // dedupe by email so each person appears once
      if (email && seenEmails.has(key)) continue;
      if (email) seenEmails.add(key);

      rows.push({
        first: o.first_name || "",
        last: o.lastname || "",
        email: email,
      });
    }

    process.stdout.write(
      `\rFetched page ${page} — ${totalOrders} orders, ${rows.length} unique buyers so far...`
    );

    page += 1;
    await sleep(REQUEST_DELAY_MS);
  }

  console.log(""); // newline after progress

  // Build CSV
  const header = ["first_name", "last_name", "email"];
  const lines = [header.join(",")];
  for (const r of rows) {
    lines.push([csvField(r.first), csvField(r.last), csvField(r.email)].join(","));
  }
  const csv = lines.join("\r\n") + "\r\n";

  const outPath = resolveOutputPath();
  fs.writeFileSync(outPath, csv, "utf8");

  console.log(`\nDone.`);
  console.log(`  Total orders processed : ${totalOrders}`);
  console.log(`  Unique buyers exported : ${rows.length}`);
  console.log(`  CSV written to         : ${outPath}`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});