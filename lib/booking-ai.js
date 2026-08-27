//
// booking-ai.js — Claude does two jobs for the booking scraper:
//
//   authorRecipe   the conversation in the app's browser ("this is the list,
//                  artist is here, get every price tier") becomes a machine
//                  recipe stored on the source.
//   extractEvents  at scrape time, turns whatever the page gave us into
//                  clean, typed event rows.
//
// Both parse JSON defensively — a model that wraps output in prose or fences
// must never break a nightly sync.
//

const { llmComplete } = require('./ig-ai');

// ==================== JSON PLUMBING ====================

/**
 * Pulls the first JSON object/array out of a reply. Models wrap output in
 * fences, add prose, annotate with // comments and leave trailing commas —
 * none of that may break a nightly sync, so all of it is tolerated.
 */
function parseJSON(text, fallback = null) {
  if (!text) return fallback;

  const attempt = (raw) => {
    if (!raw) return undefined;
    const candidates = [raw, stripLoose(raw)];
    for (const candidate of candidates) {
      try { return JSON.parse(candidate); } catch { /* try the next form */ }
    }
    return undefined;
  };

  const cleaned = text.replace(/^```(?:json)?/gm, '').replace(/```$/gm, '').trim();
  const direct = attempt(cleaned);
  if (direct !== undefined) return direct;

  for (const [open, close] of [['{', '}'], ['[', ']']]) {
    const start = cleaned.indexOf(open);
    const end = cleaned.lastIndexOf(close);
    if (start !== -1 && end > start) {
      const sliced = attempt(cleaned.slice(start, end + 1));
      if (sliced !== undefined) return sliced;
    }
  }
  return fallback;
}

/** Removes // and /* *​/ comments plus trailing commas, keeping strings intact. */
function stripLoose(raw) {
  let out = '';
  let inString = false, quote = '', escaped = false;
  for (let i = 0; i < raw.length; i++) {
    const ch = raw[i];
    if (inString) {
      out += ch;
      if (escaped) { escaped = false; continue; }
      if (ch === '\\') { escaped = true; continue; }
      if (ch === quote) inString = false;
      continue;
    }
    if (ch === '"' || ch === "'") { inString = true; quote = ch; out += ch; continue; }
    if (ch === '/' && raw[i + 1] === '/') {
      while (i < raw.length && raw[i] !== '\n') i++;
      out += '\n';
      continue;
    }
    if (ch === '/' && raw[i + 1] === '*') {
      i += 2;
      while (i < raw.length && !(raw[i] === '*' && raw[i + 1] === '/')) i++;
      i++;
      continue;
    }
    out += ch;
  }
  return out.replace(/,(\s*[}\]])/g, '$1');
}

// ==================== EXTRACTION ====================

const EXTRACT_SYSTEM = `You extract concert/event listings from scraped web content for Produkt Studio, a Montréal promoter tracking which artists are booked elsewhere ("artist availability").

You are given the readable content of ONE page (embedded structured data, per-item blocks, or raw text) and the operator's own instructions for this website. Return ONLY a JSON array of events — no prose, no code fences.

Each event:
{
  "artist_name": string|null,     // the HEADLINE artist, cleaned: no "presents", no venue, no "w/ ..."
  "artists": string[],            // every performing artist, headliner first, support included
  "event_name": string|null,      // the show/series title if distinct from the artist
  "event_date": string|null,      // STRICT "YYYY-MM-DD". Resolve French/English month names.
  "venue": string|null,
  "location": string|null,        // the physical place, e.g. "Parc Jean-Drapeau"
  "city": string|null,
  "country": string|null,
  "url": string|null,             // link to that event's own page
  "flyer_url": string|null,       // poster/artwork image
  "price_min": number|null,
  "price_max": number|null,
  "currency": string|null,        // ISO code, e.g. "CAD"
  "price_tiers": [{"name": string, "price": number, "currency": string|null, "sold_out": boolean}]|null
}

Rules:
- Only real, dated events. Never invent one, never pad the list, never emit placeholder/example rows.
- A date you cannot resolve to a real calendar day → null, and drop the event.
- If a year is missing, infer it from the surrounding listing (upcoming events are in the future).
- Split multi-artist bills into "artists"; put the headliner in "artist_name".
- Strip marketing noise from names: "SOLD OUT", "presented by X", emojis, "@ Venue".
- Absolute URLs when the content gives them; relative paths are fine, they get resolved later.
- Return [] when the content has no events.`;

/**
 * Reads a page's content into typed events.
 * @param {string} content   what the scraper collected
 * @param {object} source    the mktg_booking_source row (venue/city context)
 * @param {object} recipe    field hints, pricing mode, operator instructions
 * @param {string} [hint]    extra per-call steer (used by the detail pass)
 */
async function extractEvents({ content, source, recipe = {}, hint = null }) {
  const pricing = recipe.pricing_mode || 'starting';
  const priceRule = pricing === 'none'
    ? 'Ignore pricing entirely — leave every price field null.'
    : pricing === 'all_tiers'
      ? 'Capture EVERY ticket tier into "price_tiers" (name + price + sold_out), and set price_min/price_max from them.'
      : 'Capture only the cheapest/starting price into price_min (and currency). Leave price_tiers null.';

  const context = [
    `Website: ${source.name} (${source.url})`,
    source.venue    ? `Operator says the venue is: ${source.venue}` : null,
    source.location ? `Operator says the location is: ${source.location}` : null,
    source.city     ? `Operator says the city is: ${source.city}` : null,
    source.country  ? `Operator says the country is: ${source.country}` : null,
    `Pricing: ${priceRule}`,
    recipe.instructions ? `Operator instructions for this site:\n${recipe.instructions}` : null,
    recipe.field_hints && Object.keys(recipe.field_hints).length
      ? `Field location hints (CSS): ${JSON.stringify(recipe.field_hints)}` : null,
    hint,
  ].filter(Boolean).join('\n');

  // Transient socket/network failures must not fail an unattended nightly
  // run — one retry costs far less than a missed day of a website.
  let reply = '';
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      reply = await extractCall();
      break;
    } catch (err) {
      const transient = /ETIMEDOUT|ECONNRESET|ECONNABORTED|socket hang up|timeout/i.test(err.message || '');
      if (!transient || attempt === 1) throw err;
      await new Promise(r => setTimeout(r, 4000));
    }
  }

  const parsedReply = parseJSON(reply, []);
  if (Array.isArray(parsedReply)) return parsedReply;
  if (Array.isArray(parsedReply?.events)) return parsedReply.events;
  return [];

  function extractCall() {
    return llmComplete(EXTRACT_SYSTEM, [
      { role: 'user', content: `${context}\n\n=== PAGE CONTENT ===\n${content}` },
    ], {
      maxTokens: 16000,
      timeout: 240_000,
    // Extraction is mechanical — reading fields out of content the scraper
    // already narrowed down. Haiku does it for a fraction of the price;
    // BOOKING_EXTRACT_MODEL overrides if a site needs more muscle.
      model: process.env.BOOKING_EXTRACT_MODEL || 'claude-haiku-4-5-20251001',
    });
  }
}

// ==================== RECIPE AUTHORING ====================

const AUTHOR_SYSTEM = `You help an operator teach a scraper how to read an event website. You see: the page URL, a sample of its HTML/structured data, any JSON API calls the page's own front-end made, elements the operator clicked to point at, and their plain-English instructions.

Reply with ONLY a JSON object — no prose outside it:
{
  "reply": string,          // 1-3 sentences to the operator: what you found and what you still need
  "ready": boolean,         // true when the recipe is good enough to test
  "recipe": {
    "mode": "api" | "html" | "browser",
    "api": { "url": string, "method": "GET"|"POST", "headers": object, "body": any, "json_path": string|null } | null,
    "list_selector": string|null,       // CSS selector matching ONE event card, repeated
    "field_hints": object,              // { artist, date, price, flyer, url, name } CSS selectors, best-effort
    "detail": { "follow_links": boolean, "link_selector": string|null, "limit": number } | null,
    "pricing_mode": "none"|"starting"|"all_tiers",
    "instructions": string              // distilled instructions the extractor will read every run
  },
  "suggested": {              // your best guess at the source's manual fields
    "venue": string|null, "location": string|null, "city": string|null, "country": string|null
  }
}

Choosing the mode — this decides whether the nightly job can run unattended:
- "api" ONLY when you can name a distinct endpoint that returns JSON directly (it appears in "json_calls_the_page_made", or you are certain of the URL). The page's own HTML address is NEVER an api url. Prefer this when it genuinely exists: fastest and most stable across redesigns.
- "html" when the events are in the served HTML — INCLUDING embedded JSON such as application/ld+json, __NEXT_DATA__ or __NUXT_DATA__. If you found the data inside the page's embedded JSON, this is the correct mode, not "api". Still fine for the nightly server job.
- "browser" ONLY when the content genuinely appears after JavaScript runs and none of the above exists. Warn the operator in "reply" that these sync from the app, not the nightly job.

Keep list_selector as stable as you can: prefer semantic classes/data attributes over generated hashes. Fold anything the operator says about navigation ("click the load-more", "open each event") into "instructions" and "detail".

When "navigation_path_recorded" is present it is the exact route the operator clicked through, in order, each with their own label. Treat it as the truth about how to reach the data: if it ends on an individual event page, set detail.follow_links true with that link's selector; if it steps through filters or a "load more", describe those steps precisely in "instructions" so the extractor knows what the reachable content looks like.`;

/**
 * One turn of the authoring conversation. Stateless: the app sends the whole
 * transcript plus the current page evidence each time.
 */
async function authorRecipe({ url, pageSample, networkSamples = [], selectedElements = [], recordedSteps = [], messages = [], currentRecipe = null }) {
  const evidence = {
    url,
    page_sample: (pageSample || '').slice(0, 45_000),
    json_calls_the_page_made: networkSamples.slice(0, 6).map(n => ({
      url: n.url,
      method: n.method || 'GET',
      sample: String(n.body || '').slice(0, 6000),
    })),
    elements_the_operator_pointed_at: selectedElements.slice(0, 20),
    // The path they walked by clicking, in order — turn this into the
    // recipe's navigation steps / detail-page follow.
    navigation_path_recorded: recordedSteps.slice(0, 15),
    current_recipe: currentRecipe,
  };

  const turns = [
    { role: 'user', content: `Evidence from the page:\n${JSON.stringify(evidence).slice(0, 60_000)}` },
    { role: 'assistant', content: 'Understood — I have the page evidence. What should I extract?' },
    ...messages.slice(-10).map(m => ({
      role: m.role === 'assistant' ? 'assistant' : 'user',
      content: String(m.content || '').slice(0, 4000),
    })),
  ];

  const reply = await llmComplete(AUTHOR_SYSTEM, turns, { maxTokens: 3000, timeout: 180_000 });
  const parsed = parseJSON(reply, null);
  if (!parsed) {
    return { reply: reply || 'I could not read that page — try selecting the event list first.', ready: false, recipe: null };
  }
  return parsed;
}

module.exports = { extractEvents, authorRecipe, parseJSON };
