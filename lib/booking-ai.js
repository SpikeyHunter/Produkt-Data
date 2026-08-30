//
// booking-ai.js — Claude does two jobs for the booking scraper:
//
//   authorRecipe   the conversation in the app's browser ("this is the list,
//                  artist is here, get every price tier") becomes a machine
//                  recipe stored on the source.
//   extractEvents  at scrape time, turns whatever the page gave us into
//                  clean, typed event rows.
//   proposeDetections  the pre-detection pass: look at a page cold and work
//                  out, unprompted, every distinct way it publishes events
//                  ("upcoming shows in the list", "past shows as flyer art"),
//                  each proved with a real sample event. The operator just
//                  confirms them instead of hand-building selectors.
//
// Both parse JSON defensively — a model that wraps output in prose or fences
// must never break a nightly sync.
//

const cheerio = require('cheerio');
const { llmComplete, visionMessage } = require('./ig-ai');
const { normalizeArtistName } = require('./name-case');

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
  // One switch: prices off, or capture everything the page shows.
  const capture = recipe.capture_prices !== false && recipe.pricing_mode !== 'none';
  const priceRule = capture
    ? 'Capture pricing in full: EVERY ticket tier you can see into "price_tiers" (name + price + sold_out), and set price_min/price_max from them. If only one price is shown, that is price_min.'
    : 'Ignore pricing entirely — leave every price field null.';

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
    // The operator can show one event done right. Nothing steers extraction
    // harder than a worked example from the person who knows the site.
    recipe.example_event
      ? `WORKED EXAMPLE — the operator says ONE event on this page should come out exactly like this. Match this shape, this level of cleaning, and these field choices for every other event:\n${
          typeof recipe.example_event === 'string'
            ? recipe.example_event
            : JSON.stringify(recipe.example_event, null, 2)}`
      : null,
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
      model: process.env.BOOKING_EXTRACT_MODEL || 'claude-haiku-4-5',
    });
  }
}

// ==================== FLYER READING (vision) ====================

const FLYER_SYSTEM = `You read event posters for Produkt Studio, a Montréal promoter tracking which artists are booked elsewhere.

You are shown a batch of flyer images from ONE promoter's website, in order, with the caption text that sat next to each one. Return ONLY a JSON array — one object per image, in the SAME ORDER, no prose, no fences:

{
  "artist_name": string|null,   // the HEADLINE act, cleaned
  "artists": string[],          // every act on the bill, headliner first
  "event_name": string|null,    // the party/series title if distinct from the artist
  "event_date": string|null,    // STRICT "YYYY-MM-DD"
  "venue": string|null,
  "price_min": number|null,
  "confidence": number          // 0..1 — how sure you are of the DATE
}

Rules:
- Flyers usually print the day and month but not the year. Infer it from the surrounding context you are given, and prefer the most recent plausible year that is not in the future.
- If a flyer shows no resolvable date, set event_date null and confidence 0 — do NOT guess a date.
- Emit exactly one object per image, even when an image is unreadable (all-null object).
- Never invent events that aren't on the flyers.`;

/**
 * Reads a batch of flyers with vision. This is the only way to get dates off
 * an image-only listing page (some promoters publish nothing but artwork).
 */
async function extractFromFlyers({ images, source, route = {}, contextNote = '' }) {
  if (!images.length) return [];

  const context = [
    `Website: ${source.name} (${source.url})`,
    source.venue   ? `Usual venue: ${source.venue}` : null,
    source.city    ? `City: ${source.city}` : null,
    `Today's date is ${new Date().toISOString().slice(0, 10)} — use it to pick the right year.`,
    route.instructions ? `Operator instructions: ${route.instructions}` : null,
    route.example_event
      ? `The operator says one of these flyers should read exactly like this — match the shape:\n${
          typeof route.example_event === 'string' ? route.example_event : JSON.stringify(route.example_event)}`
      : null,
    contextNote ? `Text that appeared alongside these flyers, in order:\n${contextNote}` : null,
    `There are ${images.length} images. Return exactly ${images.length} objects.`,
  ].filter(Boolean).join('\n');

  const reply = await llmComplete(
    FLYER_SYSTEM,
    [visionMessage(context, images.map(i => i.url))],
    { maxTokens: 8000, timeout: 240_000,
      model: process.env.BOOKING_FLYER_MODEL || 'claude-haiku-4-5' }
  );

  const parsed = parseJSON(reply, []);
  const rows = Array.isArray(parsed) ? parsed : (parsed?.events || []);

  return rows.map((row, i) => ({
    ...row,
    // The flyer's own artwork is the best poster we'll get
    flyer_url: images[i]?.url || null,
    url: images[i]?.link || null,
  })).filter(row => row && row.event_date);
}

// ==================== PRE-DETECTION ====================

const DETECT_SYSTEM = `You are shown ONE page from an event/venue/promoter website. Your job is to work out, on your own, EVERY distinct way this site publishes events, and to propose one "detection" per way.

A detection is one pass over the site. A single page very often needs more than one, because promoters publish different kinds of events differently. The classic split:
- Upcoming shows sit in a real list with text, dates and links (often out to a ticketing platform where the prices live).
- Past shows are nothing but flyer artwork — the date exists ONLY inside the image, so they need vision.
Those are two detections, not one.

Reply with ONLY a JSON object — no prose outside it:
{
  "platform": string,          // what the site is built with, if you can tell
  "reply": string,             // 2-4 sentences to the operator, plain English, no jargon: what this page is and what you propose
  "detections": [
    {
      "name": string,          // short and human: "Upcoming shows", "Past events (flyers)"
      "explains": string,      // ONE sentence: what this detection finds and how it reaches it
      "url": string|null,      // the page this pass should start on — the page you were shown unless another URL is genuinely the right start
      "mode": "api"|"html"|"browser",
      "list_selector": string|null,   // CSS selector matching ONE repeated event block
      "field_hints": object,          // { artist, date, price, flyer, url, name } best-effort CSS
      "read_flyers": boolean,         // true when the date only exists inside the artwork
      "follow_links": boolean,        // true when prices/lineup need each event's own page opened
      "confidence": number,           // 0..1 — how sure you are this detection will work
      "sample_event": {               // PROOF: one real event you can actually see, read off THIS page
        "artist_name": string|null,
        "event_name": string|null,
        "event_date": string|null,    // "YYYY-MM-DD", or null if it truly is not on the page
        "price_min": number|null
      }|null,
      "caveat": string|null           // what could go wrong: "Tixr blocks server requests — run this one in Browser mode"
    }
  ],
  "suggested": { "venue": string|null, "location": string|null, "city": string|null, "country": string|null }
}

Rules that decide whether this actually works:
- Propose between 1 and 4 detections. One per genuinely different way of publishing — never one per event, never near-duplicates.
- sample_event is the whole point: it proves the detection is real. Read a REAL event off the content you were given. If you cannot find one for a detection, set sample_event null and lower confidence — never invent one.
- read_flyers true ONLY when the listing is artwork with no date in the text. That pass costs vision tokens, so do not switch it on out of caution.
- follow_links true when this page shows no prices but links out to a ticketing page that would have them.
- mode "browser" ONLY when the content genuinely needs JavaScript. Ticketing platforms (Tixr, Dice, Eventbrite, Shotgun) commonly refuse plain server requests — when a detection points at one, say so in "caveat" and set mode "browser".
- list_selector must match EVERY card in that group, not just the first. Prefer the LOOSEST selector that still separates this group from the others, and leave off framework-generated classes (w-dyn-item, sqs-block, elementor-*) — they are usually present on only some of the cards and will silently hide the rest. When you cannot find a selector you are confident matches all of them, return null: reading the whole page costs a little more and loses nothing.
- Prefer stable selectors: semantic classes and data attributes over generated hashes.
- When you are shown detections that worked on other sites built the same way, use them: they are what this platform usually looks like. Adapt, do not copy blindly.`;

/**
 * The pre-detection pass. Instead of the operator hand-building selectors,
 * Claude reads the page cold and proposes the detections it can see, each
 * proved with a real sample event — the operator just confirms or discards.
 *
 * Prior working recipes for the same kind of site are passed in as worked
 * examples, so this gets better at new websites the more it has seen.
 */
async function proposeDetections({ url, html = '', signature = {}, patterns = [], note = '' }) {
  const $ = cheerio.load(html || '');
  // Give the model the same view the scraper will actually get, not raw
  // markup: structured data, then the page's own text, then the artwork.
  const structured = [];
  $('script[type="application/ld+json"]').slice(0, 6).each((_, el) => {
    structured.push($(el).contents().text().trim().slice(0, 4000));
  });

  const images = [];
  $('img').slice(0, 60).each((_, el) => {
    const src = $(el).attr('src') || $(el).attr('data-src') || '';
    if (!src || /sprite|logo|icon|favicon|pixel|avatar/i.test(src)) return;
    images.push({ src: src.slice(0, 200), alt: ($(el).attr('alt') || '').slice(0, 120) });
  });

  const links = [];
  $('a[href]').slice(0, 250).each((_, el) => {
    const href = $(el).attr('href') || '';
    const text = $(el).text().replace(/\s+/g, ' ').trim();
    if (!href || href.startsWith('#') || !text) return;
    links.push(`${text.slice(0, 70)} → ${href.slice(0, 160)}`);
  });

  const text = $('body').text().replace(/\s+/g, ' ').trim();

  const evidence = {
    url,
    what_the_site_is_built_with: signature.platform || 'unknown',
    traits_detected: signature.traits || [],
    embedded_structured_data: structured,
    // Markup the scraper can actually key selectors off
    markup_sample: (html || '').slice(0, 22_000),
    page_text: text.slice(0, 9_000),
    images_on_the_page: images,
    links_on_the_page: links.slice(0, 120),
    operator_note: note || null,
    detections_that_worked_on_sites_built_the_same_way: patterns.map(p => ({
      platform: p.platform, traits: p.traits, routes: p.routes, produced: p.event_count,
    })),
  };

  const reply = await llmComplete(DETECT_SYSTEM, [
    { role: 'user', content: JSON.stringify(evidence).slice(0, 90_000) },
  ], { maxTokens: 4000, timeout: 240_000 });

  const parsed = parseJSON(reply, null);
  if (!parsed || !Array.isArray(parsed.detections)) {
    return {
      reply: 'I could not make sense of that page — try navigating to the actual events listing first.',
      detections: [], platform: signature.platform || null, suggested: {},
    };
  }
  // A detection with no starting page is unusable — default it to this page.
  parsed.detections = parsed.detections.slice(0, 4).map(d => ({
    ...d,
    url: d.url || url,
    // A null hint says nothing and is a decode hazard on the client — a
    // typed [String: String] refuses {"price": null} outright. Drop them.
    field_hints: Object.fromEntries(
      Object.entries(d.field_hints && typeof d.field_hints === 'object' ? d.field_hints : {})
        .filter(([, v]) => typeof v === 'string' && v.trim() !== '')),
  }));
  return { platform: signature.platform || null, ...parsed };
}

// ==================== TRAINED DRAFT ====================

const DRAFT_SYSTEM = `The operator has pointed at ONE event on a website and the app has found every block on the page with the same structure. You are given those blocks. Each one is a single event.

Your job is only to READ them. The structure is already decided and is not yours to second-guess: one block in, one event out, in the same order.

Return ONLY a JSON array, one object per block:
{
  "artist_name": string|null,   // the HEADLINE act, cleaned: no "presents", no venue, no "w/ ..."
  "artists": string[],          // every act on the bill, headliner first
  "event_name": string|null,    // the show or party title, if distinct from the artist
  "event_date": string|null,    // STRICT "YYYY-MM-DD". Resolve French and English month names.
  "venue": string|null,
  "city": string|null,
  "url": string|null,
  "flyer_url": string|null
}

Rules:
- ONE object per block, in the SAME ORDER, even when a block is unreadable — return an all-null object rather than skipping it, or everything after it lines up against the wrong block.
- The operator may have labelled which part of the block is the artist, the date and so on. Where they have, TRUST IT over your own reading of the text.
- Flyers usually print the day and month but not the year. Infer the year from the surrounding blocks and today's date; prefer the most recent plausible year that is not in the future.
- A date you cannot resolve to a real calendar day → null. Never guess one.
- Ignore ticket prices entirely. They are not wanted.
- Never invent an event that isn't in the blocks.`;

/** Same house style the saved rows get, so the preview matches the database. */
function houseStyle(row) {
  if (!row) return row;
  return {
    ...row,
    artist_name: normalizeArtistName(row.artist_name),
    artists: Array.isArray(row.artists) ? row.artists.map(normalizeArtistName) : row.artists,
  };
}

/**
 * Reads the blocks the operator selected by example. The structure came from
 * them and is exact; this only interprets the messy human parts — French
 * dates, "w/ support", a lineup crammed into one line.
 */
async function draftFromCards({ cards = [], fields = {}, source = {}, note = '', readFlyers = false }) {
  if (!cards.length) return { events: [], reply: 'No blocks were selected.' };

  const labelled = Object.entries(fields).filter(([, v]) => v);
  const context = [
    `Website: ${source.name || ''} (${source.url || ''})`,
    source.venue ? `The operator says the venue is: ${source.venue}` : null,
    source.city ? `The operator says the city is: ${source.city}` : null,
    `Today is ${new Date().toISOString().slice(0, 10)} — use it to pick the right year.`,
    labelled.length
      ? `The operator labelled these parts of each block — trust these over your own reading:\n${
          labelled.map(([k, v]) => `  ${k}: ${v}`).join('\n')}`
      : null,
    note ? `Operator's note: ${note}` : null,
    `There are ${cards.length} blocks. Return exactly ${cards.length} objects.`,
  ].filter(Boolean).join('\n');

  // Artwork-only listings: the date exists nowhere but inside the image.
  if (readFlyers) {
    const images = cards.map(c => c.image).filter(Boolean);
    if (!images.length) {
      return { events: [], reply: 'None of the selected blocks contain an image, so there is nothing for vision to read. Switch that option off.' };
    }
    const rows = [];
    for (let i = 0; i < cards.length; i += 10) {
      const batch = cards.slice(i, i + 10).filter(c => c.image);
      if (!batch.length) continue;
      const note2 = batch.map((c, j) => `${i + j + 1}. ${c.text || ''}`).join('\n').slice(0, 4000);
      rows.push(...await extractFromFlyers({
        images: batch.map(c => ({ url: c.image, link: c.href })),
        source, route: { instructions: [note, context].filter(Boolean).join('\n') },
        contextNote: note2,
      }));
    }
    return { events: rows.map(houseStyle), reply: `Read ${images.length} images with vision.` };
  }

  const body = cards.map((c, i) =>
    `--- BLOCK ${i + 1} ---\n${c.text || ''}\n${c.image ? `image: ${c.image}\n` : ''}${c.href ? `link: ${c.href}\n` : ''}`
  ).join('\n').slice(0, 60_000);

  const reply = await llmComplete(DRAFT_SYSTEM, [
    { role: 'user', content: `${context}\n\n=== BLOCKS ===\n${body}` },
  ], { maxTokens: 16000, timeout: 240_000,
       model: process.env.BOOKING_EXTRACT_MODEL || 'claude-haiku-4-5' });

  const parsed = parseJSON(reply, []);
  const rows = Array.isArray(parsed) ? parsed : (parsed?.events || []);
  return {
    events: rows.map((row, i) => houseStyle({
      ...row,
      flyer_url: row?.flyer_url || cards[i]?.image || null,
      url: row?.url || cards[i]?.href || null,
    })),
    reply: `Read ${cards.length} blocks.`,
  };
}

// ==================== TICKET PAGE ====================

const TICKET_SYSTEM = `A listing page gave the operator events but no prices. They have now opened ONE event's ticket page so you can work out how to read it, and how to get the scraper there on every run.

You are given: the ticket page itself, the listing page it was reached from, and the detection as it currently stands.

Reply with ONLY a JSON object:
{
  "reply": string,          // 2-4 sentences, plain English: what this page is, where the prices live, what you changed
  "steps": string[],        // if reaching this page needs navigation the scraper can't guess (a modal, a "Tickets" tab, an age gate), list the clicks in order. Empty array when the link goes straight there.
  "found_prices": boolean,
  "sample_tiers": [{"name": string, "price": number, "currency": string|null}],  // PROOF: real tiers you can see. Empty if there are none.
  "patch": {                // changes to fold into the detection
    "follow_links": boolean,
    "detail": { "link_selector": string|null, "limit": number } | null,
    "field_hints": object,        // string values only, never null
    "instructions": string,       // ONE paragraph to append to the detection's brief — how to read this ticket page
    "mode": "api"|"html"|"browser"|null   // "browser" if this platform refuses plain server requests
  }
}

What matters:
- Read the ACTUAL tiers off the page — names and numbers. "GA Tier 1 $30" beats any description of where prices might be.
- Prices are before tax and fees. If the page shows an all-in price, say so in "instructions" so the extractor takes the base price.
- Ignore comps, guest list, staff and physical-ticket rows — they aren't real prices.
- link_selector is how the LISTING page links to pages like this one. Give the selector as it applies on the listing page, not this one.
- If this platform blocks plain server requests (Tixr, Dice, Eventbrite commonly do), set mode "browser" and say in "reply" that this detection syncs from the app rather than the nightly job.
- If the page is a dead end (sold out, removed, redirected to a home page), set found_prices false and say so plainly rather than inventing a recipe.`;

/**
 * The operator opened an event's ticket page. Work out where the prices live
 * and how the scraper reaches them on every run, and hand back a patch for
 * the detection that produced the event.
 */
async function analyseTicketPage({ url, html = '', listingURL = '', route = {} }) {
  const $ = cheerio.load(html || '');

  const structured = [];
  $('script[type="application/ld+json"]').slice(0, 6).each((_, el) => {
    structured.push($(el).contents().text().trim().slice(0, 4000));
  });

  // Anything that reads like money, with the text around it — this is what
  // the question is actually about.
  const priceBits = [];
  $('*').slice(0, 4000).each((_, el) => {
    const $el = $(el);
    if ($el.children().length) return;              // leaf nodes only
    const text = $el.text().replace(/\s+/g, ' ').trim();
    if (!text || text.length > 90) return;
    if (!/(\$|CAD|USD|\d+[.,]\d{2})/.test(text)) return;
    const context = $el.closest('li, tr, .tier, [class*=tier], [class*=ticket], div')
      .text().replace(/\s+/g, ' ').trim().slice(0, 160);
    priceBits.push({ text, context });
  });

  const evidence = {
    ticket_page_url: url,
    listing_page_it_was_reached_from: listingURL,
    the_detection_as_it_stands: route,
    embedded_structured_data: structured,
    money_looking_text_and_its_context: priceBits.slice(0, 60),
    page_text: $('body').text().replace(/\s+/g, ' ').trim().slice(0, 9_000),
    markup_sample: (html || '').slice(0, 18_000),
  };

  const reply = await llmComplete(TICKET_SYSTEM, [
    { role: 'user', content: JSON.stringify(evidence).slice(0, 80_000) },
  ], { maxTokens: 4000, timeout: 240_000 });

  const parsed = parseJSON(reply, null);
  if (!parsed) {
    return {
      reply: 'I could not read that ticket page. If it needs a login or an age gate, click through it here first, then analyse again.',
      steps: [], found_prices: false, sample_tiers: [], patch: null,
    };
  }
  if (parsed.patch?.field_hints && typeof parsed.patch.field_hints === 'object') {
    parsed.patch.field_hints = Object.fromEntries(
      Object.entries(parsed.patch.field_hints)
        .filter(([, v]) => typeof v === 'string' && v.trim() !== ''));
  }
  return parsed;
}

// ==================== REFINEMENT ====================

const REFINE_SYSTEM = `An operator has just test-run their detections against a live website and is telling you what came back wrong. Your job is to fix the detections so the next run is right.

You are given: the detections as they stand, a sample of the events they produced, the operator's corrections (rows they edited by hand, as before → after), rows they marked as NOT events at all, and their free-text notes.

Reply with ONLY a JSON object:
{
  "reply": string,          // 2-4 sentences: what you understood was wrong and what you changed. Plain English.
  "routes": [               // the FULL updated set of detections — return every one, edited or not
    {
      "id": string,         // keep the id you were given, exactly — it is how the app matches them up
      "name": string,
      "url": string,
      "instructions": string,     // the standing brief for this pass — fold the fix in here
      "list_selector": string|null,
      "field_hints": object,      // string values only, never null — omit a field instead
      "read_flyers": boolean,
      "follow_links": boolean,
      "mode": "api"|"html"|"browser",
      "example_event": string|null  // ONE LINE OF TEXT, not an object — one event written out correctly, from THEIR corrections, e.g. "jigitz b2b Killen · 2026-09-12 · 50 BALLERINAS TOUR · from $30"
    }
  ]
}

How to actually fix things:
- A correction is ground truth. If they changed a date, the date parsing is wrong — say so in "instructions" ("dates are DD/MM, not MM/DD"), and put their corrected row into "example_event".
- If they rejected rows, work out what those rows have in common (a venue-hire listing, a merch item, a recurring "every Friday" template) and add an explicit exclusion to "instructions".
- Missing prices usually means the price is on the event's own page: set follow_links true.
- Missing or wrong dates on an artwork-only listing means the date is in the image: set read_flyers true.
- Wrong events entirely, or far too few, usually means list_selector is aimed at the wrong block. Widen it or drop it to null so the whole page is read.
- MISSED events are the loudest signal: a detection is aimed at the wrong page or its list_selector is too tight. Widen the selector, drop it to null, or add a section for the page those events actually live on. Say which you did.
- If their note contradicts what you inferred, THEY are right.
- Never drop a detection unless they asked you to. Return every id you were given.`;

/** "jigitz b2b Killen · 50 BALLERINAS TOUR · 2026-09-12 · from $30" */
function exampleLine(value) {
  if (value == null) return null;
  if (typeof value === 'string') return value;
  if (typeof value !== 'object') return String(value);

  const pick = key => {
    const raw = value[key];
    return raw == null || raw === '' ? null : String(raw);
  };
  const price = pick('price_min') || pick('price');
  const parts = [
    pick('artist_name') || pick('artist'),
    pick('event_name') || pick('name'),
    pick('event_date') || pick('date'),
    pick('venue'),
    price ? `from $${price.replace(/^\$/, '')}` : null,
  ].filter(Boolean);
  // Something we didn't anticipate — better raw than lost.
  return parts.length ? parts.join(' · ') : JSON.stringify(value);
}

/**
 * The correction loop: the operator tests, marks up what came back, and the
 * detections get rewritten from their corrections. This is where a site
 * actually gets taught — the first analysis is only ever a first guess.
 */
async function refineDetections({ url, routes = [], events = [], corrections = [],
                                  rejected = [], missing = [], feedback = '' }) {
  const evidence = {
    url,
    detections_as_they_stand: routes,
    what_they_produced: events.slice(0, 25),
    operator_corrections: corrections.slice(0, 25),
    rows_the_operator_says_are_not_events: rejected.slice(0, 25),
    // Real events on the site that no detection returned — the strongest
    // signal there is that something is pointed at the wrong place.
    events_that_were_MISSED_entirely: missing.slice(0, 25),
    operator_notes: feedback || null,
  };

  const reply = await llmComplete(REFINE_SYSTEM, [
    { role: 'user', content: JSON.stringify(evidence).slice(0, 60_000) },
  ], { maxTokens: 4000, timeout: 180_000 });

  const parsed = parseJSON(reply, null);
  if (!parsed || !Array.isArray(parsed.routes)) {
    return { reply: reply || 'I could not work out what to change — tell me in your own words what came back wrong.', routes: null };
  }
  parsed.routes = parsed.routes.map(r => ({
    ...r,
    field_hints: Object.fromEntries(
      Object.entries(r.field_hints && typeof r.field_hints === 'object' ? r.field_hints : {})
        .filter(([, v]) => typeof v === 'string' && v.trim() !== '')),
    // Asked for a line of text, the model answers with an object anyway.
    // It lands in a field the operator reads and edits, so render it as a
    // readable line rather than raw JSON.
    example_event: exampleLine(r.example_event),
  }));
  return parsed;
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
    "capture_prices": boolean,          // true = pull every ticket tier; false = no prices at all
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

module.exports = { extractEvents, extractFromFlyers, authorRecipe, proposeDetections,
                   refineDetections, analyseTicketPage, draftFromCards, parseJSON };
