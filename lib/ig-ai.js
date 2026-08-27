// ============================================================================
// PHASE 8 — AI naming + event matching.
//
// ONE OpenAI call per content item, at insert time, never again. Structured
// outputs (JSON schema) so a malformed response can't break anything.
//
// Signal priority (from the build plan): artist name in caption > date
// proximity > venue > generic brand terms.
//
// Confidence tiers:
//   >= 0.85       auto-link (project created on the fly if needed)
//   0.50 - 0.85   link + needs_review = true (review queue in the Mac app)
//   <  0.50       no link, needs_review = true (manual queue)
//
// Cost: ~$0.03/month at ~50 items (gpt-4o-mini).
// ============================================================================

const axios = require('axios');

const { OPENAI_API_KEY, ANTHROPIC_API_KEY, ANTHROPIC_MODEL } = process.env;

// ==================== LLM PROVIDER ====================
//
// The assistant prefers Claude when ANTHROPIC_API_KEY is set (Render env +
// .env), falling back to OpenAI. Classification stays on gpt-4o-mini —
// structured outputs are load-bearing there.

async function llmComplete(system, messages, { maxTokens = 600, timeout = 60000, model = null } = {}) {
  if (ANTHROPIC_API_KEY) {
    const { data } = await axios.post(
      'https://api.anthropic.com/v1/messages',
      {
        model: model || ANTHROPIC_MODEL || 'claude-sonnet-5',
        max_tokens: maxTokens,
        // The system prompt is byte-identical on every call, so cache it:
        // repeat runs read it at a fraction of the input price.
        system: [{ type: 'text', text: system, cache_control: { type: 'ephemeral' } }],
        messages,
      },
      {
        headers: {
          'x-api-key': ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
          'content-type': 'application/json',
        },
        timeout,
      }
    );
    // The response is a list of blocks and the first one is NOT necessarily
    // the answer — Claude returns ["thinking", "text"]. Read every text block.
    const text = (data.content || [])
      .filter(block => block?.type === 'text' && typeof block.text === 'string')
      .map(block => block.text)
      .join('')
      .trim();
    if (!text && data.stop_reason === 'max_tokens') {
      throw new Error('Claude hit max_tokens before producing an answer — raise maxTokens');
    }
    return text;
  }
  if (!OPENAI_API_KEY) throw new Error('No AI provider configured (set ANTHROPIC_API_KEY or OPENAI_API_KEY)');
  const { data } = await axios.post(
    'https://api.openai.com/v1/chat/completions',
    {
      model: 'gpt-4o-mini',
      max_tokens: maxTokens,
      messages: [{ role: 'system', content: system }, ...messages],
      temperature: 0.3,
    },
    { headers: { Authorization: `Bearer ${OPENAI_API_KEY}` }, timeout }
  );
  return data.choices[0].message.content.trim();
}

const CATEGORIES = ['announce', 'presale', 'lineup', 'teaser', 'recap',
                    'aftermovie', 'brand', 'giveaway', 'facts', 'other'];

const RESPONSE_SCHEMA = {
  name: 'ig_content_classification',
  strict: true,
  schema: {
    type: 'object',
    additionalProperties: false,
    properties: {
      ai_name:          { type: 'string', description: 'Short human name, e.g. "Bob Sinclar Announce", "PW26 Facts"' },
      content_category: { type: 'string', enum: CATEGORIES },
      features:         { type: 'array', items: { type: 'string' }, description: 'Notable traits: artist names, offers, CTA type, giveaway mechanics...' },
      event_id:         { type: ['integer', 'null'], description: 'Matched event id from the candidate list, or null' },
      confidence:       { type: 'number', description: '0..1 match confidence' },
      reason:           { type: 'string', description: 'One sentence: why this match (or why none)' },
    },
    required: ['ai_name', 'content_category', 'features', 'event_id', 'confidence', 'reason'],
  },
};

const SYSTEM_PROMPT = `You classify Instagram content for Produkt Studio, a Montréal event promoter (venues: New City Gas, Bazart, Produktworld).
Given one content item (caption, type, posted date) and a list of candidate events, you:
1. Name it briefly ("<Artist> Announce", "KARNAVALE Teaser", "PW26 Facts").
2. Categorize it (${CATEGORIES.join(', ')}).
3. Match it to ONE candidate event, or null.
Matching signal priority: artist/event name appearing in the caption is strongest; then date proximity (content usually promotes an upcoming event within ~60 days, or recaps one within days after); then venue mentions; generic brand terms are weakest.
Brand-level content ("5 facts", venue hype with no specific show) gets event_id null.
Be conservative: when torn between two events, lower the confidence.`;

/** Existing project for an event, or null. The AI NEVER creates projects —
 *  it only associates with existing ones; otherwise the match is stored as a
 *  suggestion and the user creates the project from the review flow. */
async function findProject(supabase, eventId) {
  const { data: existing, error } = await supabase
    .from('events_marketing').select('project_id').eq('event_id', eventId).limit(1);
  if (error) throw new Error(`events_marketing read failed: ${error.message}`);
  return existing && existing.length > 0 ? existing[0].project_id : null;
}

/**
 * Classify one just-inserted content row: name + category + event match.
 * Best-effort — any failure is logged and swallowed (the insert must survive).
 */
async function classifyContent(supabase, row, { log = console.log } = {}) {
  if (!OPENAI_API_KEY) return;

  try {
    // Candidate events: ±90 days around the posting date, real Tixr events only.
    const posted = new Date(row.posted_at);
    const lo = new Date(posted.getTime() - 90 * 86400000).toISOString().slice(0, 10);
    const hi = new Date(posted.getTime() + 90 * 86400000).toISOString().slice(0, 10);

    const { data: events, error: evErr } = await supabase
      .from('events')
      .select('event_id, event_name, event_date, event_venue, event_artist, event_status')
      .gte('event_date', lo).lte('event_date', hi)
      .gte('event_id', 10000)
      .order('event_date');
    if (evErr) throw new Error(`events read failed: ${evErr.message}`);

    const userPayload = {
      content: {
        type: row.media_product,
        media: row.media_type,
        posted_at: row.posted_at,
        caption: (row.caption || '').slice(0, 2000),
      },
      candidate_events: (events || []).map(e => ({
        event_id: e.event_id,
        name: e.event_name,
        date: e.event_date,
        venue: e.event_venue,
        artist: e.event_artist,
      })),
    };

    const { data: res } = await axios.post(
      'https://api.openai.com/v1/chat/completions',
      {
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user', content: JSON.stringify(userPayload) },
        ],
        response_format: { type: 'json_schema', json_schema: RESPONSE_SCHEMA },
        temperature: 0.2,
      },
      { headers: { Authorization: `Bearer ${OPENAI_API_KEY}` }, timeout: 30000 }
    );

    const parsed = JSON.parse(res.choices[0].message.content);

    const update = {
      ai_name:          parsed.ai_name || null,
      content_category: CATEGORIES.includes(parsed.content_category) ? parsed.content_category : 'other',
      ai_features:      { features: parsed.features || [] },
      match: {
        event_id:   parsed.event_id ?? null,
        confidence: parsed.confidence ?? 0,
        reason:     parsed.reason || '',
        model:      'gpt-4o-mini',
        at:         new Date().toISOString(),
      },
      updated_at: new Date().toISOString(),
    };

    // SUGGEST ONLY — the AI never assigns a link. A match becomes a review
    // item the user confirms (or corrects) in the app; no match = nothing to
    // review, the content simply stays unlinked.
    const confidence = parsed.confidence ?? 0;
    update.needs_review = !!(parsed.event_id && confidence >= 0.4);

    const { error: upErr } = await supabase
      .from('mktg_ig_stats').update(update).eq('ig_media_id', row.ig_media_id);
    if (upErr) throw new Error(`mktg_ig_stats update failed: ${upErr.message}`);

    log(`  🤖 ${row.ig_media_id}: "${parsed.ai_name}" [${update.content_category}] ` +
        (update.needs_review
          ? `→ suggests event ${parsed.event_id} (${Math.round(confidence * 100)}%, awaiting review)`
          : `→ no suggestion (${parsed.reason})`));
  } catch (err) {
    log(`  ⚠️ AI classification failed for ${row.ig_media_id}: ${err.response?.data?.error?.message || err.message}`);
  }
}

// ==================== ADS ====================

const AD_SCHEMA = {
  name: 'ad_event_match',
  strict: true,
  schema: {
    type: 'object',
    additionalProperties: false,
    properties: {
      event_id:   { type: ['integer', 'null'], description: 'Matched event id from the candidate list, or null' },
      confidence: { type: 'number', description: '0..1 match confidence' },
      reason:     { type: 'string', description: 'One sentence: why this match (or why none)' },
    },
    required: ['event_id', 'confidence', 'reason'],
  },
};

const AD_SYSTEM_PROMPT = `You match Meta ads to events for Produkt Studio, a Montréal event promoter (venues: New City Gas, Bazart, Produktworld).
Given one ad (ad name, campaign name, adset name, objective, created date) and candidate events, match it to ONE event or null.
Signal priority: artist/event name in the ad/campaign/adset name is strongest; then date proximity (ads usually run in the weeks before their event); brand-level campaigns (venue hype, general traffic) get null.
Be conservative: when torn between two events, lower the confidence.`;

/**
 * Suggest an event link for a just-imported ad. SUGGEST ONLY — writes match +
 * needs_review, never project_id. Best-effort: failures logged and swallowed.
 */
async function classifyAd(supabase, row, { log = console.log } = {}) {
  if (!OPENAI_API_KEY) return;

  try {
    const created = new Date(row.created_time || Date.now());
    const lo = new Date(created.getTime() - 30 * 86400000).toISOString().slice(0, 10);
    const hi = new Date(created.getTime() + 120 * 86400000).toISOString().slice(0, 10);

    const { data: events, error: evErr } = await supabase
      .from('events')
      .select('event_id, event_name, event_date, event_venue, event_artist')
      .gte('event_date', lo).lte('event_date', hi)
      .gte('event_id', 10000)
      .order('event_date');
    if (evErr) throw new Error(`events read failed: ${evErr.message}`);

    const userPayload = {
      ad: {
        name: row.ad_name,
        campaign: row.campaign_name,
        adset: row.adset_name,
        objective: row.campaign_objective,
        created: row.created_time,
      },
      candidate_events: (events || []).map(e => ({
        event_id: e.event_id,
        name: e.event_name,
        date: e.event_date,
        venue: e.event_venue,
        artist: e.event_artist,
      })),
    };

    const { data: res } = await axios.post(
      'https://api.openai.com/v1/chat/completions',
      {
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: AD_SYSTEM_PROMPT },
          { role: 'user', content: JSON.stringify(userPayload) },
        ],
        response_format: { type: 'json_schema', json_schema: AD_SCHEMA },
        temperature: 0.2,
      },
      { headers: { Authorization: `Bearer ${OPENAI_API_KEY}` }, timeout: 30000 }
    );

    const parsed = JSON.parse(res.choices[0].message.content);
    const confidence = parsed.confidence ?? 0;

    const update = {
      match: {
        event_id:   parsed.event_id ?? null,
        confidence,
        reason:     parsed.reason || '',
        model:      'gpt-4o-mini',
        at:         new Date().toISOString(),
      },
      needs_review: !!(parsed.event_id && confidence >= 0.4),
      updated_at: new Date().toISOString(),
    };

    const { error: upErr } = await supabase
      .from('mktg_ads').update(update).eq('ad_id', row.ad_id);
    if (upErr) throw new Error(`mktg_ads update failed: ${upErr.message}`);

    log(`  🤖 ad ${row.ad_id}: ` +
        (update.needs_review
          ? `suggests event ${parsed.event_id} (${Math.round(confidence * 100)}%, awaiting review)`
          : `no suggestion (${parsed.reason})`));
  } catch (err) {
    log(`  ⚠️ AI ad classification failed for ${row.ad_id}: ${err.response?.data?.error?.message || err.message}`);
  }
}

// ==================== PRODUKT AI — DASHBOARD INSIGHT ====================

const INSIGHT_SYSTEM_PROMPT = `You are Produkt AI, the marketing analyst for Produkt Studio (Montréal event promoter — New City Gas, Bazart, Produktworld).
You receive a JSON snapshot of one event or event group: ticket sales, revenue, daily pace, ticket types, sale phases, Instagram content performance, and ad results.
Write a SHORT plain-text analysis (under 130 words):
- One opening line with the headline read (pace, standout number, or concern).
- 3 to 5 bullet points ("• "), each ONE concrete observation with its number (best sales day, content that moved tickets, paid efficiency, GA/VIP mix, pace vs days remaining...).
- If something needs action (slow pace, no content linked, unspent window), say it plainly in the last bullet.
Numbers come from the snapshot only — never invent or extrapolate figures. No headers, no markdown besides bullets, no fluff.`;

const INSIGHT_COOLDOWN_MS = 60 * 60 * 1000;   // 1h between generations per project

/**
 * Produkt AI dashboard card. Returns { insight, generated_at, cached }.
 * Serves the cached row when younger than 1h unless force=true.
 */
async function generateProjectInsight(supabase, projectId, context, { force = false } = {}) {
  const { data: existing } = await supabase
    .from('mktg_ai_insights').select('insight, generated_at')
    .eq('project_id', projectId).single();

  if (existing && !force) {
    const age = Date.now() - new Date(existing.generated_at).getTime();
    if (age < INSIGHT_COOLDOWN_MS) {
      return { insight: existing.insight, generated_at: existing.generated_at, cached: true };
    }
  }

  const insight = await llmComplete(INSIGHT_SYSTEM_PROMPT, [
    { role: 'user', content: JSON.stringify(context).slice(0, 24000) },
  ], { maxTokens: 400 });
  const generatedAt = new Date().toISOString();

  await supabase.from('mktg_ai_insights').upsert({
    project_id: projectId,
    insight,
    model: 'gpt-4o-mini',
    context,
    generated_at: generatedAt,
  }, { onConflict: 'project_id' });

  return { insight, generated_at: generatedAt, cached: false };
}

// ==================== PRODUKT AI — CHAT ====================

const CHAT_SYSTEM_PROMPT = `You are Produkt AI, the marketing analyst for Produkt Studio (Montréal event promoter — New City Gas, Bazart, Produktworld).
The first user message carries a JSON snapshot of the event/group being discussed (sales, pace, content, ads). Answer follow-up questions about it conversationally but CONCISELY (under 100 words unless asked for more).
Numbers come from the snapshot only — if something isn't in it, say you don't have that data rather than guessing. Plain text, bullets allowed, no headers.`;

/**
 * Stateless chat turn: the app sends the running transcript + the data
 * snapshot; returns the assistant's reply.
 */
async function chatWithAnalyst(context, messages) {
  const turns = [
    { role: 'user', content: `Data snapshot:\n${JSON.stringify(context).slice(0, 24000)}` },
    { role: 'assistant', content: 'Got it — I have the numbers. What would you like to know?' },
    ...messages.slice(-12).map(m => ({
      role: m.role === 'assistant' ? 'assistant' : 'user',
      content: String(m.content || '').slice(0, 4000),
    })),
  ];
  return llmComplete(CHAT_SYSTEM_PROMPT, turns, { maxTokens: 500 });
}

module.exports = { classifyContent, classifyAd, generateProjectInsight, chatWithAnalyst, llmComplete };
