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

const { OPENAI_API_KEY } = process.env;

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

/** events_marketing row for an event — reuse or create. Returns project_id. */
async function ensureProject(supabase, eventId) {
  const { data: existing, error } = await supabase
    .from('events_marketing').select('project_id').eq('event_id', eventId).limit(1);
  if (error) throw new Error(`events_marketing read failed: ${error.message}`);
  if (existing && existing.length > 0) return existing[0].project_id;

  const { data: created, error: insErr } = await supabase
    .from('events_marketing').insert({ event_id: eventId }).select('project_id').single();
  if (insErr) throw new Error(`events_marketing insert failed: ${insErr.message}`);
  return created.project_id;
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

    const confidence = parsed.confidence ?? 0;
    if (parsed.event_id && confidence >= 0.5) {
      update.project_id = await ensureProject(supabase, parsed.event_id);
      update.needs_review = confidence < 0.85;
    } else {
      update.needs_review = true;
    }

    const { error: upErr } = await supabase
      .from('mktg_ig_stats').update(update).eq('ig_media_id', row.ig_media_id);
    if (upErr) throw new Error(`mktg_ig_stats update failed: ${upErr.message}`);

    log(`  🤖 ${row.ig_media_id}: "${parsed.ai_name}" [${update.content_category}] ` +
        (update.project_id
          ? `→ event ${parsed.event_id} (${Math.round(confidence * 100)}%${update.needs_review ? ', review' : ''})`
          : `→ no link (${parsed.reason})`));
  } catch (err) {
    log(`  ⚠️ AI classification failed for ${row.ig_media_id}: ${err.response?.data?.error?.message || err.message}`);
  }
}

module.exports = { classifyContent };
