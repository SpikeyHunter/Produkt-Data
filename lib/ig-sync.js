// ============================================================================
// INSTAGRAM SYNC (Phases 5+6, with first-sight media capture from Phase 7)
//
// One cycle (runIgCycle), called every 5 minutes from webhook-server.js:
//   1. DETECT   — poll /media and /stories; new IDs -> insert into mktg_ig_stats
//                 (Meta has NO webhook for own posts; polling is the only way)
//   2. CAPTURE  — download a preview image on first sight into the ig-media
//                 bucket (Meta CDN URLs expire in hours — this cannot wait)
//   3. REFRESH  — fetch insights for every row whose next_refresh_at is due,
//                 append to stats_timeline ONLY when values changed, and
//                 reschedule on the decaying ladder
//   4. SNAPSHOT — one mktg_ig_account row per day (followers etc.)
//
// THE LADDER (posts/reels/carousels):        STORIES:
//   0-2h   -> every 15 min                     every 20 min while live
//   2-12h  -> every 1 h                        FINAL CAPTURE at ~23h  <- hard
//   12h-3d -> every 4 h                        deadline; gone from API at 24h
//   3d+    -> every 24 h, until stop_at
//
// stop rule: posted_at + 28 days (event-linked stop comes with Phase 8).
// Rate safety: reads Meta's x-app-usage header; >80% -> skip the cycle.
// All state lives in the DB — redeploys are harmless.
// ============================================================================

const axios = require('axios');

const { META_ACCESS_TOKEN, IG_USER_ID } = process.env;

const GRAPH = 'https://graph.facebook.com/v23.0';
const STORY_LIFETIME_MS = 24 * 60 * 60 * 1000;
const STORY_FINAL_AGE_MS = 23 * 60 * 60 * 1000;      // final capture deadline
const STORY_REFRESH_MS = 20 * 60 * 1000;
const POST_STOP_DAYS = 28;

let appUsageHigh = false;   // set from x-app-usage response headers

// ==================== GRAPH HELPERS ====================

async function graphGet(path, params = {}) {
  const url = `${GRAPH}${path}`;
  const res = await axios.get(url, {
    params: { ...params, access_token: META_ACCESS_TOKEN },
    timeout: 20000,
  });
  const usage = res.headers['x-app-usage'];
  if (usage) {
    try {
      const u = JSON.parse(usage);
      appUsageHigh = Math.max(u.call_count || 0, u.total_time || 0, u.total_cputime || 0) >= 80;
    } catch { /* header not JSON — ignore */ }
  }
  return res.data;
}

function msFromIso(iso) { return new Date(iso).getTime(); }

// ==================== 1. DETECTION ====================

const MEDIA_FIELDS = 'id,media_type,media_product_type,timestamp,permalink,caption,thumbnail_url,media_url,children{media_url,thumbnail_url,media_type}';

async function fetchRecentMedia() {
  const data = await graphGet(`/${IG_USER_ID}/media`, { fields: MEDIA_FIELDS, limit: 10 });
  return data.data || [];
}

async function fetchActiveStories() {
  const data = await graphGet(`/${IG_USER_ID}/stories`, { fields: 'id,media_type,timestamp,permalink,caption,thumbnail_url,media_url' });
  return (data.data || []).map(s => ({ ...s, media_product_type: 'STORY' }));
}

/** Best preview URL for an item: video -> thumbnail, image -> media_url,
 *  carousel -> first child. */
function previewUrl(m) {
  if (m.media_type === 'VIDEO') return m.thumbnail_url || m.media_url || null;
  if (m.media_type === 'CAROUSEL_ALBUM') {
    const child = m.children?.data?.[0];
    return child ? (child.media_type === 'VIDEO' ? child.thumbnail_url : child.media_url) : (m.media_url || null);
  }
  return m.media_url || m.thumbnail_url || null;
}

function initialRow(m, followerCount) {
  const postedAt = new Date(msFromIso(m.timestamp)).toISOString();
  const isStory = m.media_product_type === 'STORY';
  return {
    ig_media_id:            m.id,
    media_product:          m.media_product_type || 'FEED',
    media_type:             m.media_type || null,
    posted_at:              postedAt,
    expires_at:             isStory ? new Date(msFromIso(m.timestamp) + STORY_LIFETIME_MS).toISOString() : null,
    permalink:              m.permalink || null,
    caption:                m.caption || null,
    follower_count_at_post: followerCount ?? null,
    meta:                   {},
    next_refresh_at:        new Date().toISOString(),   // first insights fetch ASAP
    stop_at:                isStory
                              ? new Date(msFromIso(m.timestamp) + STORY_LIFETIME_MS).toISOString()
                              : new Date(msFromIso(m.timestamp) + POST_STOP_DAYS * 86400000).toISOString(),
    is_final:               false,
    updated_at:             new Date().toISOString(),
  };
}

async function detectNew(supabase, log) {
  const [media, stories] = await Promise.all([fetchRecentMedia(), fetchActiveStories()]);
  const seenItems = [...media, ...stories];
  if (seenItems.length === 0) return [];

  const ids = seenItems.map(m => m.id);
  const { data: existing, error } = await supabase
    .from('mktg_ig_stats').select('ig_media_id').in('ig_media_id', ids);
  if (error) throw new Error(`mktg_ig_stats read failed: ${error.message}`);
  const known = new Set((existing || []).map(r => r.ig_media_id));

  const fresh = seenItems.filter(m => !known.has(m.id));
  if (fresh.length === 0) return [];

  // Follower snapshot for the new rows
  let followers = null;
  try {
    const acc = await graphGet(`/${IG_USER_ID}`, { fields: 'followers_count' });
    followers = acc.followers_count ?? null;
  } catch (e) { log(`  ⚠️ follower fetch failed: ${e.message}`); }

  for (const m of fresh) {
    const row = initialRow(m, followers);
    const { error: insErr } = await supabase.from('mktg_ig_stats').insert(row);
    if (insErr) {
      if (insErr.code === '23505') continue;  // raced with a parallel cycle — fine
      log(`  ❌ insert failed for ${m.id}: ${insErr.message}`);
      continue;
    }
    log(`  🆕 ${row.media_product} ${row.media_type} ${m.id} (posted ${row.posted_at})`);

    // First-sight media capture — CDN URLs expire in hours, so do it NOW
    try {
      const url = previewUrl(m);
      if (url) {
        const path = await captureMedia(supabase, m.id, url);
        await supabase.from('mktg_ig_stats')
          .update({ meta: { preview_path: path }, updated_at: new Date().toISOString() })
          .eq('ig_media_id', m.id);
      }
    } catch (e) { log(`  ⚠️ media capture failed for ${m.id}: ${e.message}`); }
  }
  return fresh;
}

// ==================== 2. MEDIA CAPTURE ====================

async function captureMedia(supabase, mediaId, url) {
  const res = await axios.get(url, { responseType: 'arraybuffer', timeout: 30000, maxContentLength: 10 * 1024 * 1024 });
  const contentType = res.headers['content-type'] || 'image/jpeg';
  const ext = contentType.includes('png') ? 'png' : 'jpg';
  const path = `${mediaId}/preview.${ext}`;

  const { error } = await supabase.storage.from('ig-media')
    .upload(path, Buffer.from(res.data), { contentType, upsert: true });
  if (error) throw new Error(`storage upload failed: ${error.message}`);
  return path;
}

// ==================== 3. INSIGHTS REFRESH ====================

const METRICS = {
  STORY: 'views,reach,replies',
  FEED:  'views,reach,likes,comments,saved,shares,total_interactions',
  REELS: 'views,reach,likes,comments,saved,shares,total_interactions',
};

async function fetchInsights(mediaId, product) {
  const metric = METRICS[product] || METRICS.FEED;
  const data = await graphGet(`/${mediaId}/insights`, { metric });
  const out = {};
  for (const m of (data.data || [])) out[m.name] = m.values?.[0]?.value ?? null;
  return out;
}

/** Ladder: milliseconds until the next refresh, given the row's age. */
function nextInterval(row) {
  const ageMs = Date.now() - new Date(row.posted_at).getTime();
  if (row.media_product === 'STORY') return STORY_REFRESH_MS;
  if (ageMs < 2 * 3600000)  return 15 * 60000;
  if (ageMs < 12 * 3600000) return 60 * 60000;
  if (ageMs < 3 * 86400000) return 4 * 3600000;
  return 24 * 3600000;
}

async function refreshDue(supabase, log) {
  const { data: due, error } = await supabase
    .from('mktg_ig_stats')
    .select('ig_media_id, media_product, posted_at, stop_at, stats_timeline, reach, views, likes, comments, shares, saves, error_count')
    .eq('is_final', false)
    .lte('next_refresh_at', new Date().toISOString())
    .order('next_refresh_at')
    .limit(50);
  if (error) throw new Error(`due-rows read failed: ${error.message}`);
  if (!due || due.length === 0) return 0;

  let refreshed = 0;
  for (const row of due) {
    const ageMs = Date.now() - new Date(row.posted_at).getTime();
    const isStory = row.media_product === 'STORY';
    const pastStop = row.stop_at && Date.now() >= new Date(row.stop_at).getTime();
    // Story hard deadline: this fetch at >=23h is the FINAL capture.
    const finalize = (isStory && ageMs >= STORY_FINAL_AGE_MS) || (!isStory && pastStop);

    try {
      const ins = await fetchInsights(row.ig_media_id, row.media_product);

      const update = {
        views:    ins.views ?? null,
        reach:    ins.reach ?? null,
        likes:    ins.likes ?? null,
        comments: ins.comments ?? (isStory ? ins.replies ?? null : null),
        shares:   ins.shares ?? null,
        saves:    ins.saved ?? null,
        error_count: 0,
        updated_at: new Date().toISOString(),
      };

      // Timeline: short keys, append ONLY when something changed
      const point = { t: new Date().toISOString(), v: update.views, r: update.reach,
                      l: update.likes, c: update.comments, sh: update.shares, sv: update.saves };
      const timeline = Array.isArray(row.stats_timeline) ? row.stats_timeline : [];
      const last = timeline[timeline.length - 1];
      const changed = !last || ['v','r','l','c','sh','sv'].some(k => last[k] !== point[k]);
      if (changed) update.stats_timeline = [...timeline, point];

      if (finalize) {
        update.is_final = true;
        log(`  🏁 FINAL capture for ${row.media_product} ${row.ig_media_id}`);
      } else {
        update.next_refresh_at = new Date(Date.now() + nextInterval(row)).toISOString();
      }

      const { error: upErr } = await supabase.from('mktg_ig_stats')
        .update(update).eq('ig_media_id', row.ig_media_id);
      if (upErr) throw new Error(upErr.message);
      refreshed++;
    } catch (e) {
      // A story already past 24h returns errors forever — freeze it as final.
      const gone = isStory && ageMs >= STORY_LIFETIME_MS;
      log(`  ⚠️ insights failed for ${row.ig_media_id}: ${e.response?.data?.error?.message || e.message}${gone ? ' (story expired — finalizing)' : ''}`);
      await supabase.from('mktg_ig_stats').update({
        error_count: (row.error_count || 0) + 1,
        is_final: gone || (row.error_count || 0) + 1 >= 10,
        next_refresh_at: new Date(Date.now() + 30 * 60000).toISOString(),
        updated_at: new Date().toISOString(),
      }).eq('ig_media_id', row.ig_media_id);
    }
  }
  return refreshed;
}

// ==================== 4. DAILY ACCOUNT SNAPSHOT ====================

async function dailySnapshot(supabase, log) {
  const today = new Date().toISOString().slice(0, 10);
  const { data: existing } = await supabase
    .from('mktg_ig_account').select('snapshot_date').eq('snapshot_date', today).maybeSingle();
  if (existing) return;

  const acc = await graphGet(`/${IG_USER_ID}`, { fields: 'followers_count,follows_count,media_count' });
  let audience = null;
  try {
    const demo = await graphGet(`/${IG_USER_ID}/insights`, {
      metric: 'follower_demographics', period: 'lifetime',
      metric_type: 'total_value', breakdown: 'city',
    });
    audience = demo.data?.[0]?.total_value ?? null;
  } catch { /* demographics need >=100 followers per bucket; optional */ }

  const { error } = await supabase.from('mktg_ig_account').insert({
    snapshot_date: today,
    followers:     acc.followers_count ?? null,
    following:     acc.follows_count ?? null,
    media_count:   acc.media_count ?? null,
    audience,
  });
  if (error && error.code !== '23505') log(`  ⚠️ account snapshot failed: ${error.message}`);
  else if (!error) log(`  📸 Account snapshot: ${acc.followers_count} followers`);
}

// ==================== THE CYCLE ====================

async function runIgCycle(supabase, { log = console.log } = {}) {
  if (!META_ACCESS_TOKEN || !IG_USER_ID) {
    throw new Error('META_ACCESS_TOKEN / IG_USER_ID not set');
  }
  if (appUsageHigh) {
    log('  🐢 Meta app usage >= 80% — skipping this cycle to back off.');
    appUsageHigh = false;   // re-evaluated by the next cycle's first call
    return { skipped: true };
  }

  const fresh = await detectNew(supabase, log);
  const refreshed = await refreshDue(supabase, log);
  await dailySnapshot(supabase, log);
  return { skipped: false, detected: fresh.length, refreshed };
}

module.exports = { runIgCycle };
