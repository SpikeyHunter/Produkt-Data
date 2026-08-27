//
// ads-sync.js — Meta Ads → mktg_ads (MANUALLY CURATED)
//
// mktg_ads holds only ads the user imported from the app's "Add Ads" browser
// (same philosophy as Add Content — no 2015 archaeology). Pieces:
//
//   fetchAdsCatalog   browse the ad account (paged, tracked-flagged)
//   importAds         import selected ads: full fields + lifetime insights,
//                     capture a hi-res preview to storage, AI-suggest a link
//   refreshTrackedAds refresh insights for tracked ads (30-min active cycle,
//                     daily full) — batched id lookups, no account paging
//   countNewAds       untracked ads created in the last 60 days (badge)
//   fetchAdMedia      full-res media for the expanded viewer
//
// The manual layer (project_id, extras, no_link, ad_format, review) is never
// touched by sync. Money: budgets in CENTS→dollars, insights spend dollars.
//

const axios = require('axios');
const { classifyAd } = require('./ig-ai');
const { fetchMediaChildren } = require('./ig-sync');

const { META_ACCESS_TOKEN, AD_ACCOUNT_ID } = process.env;

const GRAPH = 'https://graph.facebook.com/v23.0';

const CREATIVE_FIELDS = 'creative.thumbnail_width(1080).thumbnail_height(1080)'
  + '{id,object_type,thumbnail_url,image_url,video_id,effective_instagram_media_id,instagram_permalink_url}';

const AD_FIELDS = [
  'id', 'name', 'status', 'effective_status', 'created_time',
  'campaign{id,name,objective}',
  'adset{id,name,start_time,end_time,daily_budget,lifetime_budget}',
  CREATIVE_FIELDS,
  'insights.date_preset(maximum){spend,impressions,reach,clicks,ctr,cpm,actions,action_values,video_thruplay_watched_actions}',
].join(',');

// ==================== GRAPH ====================

async function graphGet(path, params = {}) {
  const res = await axios.get(`${GRAPH}${path}`, {
    params: { access_token: META_ACCESS_TOKEN, ...params },
    timeout: 60_000,
  });
  return res.data;
}

function isRateLimit(err) {
  const e = err.response?.data?.error;
  return e?.code === 17 || e?.code === 80004 || /too many calls/i.test(e?.message || '');
}

let cachedAccountId = AD_ACCOUNT_ID || null;

async function resolveAccountId() {
  if (cachedAccountId) return cachedAccountId;
  const data = await graphGet('/me/adaccounts', { fields: 'id,name', limit: 5 });
  const first = data?.data?.[0];
  if (!first) throw new Error('No ad account visible to this token');
  cachedAccountId = first.id;   // "act_13497905"
  return cachedAccountId;
}

// ==================== TRANSFORMS ====================

function actionValue(actions, type) {
  const hit = (actions || []).find(a => a.action_type === type);
  return hit ? Number(hit.value) : null;
}

function centsToDollars(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n / 100 : null;
}

function projectAd(ad) {
  const insights = ad.insights?.data?.[0] || null;
  const actions = insights?.actions || null;
  const creative = ad.creative || {};

  const thruplay = insights?.video_thruplay_watched_actions?.[0]?.value;
  const isVideo = !!creative.video_id || (thruplay != null && Number(thruplay) > 0);

  return {
    ad_id: ad.id,
    ad_name: ad.name || null,
    campaign_id: ad.campaign?.id || null,
    campaign_name: ad.campaign?.name || null,
    campaign_objective: ad.campaign?.objective || null,
    adset_id: ad.adset?.id || null,
    adset_name: ad.adset?.name || null,
    status: ad.effective_status || ad.status || null,
    created_time: ad.created_time || null,
    start_time: ad.adset?.start_time || null,
    stop_time: ad.adset?.end_time || null,
    daily_budget: centsToDollars(ad.adset?.daily_budget),
    lifetime_budget: centsToDollars(ad.adset?.lifetime_budget),

    detected_format: isVideo ? 'VIDEO' : 'STATIC',
    ig_media_id: creative.effective_instagram_media_id || null,
    ig_permalink: creative.instagram_permalink_url || null,
    thumbnail_url: creative.image_url || creative.thumbnail_url || null,

    spend: insights?.spend != null ? Number(insights.spend) : null,
    impressions: insights?.impressions != null ? Number(insights.impressions) : null,
    reach: insights?.reach != null ? Number(insights.reach) : null,
    clicks: insights?.clicks != null ? Number(insights.clicks) : null,
    link_clicks: actionValue(actions, 'link_click'),
    landing_page_views: actionValue(actions, 'landing_page_view'),
    purchases: actionValue(actions, 'omni_purchase') ?? actionValue(actions, 'purchase'),
    purchase_value: actionValue(insights?.action_values, 'omni_purchase')
      ?? actionValue(insights?.action_values, 'purchase'),
    add_to_cart: actionValue(actions, 'add_to_cart'),
    initiate_checkout: actionValue(actions, 'initiate_checkout'),
    post_engagement: actionValue(actions, 'post_engagement'),
    video_thruplay: thruplay != null ? Number(thruplay) : null,
    cpm: insights?.cpm != null ? Number(insights.cpm) : null,
    ctr: insights?.ctr != null ? Number(insights.ctr) : null,
    actions: actions,
    insights_updated_at: insights ? new Date().toISOString() : null,

    updated_at: new Date().toISOString(),
  };
}

// ==================== CATALOG (Add Ads browser) ====================

async function fetchAdsCatalog(supabase, { after = null } = {}) {
  const account = await resolveAccountId();
  const params = {
    fields: 'id,name,effective_status,created_time,campaign{name},'
      + 'creative.thumbnail_width(512).thumbnail_height(512){thumbnail_url,image_url,video_id},'
      + 'insights.date_preset(maximum){spend}',
    limit: 24,
  };
  if (after) params.after = after;

  let data;
  try {
    data = await graphGet(`/${account}/ads`, params);
  } catch {
    data = await graphGet(`/${account}/ads`, { ...params, limit: 10 });
  }

  const raw = data.data || [];
  const ids = raw.map(a => a.id);
  const { data: existing } = await supabase
    .from('mktg_ads').select('ad_id').in('ad_id', ids);
  const tracked = new Set((existing || []).map(r => r.ad_id));

  const items = raw.map(a => ({
    id: a.id,
    name: a.name || 'Untitled',
    campaign: a.campaign?.name || null,
    status: a.effective_status || null,
    created_time: a.created_time || null,
    preview_url: a.creative?.image_url || a.creative?.thumbnail_url || null,
    is_video: !!a.creative?.video_id,
    spend: a.insights?.data?.[0]?.spend != null ? Number(a.insights.data[0].spend) : null,
    tracked: tracked.has(a.id),
  }));

  return {
    items,
    next: data.paging?.next ? data.paging?.cursors?.after : null,
  };
}

/** Untracked ads created in the last 60 days — the "N new ads" badge. */
async function countNewAds(supabase) {
  const account = await resolveAccountId();
  const since = Math.floor(Date.now() / 1000) - 60 * 86400;
  const data = await graphGet(`/${account}/ads`, {
    fields: 'id',
    limit: 500,
    filtering: JSON.stringify([
      { field: 'ad.created_time', operator: 'GREATER_THAN', value: since },
    ]),
  });
  const ids = (data.data || []).map(a => a.id);
  if (ids.length === 0) return 0;
  const { data: existing } = await supabase
    .from('mktg_ads').select('ad_id').in('ad_id', ids);
  return ids.length - (existing || []).length;
}

// ==================== PREVIEW CAPTURE ====================

/** Meta CDN thumbnails expire — capture the 1080px still once, forever. */
async function captureAdPreview(supabase, adId, url) {
  const res = await axios.get(url, {
    responseType: 'arraybuffer', timeout: 30000, maxContentLength: 10 * 1024 * 1024,
  });
  const contentType = res.headers['content-type'] || 'image/jpeg';
  const ext = contentType.includes('png') ? 'png' : 'jpg';
  const path = `ads/${adId}.${ext}`;
  const { error } = await supabase.storage.from('ig-media')
    .upload(path, Buffer.from(res.data), { contentType, upsert: true });
  if (error) throw new Error(`storage upload failed: ${error.message}`);
  return path;
}

async function ensurePreviews(supabase, ads, log) {
  for (const ad of ads) {
    const creative = ad.creative || {};
    const url = creative.image_url || creative.thumbnail_url;
    if (!url) continue;
    const { data: row } = await supabase
      .from('mktg_ads').select('meta').eq('ad_id', ad.id).single();
    const meta = row?.meta || {};
    if (meta.preview_path) continue;
    try {
      meta.preview_path = await captureAdPreview(supabase, ad.id, url);
      if (creative.video_id) meta.video_id = creative.video_id;
      await supabase.from('mktg_ads').update({ meta }).eq('ad_id', ad.id);
    } catch (err) {
      log(`  ⚠️ preview capture failed for ad ${ad.id}: ${err.message}`);
    }
  }
}

// ==================== BATCH FETCH ====================

/** Full ad objects for specific ids, chunked (nested insights are heavy). */
async function fetchAdsByIds(adIds, log = console.log) {
  const out = [];
  for (let i = 0; i < adIds.length; i += 10) {
    const chunk = adIds.slice(i, i + 10);
    try {
      const data = await graphGet('/', { ids: chunk.join(','), fields: AD_FIELDS });
      for (const id of chunk) if (data[id]) out.push(data[id]);
    } catch (err) {
      if (isRateLimit(err)) {
        log('  ⏳ ads rate-limited — waiting 70s');
        await new Promise(r => setTimeout(r, 70_000));
        i -= 10;   // retry this chunk
        continue;
      }
      // One-by-one salvage so a single broken ad can't sink the batch
      for (const id of chunk) {
        try {
          out.push(await graphGet(`/${id}`, { fields: AD_FIELDS }));
        } catch (e2) {
          log(`  ⚠️ ad ${id} fetch failed: ${e2.response?.data?.error?.message || e2.message}`);
        }
      }
    }
  }
  return out;
}

// ==================== IMPORT (user-selected) ====================

async function importAds(supabase, adIds, { log = console.log } = {}) {
  const unique = [...new Set(adIds)].slice(0, 50);
  if (unique.length === 0) return { imported: 0 };

  const ads = await fetchAdsByIds(unique, log);
  const rows = ads.map(projectAd);
  if (rows.length) {
    const { error } = await supabase.from('mktg_ads').upsert(rows, { onConflict: 'ad_id' });
    if (error) throw new Error(`mktg_ads upsert: ${error.message}`);
  }

  await ensurePreviews(supabase, ads, log);

  // AI link suggestion — only for ads that can't inherit from a linked post.
  for (const row of rows) {
    let inherited = false;
    if (row.ig_media_id) {
      const { data: ig } = await supabase
        .from('mktg_ig_stats').select('project_id').eq('ig_media_id', row.ig_media_id).single();
      inherited = !!ig?.project_id;
    }
    if (!inherited) {
      await classifyAd(supabase, row, { log });
    }
  }

  log(`[ads] imported ${rows.length} ad(s)`);
  return { imported: rows.length };
}

// ==================== REFRESH (tracked ads only) ====================

async function refreshTrackedAds(supabase, { activeOnly = true, log = console.log } = {}) {
  let query = supabase.from('mktg_ads').select('ad_id, status, meta');
  if (activeOnly) query = query.in('status', ['ACTIVE', 'PENDING_REVIEW', 'WITH_ISSUES']);
  const { data: trackedRows, error } = await query;
  if (error) throw new Error(`mktg_ads read: ${error.message}`);
  const ids = (trackedRows || []).map(r => r.ad_id);
  if (ids.length === 0) return { total: 0 };

  const ads = await fetchAdsByIds(ids, log);
  const rows = ads.map(projectAd);
  if (rows.length) {
    const { error: upErr } = await supabase.from('mktg_ads').upsert(rows, { onConflict: 'ad_id' });
    if (upErr) throw new Error(`mktg_ads upsert: ${upErr.message}`);
  }

  // Self-heal previews that failed at import
  const missing = new Set((trackedRows || []).filter(r => !r.meta?.preview_path).map(r => r.ad_id));
  if (missing.size > 0) {
    await ensurePreviews(supabase, ads.filter(a => missing.has(a.id)), log);
  }

  log(`[ads] refreshed ${rows.length}/${ids.length} tracked ad(s)${activeOnly ? ' (active)' : ''}`);
  return { total: rows.length };
}

// ==================== EXPANDED VIEWER MEDIA ====================

/** Full-res media for one ad. Boosted IG posts come back playable through
 *  the IG media path; ads-manager creatives get the captured/fresh still
 *  (Meta blocks ad video files for API download). */
async function fetchAdMedia(supabase, adId) {
  const { data: row, error } = await supabase
    .from('mktg_ads').select('ig_media_id, meta, thumbnail_url').eq('ad_id', adId).single();
  if (error) throw new Error(`mktg_ads read: ${error.message}`);

  if (row.ig_media_id) {
    try {
      return await fetchMediaChildren(row.ig_media_id);
    } catch { /* dark post whose media id isn't readable — fall through */ }
  }

  // Fresh hi-res still from the creative (CDN links expire)
  try {
    const data = await graphGet(`/${adId}`, { fields: CREATIVE_FIELDS });
    const creative = data.creative || {};
    const url = creative.image_url || creative.thumbnail_url;
    if (url) return [{ index: 0, type: 'IMAGE', url, thumbnail: url }];
  } catch { /* fall through to stored preview */ }

  return [];
}

module.exports = { fetchAdsCatalog, countNewAds, importAds, refreshTrackedAds, fetchAdMedia };
