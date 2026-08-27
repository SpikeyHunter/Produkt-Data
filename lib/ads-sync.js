//
// ads-sync.js — Meta Ads → mktg_ads
//
// Pulls every ad in the NCG ad account (campaign/adset denormalized) with
// LIFETIME insights, and upserts into mktg_ads. The manual layer written by
// the Mac app (project_id, extra_project_ids, no_link, ad_format, review) is
// NEVER touched here — upserts only write sync-owned columns.
//
// Ad→project linking is mostly automatic: the creative's
// effective_instagram_media_id joins to mktg_ig_stats in the mktg_ads_full
// view, so an ad inherits the project of the post it boosts. Manual links in
// the app override that.
//
// Money: Meta budgets come in CENTS, insights spend in DOLLARS. Everything is
// stored in dollars.
//

const axios = require('axios');

const { META_ACCESS_TOKEN, AD_ACCOUNT_ID } = process.env;

const GRAPH = 'https://graph.facebook.com/v23.0';

const AD_FIELDS = [
  'id', 'name', 'status', 'effective_status', 'created_time',
  'campaign{id,name,objective}',
  'adset{id,name,start_time,end_time,daily_budget,lifetime_budget}',
  'creative{id,object_type,thumbnail_url,image_url,video_id,effective_instagram_media_id,instagram_permalink_url}',
  'insights.date_preset(maximum){spend,impressions,reach,clicks,ctr,cpm,actions,action_values,video_thruplay_watched_actions}',
].join(',');

// ==================== GRAPH ====================

async function graphGet(path, params = {}) {
  const res = await axios.get(`${GRAPH}${path}`, {
    params: { access_token: META_ACCESS_TOKEN, ...params },
    timeout: 60_000,
  });
  const usage = res.headers['x-app-usage'];
  if (usage) {
    try {
      const u = JSON.parse(usage);
      const max = Math.max(u.call_count || 0, u.total_time || 0, u.total_cputime || 0);
      if (max >= 80) {
        await new Promise(r => setTimeout(r, 30_000));
      }
    } catch { /* unparseable header — ignore */ }
  }
  return res.data;
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

// ==================== SYNC ====================

function isRateLimit(err) {
  const e = err.response?.data?.error;
  return e?.code === 17 || e?.code === 80004 || /too many calls/i.test(e?.message || '');
}

// Full sweep = every ad ever (4k+, ~180 pages — hits the ad-account rate
// limit if run often). Incremental = ACTIVE ads only (the ones whose spend
// moves). The server runs incremental every 30 min and full once a day.
async function runAdsCycle(supabase, { full = false, log = console.log } = {}) {
  const account = await resolveAccountId();
  const url = `/${account}/ads`;
  let after = null;
  let total = 0;
  let pages = 0;

  while (true) {
    pages += 1;
    const params = { fields: AD_FIELDS, limit: 25 };
    if (!full) {
      params.filtering = JSON.stringify([
        { field: 'effective_status', operator: 'IN', value: ['ACTIVE', 'PENDING_REVIEW', 'WITH_ISSUES'] },
      ]);
    }
    if (after) params.after = after;

    let data = null;
    for (let attempt = 0; attempt < 3 && !data; attempt++) {
      try {
        data = await graphGet(url, attempt === 0 ? params : { ...params, limit: 10 });
      } catch (err) {
        const msg = err.response?.data?.error?.message || err.message;
        if (isRateLimit(err)) {
          if (attempt === 2) {
            // Give up gracefully — upserts are idempotent, next cycle resumes.
            log(`[ads] rate-limited, stopping at page ${pages} (${total} synced so far).`);
            return { total, pages, rateLimited: true };
          }
          log(`[ads] rate-limited on page ${pages} — waiting 70s`);
          await new Promise(r => setTimeout(r, 70_000));
        } else {
          log(`[ads] page ${pages} failed (${msg}) — retrying slim`);
        }
      }
    }
    if (!data) break;

    const rows = (data.data || []).map(projectAd);
    if (rows.length) {
      const { error } = await supabase.from('mktg_ads').upsert(rows, { onConflict: 'ad_id' });
      if (error) throw new Error(`mktg_ads upsert: ${error.message}`);
      total += rows.length;
    }

    after = data.paging?.next ? data.paging?.cursors?.after : null;
    if (!after) break;
  }

  log(`[ads] ${full ? 'FULL' : 'active'} cycle done — ${total} ads across ${pages} page(s)`);
  return { total, pages };
}

module.exports = { runAdsCycle };
