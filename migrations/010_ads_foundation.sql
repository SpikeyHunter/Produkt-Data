-- ============================================================================
-- 010 — ADS FOUNDATION (Meta ad analytics + management) · 2026-08-27
--
--   mktg_ads       one row per Meta ad (campaign/adset denormalized, lifetime
--                  insights refreshed by Produkt-Data ads sync)
--   mktg_ads_full  view: ad -> resolved project -> event. An ad INHERITS the
--                  project of the IG post it boosts (via ig_media_id ->
--                  mktg_ig_stats.project_id) unless manually overridden.
--
-- project_id ONLY — never event_id. Money in account currency (CAD), dollars.
-- ============================================================================

create table if not exists public.mktg_ads (
  ad_id               text primary key,
  ad_name             text,
  campaign_id         text,
  campaign_name       text,
  campaign_objective  text,
  adset_id            text,
  adset_name          text,
  status              text,             -- effective_status: ACTIVE | PAUSED | ...
  created_time        timestamptz,
  start_time          timestamptz,
  stop_time           timestamptz,
  daily_budget        numeric,          -- dollars (Meta sends cents)
  lifetime_budget     numeric,
  currency            text default 'CAD',

  -- creative
  detected_format     text,             -- VIDEO | STATIC (sync heuristic)
  ad_format           text,             -- user override from the app (wins)
  ig_media_id         text,             -- creative.effective_instagram_media_id
  ig_permalink        text,
  thumbnail_url       text,             -- Meta CDN (expires; display fallback)

  -- lifetime insights (refreshed each sync cycle)
  spend               numeric,
  impressions         bigint,
  reach               bigint,
  clicks              bigint,
  link_clicks         bigint,
  landing_page_views  bigint,
  purchases           bigint,
  purchase_value      numeric,
  add_to_cart         bigint,
  initiate_checkout   bigint,
  post_engagement     bigint,
  video_thruplay      bigint,
  cpm                 numeric,
  ctr                 numeric,
  actions             jsonb,            -- raw actions array (display-only)
  insights_updated_at timestamptz,

  -- manual layer (Mac app)
  project_id          integer,          -- manual link, beats inherited
  extra_project_ids   integer[] not null default '{}',
  no_link             boolean not null default false,
  review              jsonb not null default '{}'::jsonb,  -- {brought, improve, notes}

  meta                jsonb not null default '{}'::jsonb,
  first_seen_at       timestamptz not null default now(),
  updated_at          timestamptz not null default now()
);

create index if not exists mktg_ads_campaign_idx on public.mktg_ads (campaign_id, created_time desc);
create index if not exists mktg_ads_project_idx  on public.mktg_ads (project_id);
create index if not exists mktg_ads_ig_idx       on public.mktg_ads (ig_media_id);
create index if not exists mktg_ads_status_idx   on public.mktg_ads (status);

drop view if exists public.mktg_ads_full;
create view public.mktg_ads_full
with (security_invoker = on) as
select
  a.*,
  ig.project_id                          as ig_project_id,
  ig.meta->>'preview_path'               as ig_preview_path,
  ig.media_product                       as ig_media_product,
  coalesce(a.project_id, ig.project_id)  as resolved_project_id,
  (a.project_id is null and ig.project_id is not null) as link_inherited,
  em.event_id,
  em.project_name                        as linked_project_name,
  em.is_branding                         as linked_is_branding,
  e.event_name,
  e.event_date,
  e.event_venue,
  e.event_status,
  e.event_flyer
from public.mktg_ads a
left join public.mktg_ig_stats ig      on ig.ig_media_id = a.ig_media_id
left join public.events_marketing em   on em.project_id  = coalesce(a.project_id, ig.project_id)
left join public.events e              on e.event_id     = em.event_id;

-- RLS: app users read + write the manual layer, service key (Render) owns sync
alter table public.mktg_ads enable row level security;

drop policy if exists "authenticated read" on public.mktg_ads;
create policy "authenticated read" on public.mktg_ads
  for select to authenticated using (true);

drop policy if exists "authenticated update" on public.mktg_ads;
create policy "authenticated update" on public.mktg_ads
  for update to authenticated using (true) with check (true);
