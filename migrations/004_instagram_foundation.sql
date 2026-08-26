-- ============================================================================
-- 004 — INSTAGRAM FOUNDATION (Phases 5-7 tables, deferred from 001) · 2026-08-25
-- Run in: Supabase Dashboard > SQL Editor. Safe to re-run.
--
--   mktg_ig_stats        IG content + performance (one row per post/reel/story)
--   mktg_ig_account      daily account snapshots (followers, demographics)
--   mktg_ig_stats_full   view: stats -> project -> event (never hard-code event_id)
--
-- Real columns = anything filtered/sorted. JSONB = display-only detail.
-- project_id ONLY — never event_id (brand posts map to a project).
-- ============================================================================

create table if not exists public.mktg_ig_stats (
  ig_media_id            text primary key,
  project_id             integer,          -- -> events_marketing.project_id (Phase 8 fills this)
  media_product          text,             -- FEED | REELS | STORY
  media_type             text,             -- IMAGE | VIDEO | CAROUSEL_ALBUM
  content_category       text,             -- announce | teaser | recap | ... (AI, Phase 8)
  ai_name                text,             -- "Trym Presale", "PW26 Facts"    (AI, Phase 8)
  needs_review           boolean not null default false,
  posted_at              timestamptz not null,
  expires_at             timestamptz,      -- stories: posted_at + 24h
  permalink              text,
  caption                text,

  -- headline numbers (views, NOT deprecated impressions)
  reach                  integer,
  views                  integer,
  likes                  integer,
  comments               integer,
  shares                 integer,
  saves                  integer,
  follower_count_at_post integer,          -- snapshot at insert, never recomputed

  -- detail (display-only, never WHERE'd)
  meta                   jsonb not null default '{}'::jsonb,
  ai_features            jsonb,
  stats_timeline         jsonb not null default '[]'::jsonb,  -- append-only, short keys, only-on-change
  link                   jsonb,            -- YOURLS: keyword, clicks baseline/current (Phase 9)
  match                  jsonb,            -- candidates, confidence, reason      (Phase 8)

  -- scheduler (DB-driven so redeploys are harmless)
  next_refresh_at        timestamptz,
  stop_at                timestamptz,      -- event_date + 1d, else posted_at + 28d
  is_final               boolean not null default false,
  error_count            integer not null default 0,

  created_at             timestamptz not null default now(),
  updated_at             timestamptz not null default now()
);

create index if not exists ig_stats_refresh_idx  on public.mktg_ig_stats (next_refresh_at) where is_final = false;
create index if not exists ig_stats_project_idx  on public.mktg_ig_stats (project_id, posted_at desc);
create index if not exists ig_stats_category_idx on public.mktg_ig_stats (content_category, media_type);
create index if not exists ig_stats_review_idx   on public.mktg_ig_stats (needs_review) where needs_review = true;

create table if not exists public.mktg_ig_account (
  snapshot_date date primary key,
  followers     integer,
  following     integer,
  media_count   integer,
  audience      jsonb,               -- age/gender/city breakdowns
  created_at    timestamptz not null default now()
);

create or replace view public.mktg_ig_stats_full
with (security_invoker = on) as
select
  s.*,
  em.event_id,
  e.event_name,
  e.event_date,
  e.event_venue,
  e.event_status,
  e.event_flyer
from public.mktg_ig_stats s
left join public.events_marketing em on em.project_id = s.project_id
left join public.events e            on e.event_id    = em.event_id;

-- RLS: app users read, only the service key (Render) writes
alter table public.mktg_ig_stats   enable row level security;
alter table public.mktg_ig_account enable row level security;

drop policy if exists "authenticated read" on public.mktg_ig_stats;
create policy "authenticated read" on public.mktg_ig_stats   for select to authenticated using (true);
drop policy if exists "authenticated read" on public.mktg_ig_account;
create policy "authenticated read" on public.mktg_ig_account for select to authenticated using (true);

-- Review queue: the Mac app writes back project corrections (Phase 8/10)
drop policy if exists "authenticated update" on public.mktg_ig_stats;
create policy "authenticated update" on public.mktg_ig_stats
  for update to authenticated using (true) with check (true);
