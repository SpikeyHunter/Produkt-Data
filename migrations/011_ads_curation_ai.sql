-- ============================================================================
-- 011 — ADS MANUAL CURATION + AI · 2026-08-27
--
-- mktg_ads becomes a hand-picked set (like Add Content): the app browses the
-- ad account and imports only the ads worth tracking. AI suggests the event
-- link at import (suggest-only, same rule as content). Plus the Produkt AI
-- dashboard-insight cache (1h cooldown lives server-side).
-- ============================================================================

alter table public.mktg_ads add column if not exists match jsonb;
alter table public.mktg_ads add column if not exists needs_review boolean not null default false;

-- Recreate the view so the new columns (in a.*) are picked up
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

-- Produkt AI — per-project dashboard analysis, cached server-side
create table if not exists public.mktg_ai_insights (
  project_id    integer primary key,
  insight       text,
  model         text,
  context       jsonb,
  generated_at  timestamptz not null default now()
);

alter table public.mktg_ai_insights enable row level security;
drop policy if exists "authenticated read" on public.mktg_ai_insights;
create policy "authenticated read" on public.mktg_ai_insights
  for select to authenticated using (true);
