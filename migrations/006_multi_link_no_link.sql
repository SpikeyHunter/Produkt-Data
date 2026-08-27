-- ============================================================================
-- 006 — MULTI-LINK + EXPLICIT NO-LINK · 2026-08-26
--
--   no_link            content deliberately left unlinked (multi-topic recap,
--                      unrelated post) — reviewed, not forgotten
--   extra_project_ids  additional projects beyond the primary project_id
--                      (e.g. a KARNAVALE post about 2 artists → both days)
--
-- View recreated so the new columns flow through mktg_ig_stats_full.
-- ============================================================================

alter table public.mktg_ig_stats
  add column if not exists no_link boolean not null default false;

alter table public.mktg_ig_stats
  add column if not exists extra_project_ids integer[] not null default '{}';

-- s.* gained columns, which shifts positions — a view can't be replaced with
-- reordered columns, so drop + recreate.
drop view if exists public.mktg_ig_stats_full;

create view public.mktg_ig_stats_full
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
