-- 009 — resolve GROUP links in the content view · 2026-08-27
-- Content linked to an umbrella (group) has no event; expose the umbrella's
-- own name so the app can display the link.
drop view if exists public.mktg_ig_stats_full;

create view public.mktg_ig_stats_full
with (security_invoker = on) as
select
  s.*,
  em.event_id,
  em.project_name as linked_project_name,
  em.is_branding  as linked_is_branding,
  e.event_name,
  e.event_date,
  e.event_venue,
  e.event_status,
  e.event_flyer
from public.mktg_ig_stats s
left join public.events_marketing em on em.project_id = s.project_id
left join public.events e            on e.event_id    = em.event_id;
