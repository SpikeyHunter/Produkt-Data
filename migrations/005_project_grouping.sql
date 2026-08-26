-- ============================================================================
-- 005 — PROJECT GROUPING (umbrella projects / brand campaigns) · 2026-08-26
-- Run in: Supabase Dashboard > SQL Editor. Safe to re-run.
--
-- An UMBRELLA project has a project_name and no event — e.g. "KARNAVALE"
-- regrouping the Oct 30 + Oct 31 shows. Child projects point at it via
-- parent_project_id. Festival-level content/ads link to the umbrella; each
-- day's content links to its own project. Dashboards aggregate the family.
-- ============================================================================

alter table public.events_marketing
  add column if not exists parent_project_id integer
    references public.events_marketing(project_id) on delete set null;

alter table public.events_marketing
  add column if not exists project_name text;

create index if not exists events_marketing_parent_idx
  on public.events_marketing (parent_project_id);
