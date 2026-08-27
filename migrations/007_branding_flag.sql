-- 007 — explicit Branding flag · 2026-08-27
-- A Branding is a manually created top-level folder that contains GROUPS
-- (which contain projects). The flag distinguishes an empty branding from an
-- empty group.
alter table public.events_marketing
  add column if not exists is_branding boolean not null default false;
