-- 008 — shared ordering for brandings/groups · 2026-08-27
-- sort_order ASC (nulls last); new folders get min-1 so they land on top.
alter table public.events_marketing
  add column if not exists sort_order integer;
