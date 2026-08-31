-- 022 — venue groups, renaming, and our own rooms · 2026-08-31
--
-- Three things the board needs that the flat watchlist could not express.
--
-- 1. GROUPS. evenko is one operator running five Montreal rooms. On the board
--    that should read as "evenko" with its rooms underneath, not as five
--    unrelated venues. Everything else stays independent.
--
-- 2. RENAMING. A source's name is the source's business — RA calls it
--    "Piknic Électronik / Parc Jean Drapeau". What the board shows is ours,
--    so display_name overrides it without breaking the id we match on.
--
-- 3. OUR OWN ROOMS. New City Gas and Bazart must come from the events table
--    and nowhere else. A third party's listing of our own show is never more
--    correct than our own record of it.

create table if not exists public.mktg_venue_group (
  id         uuid primary key default gen_random_uuid(),
  name       text not null unique,
  color      text,
  sort_order int not null default 0,
  created_at timestamptz default now()
);

alter table public.mktg_tracked_venue add column if not exists group_id     uuid references public.mktg_venue_group(id) on delete set null;
alter table public.mktg_tracked_venue add column if not exists display_name text;
alter table public.mktg_tracked_venue add column if not exists sort_order   int not null default 0;
-- For platform 'internal': which event_venue values in our events table are
-- this venue. An array because one room can be recorded under several names.
alter table public.mktg_tracked_venue add column if not exists internal_venues text[];

create index if not exists mktg_tracked_venue_group on public.mktg_tracked_venue (group_id);

alter table public.mktg_venue_group enable row level security;
drop policy if exists "service role all" on public.mktg_venue_group;
create policy "service role all" on public.mktg_venue_group
  for all to service_role using (true) with check (true);
drop policy if exists "authenticated all" on public.mktg_venue_group;
create policy "authenticated all" on public.mktg_venue_group
  for all to authenticated using (true) with check (true);
