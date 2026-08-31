-- 020 — sticky identity + multi-platform venues · 2026-08-31
--
-- Two corrections to 019, both found by adversarially re-checking it.
--
-- 1. A venue is not owned by one platform. SAT is RA venue 73778 AND evenko
--    slug "societe-des-arts-technologiques-sat" AND a Tixr group, all at once.
--    Keying the watchlist on (platform, source_slug) forces three rows for one
--    building and makes cross-source merging impossible.
--
-- 2. `fingerprint` cannot be the identity. It is derived from normalised
--    artist and venue names, so the moment name normalisation is improved
--    every key changes, every upsert misses, and the whole table duplicates.
--    That is not hypothetical: this project has already changed
--    normalizeArtistName twice. Identity is now a uuid, and each source's view
--    of an event is BOUND to it permanently the first time it is seen.

-- ---------- venues carry an id per platform ----------
alter table public.mktg_tracked_venue add column if not exists ra_venue_id      text;
alter table public.mktg_tracked_venue add column if not exists evenko_slug      text;
alter table public.mktg_tracked_venue add column if not exists tixr_group_slug  text;
alter table public.mktg_tracked_venue add column if not exists instagram_handle text;

-- Carry the existing RA ids across before the old column stops being read.
update public.mktg_tracked_venue
   set ra_venue_id = source_slug
 where platform = 'ra' and source_slug is not null and ra_venue_id is null;

create unique index if not exists mktg_tracked_venue_ra     on public.mktg_tracked_venue (ra_venue_id)     where ra_venue_id is not null;
create unique index if not exists mktg_tracked_venue_evenko on public.mktg_tracked_venue (evenko_slug)     where evenko_slug is not null;
create unique index if not exists mktg_tracked_venue_tixr   on public.mktg_tracked_venue (tixr_group_slug) where tixr_group_slug is not null;

-- ---------- one row per source's view of an event ----------
-- The binding table. Once RA event 2490782 is bound to an event, it stays
-- bound — renaming or renormalising anything can never orphan it.
create table if not exists public.mktg_event_observation (
  id              uuid primary key default gen_random_uuid(),
  event_id        uuid not null references public.mktg_event(id) on delete cascade,
  source          text not null,              -- ra | tixr | evenko | internal
  source_event_id text not null,
  source_url      text,
  payload         jsonb,                      -- what that source said, verbatim
  first_seen_at   timestamptz default now(),
  last_seen_at    timestamptz default now()
);

create unique index if not exists mktg_event_observation_source
  on public.mktg_event_observation (source, source_event_id);
create index if not exists mktg_event_observation_event
  on public.mktg_event_observation (event_id);

-- fingerprint stays, but as a MERGE HINT for first binding, not as identity.
-- Dropping the unique constraint is the point: two sources describing the same
-- night resolve through the observation table, and a changed normalisation
-- rule can no longer split a row in two.
alter table public.mktg_event drop constraint if exists mktg_event_fingerprint_key;
create index if not exists mktg_event_fingerprint on public.mktg_event (fingerprint);

alter table public.mktg_event_observation enable row level security;
drop policy if exists "service role all" on public.mktg_event_observation;
create policy "service role all" on public.mktg_event_observation
  for all to service_role using (true) with check (true);
drop policy if exists "authenticated read" on public.mktg_event_observation;
create policy "authenticated read" on public.mktg_event_observation
  for select to authenticated using (true);
