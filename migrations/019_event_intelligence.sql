-- 019 — Event Intelligence: source-first rebuild · 2026-08-31
--
-- Replaces per-site DOM recipes with a tiered, source-first model. The old
-- mktg_booking* tables stay untouched for now so nothing is lost during the
-- cutover; they are retired in a later migration once this is proven.
--
-- Two rules from the plan are enforced here by SHAPE, not by convention:
--   * "past vs upcoming" is never stored. There is no is_past column — it is
--     a comparison against event_date at read time, so it can never go stale.
--   * an event discovered on one source and priced on another is ONE row.
--     `fingerprint` is the merge key; discovery and pricing provenance are
--     recorded separately on that same row.

-- ---------- the watchlist ----------
create table if not exists public.mktg_tracked_venue (
  id                 uuid primary key default gen_random_uuid(),
  name               text not null,
  -- Where this venue's events are DISCOVERED: ra | tixr | evenko | instagram | ai_fallback
  platform           text not null default 'ra',
  source_slug        text,                       -- RA venue id, evenko slug, Tixr group
  source_url         text,
  -- Where PRICE comes from, when that is a different platform than discovery.
  ticketing_platform text,
  city               text default 'Montreal',
  country            text default 'Canada',
  color              text,                       -- tag colour on the board
  enabled            boolean not null default true,
  notes              text,
  last_sync_at       timestamptz,
  last_status        text,
  last_error         text,
  last_count         int not null default 0,
  created_at         timestamptz default now(),
  updated_at         timestamptz default now()
);

-- Plain (not partial): ON CONFLICT cannot target a partial index through
-- PostgREST. Postgres treats NULLs as distinct, so a hand-added venue with no
-- slug is still permitted.
create unique index if not exists mktg_tracked_venue_source
  on public.mktg_tracked_venue (platform, source_slug);

-- ---------- the events ----------
create table if not exists public.mktg_event (
  id               uuid primary key default gen_random_uuid(),

  -- The merge key: artist set + date + venue, normalised. One real-world
  -- event is one row no matter how many sources describe it.
  fingerprint      text not null unique,

  title            text,
  artists          text[] not null default '{}',
  artist_name      text,
  lineup_text      text,
  event_date       date not null,
  start_time       timestamptz,
  end_time         timestamptz,

  venue_name       text,
  venue_id         uuid references public.mktg_tracked_venue(id) on delete set null,
  city             text,
  country          text,
  flyer_url        text,

  -- provenance: which tier found it, and where
  discovery_source text not null,
  discovery_url    text,
  source_event_id  text,

  -- pricing, attached by whichever tier could actually read it
  price_tiers      jsonb,
  price_min        numeric,
  price_max        numeric,
  currency         text,
  price_source     text,
  door_price_text  text,                        -- RA's free-text door price

  genres           text[] default '{}',
  promoters        text[] default '{}',
  minimum_age      int,
  is_ticketed      boolean,

  content_hash     text,                        -- skip re-extraction when unchanged
  last_validated   timestamptz default now(),
  hidden           boolean not null default false,
  raw              jsonb,

  created_at       timestamptz default now(),
  updated_at       timestamptz default now()
);

-- The board reads by date window constantly; the sync reads by source id.
create index if not exists mktg_event_date       on public.mktg_event (event_date desc);
create index if not exists mktg_event_venue      on public.mktg_event (venue_id, event_date desc);
create index if not exists mktg_event_source     on public.mktg_event (discovery_source, source_event_id);
create index if not exists mktg_event_artist     on public.mktg_event using gin (artists);
create index if not exists mktg_event_visible    on public.mktg_event (event_date desc) where hidden = false;

-- ---------- recipes (tier 2 + tier 5 only) ----------
-- Tiers 1 and 3 are structured APIs and need none of this. Only the pages we
-- genuinely have to read carry a recipe, and only those self-heal.
create table if not exists public.mktg_scrape_recipe (
  id             uuid primary key default gen_random_uuid(),
  domain         text not null unique,
  tier           text not null default 'tixr',
  selectors      jsonb not null default '{}'::jsonb,
  strategy       text,                          -- jsonld | state_blob | selectors | ai
  failure_count  int not null default 0,
  success_count  int not null default 0,
  last_validated timestamptz,
  last_error     text,
  created_at     timestamptz default now(),
  updated_at     timestamptz default now()
);

-- ---------- RLS ----------
alter table public.mktg_tracked_venue enable row level security;
alter table public.mktg_event         enable row level security;
alter table public.mktg_scrape_recipe enable row level security;

do $$
declare t text;
begin
  foreach t in array array['mktg_tracked_venue','mktg_event','mktg_scrape_recipe'] loop
    execute format('drop policy if exists "service role all" on public.%I', t);
    execute format('create policy "service role all" on public.%I for all to service_role using (true) with check (true)', t);
    execute format('drop policy if exists "authenticated read" on public.%I', t);
    execute format('create policy "authenticated read" on public.%I for select to authenticated using (true)', t);
    execute format('drop policy if exists "authenticated write" on public.%I', t);
    execute format('create policy "authenticated write" on public.%I for all to authenticated using (true) with check (true)', t);
  end loop;
end $$;
