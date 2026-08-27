-- ============================================================================
-- 013 — ARTIST AVAILABILITY / BOOKING SCRAPER · 2026-08-27
--
-- Replaces the hand-written per-site scrapers with a recipe engine:
--
--   mktg_booking_source   one row per website to scrape. `recipe` holds the
--                         AI-authored extraction plan (mode + steps + hints +
--                         plain-English instructions). Venue/color/city are
--                         set by hand; the rest is detected.
--   mktg_booking          one row per scraped event, denormalized (artists as
--                         an array — no join table). `fingerprint` dedupes
--                         across re-runs.
--
-- Old booking_artist / booking_event are left untouched until the new table
-- is verified, then dropped in a later cleanup.
-- ============================================================================

create table if not exists public.mktg_booking_source (
  id            uuid primary key default gen_random_uuid(),
  name          text not null,              -- "Piknic Electronik"
  url           text not null,              -- entry point
  enabled       boolean not null default true,

  -- Set by hand in the app
  venue         text,                       -- "Piknic Electronik"
  location      text,                       -- "Parc Jean-Drapeau"
  city          text,
  country       text,
  color         text,                       -- hex tag color

  -- Extraction plan authored with Claude in the in-app browser
  -- { mode: api|html|browser, steps: [...], list_selector, field_hints: {...},
  --   api: { url, method, headers, body, json_path }, pricing_mode,
  --   instructions, detail: { follow_links, link_selector, limit } }
  recipe        jsonb not null default '{}'::jsonb,

  -- Sync state
  last_sync_at  timestamptz,
  last_status   text,                       -- ok | error | running
  last_error    text,
  last_count    integer not null default 0,

  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);

create index if not exists mktg_booking_source_enabled_idx on public.mktg_booking_source (enabled);

create table if not exists public.mktg_booking (
  id             uuid primary key default gen_random_uuid(),
  source_id      uuid references public.mktg_booking_source(id) on delete cascade,
  source_name    text,                      -- kept for legacy rows with no source

  -- Who / when / where
  artist_name    text,                      -- headline artist (first)
  artists        text[] not null default '{}',
  event_name     text,
  event_date     date,
  venue          text,
  location       text,
  city           text,
  country        text,

  -- Links + media
  url            text,
  flyer_url      text,

  -- Pricing (starting tier, or every tier when the recipe asks for it)
  price_min      numeric,
  price_max      numeric,
  currency       text,
  price_tiers    jsonb,                     -- [{ name, price, currency, sold_out }]

  -- Dedupe key: source + date + artist/name, lowercased
  fingerprint    text not null,

  raw            jsonb,                     -- what the extractor returned
  first_seen_at  timestamptz not null default now(),
  last_seen_at   timestamptz not null default now(),
  created_at     timestamptz not null default now(),
  updated_at     timestamptz not null default now()
);

create unique index if not exists mktg_booking_fingerprint_idx on public.mktg_booking (fingerprint);
create index if not exists mktg_booking_date_idx    on public.mktg_booking (event_date desc);
create index if not exists mktg_booking_artist_idx  on public.mktg_booking (lower(artist_name));
create index if not exists mktg_booking_source_idx  on public.mktg_booking (source_id, event_date desc);
create index if not exists mktg_booking_artists_idx on public.mktg_booking using gin (artists);

-- RLS: the app reads everything and manages sources; the service key writes
-- scraped rows.
alter table public.mktg_booking_source enable row level security;
alter table public.mktg_booking        enable row level security;

drop policy if exists "authenticated read" on public.mktg_booking_source;
create policy "authenticated read" on public.mktg_booking_source
  for select to authenticated using (true);
drop policy if exists "authenticated write" on public.mktg_booking_source;
create policy "authenticated write" on public.mktg_booking_source
  for all to authenticated using (true) with check (true);

drop policy if exists "authenticated read" on public.mktg_booking;
create policy "authenticated read" on public.mktg_booking
  for select to authenticated using (true);
drop policy if exists "authenticated delete" on public.mktg_booking;
create policy "authenticated delete" on public.mktg_booking
  for delete to authenticated using (true);
