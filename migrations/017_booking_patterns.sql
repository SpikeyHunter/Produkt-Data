-- 017 — the scraper's memory · 2026-08-29
-- Every detection that actually produces events is kept, keyed by the
-- platform the page was built with (Webflow, Squarespace, Tixr…). When a new
-- site with the same fingerprint is analysed, these go into Claude's prompt
-- as worked examples, so it already knows where that kind of site hides its
-- dates. The scraper gets better at new websites the more of them it sees.
create table if not exists public.mktg_booking_pattern (
  id            uuid primary key default gen_random_uuid(),
  platform      text not null,             -- webflow | squarespace | tixr | generic…
  host          text,                      -- the site this lesson came from
  traits        jsonb default '[]'::jsonb, -- schema-events, flyer-heavy, prices-on-page…
  routes        jsonb not null,            -- the detection shapes that worked
  notes         text,
  event_count   int not null default 0,    -- how much it produced — the ranking signal
  success_count int not null default 1,
  last_used_at  timestamptz default now(),
  created_at    timestamptz default now()
);

create unique index if not exists mktg_booking_pattern_site
  on public.mktg_booking_pattern (platform, host);
create index if not exists mktg_booking_pattern_rank
  on public.mktg_booking_pattern (platform, event_count desc);

alter table public.mktg_booking_pattern enable row level security;

drop policy if exists "service role all" on public.mktg_booking_pattern;
create policy "service role all" on public.mktg_booking_pattern
  for all to service_role using (true) with check (true);

drop policy if exists "authenticated read" on public.mktg_booking_pattern;
create policy "authenticated read" on public.mktg_booking_pattern
  for select to authenticated using (true);
