-- 021 — resolved link cache · 2026-08-31
--
-- tixr.com answers every server-side request from Render with a DataDome
-- challenge, so the Tixr event id cannot be read off the page. It CAN be read
-- off a short link's first-hop Location header, because the redirector is
-- Cloudflare rather than Tixr and answers us normally.
--
-- That answer is worth storing permanently. A short link points where it
-- points: re-resolving one on every sync spends a request on something we
-- already know, against somebody else's redirector. Keyed on the source url
-- so the same link shared by two events is resolved once between them.
--
-- Failures are rows too, not absences. A link that 404s should be remembered
-- as dead rather than retried nightly forever; clearing the row is how a
-- retry is requested.

create table if not exists public.mktg_link_resolution (
  source_url    text primary key,
  final_url     text,
  final_host    text,
  status        int,
  hops          int not null default 0,
  -- tixr | universe | ticketmaster | lepointdevente | eventim | other
  vendor        text,
  -- The join key itself: null unless final_host is tixr.com.
  tixr_event_id bigint,
  error         text,
  resolved_at   timestamptz default now()
);

create index if not exists mktg_link_resolution_vendor on public.mktg_link_resolution (vendor);
create index if not exists mktg_link_resolution_tixr   on public.mktg_link_resolution (tixr_event_id)
  where tixr_event_id is not null;

alter table public.mktg_link_resolution enable row level security;
drop policy if exists "service role all" on public.mktg_link_resolution;
create policy "service role all" on public.mktg_link_resolution
  for all to service_role using (true) with check (true);
drop policy if exists "authenticated read" on public.mktg_link_resolution;
create policy "authenticated read" on public.mktg_link_resolution
  for select to authenticated using (true);
