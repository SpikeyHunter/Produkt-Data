-- 016 — hide individual booking events · 2026-08-27
-- Right-click → Hide: the row stays (fingerprint keeps re-scrapes from
-- resurrecting it as a "new" event) but leaves the board and the counts.
alter table public.mktg_booking add column if not exists hidden boolean not null default false;

drop policy if exists "authenticated update" on public.mktg_booking;
create policy "authenticated update" on public.mktg_booking
  for update to authenticated using (true) with check (true);
