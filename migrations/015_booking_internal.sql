-- ============================================================================
-- 015 — booking sources: internal venues + non-destructive deletes · 2026-08-27
--
-- 1. Removing a website must NOT delete its scraped history. The original
--    cascade wiped Evenko's events when its recipe was deleted.
-- 2. A source can now be `internal`: instead of scraping, it reads our own
--    events table (New City Gas), with an exclusion list for the rows that
--    aren't real shows (reservations, passes, templates…).
-- ============================================================================

alter table public.mktg_booking drop constraint if exists mktg_booking_source_id_fkey;
alter table public.mktg_booking
  add constraint mktg_booking_source_id_fkey
  foreign key (source_id) references public.mktg_booking_source(id) on delete set null;

alter table public.mktg_booking_source
  add column if not exists kind text not null default 'scrape';   -- scrape | internal
