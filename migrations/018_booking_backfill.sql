-- 018 — separate the archive from the upcoming shows · 2026-08-30
-- Sites keep their upcoming events and their archive in two different places,
-- so a source has detections of two kinds. Upcoming runs nightly; the archive
-- barely changes and is expensive to read (vision over every flyer), so it
-- runs only when it is due or asked for. This records when it last ran.
alter table public.mktg_booking_source
  add column if not exists backfill_at timestamptz;
