-- 014 — booking scraper cost controls · 2026-08-27
-- content_hash lets a nightly run skip the LLM entirely when a page hasn't
-- changed since the last successful scrape.
alter table public.mktg_booking_source add column if not exists content_hash text;
