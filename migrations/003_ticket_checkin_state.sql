-- ============================================================================
-- 003 — events_tickets.checkin_state · 2026-08-24
-- Run in: Supabase Dashboard > SQL Editor.
--
-- Two write sources, two columns, no clobbering:
--   status         <- order API ticket status (written by the 15-min sweep,
--                     e.g. VALID / CANCELED / UPGRADED)
--   checkin_state  <- ticket WEBHOOK action (live door: CHECKED_IN,
--                     CHECKED_OUT, UNDO_CHECK_IN, VOID, ...)
-- Dashboards read checkin_state for attendance, status for validity.
-- ============================================================================

alter table public.events_tickets add column if not exists checkin_state text;

create index if not exists events_tickets_checkin_idx
  on public.events_tickets (event_id, checkin_state);
