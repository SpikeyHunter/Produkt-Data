-- ============================================================================
-- 002 — RENAME: events_ prefix on the order tables · 2026-08-24
-- Run in: Supabase Dashboard > SQL Editor (after 001).
--
--   orders       -> events_orders
--   order_items  -> events_order_items
--   tickets      -> events_tickets
--
-- tixr_sync_state and tixr_webhook_events keep their tixr_ prefix (they're
-- sync plumbing, not event data). RLS policies and the FK follow the rename
-- automatically; indexes are renamed below to match.
-- ============================================================================

alter table if exists public.orders      rename to events_orders;
alter table if exists public.order_items rename to events_order_items;
alter table if exists public.tickets     rename to events_tickets;

alter index if exists orders_event_purchase_idx  rename to events_orders_event_purchase_idx;
alter index if exists orders_user_idx            rename to events_orders_user_idx;
alter index if exists orders_status_idx          rename to events_orders_status_idx;

alter index if exists order_items_event_idx      rename to events_order_items_event_idx;
alter index if exists order_items_category_idx   rename to events_order_items_category_idx;

alter index if exists tickets_event_status_idx   rename to events_tickets_event_status_idx;
alter index if exists tickets_order_idx          rename to events_tickets_order_idx;
