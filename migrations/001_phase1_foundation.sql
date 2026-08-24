-- ============================================================================
-- PHASE 1 — FOUNDATION MIGRATION (Tixr only — Instagram phases come later)
-- Produkt Data Pipeline · 2026-08-21
--
-- Run in: Supabase Dashboard > SQL Editor (single paste, runs as one script)
-- Safe to re-run: every statement is IF NOT EXISTS / OR REPLACE / IF EXISTS.
--
-- What this does:
--   NEW ORDER PIPELINE
--     orders               order-level facts, money stored ONCE per order
--     order_items          one row per sale item (fixes the double-count bug)
--     tickets              one row per serial number (live door / check-ins)
--     tixr_sync_state      per-event sync switchboard (backfill flags, cursors)
--     tixr_webhook_events  append-only raw webhook log
--   SECURITY
--     RLS on all new tables + closes the public read hole on events_users
--   CLEANUP
--     Drops events_orders and events_sales (fresh start — deploy the updated
--     webhook-server.js right after running this)
--
-- KEPT untouched: events, events_marketing, events_users (data — only RLS added)
-- ============================================================================


-- ============================================================================
-- 1. ORDERS — order-level facts. Money lives HERE, exactly once per order.
--    All money columns are DOLLARS (Tixr Studio API returns decimal dollars).
-- ============================================================================
create table if not exists public.orders (
  order_id              text primary key,
  event_id              bigint not null,
  user_id               text,

  status                text,          -- COMPLETE | PENDING | CANCELED | REFUNDED | QUEUED | VOID
  order_type            text,          -- REGULAR | MASTER | CHILD
  order_source          text,          -- ONLINE | DOOR
  fulfillment_path      text,          -- IMMEDIATE | PAYMENT_PLAN | QUEUE
  purchase_date         timestamptz,
  refund_date           timestamptz,
  cancellation_date     timestamptz,

  -- money (order level)
  currency              text,
  exchange_rate         numeric,
  gross_sales           numeric(12,2),
  net                   numeric(12,2),
  total                 numeric(12,2),
  taxes                 numeric(12,2),
  fees                  numeric(12,2),
  credit_card_fees      numeric(12,2),
  delivery_fees         numeric(12,2),
  discount              numeric(12,2),

  -- buyer
  first_name            text,
  last_name             text,
  email                 text,
  opt_in                boolean,       -- legally-clean marketing list (respect it)
  opt_in_date           timestamptz,
  geo_city              text,
  geo_state             text,
  geo_country           text,
  geo_postal            text,
  geo_lat               double precision,
  geo_lng               double precision,

  -- attribution / channel
  ref_id                text,
  ref_type              text,
  referrer              text,
  seller_id             text,
  user_agent_type       text,
  card_type             text,

  -- sync bookkeeping
  last_transaction_type text,          -- last webhook transaction_type seen
  webhook_updated_at    timestamptz,   -- last time a webhook touched this order
  api_synced_at         timestamptz,   -- last time the Studio API confirmed this order
  created_at            timestamptz not null default now(),
  updated_at            timestamptz not null default now()
);

create index if not exists orders_event_purchase_idx on public.orders (event_id, purchase_date desc);
create index if not exists orders_user_idx           on public.orders (user_id);
create index if not exists orders_status_idx         on public.orders (event_id, status);


-- ============================================================================
-- 2. ORDER_ITEMS — one row per sale item. Item money is the ITEM's share only;
--    summing order_items.total for an event is now safe (no double counting).
-- ============================================================================
create table if not exists public.order_items (
  order_id     text   not null references public.orders(order_id) on delete cascade,
  sale_id      bigint not null,
  event_id     bigint not null,
  tier_id      bigint,
  name         text,
  category     text,               -- GA | VIP | TABLE_SERVICE | OUTLET | GUEST | ...
  quantity     integer not null default 0,
  net          numeric(12,2),
  total        numeric(12,2),
  tax          numeric(12,2),
  group_fee    numeric(12,2),
  delivery_fee numeric(12,2),
  hold_id      bigint,
  hold_label   text,
  primary key (order_id, sale_id)
);

create index if not exists order_items_event_idx    on public.order_items (event_id);
create index if not exists order_items_category_idx on public.order_items (event_id, category);


-- ============================================================================
-- 3. TICKETS — one row per serial number. This is what makes the live door
--    dashboard possible later (ticket webhook updates status here).
--    No FK to orders: a ticket webhook may arrive before the order row exists.
-- ============================================================================
create table if not exists public.tickets (
  serial_number     text primary key,
  order_id          text not null,
  sale_id           bigint,
  event_id          bigint not null,
  status            text,            -- IN_HAND | CHECKED_IN | CHECKED_OUT | VOID
  holder_first_name text,
  holder_last_name  text,
  checkin_time      timestamptz,
  checkin_agent     text,            -- agent_email from ticket webhook
  checkin_device    text,            -- device_name
  checkin_scanner   text,            -- LINEA | IOS | ...
  updated_at        timestamptz not null default now()
);

create index if not exists tickets_event_status_idx on public.tickets (event_id, status);
create index if not exists tickets_order_idx        on public.tickets (order_id);


-- ============================================================================
-- 4. TIXR_SYNC_STATE — per-event switchboard. THE RULES:
--    · event_status = 'LIVE'      -> always synced (webhook + API reconciliation);
--                                    a LIVE event with zero orders rows triggers
--                                    an automatic backfill (the safety net when
--                                    webhooks fail).
--    · past / non-live events     -> synced ONLY when you set backfill_requested
--                                    = true (manual switch, e.g. from the Mac app).
--    Cursors live in the DB so redeploys never lose position.
-- ============================================================================
create table if not exists public.tixr_sync_state (
  event_id           bigint primary key,
  backfill_requested boolean not null default false,
  backfill_done_at   timestamptz,
  orders_cursor      timestamptz,     -- newest purchase_date confirmed via API
  last_synced_at     timestamptz,
  last_webhook_at    timestamptz,
  error_count        integer not null default 0,
  last_error         text,
  updated_at         timestamptz not null default now()
);


-- ============================================================================
-- 5. TIXR_WEBHOOK_EVENTS — append-only raw log. Log first, project after.
--    Retention: purge rows older than ~90 days (Law 25) — cron in a later phase.
-- ============================================================================
create table if not exists public.tixr_webhook_events (
  id               bigint generated always as identity primary key,
  received_at      timestamptz not null default now(),
  entity           text not null,     -- order | ticket | event | fan-transfer
  event_id         bigint,
  order_id         text,
  transaction_type text,
  idempotency_key  text,              -- sha256(order_id : transaction_type : payload)
  payload          jsonb not null,
  processed_at     timestamptz,
  process_error    text
);

create unique index if not exists webhook_events_idem_idx
  on public.tixr_webhook_events (idempotency_key) where idempotency_key is not null;
create index if not exists webhook_events_received_idx on public.tixr_webhook_events (received_at desc);
create index if not exists webhook_events_order_idx    on public.tixr_webhook_events (order_id);


-- ============================================================================
-- 6. RLS — new tables: logged-in app users can READ, only the service key
--    (Render / GitHub Actions) can write. The anon key alone gets nothing.
-- ============================================================================
alter table public.orders              enable row level security;
alter table public.order_items         enable row level security;
alter table public.tickets             enable row level security;
alter table public.tixr_sync_state     enable row level security;
alter table public.tixr_webhook_events enable row level security;

drop policy if exists "authenticated read" on public.orders;
create policy "authenticated read" on public.orders          for select to authenticated using (true);
drop policy if exists "authenticated read" on public.order_items;
create policy "authenticated read" on public.order_items     for select to authenticated using (true);
drop policy if exists "authenticated read" on public.tickets;
create policy "authenticated read" on public.tickets         for select to authenticated using (true);
drop policy if exists "authenticated read" on public.tixr_sync_state;
create policy "authenticated read" on public.tixr_sync_state for select to authenticated using (true);
-- tixr_webhook_events: raw payloads contain PII -> service key only, no read policy.

-- Mac app needs to flip the manual-backfill switch (and create the row if missing):
drop policy if exists "authenticated write" on public.tixr_sync_state;
create policy "authenticated write" on public.tixr_sync_state
  for update to authenticated using (true) with check (true);
drop policy if exists "authenticated insert" on public.tixr_sync_state;
create policy "authenticated insert" on public.tixr_sync_state
  for insert to authenticated with check (true);


-- ============================================================================
-- 7. CLOSE THE PUBLIC READ HOLE ON events_users (127k rows of PII currently
--    readable by anyone holding the app's anon key). Data untouched.
--    Server keeps writing fine: Render + GitHub Actions use the service_role
--    key (verified in .env), which bypasses RLS.
-- ============================================================================
alter table public.events_users enable row level security;
drop policy if exists "authenticated read" on public.events_users;
create policy "authenticated read" on public.events_users for select to authenticated using (true);


-- ============================================================================
-- 8. FRESH START — drop the old order tables.
--    ⚠️ Deploy the updated webhook-server.js right after running this;
--    until then the /webhook/order endpoint will error (harmless — orders
--    are recovered by backfill, and nothing reads these tables).
-- ============================================================================
drop table if exists public.events_orders;
drop table if exists public.events_sales;
