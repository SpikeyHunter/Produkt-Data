-- 023 — promoters as first-class · 2026-08-31
--
-- A venue and a promoter are different things and the board has to say which
-- is which. MTELUS is a room; evenko is who books it. New City Gas and Bazart
-- are rooms; Produkt books them. The old "group" was really a promoter all
-- along, so it is renamed rather than duplicated.
alter table if exists public.mktg_venue_group rename to mktg_promoter;
alter table public.mktg_tracked_venue rename column group_id to promoter_id;

alter table public.mktg_promoter add column if not exists is_ours boolean not null default false;
alter table public.mktg_promoter add column if not exists notes text;

-- A venue with no promoter is an independent room, which is a real state and
-- not a missing value.
comment on column public.mktg_tracked_venue.promoter_id is
  'Who books this room. NULL means independent.';
