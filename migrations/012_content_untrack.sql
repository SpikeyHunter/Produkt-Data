-- ============================================================================
-- 012 — CONTENT UNTRACKING + AD DELETE · 2026-08-27
--
-- "Remove content tracking" deletes the row so the item drops out of the
-- tracker grid and becomes available again in "+ Add Content". Same for ads.
-- The captured media in the ig-media bucket is left alone — a re-import
-- reuses it instead of re-downloading.
-- ============================================================================

drop policy if exists "authenticated delete" on public.mktg_ig_stats;
create policy "authenticated delete" on public.mktg_ig_stats
  for delete to authenticated using (true);

drop policy if exists "authenticated delete" on public.mktg_ads;
create policy "authenticated delete" on public.mktg_ads
  for delete to authenticated using (true);
