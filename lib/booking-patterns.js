//
// booking-patterns.js — the scraper's memory.
//
// Every website Charles teaches it is a lesson. Sites are not snowflakes:
// a Webflow promoter page looks like every other Webflow promoter page, a
// Squarespace one like every other Squarespace one, and half the event web
// is one of six site builders wearing different paint.
//
// So when a detection actually works, we keep it — keyed by the platform
// the page was built with. Next time a page with the same fingerprint shows
// up, those working recipes go into Claude's prompt as worked examples, and
// it proposes a detection that already knows where that platform hides its
// dates. The scraper gets better at new websites the more of them it sees,
// without anyone writing another line of site-specific code.
//

// ==================== PLATFORM FINGERPRINT ====================

// Ordered: the first match wins, so ticketing platforms (which tell us the
// most) are checked before the generic site builders they're embedded in.
const SIGNATURES = [
  { platform: 'tixr',          test: h => /tixr\.com|__TIXR|tixr-checkout/i.test(h) },
  { platform: 'eventbrite',    test: h => /eventbrite\.(com|ca)|eb-event|__SERVER_DATA__/i.test(h) },
  { platform: 'dice',          test: h => /dice\.fm|dice-event/i.test(h) },
  { platform: 'residentadvisor', test: h => /ra\.co|residentadvisor/i.test(h) },
  { platform: 'shotgun',       test: h => /shotgun\.live/i.test(h) },
  { platform: 'lepointdevente', test: h => /lepointdevente\.com/i.test(h) },
  { platform: 'ticketmaster',  test: h => /ticketmaster\.|livenation\./i.test(h) },
  { platform: 'seetickets',    test: h => /seetickets\./i.test(h) },
  { platform: 'webflow',       test: h => /webflow\.(io|com)|data-wf-page|w-dyn-item/i.test(h) },
  { platform: 'squarespace',   test: h => /squarespace\.com|Static\.SQUARESPACE_CONTEXT|sqs-block/i.test(h) },
  { platform: 'wix',           test: h => /wix\.com|wixstatic|_wixCssStates/i.test(h) },
  { platform: 'shopify',       test: h => /cdn\.shopify\.com|Shopify\.theme/i.test(h) },
  { platform: 'nextjs',        test: h => /__NEXT_DATA__|\/_next\//.test(h) },
  { platform: 'nuxt',          test: h => /__NUXT_DATA__|__NUXT__/.test(h) },
  { platform: 'wordpress',     test: h => /wp-content|wp-json|wp-includes/i.test(h) },
];

/**
 * What kind of website is this, and what does it hand us for free?
 * The traits matter as much as the platform — "has schema.org events" is the
 * single most useful thing to know before proposing a detection.
 */
function pageSignature(html = '', url = '') {
  const head = String(html).slice(0, 120_000);
  const platform = SIGNATURES.find(s => s.test(head))?.platform || 'generic';

  const traits = [];
  if (/application\/ld\+json/i.test(head)) {
    traits.push(/"@type"\s*:\s*"[^"]*event/i.test(head) ? 'schema-events' : 'schema-other');
  }
  if (/__NEXT_DATA__|__NUXT_DATA__/.test(head)) traits.push('embedded-json');
  // Image-only listings: lots of artwork, very little text around it.
  const images = (head.match(/<img\b/gi) || []).length;
  const text = head.replace(/<script[\s\S]*?<\/script>/gi, '').replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ').trim().length;
  if (images >= 12 && text / Math.max(images, 1) < 260) traits.push('flyer-heavy');
  if (/\$\s?\d|\d+\s?\$|CAD|prix|price/i.test(head)) traits.push('prices-on-page');

  let host = '';
  try { host = new URL(url).hostname.replace(/^www\./, ''); } catch { /* not a url */ }

  return { platform, traits, host };
}

// ==================== MEMORY ====================

/**
 * The detections that have worked before on this kind of site. Same platform
 * first (that's where the transferable knowledge is), then the best-performing
 * patterns overall so there's always something to learn from.
 */
async function recallPatterns(supabase, signature, { limit = 5 } = {}) {
  if (!supabase) return [];
  try {
    const { data, error } = await supabase
      .from('mktg_booking_pattern')
      .select('platform, host, traits, routes, notes, event_count, success_count')
      .or(`platform.eq.${signature.platform},platform.eq.generic`)
      .order('event_count', { ascending: false })
      .limit(limit * 2);
    if (error) return [];

    const rows = data || [];
    const samePlatform = rows.filter(r => r.platform === signature.platform);
    const others = rows.filter(r => r.platform !== signature.platform);
    // Never teach a site with its own recipe — that's memorising, not learning.
    return [...samePlatform, ...others]
      .filter(r => r.host !== signature.host)
      .slice(0, limit);
  } catch {
    return [];
  }
}

/**
 * A detection produced real events — remember how. Keyed per host so a site
 * that gets re-taught replaces its own lesson instead of stacking duplicates.
 */
async function rememberPattern(supabase, { source, signature, routes, eventCount }) {
  if (!supabase || !eventCount || !Array.isArray(routes) || !routes.length) return;
  try {
    // Only the shape is worth keeping — names, URLs and operator prose are
    // this site's business, not a transferable lesson.
    const shape = routes.map(r => ({
      name: r.name || 'Default',
      list_selector: r.list_selector || null,
      field_hints: r.field_hints || null,
      read_flyers: r.read_flyers === true,
      follow_links: r.follow_links === true,
      mode: r.mode || 'html',
      instructions: String(r.instructions || '').slice(0, 400),
    }));

    const { data: existing } = await supabase
      .from('mktg_booking_pattern')
      .select('id, success_count')
      .eq('platform', signature.platform)
      .eq('host', signature.host)
      .maybeSingle();

    const row = {
      platform: signature.platform,
      host: signature.host,
      traits: signature.traits,
      routes: shape,
      event_count: eventCount,
      notes: `${source.name}: ${routes.length} detection(s) → ${eventCount} events`,
      last_used_at: new Date().toISOString(),
    };

    if (existing?.id) {
      await supabase.from('mktg_booking_pattern')
        .update({ ...row, success_count: (existing.success_count || 0) + 1 })
        .eq('id', existing.id);
    } else {
      await supabase.from('mktg_booking_pattern').insert(row);
    }
  } catch { /* memory is an optimisation — never fail a sync over it */ }
}

module.exports = { pageSignature, recallPatterns, rememberPattern };
