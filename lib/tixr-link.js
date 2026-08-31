//
// tixr-link.js — the pricing join key, extracted from a redirect header.
//
// The problem this solves: an event discovered on one source has a ticket link
// pointing at a vendor, and we need that vendor's internal event id to ask for
// prices. For Tixr that id is normally read off the page — and tixr.com answers
// every server-side request from Render with a DataDome challenge. It is a
// client-fingerprint block, not an IP block, so retrying, rotating IPs or
// setting a browser User-Agent does not move it. Do not try.
//
// The way through is to never fetch tixr.com at all. A short link's FIRST HOP
// already names the destination in its Location header:
//
//   curl --max-redirs 0 https://link.produkt.ca/26-r3
//   -> 302 Location: https://tixr.com/e/200970
//
// The redirector is Cloudflare, not Tixr, so it answers us normally, and the
// id is in the header before any Tixr server is contacted. That is why this
// module stops the moment the host becomes tixr.com: following one hop further
// gains nothing and buys a 403 plus a fingerprint strike.
//
// Redirect chains are stable — a short link points where it points — so every
// resolution is cached permanently in the DB. A link is resolved once, ever.
//

const axios = require('axios');

// Deliberately terminal: a Location naming any of these is the answer, and
// fetching it would only hand us a bot challenge.
const NEVER_FETCH = /(^|\.)tixr\.com$/i;

// A bare http->https upgrade already costs one hop, and click-trackers add
// another before the real redirector is reached, so a cap of 2 truncates
// ordinary chains. Every hop is paid once ever, so the cap can afford to sit
// above the realistic worst case rather than at it.
const MAX_HOPS = 5;

const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
  + '(KHTML, like Gecko) Chrome/126.0 Safari/537.36';

// ==================== VENDORS ====================

const VENDORS = [
  [/(^|\.)tixr\.com$/i,           'tixr'],
  [/(^|\.)universe\.com$/i,       'universe'],
  [/(^|\.)ticketmaster\./i,       'ticketmaster'],
  [/(^|\.)livenation\./i,         'ticketmaster'],
  [/(^|\.)lepointdevente\.com$/i, 'lepointdevente'],
  [/(^|\.)eventim\./i,            'eventim'],
];

/** Which pricing adapter, if any, could read this host. */
function classifyVendor(host) {
  const h = String(host || '').replace(/^www\./i, '').toLowerCase();
  if (!h) return 'other';
  for (const [pattern, name] of VENDORS) if (pattern.test(h)) return name;
  return 'other';
}

// ==================== ID EXTRACTION ====================

// Both public shapes end in the same integer:
//   https://tixr.com/e/200970
//   https://tixr.com/groups/piknicelectronik/events/some-slug-200970
const SHORT_FORM = /\/e\/(\d+)/i;
const LONG_FORM  = /\/groups\/[^/]+\/events\/[^/?#]*?(\d+)(?:[/?#]|$)/i;

/**
 * The Tixr event id, or null. Only trusted on a tixr.com URL — a trailing
 * integer on some other vendor's path is that vendor's id, and treating it as
 * a Tixr one would silently price the wrong event.
 */
function tixrEventId(url) {
  if (!url) return null;
  let host, path;
  try {
    const u = new URL(String(url));
    host = u.hostname;
    path = u.pathname + u.search;
  } catch {
    return null;
  }
  if (!NEVER_FETCH.test(host.replace(/^www\./i, ''))) return null;

  const m = path.match(SHORT_FORM) || path.match(LONG_FORM);
  if (!m) return null;
  const id = Number(m[1]);
  return Number.isSafeInteger(id) && id > 0 ? id : null;
}

// ==================== RESOLUTION ====================

/**
 * A failure worth forgetting. Nothing here says the link is bad — it says we
 * never got an answer about it. Caching one of these would let a single bad
 * night mark a live link dead permanently, and nothing downstream would ever
 * know to ask again.
 */
function isTransient(status, err) {
  if (err) return true;                       // no HTTP answer at all
  return status === 408 || status === 429 || (status >= 500 && status <= 599);
}

/**
 * Walk a redirect chain by hand, one hop at a time, so the chain can be
 * stopped on the destination's NAME rather than on its response. axios'
 * own follower would fetch tixr.com to find out where it landed, which is
 * exactly the request that gets blocked.
 *
 * `error` is null ONLY when the walk reached a real endpoint. A chain that
 * was still redirecting when the cap ran out is a truncated answer, not a
 * negative one, and says so — otherwise it is indistinguishable from a link
 * that genuinely ends on a non-vendor page, and the cache would record "not
 * Tixr" about a link nobody ever finished following.
 *
 * @returns {{final_url, final_host, status, hops, vendor, tixr_event_id, error, transient}}
 */
async function resolveOnce(url, { timeout = 12_000, maxHops = MAX_HOPS } = {}) {
  const out = {
    final_url: url || null, final_host: null, status: null, hops: 0,
    vendor: 'other', tixr_event_id: null, error: null, transient: false,
  };

  let current;
  try {
    current = new URL(String(url));
  } catch {
    out.error = 'unparseable url';
    return out;
  }
  out.final_host = current.hostname.replace(/^www\./i, '');
  out.vendor = classifyVendor(out.final_host);

  // Already there. Costs zero requests, which is the whole point.
  if (NEVER_FETCH.test(out.final_host)) {
    out.tixr_event_id = tixrEventId(current.href);
    return out;
  }

  // A redirector that points at itself would otherwise just burn the hop
  // budget and report the same truncation as an honestly-too-long chain.
  const seen = new Set([current.href]);

  for (let hop = 0; hop < maxHops; hop++) {
    let res;
    try {
      res = await axios.get(current.href, {
        timeout,
        maxRedirects: 0,
        validateStatus: () => true,
        headers: { 'User-Agent': UA, 'Accept': 'text/html,*/*' },
        // A destination page can be megabytes of HTML we will never read; only
        // the headers matter here.
        responseType: 'stream',
      });
      res.data?.destroy?.();
    } catch (err) {
      out.error = (err.message || 'request failed').slice(0, 200);
      out.transient = true;
      return out;
    }

    out.status = res.status;
    const location = res.headers?.location;
    const redirecting = res.status >= 300 && res.status < 400;

    if (!redirecting) {
      out.transient = isTransient(res.status, null);
      if (out.transient) out.error = `upstream ${res.status}`;
      return out;
    }
    // A 3xx that names nowhere is a broken redirector, not a destination.
    if (!location) {
      out.error = `redirect ${res.status} without Location`;
      return out;
    }

    let next;
    try {
      next = new URL(location, current.href);
    } catch {
      out.error = `unparseable Location: ${String(location).slice(0, 120)}`;
      return out;
    }

    out.hops = hop + 1;
    out.final_url = next.href;
    out.final_host = next.hostname.replace(/^www\./i, '');
    out.vendor = classifyVendor(out.final_host);

    // The header named Tixr. That IS the answer — stop before the request
    // that would be challenged.
    if (NEVER_FETCH.test(out.final_host)) {
      out.tixr_event_id = tixrEventId(next.href);
      return out;
    }

    if (seen.has(next.href)) {
      out.error = `redirect loop at ${next.href.slice(0, 120)}`;
      return out;
    }
    seen.add(next.href);
    current = next;
  }

  // Still redirecting with the budget spent. The destination is unknown, and
  // saying so is the difference between "this link is not Tixr" and "nobody
  // ever found out".
  out.error = `redirect chain longer than ${maxHops} hops`;
  return out;
}

// ==================== CACHE ====================

/**
 * resolveOnce, memoised in mktg_link_resolution. A short link's destination
 * does not change, so re-resolving one is a request spent on an answer we
 * already have — and these are other people's redirectors.
 *
 * VERDICTS are cached, transient failures are not. A 404 is a fact about the
 * link and belongs in the table so it is not retried nightly forever. A
 * timeout, a DNS blip or a 503 is a fact about the network on one night;
 * writing it here would let a single bad run condemn a live link permanently,
 * with no way to tell those rows from real dead ones later.
 */
async function resolveCached(supabase, url, opts = {}) {
  if (!url) return null;
  const key = String(url).trim();

  const { data: hit, error: readErr } = await supabase
    .from('mktg_link_resolution')
    .select('source_url, final_url, final_host, status, hops, vendor, tixr_event_id, error')
    .eq('source_url', key)
    .maybeSingle();
  if (readErr) throw new Error(`link cache read: ${readErr.message}`);
  // source_url is dropped so a hit and a miss hand back the same shape.
  if (hit) {
    const { source_url, ...rest } = hit;
    return { ...rest, transient: false, cached: true };
  }

  const resolved = await resolveOnce(key, opts);
  if (resolved.transient) return { ...resolved, cached: false };

  const { error: writeErr } = await supabase
    .from('mktg_link_resolution')
    .upsert({
      source_url:    key,
      final_url:     resolved.final_url,
      final_host:    resolved.final_host,
      status:        resolved.status,
      hops:          resolved.hops,
      vendor:        resolved.vendor,
      tixr_event_id: resolved.tixr_event_id,
      error:         resolved.error,
      resolved_at:   new Date().toISOString(),
    }, { onConflict: 'source_url' });
  if (writeErr) throw new Error(`link cache write: ${writeErr.message}`);

  return { ...resolved, cached: false };
}

module.exports = { resolveOnce, resolveCached, tixrEventId, classifyVendor, NEVER_FETCH };
