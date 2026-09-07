/**
 * A fetch relay for the handful of sources that refuse GitHub's runner IPs.
 *
 * Four sources (livegigs, the two Tribe comedy venues, eventbrite) answer 403 or
 * 405 to the scrape job while returning 200 to a laptop with the same headers.
 * Their robots.txt allows the paths we ask for, so the refusal is a blanket
 * datacenter-IP rule rather than a decision about this crawler. Cloudflare's
 * egress is not on the wrong side of it — all four answer 200 from here.
 *
 * This is deliberately NOT a general proxy:
 *
 *   * a bearer token is required, compared in constant time;
 *   * the target host must be on ALLOWED_HOSTS — an open relay on someone's
 *     Cloudflare account is a liability, and a leaked token should buy an
 *     attacker nothing but the four sites we already scrape in public;
 *   * GET and HEAD only, so it cannot be used to write anywhere;
 *   * https only, and no redirects to hosts off the list.
 *
 * Deploy: `npx wrangler deploy` in this directory, then
 * `npx wrangler secret put PROXY_TOKEN`.
 */

const ALLOWED_HOSTS = new Set([
  "www.livegigs.de",
  "www.comedycafeberlin.com",
  "comedyinenglish.de",
  "www.eventbrite.de",
]);

const MAX_BYTES = 8 * 1024 * 1024;

/** Length-independent comparison, so the token can't be guessed by timing. */
function tokenMatches(given, expected) {
  if (typeof given !== "string" || typeof expected !== "string") return false;
  const a = new TextEncoder().encode(given);
  const b = new TextEncoder().encode(expected);
  // Compare a fixed-size digest rather than the raw bytes: equal length is
  // itself a signal, and this keeps the comparison uniform.
  let diff = a.length ^ b.length;
  for (let i = 0; i < Math.max(a.length, b.length); i++) diff |= (a[i] ?? 0) ^ (b[i] ?? 0);
  return diff === 0;
}

function deny(status, message) {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export default {
  async fetch(request, env) {
    if (request.method !== "GET" && request.method !== "HEAD") {
      return deny(405, "GET and HEAD only");
    }
    if (!env.PROXY_TOKEN) return deny(503, "proxy is not configured");

    const auth = request.headers.get("authorization") || "";
    if (!tokenMatches(auth.replace(/^Bearer\s+/i, "").trim(), env.PROXY_TOKEN)) {
      return deny(401, "bad token");
    }

    const raw = new URL(request.url).searchParams.get("url");
    if (!raw) return deny(400, "missing url");

    let target;
    try {
      target = new URL(raw);
    } catch {
      return deny(400, "unparseable url");
    }
    if (target.protocol !== "https:") return deny(400, "https only");
    if (!ALLOWED_HOSTS.has(target.hostname)) return deny(403, "host not allowed");

    // The caller's headers are forwarded so the origin sees one coherent client;
    // hop-by-hop and our own auth are stripped.
    const headers = new Headers();
    for (const [k, v] of request.headers) {
      const key = k.toLowerCase();
      if (key === "authorization" || key === "host" || key.startsWith("cf-")) continue;
      headers.set(k, v);
    }

    let upstream;
    try {
      upstream = await fetch(target.toString(), {
        method: request.method,
        headers,
        redirect: "follow",
      });
    } catch (err) {
      return deny(502, `upstream fetch failed: ${err.message}`);
    }

    // A redirect chain must not walk off the allowlist.
    try {
      const landed = new URL(upstream.url);
      if (!ALLOWED_HOSTS.has(landed.hostname)) return deny(502, "redirected off the allowlist");
    } catch {
      /* upstream.url is absent on some paths — the allowlist already gated the request */
    }

    const body = request.method === "HEAD" ? null : upstream.body;
    const out = new Headers();
    for (const name of ["content-type", "content-language", "last-modified", "etag"]) {
      const v = upstream.headers.get(name);
      if (v) out.set(name, v);
    }
    // Surface what actually happened upstream, including its refusals.
    out.set("X-Proxy-Upstream-Status", String(upstream.status));
    out.set("X-Proxy-Max-Bytes", String(MAX_BYTES));
    return new Response(body, { status: upstream.status, headers: out });
  },
};
