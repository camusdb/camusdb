/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Security.Cryptography;
using CamusDB.App.Services;

namespace CamusDB.App.Middleware;

/// <summary>
/// Sets browser security headers on the HTML pages this host serves, and nothing else.
///
/// <para><b>Why it is scoped to pages.</b> A Content-Security-Policy governs what a browser will load
/// for a document; it means nothing on a JSON response, which no browser treats as a document. Sending
/// one on the whole API surface would be inert today and a source of breakage later, when a policy
/// written for the dashboard is quietly inherited by an endpoint nobody re-checked.</para>
///
/// <para><b>Why a nonce and not <c>'unsafe-inline'</c>.</b> Two pages carry an inline script — the
/// theme stamp that has to run before the first paint, and the sign-in form's handler. Allowing inline
/// script wholesale would give up most of what the policy is for, since injected script is inline
/// script. Each request gets a fresh random nonce in <c>HttpContext.Items["csp-nonce"]</c>, the layouts
/// echo it onto those two tags, and everything else inline is refused.</para>
///
/// <para>Inline <c>style</c> attributes are a different case: a nonce cannot cover them, so the two the
/// dashboard used were moved into the stylesheet rather than weakening <c>style-src</c>.</para>
/// </summary>
public sealed class SecurityHeadersMiddleware
{
    /// <summary>Key under which the per-request nonce is published to the Razor layouts.</summary>
    public const string NonceItemKey = "csp-nonce";

    private readonly RequestDelegate next;

    public SecurityHeadersMiddleware(RequestDelegate next) => this.next = next;

    public async Task Invoke(HttpContext context)
    {
        if (!IsHtmlPage(context.Request.Path.Value ?? ""))
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        // 16 random bytes: the nonce only has to be unguessable for the life of one response, and
        // base64 of that width is what the CSP specification recommends.
        string nonce = Convert.ToBase64String(RandomNumberGenerator.GetBytes(16));
        context.Items[NonceItemKey] = nonce;

        IHeaderDictionary headers = context.Response.Headers;

        headers["Content-Security-Policy"] =
            "default-src 'self'; " +
            $"script-src 'self' 'nonce-{nonce}'; " +
            "style-src 'self'; " +
            "img-src 'self' data:; " +
            "font-src 'self'; " +
            "connect-src 'self'; " +
            // Both bar this page from being framed. frame-ancestors is the one browsers honor now;
            // X-Frame-Options below is for anything that still reads only the older header.
            "frame-ancestors 'none'; " +
            "base-uri 'self'; " +
            "form-action 'self'";

        headers["X-Frame-Options"] = "DENY";
        headers["X-Content-Type-Options"] = "nosniff";
        headers["Referrer-Policy"] = "same-origin";

        await next(context).ConfigureAwait(false);
    }

    /// <summary>
    /// Whether this path serves an HTML document rather than JSON.
    ///
    /// <para>The list is the dashboard's own pages plus the error page. It is written out rather than
    /// derived from the response content type because these headers must be set <em>before</em> the
    /// response starts, and the content type is not known until after the endpoint runs.</para>
    /// </summary>
    private static bool IsHtmlPage(string path)
        => DashboardSession.IsDashboardPage(path) || DashboardSession.IsUnauthenticatedPage(path);
}
