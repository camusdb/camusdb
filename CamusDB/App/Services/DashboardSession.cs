/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;
using CamusDB.Core;

namespace CamusDB.App.Services;

/// <summary>
/// The one definition of what a dashboard route is and where the browser's session token lives.
///
/// <para>It exists as a shared type because two components must agree on it exactly: the controller
/// that issues the cookie, and the authentication middleware that accepts it. If those two lists
/// ever drifted apart the failure would be silent and serious — a path the middleware treats as a
/// dashboard route but the controller does not (or the reverse) either locks an operator out of a
/// working page, or extends cookie authentication somewhere it was never meant to reach.</para>
///
/// <para><b>Why the cookie is scoped at all.</b> Every other authenticated route accepts a bearer
/// token in the <c>Authorization</c> header and nothing else. A browser never attaches that header
/// on its own, so a cross-site form post cannot reach <c>/execute-sql-non-query</c>, <c>/insert</c>
/// or <c>/start-transaction</c> however it is crafted. A cookie is different: the browser attaches
/// it automatically. Accepting one on a write route would hand that protection away. So the cookie
/// is accepted here and only here, on routes that read and never write. <c>SameSite=Strict</c>
/// narrows the exposure further, but it is a second line, not the boundary.</para>
/// </summary>
public static class DashboardSession
{
    /// <summary>
    /// Name of the browser session cookie. It carries the same opaque token <c>/login</c> issues.
    ///
    /// <para>The <c>__Host-</c> prefix is not decoration: a browser refuses to store a cookie under it
    /// unless the cookie is <c>Secure</c>, has <c>Path=/</c>, and names no <c>Domain</c>. That makes the
    /// three properties this cookie depends on enforced by the browser rather than only asserted by the
    /// server, and it stops a sibling host on the same registrable domain from writing a cookie this
    /// host would then read.</para>
    ///
    /// <para>The consequence is worth stating: the dashboard must be reached over HTTPS, or over
    /// loopback, which browsers treat as a secure context. A plaintext connection to a remote address
    /// cannot hold this cookie, so sign-in there does not work — which is the intended outcome for a
    /// page that exchanges a password.</para>
    ///
    /// <para>Renaming it invalidates sessions in flight at upgrade. A signed-in operator signs in
    /// again, once.</para>
    /// </summary>
    public const string CookieName = "__Host-camus_session";

    /// <summary>Prefix of the dashboard's JSON endpoints.</summary>
    public const string ApiPrefix = "/v1/dashboard/";

    /// <summary>
    /// The dashboard page. Razor Pages serves the index at both <c>/</c> and <c>/Index</c>, so both
    /// are recognized — matching only the first would let <c>/Index</c> slip past the cookie rule.
    /// </summary>
    public const string DashboardPage = "/";
    public const string DashboardPageAlias = "/Index";

    /// <summary>
    /// The sign-in page. It is deliberately <b>not</b> <c>/login</c>: that path is already the JSON
    /// credential-exchange endpoint on <c>AuthController</c>, and a Razor page sharing it would make
    /// the route ambiguous and the exemption list wrong for one of the two.
    /// </summary>
    public const string SignInPage = "/SignIn";

    /// <summary>
    /// Whether <paramref name="path"/> is a dashboard route, and therefore may authenticate by
    /// cookie. Both the pages and the JSON endpoints qualify; nothing else does.
    /// </summary>
    public static bool IsDashboardRoute(string path) =>
        IsDashboardPage(path) || path.StartsWith(ApiPrefix, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Whether <paramref name="path"/> is a dashboard <b>page</b> rather than one of its endpoints.
    /// The two need different failure answers: a browser asking for a page must be redirected to
    /// sign in, while the page's own polling script needs a JSON status it can branch on.
    /// </summary>
    public static bool IsDashboardPage(string path) =>
        string.Equals(path, DashboardPage, StringComparison.Ordinal)
        || string.Equals(path, DashboardPageAlias, StringComparison.OrdinalIgnoreCase)
        || string.Equals(path, SignInPage, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Whether the dashboard may be served to a caller at <paramref name="remote"/>.
    ///
    /// <para>With authentication enabled the answer is always yes, and each panel is gated by the
    /// statement behind it. With authentication <b>disabled</b> there is no principal to gate on at
    /// all, and the dashboard would otherwise hand this node's configuration, metrics and database
    /// list to anyone who can reach the port — so it is restricted to loopback, the same rule the
    /// backup admin surface applies for the same reason.</para>
    ///
    /// <para>It lives here, beside the route list, because two callers must agree on it: the JSON
    /// endpoints and the Razor pages. Gating only the endpoints would still serve the page to the
    /// network, where it would render a shell of panels that all answer 403 — an outcome that looks
    /// like a broken dashboard rather than a closed one.</para>
    /// </summary>
    public static bool IsServedTo(CamusDBOptions options, IPAddress? remote) =>
        options.AuthenticationEnabled || (remote is not null && IPAddress.IsLoopback(remote));

    /// <summary>Message shown when <see cref="IsServedTo"/> refuses. It names the setting and nothing else.</summary>
    public const string NetworkRefusalMessage =
        "The dashboard over the network requires authentication_enabled to be turned on";

    /// <summary>
    /// Whether the sign-in page may be served without a credential. It always may — that is the page
    /// whose job is to obtain one — so this exists to name the exemption rather than bury it in a
    /// path comparison at the call site.
    /// </summary>
    public static bool IsAnonymousDashboardRoute(string path) =>
        string.Equals(path, SignInPage, StringComparison.OrdinalIgnoreCase)
        || string.Equals(path, "/v1/dashboard/login", StringComparison.OrdinalIgnoreCase)
        || string.Equals(path, "/v1/dashboard/logout", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The page the exception handler re-executes when a request fails
    /// (<c>app.UseExceptionHandler</c>). It renders a request id and nothing else.
    /// </summary>
    public const string ErrorPage = "/Error";

    /// <summary>
    /// Whether <paramref name="path"/> may be served with no credential at all, independently of the
    /// dashboard's cookie rule.
    ///
    /// <para>Only the error page qualifies. It needs its own answer because neither of the other two
    /// is right for it. Left in the public-user bucket, the exception handler's re-execution is itself
    /// refused, so a browser that hits a server fault receives a 401 JSON body instead of the page —
    /// and the access log records an authentication failure in place of the original 500, which points
    /// incident response at the wrong thing. Classified as a dashboard page instead, the failure answer
    /// becomes a redirect to sign-in, which hides a server fault behind a login prompt.</para>
    ///
    /// <para>Serving it anonymously is safe because the page carries no data: its whole content is the
    /// request id its own model reads from the trace identifier.</para>
    /// </summary>
    public static bool IsUnauthenticatedPage(string path) =>
        string.Equals(path, ErrorPage, StringComparison.OrdinalIgnoreCase);
}
