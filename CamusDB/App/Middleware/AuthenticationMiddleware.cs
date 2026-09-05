/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.App.Services;

namespace CamusDB.App.Middleware;

/// <summary>
/// Transport-wide authentication and coarse authorization for every HTTP route, so authentication is a
/// single boundary rather than a per-controller opt-in. When <see cref="CamusDBOptions.AuthenticationEnabled"/>
/// is off it is a pass-through.
///
/// <para>When on, it rejects any request to a data / DDL / transaction route that lacks a valid bearer
/// token (401), closing the legacy-JSON-API bypass where <c>/query</c>, <c>/insert</c>,
/// <c>/create-db</c>, <c>/start-transaction</c>, etc. reached <c>CommandExecutor</c> unauthenticated.
/// It then publishes the resolved <see cref="Principal"/> to the ambient
/// <see cref="AuthorizationContext"/> together with the route's required privilege, so the per-table
/// check in <c>TableOpener.Open</c> enforces authorization on the legacy data routes too — the
/// <see cref="AsyncLocal{T}"/> value flows down through the endpoint into the executor. The SQL routes
/// (<c>/execute-sql-*</c>) additionally run their own statement-level gate, which overwrites the ambient
/// with the exact per-statement privilege.</para>
///
/// <para>Node-to-node cluster forwarding (<c>/internal/*</c>) is authenticated by a shared
/// <see cref="CamusDBOptions.NodeSecret"/> header, not a public user token; with auth on and no node
/// secret configured those routes are refused (fail-closed).</para>
/// </summary>
public sealed class AuthenticationMiddleware
{
    private readonly RequestDelegate next;

    // Open to everyone: liveness and the credential-exchange endpoints (which validate their own input).
    // The gRPC entries are the CamusAuth service's method paths — this middleware sits in front of the
    // gRPC endpoints too, so without them a client could never obtain the token those endpoints demand.
    // The proto declares no package, so a method path is "/{service}/{method}".
    private static readonly HashSet<string> Exempt = new(StringComparer.OrdinalIgnoreCase)
    {
        "/ping", "/health", "/login", "/logout",
        "/CamusAuth/Login", "/CamusAuth/Logout",
    };

    // Database lifecycle requires the superuser attribute.
    private static readonly HashSet<string> SuperuserRoutes = new(StringComparer.OrdinalIgnoreCase)
    {
        "/create-db", "/drop-db",
    };

    // Legacy data routes → the privilege the per-table check must require (enforced at TableOpener).
    private static readonly Dictionary<string, Privilege> DataRoutes = new(StringComparer.OrdinalIgnoreCase)
    {
        ["/query"] = Privilege.Select,
        ["/query-by-id"] = Privilege.Select,
        ["/insert"] = Privilege.Insert,
        ["/update"] = Privilege.Update,
        ["/delete"] = Privilege.Delete,
        ["/create-table"] = Privilege.CreateTable,
    };

    public AuthenticationMiddleware(RequestDelegate next) => this.next = next;

    public async Task Invoke(HttpContext context, CommandExecutor executor, CamusDBOptions options)
    {
        if (!options.AuthenticationEnabled)
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        string path = context.Request.Path.Value ?? "";

        if (Exempt.Contains(path))
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        // The dashboard's sign-in page and its credential-exchange endpoints must be reachable
        // without a credential, for the same reason /login is: they are how a browser obtains one.
        if (DashboardSession.IsAnonymousDashboardRoute(path))
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        // The error page renders a request id and nothing else, and refusing it would answer a server
        // fault with an authentication failure. DashboardSession.IsUnauthenticatedPage holds the rest.
        if (DashboardSession.IsUnauthenticatedPage(path))
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        // Node-to-node forwarding: authenticate the peer by shared secret, not a user token.
        if (path.StartsWith("/internal/", StringComparison.OrdinalIgnoreCase))
        {
            if (!NodeSecretMatches(context, options.NodeSecret))
            {
                await WriteErrorAsync(context, CamusDBErrorCodes.AuthenticationFailed, "Node authentication required").ConfigureAwait(false);
                return;
            }
            await next(context).ConfigureAwait(false);
            return;
        }

        // Every other route is a public user route: require a valid bearer token over a secure transport.
        bool dashboardRoute = DashboardSession.IsDashboardRoute(path);

        Principal principal;
        try
        {
            EnsureSecureTransport(context, options);
            string? bearer = ExtractBearer(context) ?? (dashboardRoute ? ExtractSessionCookie(context) : null);
            principal = await executor.ResolvePrincipalAsync(bearer).ConfigureAwait(false);
        }
        catch (CamusDBException e)
        {
            // A browser asking for a page cannot act on a JSON error body — send it to sign in
            // instead. The page's polling endpoints keep the JSON answer their script branches on.
            if (DashboardSession.IsDashboardPage(path))
            {
                context.Response.Redirect(DashboardSession.SignInPage);
                return;
            }

            await WriteErrorAsync(context, e.Code, e.Message).ConfigureAwait(false);
            return;
        }

        // Coarse authorization for the non-SQL routes.
        if (SuperuserRoutes.Contains(path) && !principal.IsSuperuser)
        {
            await WriteErrorAsync(context, CamusDBErrorCodes.InsufficientPrivilege, "Database administration requires a superuser").ConfigureAwait(false);
            return;
        }

        // Publish the principal + the route's privilege so TableOpener enforces per table. SQL routes
        // pass Privilege.None here and set their own precise privilege from the parsed statement.
        Privilege? routePrivilege = DataRoutes.TryGetValue(path, out Privilege p) ? p : null;
        AuthorizationContext.Current = new AuthorizationScope(principal, routePrivilege);
        context.Items["camus.principal"] = principal;

        await next(context).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the browser session token from the dashboard cookie.
    ///
    /// <para><b>Only ever call this for a dashboard route.</b> Every other route authenticates by
    /// the <c>Authorization</c> header alone, and that is what makes cross-site request forgery
    /// impossible against them: a browser attaches a cookie by itself, but never a header. Accepting
    /// this cookie on a write route would give that protection away. <see cref="DashboardSession"/>
    /// holds the route list and the full reasoning.</para>
    /// </summary>
    private static string? ExtractSessionCookie(HttpContext context)
    {
        string? token = context.Request.Cookies[DashboardSession.CookieName];
        return string.IsNullOrWhiteSpace(token) ? null : token;
    }

    private static string? ExtractBearer(HttpContext context)
    {
        string authorization = context.Request.Headers.Authorization.ToString();
        return authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)
            ? authorization["Bearer ".Length..].Trim()
            : null;
    }

    /// <summary>
    /// Compares the presented <c>X-Camus-Node-Secret</c> header with the configured node secret in
    /// constant time. An unconfigured secret is refused outright — the peer routes fail closed rather
    /// than accepting an empty header.
    ///
    /// <para>The comparison is constant-time for the same reason
    /// <see cref="Core.Auth.TokenCodec.MacEquals"/> and <c>PasswordHasher.Verify</c> are: this value
    /// authenticates a peer in place of a user credential, and an early-exit comparison over it is the
    /// one place in the authentication path that leaks a per-byte timing signal. Exploiting that over a
    /// network is impractical, but the codebase holds itself to a single rule here and this was the
    /// exception.</para>
    ///
    /// <para>Lengths are compared first and returned on separately, so the fixed-time comparison never
    /// runs over mismatched buffers — <see cref="CryptographicOperations.FixedTimeEquals"/> returns
    /// immediately in that case, which is itself an observable difference.</para>
    /// </summary>
    private static bool NodeSecretMatches(HttpContext context, string configuredSecret)
    {
        if (string.IsNullOrEmpty(configuredSecret))
            return false;

        string presented = context.Request.Headers["X-Camus-Node-Secret"].ToString();
        if (presented.Length == 0)
            return false;

        byte[] presentedBytes = Encoding.UTF8.GetBytes(presented);
        byte[] configuredBytes = Encoding.UTF8.GetBytes(configuredSecret);

        if (presentedBytes.Length != configuredBytes.Length)
            return false;

        return CryptographicOperations.FixedTimeEquals(presentedBytes, configuredBytes);
    }

    private static void EnsureSecureTransport(HttpContext context, CamusDBOptions options)
    {
        if (!options.RequireTlsWhenAuthEnabled || context.Request.IsHttps)
            return;

        System.Net.IPAddress? remote = context.Connection.RemoteIpAddress;
        if (remote is not null && System.Net.IPAddress.IsLoopback(remote))
            return;

        throw new CamusDBException(CamusDBErrorCodes.InsecureTransport, "Authentication is enabled and requires a TLS (HTTPS) connection");
    }

    private static async Task WriteErrorAsync(HttpContext context, string code, string message)
    {
        context.Response.StatusCode = CamusDBErrorCodes.GetHttpStatus(code);
        context.Response.ContentType = "application/json";
        await context.Response.WriteAsync(JsonSerializer.Serialize(new { status = "failed", code, message })).ConfigureAwait(false);
    }
}
