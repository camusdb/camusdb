/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Middleware;

/// <summary>
/// Transport-wide authentication and coarse authorization for every HTTP route, so authentication is a
/// single boundary rather than a per-controller opt-in. When <see cref="CamusDBConfig.AuthenticationEnabled"/>
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
/// <see cref="CamusDBConfig.NodeSecret"/> header, not a public user token; with auth on and no node
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

    public async Task Invoke(HttpContext context, CommandExecutor executor)
    {
        if (!CamusDBConfig.AuthenticationEnabled)
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

        // Node-to-node forwarding: authenticate the peer by shared secret, not a user token.
        if (path.StartsWith("/internal/", StringComparison.OrdinalIgnoreCase))
        {
            if (string.IsNullOrEmpty(CamusDBConfig.NodeSecret)
                || !string.Equals(context.Request.Headers["X-Camus-Node-Secret"], CamusDBConfig.NodeSecret, StringComparison.Ordinal))
            {
                await WriteErrorAsync(context, CamusDBErrorCodes.AuthenticationFailed, "Node authentication required").ConfigureAwait(false);
                return;
            }
            await next(context).ConfigureAwait(false);
            return;
        }

        // Every other route is a public user route: require a valid bearer token over a secure transport.
        Principal principal;
        try
        {
            EnsureSecureTransport(context);
            string? bearer = ExtractBearer(context);
            principal = await executor.ResolvePrincipalAsync(bearer).ConfigureAwait(false);
        }
        catch (CamusDBException e)
        {
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

    private static string? ExtractBearer(HttpContext context)
    {
        string authorization = context.Request.Headers.Authorization.ToString();
        return authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase)
            ? authorization["Bearer ".Length..].Trim()
            : null;
    }

    private static void EnsureSecureTransport(HttpContext context)
    {
        if (!CamusDBConfig.RequireTlsWhenAuthEnabled || context.Request.IsHttps)
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
