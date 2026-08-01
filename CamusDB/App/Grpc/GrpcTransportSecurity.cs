
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Core;
using CamusDB.Core;

namespace CamusDB.App.Grpc;

/// <summary>
/// Transport-security gate for gRPC calls that carry credentials, mirroring the REST
/// <c>CommandsController.EnsureSecureTransport</c>.
///
/// <para>Shared by every gRPC service that touches credentials so the rule cannot drift between them —
/// a data-plane call presenting a bearer token and a <c>CamusAuth.Login</c> carrying a password must be
/// held to the same standard, and the password case is the stricter of the two to get wrong.</para>
/// </summary>
internal static class GrpcTransportSecurity
{
    /// <summary>
    /// Throws <see cref="CamusDBErrorCodes.InsecureTransport"/> when authentication is enabled, TLS is
    /// required, and the call arrived in the clear from a non-loopback peer. A loopback peer is exempt so
    /// single-host development works without certificates.
    ///
    /// <para>Skipped when the call has no ASP.NET <see cref="HttpContext"/> — an in-process unit-test
    /// invocation — because transport security cannot be assessed there.</para>
    /// </summary>
    public static void EnsureSecureTransport(ServerCallContext context, CamusDBOptions options)
    {
        if (!options.AuthenticationEnabled || !options.RequireTlsWhenAuthEnabled)
            return;

        HttpContext? http;
        try
        {
            http = context.GetHttpContext();
        }
        catch
        {
            return; // no ASP.NET HttpContext (e.g. an in-process unit-test call) — cannot assess transport
        }

        if (http is null || http.Request.IsHttps)
            return;

        System.Net.IPAddress? remote = http.Connection.RemoteIpAddress;
        if (remote is not null && System.Net.IPAddress.IsLoopback(remote))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InsecureTransport,
            "Authentication is enabled and requires a TLS connection");
    }
}
