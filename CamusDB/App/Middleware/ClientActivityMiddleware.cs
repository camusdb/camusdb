/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Diagnostics;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace CamusDB.App.Middleware;

/// <summary>
/// Feeds <c>SHOW CONNECTIONS</c> and the origin columns of <c>SHOW QUERIES</c> from the web server.
///
/// <para><b>Two layers, because only the web server sees a connection.</b> A connection middleware
/// (<see cref="UseClientConnectionTracking"/>) runs once per accepted TCP or QUIC connection: it adds the
/// connection to the engine's <see cref="ClientConnectionRegistry"/> and removes it when the
/// connection closes, whichever way it closes. The request middleware (<see cref="InvokeAsync"/>)
/// runs once per request: it finds that connection by the web server's connection id, counts the
/// request, and sets <see cref="StatementOrigin.Current"/> so every statement the request runs is
/// linked to the connection.</para>
///
/// <para>A request on a listener without connection tracking — the Raft listener, which serves only
/// peers — finds no connection and passes through untouched.</para>
/// </summary>
public sealed class ClientActivityMiddleware
{
    /// <summary>The transport name of a REST request.</summary>
    public const string HttpTransport = "http";

    /// <summary>The transport name of a unary or server-streaming gRPC call.</summary>
    public const string GrpcTransport = "grpc";

    /// <summary>The transport name of an op on a <c>BatchExecute</c> stream.</summary>
    public const string GrpcStreamTransport = "grpc-stream";

    private readonly RequestDelegate next;

    private readonly ClientConnectionRegistry connections;

    public ClientActivityMiddleware(RequestDelegate next, CommandExecutor executor)
    {
        this.next = next;
        connections = executor.QueryActivity.Connections;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        ClientConnection? connection = connections.Find(context.Connection.Id);

        if (connection is null)
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        bool internalPath = context.Request.Path.StartsWithSegments("/internal", StringComparison.OrdinalIgnoreCase);
        connection.RequestStarted(context.Request.Protocol, internalPath);

        bool grpc = context.Request.ContentType?.StartsWith("application/grpc", StringComparison.OrdinalIgnoreCase) == true;

        // Set here, in the method whose await reaches the endpoint, so the value flows down into it.
        StatementOrigin.Current = connection.OriginFor(grpc ? GrpcTransport : HttpTransport);

        try
        {
            await next(context).ConfigureAwait(false);
        }
        finally
        {
            connection.RequestEnded();
        }
    }

    /// <summary>
    /// Adds every connection the listener accepts to the engine's connection registry for as long as
    /// it stays open. Use it on the client-facing listeners only; a peer-only listener would fill the
    /// list with node-to-node traffic an operator does not need to see.
    ///
    /// <para>The engine is resolved at the first connection, not here: the listener is configured
    /// before the service provider can build it.</para>
    ///
    /// <para><b>Two hooks, because the web server has two connection pipelines.</b> A TCP connection
    /// (HTTP/1.1, HTTP/2) runs the connection middleware; a QUIC connection (HTTP/3) runs the
    /// multiplexed connection middleware instead, and never the first. Both register the connection
    /// under the id that every request on it later reports as <c>HttpContext.Connection.Id</c>, so
    /// <see cref="InvokeAsync"/> finds an HTTP/3 connection the same way it finds a TCP one. The
    /// multiplexed hook is only built when the listener serves HTTP/3.</para>
    /// </summary>
    public static ListenOptions UseClientConnectionTracking(ListenOptions listen)
    {
        IServiceProvider services = listen.ApplicationServices;
        ClientConnectionRegistry? registry = null;

        ClientConnection Open(BaseConnectionContext connectionContext)
        {
            registry ??= services.GetRequiredService<CommandExecutor>().QueryActivity.Connections;

            string? address = connectionContext.RemoteEndPoint is IPEndPoint endPoint
                ? endPoint.Address.ToString()
                : connectionContext.RemoteEndPoint?.ToString();

            return registry.Open(connectionContext.ConnectionId, address);
        }

        listen.Use(nextConnection => async connectionContext =>
        {
            ClientConnection connection = Open(connectionContext);

            try
            {
                await nextConnection(connectionContext).ConfigureAwait(false);
            }
            finally
            {
                connection.Close();
            }
        });

        ((IMultiplexedConnectionBuilder)listen).Use(nextConnection => async connectionContext =>
        {
            ClientConnection connection = Open(connectionContext);

            try
            {
                await nextConnection(connectionContext).ConfigureAwait(false);
            }
            finally
            {
                connection.Close();
            }
        });

        return listen;
    }
}
