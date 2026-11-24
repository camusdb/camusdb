/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net.Sockets;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace CamusDB.App.Services;

/// <summary>
/// Turns TCP keepalive on for the connections of a client-facing listener, so that the node learns that
/// the host of a client died or became unreachable without a TCP close.
///
/// <para>A statement that returns rows ends when its client read them, or when the request token fires.
/// The token fires when the connection closes. A row stream that waits for HTTP/2 flow control has no
/// data in flight, so the operating system alone never finds out that the peer is gone. The stream would
/// wait forever, and because its statement runs, the idle reaper would leave its transaction and its
/// locks alone (see <see cref="ActiveTransaction"/>). The probes close that connection after about
/// <see cref="IdleSeconds"/> + <see cref="ProbeCount"/> × <see cref="IntervalSeconds"/> seconds.</para>
///
/// <para>The operating system of the client answers the probes, so a client process that is paused in a
/// debugger, or that reads slowly, keeps its connection. HTTP/2 pings would close it, because the client
/// process answers those. A QUIC connection has no TCP socket; its own idle timeout applies.</para>
/// </summary>
public static class ClientConnectionKeepAlive
{
    /// <summary>Seconds without traffic before the first probe.</summary>
    public const int IdleSeconds = 60;

    /// <summary>Seconds between two probes that get no answer.</summary>
    public const int IntervalSeconds = 10;

    /// <summary>Probes without an answer after which the connection is dead.</summary>
    public const int ProbeCount = 6;

    /// <summary>Applies keepalive to every TCP connection the listener accepts.</summary>
    public static ListenOptions Use(ListenOptions listen)
    {
        listen.Use(nextConnection => connectionContext =>
        {
            if (connectionContext.Features.Get<IConnectionSocketFeature>()?.Socket is { } socket)
                Enable(socket);

            return nextConnection(connectionContext);
        });

        return listen;
    }

    /// <summary>
    /// Sets the options on one accepted socket. Best effort: a platform that refuses an option, or a
    /// client that is already gone, must not cost the connection.
    /// </summary>
    internal static void Enable(Socket socket)
    {
        if (socket.AddressFamily is not (AddressFamily.InterNetwork or AddressFamily.InterNetworkV6))
            return;

        try
        {
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, IdleSeconds);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, IntervalSeconds);
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, ProbeCount);
        }
        catch (Exception ex) when (ex is SocketException or ObjectDisposedException or PlatformNotSupportedException)
        {
            // The connection works without keepalive.
        }
    }
}
