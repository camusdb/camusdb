/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace CamusDB.App.Services;

/// <summary>
/// Tracks the connections peers hold to the Raft listener, so that a stopping node can end them
/// once it has handed off its leadership.
///
/// <para>Peers open long-lived Raft and Kahuna streams to every node and never end them. The web
/// server's graceful stop waits for open requests, so with those streams open it waited out the
/// whole host shutdown timeout before the node's own graceful shutdown could start. Ending them
/// cannot happen as the host starts to stop: the leadership hand-off runs then, and the replicas it
/// hands to send their acknowledgements over exactly these streams. <see cref="LeadershipHandoffService"/>
/// closes the gate after the hand-off instead. A peer that connects again before the web server
/// has stopped listening is turned away at once.</para>
///
/// <para>Peers see the reset a node's exit has always given them, but after its leadership has
/// moved, so no partition waits for an election because of it.</para>
/// </summary>
public sealed class PeerConnectionGate
{
    private readonly ConcurrentDictionary<string, BaseConnectionContext> open = new(StringComparer.Ordinal);

    private volatile bool closed;

    /// <summary>The peer connections open now.</summary>
    public int OpenCount => open.Count;

    /// <summary>Whether <see cref="Close"/> has run.</summary>
    public bool IsClosed => closed;

    /// <summary>Puts the gate in front of every connection the listener accepts.</summary>
    public static ListenOptions Use(ListenOptions listen)
    {
        PeerConnectionGate gate = listen.ApplicationServices.GetRequiredService<PeerConnectionGate>();

        listen.Use(nextConnection => async connectionContext =>
        {
            if (!gate.TryAdmit(connectionContext))
                return;

            try
            {
                await nextConnection(connectionContext).ConfigureAwait(false);
            }
            finally
            {
                gate.Release(connectionContext);
            }
        });

        return listen;
    }

    /// <summary>
    /// Admits a connection and tracks it until <see cref="Release"/>, or aborts it when the gate is
    /// closed.
    /// </summary>
    public bool TryAdmit(BaseConnectionContext connection)
    {
        if (!closed)
        {
            open[connection.ConnectionId] = connection;

            // Close may have run between the check and the add, and missed this connection.
            if (!closed)
                return true;

            open.TryRemove(connection.ConnectionId, out _);
        }

        connection.Abort(new ConnectionAbortedException("The node is stopping"));
        return false;
    }

    /// <summary>Stops tracking a connection that has ended.</summary>
    public void Release(BaseConnectionContext connection) =>
        open.TryRemove(connection.ConnectionId, out _);

    /// <summary>
    /// Aborts every open peer connection and turns new ones away from now on. Returns how many it
    /// aborted.
    /// </summary>
    public int Close()
    {
        closed = true;

        int aborted = 0;
        foreach (BaseConnectionContext connection in open.Values)
        {
            connection.Abort(new ConnectionAbortedException("The node is stopping and has handed off its leadership"));
            aborted++;
        }

        return aborted;
    }
}
