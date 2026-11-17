/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.Logging.Abstractions;
using CamusDB.App.Services;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// <see cref="PeerConnectionGate"/> tracks the connections peers hold to the Raft listener, ends them
/// when a stopping node closes it, and turns away the ones that arrive afterwards.
/// </summary>
public sealed class TestPeerConnectionGate
{
    [Test]
    public async Task Close_AbortsEveryOpenConnection()
    {
        PeerConnectionGate gate = new();
        DefaultConnectionContext first = new("peer-1");
        DefaultConnectionContext second = new("peer-2");

        Assert.That(gate.TryAdmit(first), Is.True);
        Assert.That(gate.TryAdmit(second), Is.True);
        Assert.That(gate.OpenCount, Is.EqualTo(2));

        Assert.That(gate.Close(), Is.EqualTo(2));
        Assert.That(gate.IsClosed, Is.True);
        Assert.That(await AbortedAsync(first), Is.True, "the first connection was left open");
        Assert.That(await AbortedAsync(second), Is.True, "the second connection was left open");
    }

    [Test]
    public async Task TryAdmit_AfterClose_AbortsTheConnection()
    {
        PeerConnectionGate gate = new();
        gate.Close();

        // A peer that reconnects before the web server has stopped listening.
        DefaultConnectionContext late = new("peer-late");

        Assert.That(gate.TryAdmit(late), Is.False);
        Assert.That(gate.OpenCount, Is.Zero);
        Assert.That(await AbortedAsync(late), Is.True, "a connection that arrived after Close was admitted");
    }

    [Test]
    public async Task Close_LeavesConnectionsThatEndedAlone()
    {
        PeerConnectionGate gate = new();
        DefaultConnectionContext ended = new("peer-ended");
        DefaultConnectionContext open = new("peer-open");

        gate.TryAdmit(ended);
        gate.TryAdmit(open);
        gate.Release(ended);

        Assert.That(gate.Close(), Is.EqualTo(1));
        Assert.That(await AbortedAsync(open), Is.True);
        Assert.That(ended.ConnectionClosed.IsCancellationRequested, Is.False, "aborted a connection that had already ended");
    }

    [Test]
    public async Task ClosePeers_WaitsUntilTheClientConnectionsHaveGone()
    {
        PeerConnectionGate gate = new();
        DefaultConnectionContext peer = new("peer-1");
        gate.TryAdmit(peer);
        int clients = 2;

        Task<int> closing = LeadershipHandoffService.ClosePeersOnceClientsHaveGoneAsync(
            () => Volatile.Read(ref clients), gate, TimeSpan.FromSeconds(30), NullLogger.Instance);

        // A request still running on a client connection may need the peers.
        await Task.Delay(300);
        Assert.That(closing.IsCompleted, Is.False, "closed the peers while clients were still connected");
        Assert.That(gate.IsClosed, Is.False);

        Volatile.Write(ref clients, 0);
        Assert.That(await closing.WaitAsync(TimeSpan.FromSeconds(5)), Is.EqualTo(1));
        Assert.That(gate.IsClosed, Is.True);
        Assert.That(await AbortedAsync(peer), Is.True);
    }

    [Test]
    public async Task ClosePeers_ClosesAtTheBudgetWhenClientsStay()
    {
        PeerConnectionGate gate = new();

        Task<int> closing = LeadershipHandoffService.ClosePeersOnceClientsHaveGoneAsync(
            () => 1, gate, TimeSpan.FromMilliseconds(200), NullLogger.Instance);

        await closing.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(gate.IsClosed, Is.True, "a client that never left kept the peer streams open");
    }

    // DefaultConnectionContext cancels ConnectionClosed on the thread pool when it is aborted.
    private static async Task<bool> AbortedAsync(DefaultConnectionContext connection)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (connection.ConnectionClosed.IsCancellationRequested)
                return true;

            await Task.Delay(10);
        }
        return false;
    }
}
