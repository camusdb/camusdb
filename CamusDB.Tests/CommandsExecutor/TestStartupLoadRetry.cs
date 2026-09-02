/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using CamusDB.Core.CommandsExecutor.Controllers;
using Grpc.Core;
using Kommander;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The catalog and registry startup scans wait out a cluster that is still assembling. They used to
/// retry <see cref="RaftException"/> only, which covered the boot race they were written for and
/// missed the one a rejoining node actually hits: the scan hash-routes the <c>_system/</c> bucket, so
/// it often has to reach a peer, and a peer still restarting yields a transport failure. That escaped
/// the filter and killed the process during startup.
///
/// <para>These pin the policy that replaced it — retry on persistence, not on type — because the old
/// filter passed every test that existed.</para>
/// </summary>
[TestFixture]
public sealed class TestStartupLoadRetry
{
    /// <summary>The exact exception observed killing a node on boot: an inter-node gRPC call to a peer
    /// that was not listening yet.</summary>
    private static RpcException PeerNotListening() =>
        new(new Status(
            StatusCode.Unavailable,
            "Error connecting to subchannel.",
            new SocketException((int)SocketError.ConnectionRefused)));

    [Test]
    public void ATransportFailureFromAPeerStillBootingIsRetried()
    {
        Assert.That(StartupLoadRetry.ShouldRetry(PeerNotListening(), elapsedMs: 0), Is.True);
    }

    [Test]
    public void TheRaftBootRaceIsStillRetried()
    {
        Assert.That(StartupLoadRetry.ShouldRetry(new RaftException("Invalid partition"), elapsedMs: 0), Is.True);
    }

    /// <summary>The budget, not the type, is what separates a blip from a real failure — so a fault
    /// that outlives it surfaces unchanged however transient it looked.</summary>
    [Test]
    public void AFaultThatOutlivesTheBudgetIsSurfaced()
    {
        Assert.That(
            StartupLoadRetry.ShouldRetry(PeerNotListening(), elapsedMs: StartupLoadRetry.MaxWaitMs),
            Is.False);
        Assert.That(
            StartupLoadRetry.ShouldRetry(PeerNotListening(), elapsedMs: StartupLoadRetry.MaxWaitMs + 1),
            Is.False);
    }

    /// <summary>Cancellation means the process is shutting down. Retrying it would spin the loop for
    /// the whole budget against a token that will never become good again.</summary>
    [Test]
    public void CancellationIsNeverRetried()
    {
        Assert.That(StartupLoadRetry.ShouldRetry(new OperationCanceledException(), elapsedMs: 0), Is.False);
        Assert.That(StartupLoadRetry.ShouldRetry(new TaskCanceledException(), elapsedMs: 0), Is.False);
    }

    /// <summary>
    /// A deterministic bug is retried too, and that is the accepted cost: enumerating in advance every
    /// transient a half-formed cluster can produce is what failed here. It still surfaces, at most one
    /// budget later, on a path where boot latency is not precious.
    /// </summary>
    [Test]
    public void AnUnexpectedFaultIsRetriedWithinTheBudgetAndThenSurfaced()
    {
        InvalidOperationException unexpected = new("something else entirely");

        Assert.That(StartupLoadRetry.ShouldRetry(unexpected, elapsedMs: 0), Is.True);
        Assert.That(StartupLoadRetry.ShouldRetry(unexpected, elapsedMs: StartupLoadRetry.MaxWaitMs), Is.False);
    }
}
