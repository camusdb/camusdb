/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using NUnit.Framework;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Pins how <see cref="KahunaRetryPolicy"/> treats a Kahuna call that failed at the transport
/// instead of answering — the shape run lk8 (2026-09-15) produced ~8,000 times per surviving node in
/// the two seconds after a partition leader's SIGKILL, when the follower's forwarded key-value calls
/// were refused by the dead node and each one escaped to a client as a generic internal error.
/// The policy now answers such a failure as <c>MustRetry</c> on its ordinary budget, so the
/// refused window is waited out like any other transient and surfaced past the budget as the same
/// unconfirmed answer the callers already turn into <c>TransactionMustRetry</c>.
/// </summary>
[TestFixture]
public sealed class TestKahunaRetryPolicyTransport
{
    /// <summary>The exact exception the followers logged: the inter-node connect refused.</summary>
    private static RpcException LeaderRefused() =>
        new(new Status(
            StatusCode.Unavailable,
            "Error connecting to subchannel.",
            new SocketException((int)SocketError.ConnectionRefused)));

    private static readonly ReadOnlyKeyValueEntry Answer = new(
        [1, 2, 3], 7, HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set);

    [Test]
    public async Task ARefusedForwardIsRetriedAndTheEventualAnswerReturned()
    {
        int calls = 0;

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await KahunaRetryPolicy.RetryOnMustRetry(
            () =>
            {
                if (++calls <= 2)
                    throw LeaderRefused();
                return Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((KeyValueResponseType.Get, Answer));
            },
            CancellationToken.None);

        Assert.That(type, Is.EqualTo(KeyValueResponseType.Get));
        Assert.That(entry, Is.SameAs(Answer));
        Assert.That(calls, Is.EqualTo(3), "two refused attempts, then the answer");
    }

    [Test]
    public async Task ASetThatWasRefusedIsRetriedWithItsOwnTupleShape()
    {
        int calls = 0;
        HLCTimestamp stamped = new(1, 2, 3);

        (KeyValueResponseType type, long revision, HLCTimestamp ts) = await KahunaRetryPolicy.RetryOnMustRetry(
            () =>
            {
                if (++calls == 1)
                    throw LeaderRefused();
                return Task.FromResult((KeyValueResponseType.Set, 42L, stamped));
            },
            CancellationToken.None);

        Assert.That(type, Is.EqualTo(KeyValueResponseType.Set));
        Assert.That(revision, Is.EqualTo(42L));
        Assert.That(ts, Is.EqualTo(stamped));
        Assert.That(calls, Is.EqualTo(2));
    }

    [Test]
    public async Task AForwardThatStaysRefusedExhaustsTheBudgetAsMustRetry_NotAsAnException()
    {
        int calls = 0;
        Func<Task<KeyValueResponseType>> alwaysRefused = () =>
        {
            calls++;
            throw LeaderRefused();
        };

        KeyValueResponseType type = await KahunaRetryPolicy.RetryOnMustRetry(alwaysRefused, CancellationToken.None);

        Assert.That(type, Is.EqualTo(KeyValueResponseType.MustRetry),
            "past the budget the answer is the same unconfirmed one a real MustRetry leaves, never a generic error");
        Assert.That(calls, Is.EqualTo(KahunaRetryPolicy.MaxKahunaRetries), "the whole budget was spent waiting");
    }

    [Test]
    public void AnApplicationStatusIsNotATransportFailureAndPropagates()
    {
        int calls = 0;
        RpcException invalid = new(new Status(StatusCode.InvalidArgument, "bad key"));
        Func<Task<KeyValueResponseType>> rejected = () =>
        {
            calls++;
            throw invalid;
        };

        RpcException thrown = Assert.ThrowsAsync<RpcException>(
            () => KahunaRetryPolicy.RetryOnMustRetry(rejected, CancellationToken.None))!;

        Assert.That(thrown, Is.SameAs(invalid));
        Assert.That(calls, Is.EqualTo(1), "nothing to wait out");
    }

    [Test]
    public void ACancelledCallerIsNeverRetried()
    {
        using CancellationTokenSource cts = new();
        cts.Cancel();
        int calls = 0;
        Func<Task<KeyValueResponseType>> refused = () =>
        {
            calls++;
            throw LeaderRefused();
        };

        Assert.ThrowsAsync<RpcException>(() => KahunaRetryPolicy.RetryOnMustRetry(refused, cts.Token));

        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void TheTransientStatusSetMirrorsKahunasInterNodeRule()
    {
        CancellationToken none = CancellationToken.None;

        Assert.Multiple(() =>
        {
            Assert.That(KahunaRetryPolicy.IsTransientTransportFailure(LeaderRefused(), none), Is.True, "connect refused");
            Assert.That(KahunaRetryPolicy.IsTransientTransportFailure(
                new RpcException(new Status(StatusCode.DeadlineExceeded, "inter-node deadline")), none), Is.True, "deadline");
            Assert.That(KahunaRetryPolicy.IsTransientTransportFailure(
                new RpcException(new Status(StatusCode.Unavailable,
                    "gRPC inter-node stream write failed or timed out: ObjectDisposedException.")), none), Is.True, "evicted stream");
            Assert.That(KahunaRetryPolicy.IsTransientTransportFailure(
                new RpcException(new Status(StatusCode.Cancelled, "gRPC call disposed.")), none), Is.True, "disposed inter-node call");
            Assert.That(KahunaRetryPolicy.IsTransientTransportFailure(
                new RpcException(new Status(StatusCode.Internal, "Error reading next message.", new IOException("reset"))), none),
                Is.True, "internal wrapping a socket-level cause");

            Assert.That(KahunaRetryPolicy.IsTransientTransportFailure(
                new RpcException(new Status(StatusCode.Internal, "index out of range")), none), Is.False, "a real internal error");
            Assert.That(KahunaRetryPolicy.IsTransientTransportFailure(
                new RpcException(new Status(StatusCode.Cancelled, "Call canceled by the client.")), none), Is.False, "a plain cancel");
            Assert.That(KahunaRetryPolicy.IsTransientTransportFailure(
                new RpcException(new Status(StatusCode.InvalidArgument, "bad")), none), Is.False, "an application status");
        });
    }
}
