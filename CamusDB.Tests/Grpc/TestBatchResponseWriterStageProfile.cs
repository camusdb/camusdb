
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.Diagnostics;
using CamusDB.Grpc;
using CamusDB.App.Grpc;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// The part of a profiled query that runs after its handler: the wait of its response in the stream
/// writer's queue, the transport write, and the wait for the flush. The writer records these from
/// its own loop, before or after the op's flow ends, so the tests cover both orders and assert that
/// the clock is published exactly once, by the side that finishes last.
/// Non-parallelizable because <see cref="QueryStageProfile.Enabled"/> is process-wide.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestBatchResponseWriterStageProfile
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    private sealed record Measurement(string Instrument, double Value, string? Stage);

    private static BatchExecuteResponse Reply(int id)
        => new() { RequestId = id, NonQuery = new NonQueryReply { AffectedRows = id } };

    private static (MeterListener Listener, List<Measurement> Captured) StartListener()
    {
        List<Measurement> captured = new();
        MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == QueryStageProfile.MeterName)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((inst, value, tags, _) =>
        {
            lock (captured) captured.Add(new Measurement(inst.Name, value, StageOf(tags)));
        });
        listener.SetMeasurementEventCallback<double>((inst, value, tags, _) =>
        {
            lock (captured) captured.Add(new Measurement(inst.Name, value, StageOf(tags)));
        });
        listener.Start();
        return (listener, captured);
    }

    private static string? StageOf(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (KeyValuePair<string, object?> tag in tags)
        {
            if (tag.Key == "stage")
                return tag.Value?.ToString();
        }

        return null;
    }

    private static List<Measurement> Snapshot(List<Measurement> captured)
    {
        lock (captured) return captured.ToList();
    }

    private static int QueriesPublished(List<Measurement> captured)
        => Snapshot(captured).Count(m => m.Instrument == "camus.query.profile.queries");

    private static double? TimeOf(List<Measurement> captured, string stage)
        => Snapshot(captured).FirstOrDefault(m => m.Instrument == "camus.query.profile.time" && m.Stage == stage)?.Value;

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        Stopwatch waited = Stopwatch.StartNew();
        while (!condition())
        {
            Assert.That(waited.Elapsed, Is.LessThan(Patience), "the condition was not reached");
            await Task.Delay(5);
        }
    }

    [TearDown]
    public void ResetGate() => QueryStageProfile.Enabled = false;

    /// <summary>
    /// Stands in for a profiled op: opens a clock in its own async method, hands one entry to the
    /// writer and returns without ending the op, so the test decides when the op ends.
    /// </summary>
    private static async Task<QueryStageClock> HandOverAsync(BatchResponseWriter writer, int id, long receivedAt)
    {
        QueryStageClock clock = QueryStageProfile.Begin()!;
        clock.Path = QueryStageProfile.Paths.Autocommit;
        clock.MarkReceived(receivedAt);
        await writer.WriteAsync(Reply(id), CancellationToken.None);
        return clock;
    }

    [Test]
    public async Task TheWriterPublishesAnOpThatEndedBeforeItsResponseWasWritten()
    {
        QueryStageProfile.Enabled = true;
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            GatedStreamWriter<BatchExecuteResponse> transport = new();
            BatchResponseWriter writer = new(transport, CancellationToken.None);

            transport.ParkNextWrite();
            QueryStageClock clock = await HandOverAsync(writer, 1, Stopwatch.GetTimestamp());
            await transport.WriteIsParked.WaitAsync(Patience);

            clock.CompleteOp();
            Assert.That(QueriesPublished(captured), Is.Zero, "the response is still in the transport write");

            await Task.Delay(20);
            transport.Release();
            await writer.CompleteAsync();

            Assert.That(QueriesPublished(captured), Is.EqualTo(1));
            Assert.That(TimeOf(captured, "writer_write"), Is.GreaterThanOrEqualTo(15), "the parked write is the write stage");
            Assert.That(TimeOf(captured, "writer_queue"), Is.Not.Null);
            Assert.That(TimeOf(captured, "frame_wait"), Is.Not.Null);
            Assert.That(TimeOf(captured, "stream"), Is.GreaterThanOrEqualTo(TimeOf(captured, "writer_write")!.Value),
                "the span on the stream contains the write");
            Assert.That(TimeOf(captured, "writer_flush"), Is.Null, "a lone message flushes with its own write");
        }
    }

    [Test]
    public async Task TheOpPublishesWhenItsResponseWasWrittenBeforeItEnded()
    {
        QueryStageProfile.Enabled = true;
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            GatedStreamWriter<BatchExecuteResponse> transport = new();
            BatchResponseWriter writer = new(transport, CancellationToken.None);

            QueryStageClock clock = await HandOverAsync(writer, 1, Stopwatch.GetTimestamp());
            await WaitUntilAsync(() => transport.Written.Count == 1);
            await writer.CompleteAsync();

            Assert.That(QueriesPublished(captured), Is.Zero, "the op has not ended");

            clock.CompleteOp();

            Assert.That(QueriesPublished(captured), Is.EqualTo(1));
            Assert.That(TimeOf(captured, "writer_write"), Is.Not.Null);
            Assert.That(TimeOf(captured, "stream"), Is.Not.Null);

            clock.CompleteOp();
            Assert.That(QueriesPublished(captured), Is.EqualTo(1), "a clock publishes once");
        }
    }

    [Test]
    public async Task AnEntryWrittenWithoutAFlushWaitsForTheWriteThatFlushes()
    {
        QueryStageProfile.Enabled = true;
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            GatedStreamWriter<BatchExecuteResponse> transport = new();
            BatchResponseWriter writer = new(transport, CancellationToken.None);

            // The writer is parked on an unprofiled message, so the two messages behind it wait
            // together: the first is written with the buffer hint and the second carries the flush.
            transport.ParkNextWrite();
            await writer.WriteAsync(Reply(0), CancellationToken.None);
            await transport.WriteIsParked.WaitAsync(Patience);

            QueryStageClock buffered = await HandOverAsync(writer, 1, Stopwatch.GetTimestamp());
            buffered.CompleteOp();
            await writer.WriteAsync(Reply(2), CancellationToken.None);

            await Task.Delay(20);
            transport.Release();
            await writer.CompleteAsync();

            IReadOnlyList<(BatchExecuteResponse Message, bool Buffered)> writes = transport.Writes;
            Assert.That(writes.Count, Is.EqualTo(3));
            Assert.That(writes[1].Buffered, Is.True, "the profiled message has a message behind it");

            Assert.That(QueriesPublished(captured), Is.EqualTo(1));
            Assert.That(TimeOf(captured, "writer_queue"), Is.GreaterThanOrEqualTo(15), "it waited behind the parked write");
            Assert.That(TimeOf(captured, "writer_flush"), Is.Not.Null, "its bytes left with the next write");
        }
    }

    [Test]
    public async Task AGroupIsOneEntry()
    {
        QueryStageProfile.Enabled = true;
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            GatedStreamWriter<BatchExecuteResponse> transport = new();
            BatchResponseWriter writer = new(transport, CancellationToken.None);

            QueryStageClock clock = QueryStageProfile.Begin()!;
            clock.Path = QueryStageProfile.Paths.Autocommit;

            BatchResponseGroup group = new();
            for (int i = 0; i < 3; i++)
                group.Add(Reply(7));
            await writer.WriteGroupAsync(group, CancellationToken.None);
            clock.CompleteOp();

            await writer.CompleteAsync();

            Assert.That(transport.Written.Count, Is.EqualTo(3), "a peer without frames reads plain messages");
            Assert.That(QueriesPublished(captured), Is.EqualTo(1));

            double calls = Snapshot(captured)
                .Where(m => m.Instrument == "camus.query.profile.calls" && m.Stage == "writer_queue")
                .Sum(m => m.Value);
            Assert.That(calls, Is.EqualTo(1), "the group is charged once, at the write of its last message");
            Assert.That(TimeOf(captured, "stream"), Is.Null, "an op that did not arrive on a stream has no span");
        }
    }

    [Test]
    public async Task WithTheProfileOffNoEntryCarriesAClock()
    {
        (MeterListener listener, List<Measurement> captured) = StartListener();
        using (listener)
        {
            GatedStreamWriter<BatchExecuteResponse> transport = new();
            BatchResponseWriter writer = new(transport, CancellationToken.None);

            Assert.That(QueryStageProfile.Begin(), Is.Null);
            await writer.WriteAsync(Reply(1), CancellationToken.None);
            await writer.CompleteAsync();

            Assert.That(transport.Written.Count, Is.EqualTo(1));
            Assert.That(Snapshot(captured), Is.Empty);
        }
    }
}
