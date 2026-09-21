
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Google.Protobuf;

using CamusDB.Grpc;
using CamusDB.App.Grpc;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// Tests for <see cref="BatchResponseWriter"/> on its own, against a transport whose writes the test
/// can park. A parked write is what makes "the messages that already wait" a fixed set, so the packing
/// and backpressure rules can be asserted exactly instead of by timing.
/// </summary>
[TestFixture]
internal sealed class TestBatchResponseWriter
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    private static BatchExecuteResponse Reply(int id)
        => new() { RequestId = id, NonQuery = new NonQueryReply { AffectedRows = id } };

    /// <summary>A row message whose serialized size is a little over <paramref name="bytes"/>.</summary>
    private static BatchExecuteResponse RowOf(int id, int bytes)
    {
        ResultRow row = new();
        row.Values.Add(new Value { BytesValue = ByteString.CopyFrom(new byte[bytes]) });
        return new BatchExecuteResponse { RequestId = id, Row = row };
    }

    /// <summary>The single messages a stream carried, with every frame opened in place.</summary>
    private static List<BatchExecuteResponse> Flatten(IEnumerable<BatchExecuteResponse> written)
    {
        List<BatchExecuteResponse> flat = new();
        foreach (BatchExecuteResponse message in written)
        {
            if (message.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Frame)
                flat.AddRange(message.Frame.Items);
            else
                flat.Add(message);
        }
        return flat;
    }

    private static bool IsFrame(BatchExecuteResponse message)
        => message.PayloadCase == BatchExecuteResponse.PayloadOneofCase.Frame;

    /// <summary>Parks the transport on one message, so that whatever is written next waits behind it.</summary>
    private static async Task ParkOnAsync(BatchResponseWriter writer, GatedStreamWriter<BatchExecuteResponse> transport, int id)
    {
        transport.ParkNextWrite();
        await writer.WriteAsync(Reply(id), CancellationToken.None);
        await transport.WriteIsParked.WaitAsync(Patience);
    }

    [Test]
    public async Task APeerThatSentNoFrameNeverReceivesOne()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);

        await ParkOnAsync(writer, transport, 0);
        for (int i = 1; i <= 50; i++)
            await writer.WriteAsync(Reply(i), CancellationToken.None);

        transport.Release();
        await writer.CompleteAsync();

        Assert.AreEqual(51, transport.Written.Count, "every message must travel as its own stream message");
        Assert.IsFalse(transport.Written.Any(IsFrame));
        CollectionAssert.AreEqual(Enumerable.Range(0, 51), transport.Written.Select(m => m.RequestId));
    }

    [Test]
    public async Task WaitingMessagesTravelAsOneFrameInOrder()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);
        writer.MarkPeerReadsFrames();

        await ParkOnAsync(writer, transport, 0);
        for (int i = 1; i <= 50; i++)
            await writer.WriteAsync(Reply(i), CancellationToken.None);

        transport.Release();
        await writer.CompleteAsync();

        IReadOnlyList<BatchExecuteResponse> written = transport.Written;
        Assert.AreEqual(2, written.Count, "the parked message, then one frame of the 50 that waited");
        Assert.IsFalse(IsFrame(written[0]), "a lone message travels as the plain message, not as a frame of one");
        Assert.IsTrue(IsFrame(written[1]));
        Assert.AreEqual(0, written[1].RequestId);
        CollectionAssert.AreEqual(Enumerable.Range(0, 51), Flatten(written).Select(m => m.RequestId));
    }

    [Test]
    public async Task ALoneMessageIsWrittenPlainAndFlushed()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);
        writer.MarkPeerReadsFrames();

        await writer.WriteAsync(Reply(7), CancellationToken.None);
        await writer.CompleteAsync();

        Assert.AreEqual(1, transport.Writes.Count);
        Assert.IsFalse(IsFrame(transport.Writes[0].Message));
        Assert.IsFalse(transport.Writes[0].Buffered, "the last write of a pass must flush");
    }

    [Test]
    public async Task ABurstBuffersEveryWriteButTheLast()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);

        await ParkOnAsync(writer, transport, 0);
        for (int i = 1; i <= 5; i++)
            await writer.WriteAsync(Reply(i), CancellationToken.None);

        transport.Release();
        await writer.CompleteAsync();

        IReadOnlyList<(BatchExecuteResponse Message, bool Buffered)> writes = transport.Writes;
        Assert.AreEqual(6, writes.Count);
        // The parked write chose its options before anything waited behind it, so it is left out.
        for (int i = 1; i < 5; i++)
            Assert.IsTrue(writes[i].Buffered, $"write {i} has a message behind it and must not flush");
        Assert.IsFalse(writes[5].Buffered, "the last write of the burst carries the flush");
    }

    [Test]
    public async Task ABusyStreamStillFlushesWithinTheUnflushedBudget()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);

        await ParkOnAsync(writer, transport, 0);
        // 40 × ~8 KiB is several times the unflushed budget, all of it waiting at once.
        for (int i = 1; i <= 40; i++)
            await writer.WriteAsync(RowOf(i, 8 * 1024), CancellationToken.None);

        transport.Release();
        await writer.CompleteAsync();

        int unflushed = 0;
        foreach ((BatchExecuteResponse message, bool buffered) in transport.Writes.Skip(1))
        {
            unflushed = buffered ? unflushed + message.CalculateSize() : 0;
            Assert.Less(unflushed, BatchResponseWriter.MaxUnflushedBytes + 16 * 1024,
                "a queue that never runs empty must not postpone the flush without limit");
        }
    }

    [Test]
    public async Task FramesStayInsideTheByteBudgetAndAnOversizedMessageTravelsAlone()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);
        writer.MarkPeerReadsFrames();

        await ParkOnAsync(writer, transport, 0);

        // Large messages are admitted a few at a time, so they are written by concurrent producers
        // the way concurrent ops write, and the transport is released to let them through.
        List<Task> producers = new();
        for (int i = 1; i <= 6; i++)
            producers.Add(writer.WriteAsync(RowOf(i, 300 * 1024), CancellationToken.None).AsTask());
        producers.Add(writer.WriteAsync(RowOf(7, BatchFrames.MaxBytes + 1024), CancellationToken.None).AsTask());

        transport.Release();
        await Task.WhenAll(producers).WaitAsync(Patience);
        await writer.CompleteAsync();

        IReadOnlyList<BatchExecuteResponse> written = transport.Written;
        foreach (BatchExecuteResponse message in written.Where(IsFrame))
        {
            Assert.LessOrEqual(message.Frame.Items.Sum(i => BatchFrames.ItemCost(i.CalculateSize())), BatchFrames.MaxBytes);
            Assert.IsFalse(message.Frame.Items.Any(i => i.RequestId == 7), "a message over the budget never rides a frame");
        }

        Assert.IsTrue(written.Any(m => !IsFrame(m) && m.RequestId == 7), "the oversized message travels as a plain message");
        CollectionAssert.AreEquivalent(Enumerable.Range(0, 8), Flatten(written).Select(m => m.RequestId));
    }

    [Test]
    public async Task ASlowReaderStallsProducersAtTheQueueBound()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);

        await ParkOnAsync(writer, transport, 0);

        // The parked message left the queue, so exactly one queue of messages is accepted at once.
        for (int i = 1; i <= BatchResponseWriter.MaxQueuedMessages; i++)
            Assert.IsTrue(writer.WriteAsync(Reply(i), CancellationToken.None).IsCompletedSuccessfully, $"message {i} fits the queue");

        Task overflow = writer.WriteAsync(Reply(1000), CancellationToken.None).AsTask();
        await Task.Delay(200);
        Assert.IsFalse(overflow.IsCompleted, "a producer past the bound must wait; this is what stalls a cursor");

        transport.Release();
        await overflow.WaitAsync(Patience);
        await writer.CompleteAsync();

        Assert.AreEqual(BatchResponseWriter.MaxQueuedMessages + 2, transport.Written.Count);
    }

    [Test]
    public async Task LargeMessagesAreBoundedApartFromTheMessageCount()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);

        await ParkOnAsync(writer, transport, 0);

        List<Task> accepted = new();
        for (int i = 1; i <= BatchResponseWriter.MaxQueuedLargeMessages; i++)
            accepted.Add(writer.WriteAsync(RowOf(i, BatchResponseWriter.LargeMessageBytes), CancellationToken.None).AsTask());
        await Task.WhenAll(accepted).WaitAsync(Patience);

        Task overflow = writer.WriteAsync(RowOf(99, BatchResponseWriter.LargeMessageBytes), CancellationToken.None).AsTask();
        await Task.Delay(200);
        Assert.IsFalse(overflow.IsCompleted, "one more large message must wait although the queue has room");

        // A small terminal reply is not held by the large-message bound.
        Assert.IsTrue(writer.WriteAsync(Reply(100), CancellationToken.None).IsCompletedSuccessfully);

        transport.Release();
        await overflow.WaitAsync(Patience);
        await writer.CompleteAsync();

        Assert.AreEqual(BatchResponseWriter.MaxQueuedLargeMessages + 3, Flatten(transport.Written).Count);
    }

    [Test]
    public async Task ABrokenTransportCancelsProducersAndCompletesCleanly()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);

        await ParkOnAsync(writer, transport, 0);
        for (int i = 1; i <= BatchResponseWriter.MaxQueuedMessages; i++)
            await writer.WriteAsync(Reply(i), CancellationToken.None);
        Task blocked = writer.WriteAsync(Reply(1000), CancellationToken.None).AsTask();

        transport.Break(new IOException("the client went away"));

        Assert.CatchAsync<OperationCanceledException>(async () => await blocked.WaitAsync(Patience),
            "a producer that waits for a writer that stopped must be released, as a cancellation");
        Assert.CatchAsync<OperationCanceledException>(async () => await writer.WriteAsync(Reply(1001), CancellationToken.None));

        // A terminal write stays best-effort: no exception, whatever the state of the stream.
        await writer.TryWriteAsync(Reply(1002), CancellationToken.None);

        await writer.CompleteAsync().WaitAsync(Patience);
        Assert.AreEqual(0, transport.Written.Count, "nothing is written after the transport failed");
    }

    [Test]
    public async Task TheStreamTokenEndsTheLoopAndReleasesProducers()
    {
        using CancellationTokenSource stream = new();
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, stream.Token);

        await ParkOnAsync(writer, transport, 0);
        for (int i = 1; i <= BatchResponseWriter.MaxQueuedMessages; i++)
            await writer.WriteAsync(Reply(i), stream.Token);
        Task blocked = writer.WriteAsync(Reply(1000), stream.Token).AsTask();

        stream.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () => await blocked.WaitAsync(Patience));
        await writer.CompleteAsync().WaitAsync(Patience);
    }

    [Test]
    public async Task AStatementTokenCancelsOnlyItsOwnWait()
    {
        using CancellationTokenSource statement = new();
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);

        await ParkOnAsync(writer, transport, 0);
        for (int i = 1; i <= BatchResponseWriter.MaxQueuedMessages; i++)
            await writer.WriteAsync(Reply(i), CancellationToken.None);

        Task cancelled = writer.WriteAsync(Reply(1000), statement.Token).AsTask();
        Task survivor = writer.WriteAsync(Reply(1001), CancellationToken.None).AsTask();

        statement.Cancel();
        Assert.CatchAsync<OperationCanceledException>(async () => await cancelled.WaitAsync(Patience));
        Assert.IsFalse(survivor.IsCompleted);

        transport.Release();
        await survivor.WaitAsync(Patience);
        await writer.CompleteAsync();

        List<int> ids = Flatten(transport.Written).Select(m => m.RequestId).ToList();
        CollectionAssert.DoesNotContain(ids, 1000);
        CollectionAssert.Contains(ids, 1001);
    }

    private static BatchResponseGroup GroupOf(params BatchExecuteResponse[] messages)
    {
        BatchResponseGroup group = new();
        foreach (BatchExecuteResponse message in messages)
            group.Add(message);
        return group;
    }

    [Test]
    public async Task AGroupTravelsTogetherAndInOrderWithFramesOn()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);
        writer.MarkPeerReadsFrames();

        // Nothing waits when the group arrives: the loop wakes on the group itself and must still send
        // its three messages as one stream message.
        await writer.WriteGroupAsync(GroupOf(Reply(1), Reply(2), Reply(3)), CancellationToken.None);
        await writer.CompleteAsync();

        Assert.AreEqual(1, transport.Written.Count, "a group of three is one frame even when it is alone");
        Assert.IsTrue(IsFrame(transport.Written[0]));
        CollectionAssert.AreEqual(new[] { 1, 2, 3 }, Flatten(transport.Written).Select(m => m.RequestId));
    }

    [Test]
    public async Task AGroupReachesAPeerWithoutFramesAsPlainMessagesInOrder()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);

        await writer.WriteGroupAsync(GroupOf(Reply(1), Reply(2), Reply(3)), CancellationToken.None);
        await writer.CompleteAsync();

        Assert.AreEqual(3, transport.Written.Count);
        Assert.IsFalse(transport.Written.Any(IsFrame));
        CollectionAssert.AreEqual(new[] { 1, 2, 3 }, transport.Written.Select(m => m.RequestId));
        Assert.IsTrue(transport.Writes[0].Buffered && transport.Writes[1].Buffered && !transport.Writes[2].Buffered,
            "the group is one burst: buffered writes, then one flush");
    }

    [Test]
    public async Task NoOtherMessageComesBetweenTheItemsOfAGroup()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);
        writer.MarkPeerReadsFrames();

        await ParkOnAsync(writer, transport, 0);
        await writer.WriteGroupAsync(GroupOf(Reply(10), Reply(11), Reply(12)), CancellationToken.None);
        await writer.WriteAsync(Reply(20), CancellationToken.None);
        await writer.WriteGroupAsync(GroupOf(Reply(30), Reply(31)), CancellationToken.None);

        transport.Release();
        await writer.CompleteAsync();

        CollectionAssert.AreEqual(new[] { 0, 10, 11, 12, 20, 30, 31 }, Flatten(transport.Written).Select(m => m.RequestId));
    }

    [Test]
    public async Task AGroupLargerThanAFrameIsSplitByTheLimitsInOrder()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);
        writer.MarkPeerReadsFrames();

        // 300 items in one group: more than MaxItems, so at least two frames.
        BatchResponseGroup group = GroupOf(Enumerable.Range(1, 300).Select(Reply).ToArray());
        await writer.WriteGroupAsync(group, CancellationToken.None);
        await writer.CompleteAsync();

        IReadOnlyList<BatchExecuteResponse> written = transport.Written;
        Assert.GreaterOrEqual(written.Count, 2);
        foreach (BatchExecuteResponse frame in written.Where(IsFrame))
            Assert.LessOrEqual(frame.Frame.Items.Count, BatchFrames.MaxItems);
        CollectionAssert.AreEqual(Enumerable.Range(1, 300), Flatten(written).Select(m => m.RequestId));
    }

    [Test]
    public async Task ALargeGroupHoldsALargeSlotUntilItIsWritten()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);

        await ParkOnAsync(writer, transport, 0);

        List<Task> accepted = new();
        for (int i = 1; i <= BatchResponseWriter.MaxQueuedLargeMessages; i++)
            accepted.Add(writer.WriteGroupAsync(GroupOf(RowOf(i, BatchResponseWriter.LargeMessageBytes / 2), RowOf(i, BatchResponseWriter.LargeMessageBytes / 2)), CancellationToken.None).AsTask());
        await Task.WhenAll(accepted).WaitAsync(Patience);

        Task overflow = writer.WriteGroupAsync(GroupOf(RowOf(99, BatchResponseWriter.LargeMessageBytes)), CancellationToken.None).AsTask();
        await Task.Delay(200);
        Assert.IsFalse(overflow.IsCompleted, "a large group counts against the large bound like a large message");

        transport.Release();
        await overflow.WaitAsync(Patience);
        await writer.CompleteAsync();
        Assert.AreEqual(1 + BatchResponseWriter.MaxQueuedLargeMessages * 2 + 1, Flatten(transport.Written).Count);
    }

    [Test]
    public async Task ConcurrentProducersKeepTheOrderOfEachRequestId()
    {
        GatedStreamWriter<BatchExecuteResponse> transport = new();
        BatchResponseWriter writer = new(transport, CancellationToken.None);
        writer.MarkPeerReadsFrames();

        const int producers = 16;
        const int perProducer = 400;

        // AffectedRows carries the sequence number inside one request id.
        await Task.WhenAll(Enumerable.Range(1, producers).Select(id => Task.Run(async () =>
        {
            for (int n = 0; n < perProducer; n++)
                await writer.WriteAsync(new BatchExecuteResponse { RequestId = id, NonQuery = new NonQueryReply { AffectedRows = n } }, CancellationToken.None);
        }))).WaitAsync(TimeSpan.FromSeconds(60));

        await writer.CompleteAsync();

        IReadOnlyList<BatchExecuteResponse> written = transport.Written;
        foreach (BatchExecuteResponse frame in written.Where(IsFrame))
        {
            Assert.LessOrEqual(frame.Frame.Items.Count, BatchFrames.MaxItems);
            Assert.Greater(frame.Frame.Items.Count, 1, "a frame of one must travel as the plain message");
            Assert.IsFalse(frame.Frame.Items.Any(IsFrame), "an item never holds a frame");
        }

        List<BatchExecuteResponse> flat = Flatten(written);
        Assert.AreEqual(producers * perProducer, flat.Count);
        foreach (IGrouping<int, BatchExecuteResponse> group in flat.GroupBy(m => m.RequestId))
            CollectionAssert.AreEqual(Enumerable.Range(0, perProducer), group.Select(m => m.NonQuery.AffectedRows));
    }
}
