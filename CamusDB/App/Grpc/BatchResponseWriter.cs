
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Channels;
using Google.Protobuf.Collections;
using Grpc.Core;
using CamusDB.Grpc;

namespace CamusDB.App.Grpc;

/// <summary>
/// The only writer of one <c>BatchExecute</c> response stream. Op handlers and row sinks hand their
/// messages to a bounded queue; one loop drains the queue and writes to the transport. gRPC forbids
/// concurrent writes on a stream, and a queue with one reader gives that exclusion without a lock
/// hand-off per message.
///
/// <para><b>Order.</b> The queue is first-in first-out and has one reader, so the messages of one
/// <c>request_id</c> (<c>schema</c>, then rows, then the terminator) reach the wire in the order their
/// op produced them. Do not shard the queue and do not add a second reader: either one can reorder the
/// messages of a result.</para>
///
/// <para><b>One flush per burst.</b> A write that carries <see cref="WriteFlags.BufferHint"/> only
/// appends to the response pipe; a write without it also flushes the pipe to the socket. The loop sets
/// the hint while another message already waits, so a burst costs one flush. A lone message is written
/// and flushed exactly as before. The hint is dropped after <see cref="MaxUnflushedBytes"/>, because a
/// queue that never runs empty would otherwise never flush.</para>
///
/// <para><b>Response frames.</b> After the peer proved that it reads response frames
/// (<see cref="MarkPeerReadsFrames"/>), the loop packs the messages that <i>already wait</i> into one
/// <see cref="BatchResponseFrame"/>, inside <see cref="BatchFrames.MaxItems"/> and
/// <see cref="BatchFrames.MaxBytes"/>. It never waits for more messages, so the first row of a result
/// is never held back. A frame of one travels as the plain message, and a message over the byte budget
/// on its own travels as a plain message too. A peer that gave no proof never receives a frame: a
/// client built before frames does not know the frame payload.</para>
///
/// <para><b>Groups.</b> The loop can pack only what waits when it wakes, and it wakes on the first
/// message. The messages of one small result (<c>schema</c>, row, terminator) are produced a few
/// microseconds apart, so handed over one by one they mostly travel apart: measured at 1.6 items per
/// stream message for a point read. A producer that knows its messages belong together hands them
/// over as one <see cref="BatchResponseGroup"/>, which is one queue entry: no other op can come
/// between its items, and all of them wait together. The loop still packs <i>item by item</i>, so a
/// group can share a frame with other messages, can be split over two frames by the limits, and
/// reaches a peer without frames as plain messages in the same order.</para>
///
/// <para><b>Backpressure.</b> The queue holds at most <see cref="MaxQueuedMessages"/> entries (a
/// message or a group; a group is bounded by its producer, see <see cref="BatchQuerySink"/>), of
/// which at most <see cref="MaxQueuedLargeMessages"/> may be <see cref="LargeMessageBytes"/> or larger.
/// A producer awaits space, so a result set larger than the queue stalls its cursor when the client
/// reads slowly, instead of collecting in server memory. The bound is per stream and the loop always
/// makes progress, so a terminal reply of one op waits behind at most one queue of rows of another op;
/// it is not starved. One queue is also one full frame, so a larger queue could not make a frame
/// fuller.</para>
///
/// <para><b>A delivery failure ends the writer, not the ops.</b> When a transport write fails or the
/// stream's token fires, the loop stops, drops what is left (nobody can read it) and cancels
/// <see cref="WriteAsync"/> for every producer, so a query whose client left aborts and rolls back as
/// it did when it wrote to the transport itself.</para>
/// </summary>
internal sealed class BatchResponseWriter
{
    /// <summary>Most messages that wait for the transport. One full frame.</summary>
    internal const int MaxQueuedMessages = BatchFrames.MaxItems;

    /// <summary>A message of this many bytes or more counts against <see cref="MaxQueuedLargeMessages"/>.</summary>
    internal const int LargeMessageBytes = 64 * 1024;

    /// <summary>
    /// Most large messages that wait for the transport. Without it, a result of rows that each carry a
    /// large value could hold <see cref="MaxQueuedMessages"/> of them in memory per stream.
    /// </summary>
    internal const int MaxQueuedLargeMessages = 4;

    /// <summary>Most bytes written with the buffer hint before a write must flush.</summary>
    internal const int MaxUnflushedBytes = 64 * 1024;

    private static readonly WriteOptions BufferedWrite = new(WriteFlags.BufferHint);

    private static readonly WriteOptions FlushingWrite = new();

    private readonly IServerStreamWriter<BatchExecuteResponse> stream;

    private readonly Channel<Pending> queue;

    private readonly SemaphoreSlim largeGate = new(MaxQueuedLargeMessages, MaxQueuedLargeMessages);

    // Fires when the stream's token fires or when the loop stops for any reason. Every producer wait
    // observes it, so no producer can wait for a loop that no longer runs.
    private readonly CancellationTokenSource lifetime;

    private readonly CancellationToken lifetimeToken;

    private readonly CancellationToken streamToken;

    // One envelope per stream, refilled for every frame. An awaited write has serialized its message
    // when it returns, and the loop is the only writer, so the envelope is free again at that point.
    // It is never handed to anything that outlives the write.
    private readonly BatchExecuteResponse frameEnvelope = new() { Frame = new BatchResponseFrame() };

    private readonly Task loop;

    private volatile bool peerReadsFrames;

    // The queue entry the loop reads from at present, and the next item of it. Loop-only state. An
    // entry leaves the queue when the loop starts it, so a group can be consumed over several frames.
    private Pending head;

    private bool hasHead;

    private int headIndex;

    private int unflushedBytes;

    /// <summary>
    /// Starts the writer loop. <paramref name="streamToken"/> is the token of the call (client gone,
    /// server stopping, authority ended); it ends the loop and every producer wait.
    /// </summary>
    public BatchResponseWriter(IServerStreamWriter<BatchExecuteResponse> stream, CancellationToken streamToken)
    {
        this.stream = stream;
        this.streamToken = streamToken;

        lifetime = CancellationTokenSource.CreateLinkedTokenSource(streamToken);
        lifetimeToken = lifetime.Token;

        queue = Channel.CreateBounded<Pending>(new BoundedChannelOptions(MaxQueuedMessages)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
        });

        loop = Task.Run(RunAsync);
    }

    /// <summary>True after the peer proved that it reads response frames. Never goes back to false.</summary>
    public bool PeerReadsFrames => peerReadsFrames;

    /// <summary>
    /// Records a proof that the peer reads response frames: it said so in the request headers of the
    /// stream (<see cref="BatchFrames.AcceptHeaderName"/>), or it sent a request frame. Without a
    /// proof no frame is sent, because a client built before frames does not know the payload.
    /// Messages already in the queue may travel in a frame from this point; that is safe, because
    /// the proof concerns the peer, not the message.
    /// </summary>
    public void MarkPeerReadsFrames() => peerReadsFrames = true;

    /// <summary>
    /// Hands one message to the writer and returns when the queue accepted it, not when the transport
    /// wrote it. Awaits queue space, which is the backpressure on a cursor. Throws
    /// <see cref="OperationCanceledException"/> when <paramref name="ct"/> fires, when the stream's
    /// token fires, or when the writer stopped because the transport failed.
    /// </summary>
    public ValueTask WriteAsync(BatchExecuteResponse message, CancellationToken ct)
    {
        int cost = BatchFrames.ItemCost(message.CalculateSize());

        return WritePendingAsync(new Pending(message, null, cost, cost >= LargeMessageBytes), ct);
    }

    /// <summary>
    /// Hands the messages of one op that belong together to the writer as <b>one queue entry</b>, so
    /// they wait together and no other op's message comes between them. Same waiting, cancellation
    /// and failure rules as <see cref="WriteAsync"/>. The group belongs to the writer after the call;
    /// the producer must not touch it again.
    /// </summary>
    public ValueTask WriteGroupAsync(BatchResponseGroup group, CancellationToken ct)
        => group.Count == 0
            ? ValueTask.CompletedTask
            : WritePendingAsync(new Pending(null, group, group.Bytes, group.Bytes >= LargeMessageBytes), ct);

    private ValueTask WritePendingAsync(Pending pending, CancellationToken ct)
    {
        if (!pending.Large && !lifetimeToken.IsCancellationRequested && queue.Writer.TryWrite(pending))
            return ValueTask.CompletedTask;

        return WriteSlowAsync(pending, ct);
    }

    /// <summary>
    /// <see cref="WriteAsync"/> for a terminal message (a reply or a <c>BatchError</c>): a delivery
    /// failure is swallowed, because the real outcome of the op already happened and must not be
    /// reported a second time as an error.
    /// </summary>
    public async ValueTask TryWriteAsync(BatchExecuteResponse message, CancellationToken ct)
    {
        try
        {
            await WriteAsync(message, ct).ConfigureAwait(false);
        }
        catch
        {
            // Stream gone or cancelled — best effort.
        }
    }

    /// <summary>
    /// Closes the queue, waits until the loop wrote everything that was accepted, and releases the
    /// writer. Call it after every op of the stream completed, so no producer remains. Never throws.
    /// After it returns, no write to the transport is in progress or can start.
    /// </summary>
    public async Task CompleteAsync()
    {
        queue.Writer.TryComplete();

        try { await loop.ConfigureAwait(false); } catch { /* the loop reports nothing */ }

        lifetime.Dispose();
    }

    private async ValueTask WriteSlowAsync(Pending pending, CancellationToken ct)
    {
        // The common caller passes the stream's own token, which the lifetime token already covers.
        // Only a different token (a statement's own cancellation) needs a link.
        CancellationTokenSource? linked = ct.CanBeCanceled && ct != streamToken
            ? CancellationTokenSource.CreateLinkedTokenSource(ct, lifetimeToken)
            : null;
        CancellationToken token = linked?.Token ?? lifetimeToken;

        bool large = pending.Large;
        bool holdsLargeSlot = false;

        try
        {
            if (large)
            {
                await largeGate.WaitAsync(token).ConfigureAwait(false);
                holdsLargeSlot = true;
            }

            await queue.Writer.WriteAsync(pending, token).ConfigureAwait(false);

            // The loop releases the slot after it wrote the message.
            holdsLargeSlot = false;
        }
        catch (ChannelClosedException)
        {
            throw new OperationCanceledException("The batch response stream is closed");
        }
        finally
        {
            if (holdsLargeSlot)
                largeGate.Release();

            linked?.Dispose();
        }
    }

    private async Task RunAsync()
    {
        ChannelReader<Pending> reader = queue.Reader;

        try
        {
            while (await reader.WaitToReadAsync(lifetimeToken).ConfigureAwait(false))
            {
                while (TryPeekItem(reader, out BatchExecuteResponse message, out int cost))
                {
                    if (peerReadsFrames && cost <= BatchFrames.MaxBytes)
                        await WriteFrameAsync(reader).ConfigureAwait(false);
                    else
                        await WriteSingleAsync(reader, message, cost).ConfigureAwait(false);
                }
            }
        }
        catch
        {
            // The client went away, the call was cancelled, or the transport refused a write. What is
            // left in the queue has no reader, so it is dropped.
        }
        finally
        {
            queue.Writer.TryComplete();

            // Unblocks every producer that waits for queue space or for a large slot.
            try { lifetime.Cancel(); } catch (ObjectDisposedException) { /* already released */ }
        }
    }

    /// <summary>
    /// The next item to write, without taking it: the next item of the entry in progress, or the
    /// first item of the next queue entry. The loop is the only reader, which is what makes the item
    /// that was looked at and the item that <see cref="TakeItem"/> takes the same item.
    /// </summary>
    private bool TryPeekItem(ChannelReader<Pending> reader, out BatchExecuteResponse message, out int cost)
    {
        if (!hasHead)
        {
            if (!reader.TryRead(out head))
            {
                message = null!;
                cost = 0;
                return false;
            }

            hasHead = true;
            headIndex = 0;
        }

        if (head.Group is { } group)
        {
            message = group.MessageAt(headIndex);
            cost = group.CostAt(headIndex);
        }
        else
        {
            message = head.Single!;
            cost = head.Cost;
        }

        return true;
    }

    /// <summary>Takes the item <see cref="TryPeekItem"/> showed. Returns true when that ended an entry
    /// that holds a large slot, which the caller releases after the write.</summary>
    private bool TakeItem()
    {
        headIndex++;

        if (head.Group is { } group && headIndex < group.Count)
            return false;

        bool releasesLargeSlot = head.Large;
        head = default;
        hasHead = false;
        return releasesLargeSlot;
    }

    private bool MoreWaits(ChannelReader<Pending> reader) => hasHead || reader.TryPeek(out _);

    private async Task WriteSingleAsync(ChannelReader<Pending> reader, BatchExecuteResponse message, int cost)
    {
        bool releasesLargeSlot = TakeItem();

        try
        {
            SetFlush(reader, cost);
            await stream.WriteAsync(message, lifetimeToken).ConfigureAwait(false);
        }
        finally
        {
            if (releasesLargeSlot)
                largeGate.Release();
        }
    }

    /// <summary>
    /// Packs the next item and the items already behind it into the frame envelope and writes it
    /// once. An item is looked at, measured, and only then taken, so one that would break the byte
    /// budget stays where it is for the next pass. The caller guarantees that the first item fits.
    /// </summary>
    private async Task WriteFrameAsync(ChannelReader<Pending> reader)
    {
        RepeatedField<BatchExecuteResponse> items = frameEnvelope.Frame.Items;
        int bytes = 0;
        int largeSlots = 0;

        try
        {
            while (items.Count < BatchFrames.MaxItems
                && TryPeekItem(reader, out BatchExecuteResponse next, out int cost)
                && (long)bytes + cost <= BatchFrames.MaxBytes)
            {
                if (TakeItem())
                    largeSlots++;

                items.Add(next);
                bytes += cost;
            }

            if (items.Count == 0)
                return;

            SetFlush(reader, bytes);

            // A frame of one is the plain message: a quiet stream stays byte-identical.
            await stream.WriteAsync(items.Count == 1 ? items[0] : frameEnvelope, lifetimeToken).ConfigureAwait(false);
        }
        finally
        {
            items.Clear();

            if (largeSlots > 0)
                largeGate.Release(largeSlots);
        }
    }

    /// <summary>
    /// Chooses whether the next write flushes. It buffers only while another message already waits —
    /// that message is then certain to be written in this pass and to carry the flush, or a later one
    /// is — and only up to <see cref="MaxUnflushedBytes"/>.
    /// </summary>
    private void SetFlush(ChannelReader<Pending> reader, int bytes)
    {
        bool buffer = MoreWaits(reader) && unflushedBytes + bytes < MaxUnflushedBytes;

        unflushedBytes = buffer ? unflushedBytes + bytes : 0;
        stream.WriteOptions = buffer ? BufferedWrite : FlushingWrite;
    }

    /// <summary>One queue entry: a single message or a group, with its frame cost measured once by the
    /// producer. <c>Large</c> entries hold a slot of the large-message bound until they are written.</summary>
    private readonly record struct Pending(BatchExecuteResponse? Single, BatchResponseGroup? Group, int Cost, bool Large);
}

/// <summary>
/// Messages of one op that belong together — a small result, or a run of rows — handed to
/// <see cref="BatchResponseWriter.WriteGroupAsync"/> as one queue entry. It measures each message
/// when it is added, on the producer's thread, so the writer loop does not measure again. Not
/// thread-safe: one producer fills it, then the writer owns it.
/// </summary>
internal sealed class BatchResponseGroup
{
    private readonly List<BatchExecuteResponse> messages = new(4);

    private readonly List<int> costs = new(4);

    public int Count => messages.Count;

    /// <summary>The sum of the frame costs of the messages.</summary>
    public int Bytes { get; private set; }

    public void Add(BatchExecuteResponse message)
    {
        int cost = BatchFrames.ItemCost(message.CalculateSize());
        messages.Add(message);
        costs.Add(cost);
        Bytes += cost;
    }

    public BatchExecuteResponse MessageAt(int index) => messages[index];

    public int CostAt(int index) => costs[index];
}
