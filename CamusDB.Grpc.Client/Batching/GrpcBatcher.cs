
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

namespace CamusDB.Grpc.Client.Batching;

/// <summary>
/// Multiplexes many concurrent operations — from many concurrent transactions — over a small pool of
/// long-lived <c>BatchExecute</c> duplex streams, so the network stays busy without a stream per op.
/// Modeled on the Kahuna client batcher.
///
/// <para><b>How it stays busy, else queues.</b> Every op is registered by a monotonic
/// <c>request_id</c>, dropped on a single inbox queue, and drained by a single-flight pump onto its
/// stream; a background reader per stream demultiplexes responses back to the waiting op by id.
/// Responses interleave and arrive out of order across ops. When streams are saturated the pump simply
/// keeps draining the queue.</para>
///
/// <para><b>Two routing regimes.</b> Autocommit ops (no transaction) round-robin across the pool for
/// maximum concurrency; a transaction pins <i>all</i> of its ops — START, statements, COMMIT/ROLLBACK —
/// to one stream (the caller reserves a slot via <see cref="ReserveSlot"/> and passes it on every call)
/// so the server's per-stream ordering chain actually sees them together. The pool bounds the number of
/// streams, not the number of in-flight transactions.</para>
/// </summary>
internal sealed class GrpcBatcher : IAsyncDisposable
{
    private readonly CamusGrpcOptions options;
    private readonly Slot[] slots;
    private readonly CancellationTokenSource shutdown = new();

    private readonly ConcurrentDictionary<int, PendingOp> pending = new();
    private readonly ConcurrentQueue<QueuedItem> inbox = new();

    private static int requestIdSeq;
    private int roundRobin = -1;
    private long transportIdSeq;
    private int processing;   // 0 = idle, 1 = a pump loop is running

    /// <summary>
    /// Builds a batcher over <paramref name="options"/>.<see cref="CamusGrpcOptions.ChannelPoolSize"/>
    /// transports produced by <paramref name="transportFactory"/> (the argument is a fresh transport id).
    /// The factory is called again to rebuild a slot after its stream faults.
    /// </summary>
    public GrpcBatcher(CamusGrpcOptions options, Func<long, IBatchTransport> transportFactory)
    {
        this.options = options;
        int poolSize = Math.Max(1, options.ChannelPoolSize);
        slots = new Slot[poolSize];
        for (int i = 0; i < poolSize; i++)
        {
            slots[i] = new Slot(i);
            // Connect the first transport synchronously so a slot is never written to before it exists;
            // the reader loop then owns reads and rebuilds the slot after a fault.
            slots[i].Transport = transportFactory(Interlocked.Increment(ref transportIdSeq));
            StartReaderLoop(slots[i], transportFactory);
        }
    }

    /// <summary>Reserves a stream slot for a transaction so all of its ops pin to one stream.</summary>
    public int ReserveSlot() => NextRoundRobin();

    private int NextRoundRobin()
        => (int)((uint)Interlocked.Increment(ref roundRobin) % (uint)slots.Length);

    // ─── Public enqueue surface ───────────────────────────────────────────────

    public Task<QueryResult> EnqueueQueryAsync(
        SqlRequest request, int? slotIndex, CancellationToken ct, long? expectedTransportId = null)
        => EnqueueAsync<QueryResult>(BatchStatementKind.Query, BatchOpKind.Query, request, slotIndex, ct, expectedTransportId);

    public Task<NonQueryResult> EnqueueNonQueryAsync(
        SqlRequest request, int? slotIndex, CancellationToken ct, long? expectedTransportId = null)
        => EnqueueAsync<NonQueryResult>(BatchStatementKind.NonQuery, BatchOpKind.NonQuery, request, slotIndex, ct, expectedTransportId);

    public Task<TxnHandle> EnqueueStartAsync(SqlRequest request, int slotIndex, CancellationToken ct)
        => EnqueueAsync<TxnHandle>(BatchStatementKind.Start, BatchOpKind.Start, request, slotIndex, ct);

    public Task<CausalToken> EnqueueCommitAsync(SqlRequest request, int slotIndex, CancellationToken ct)
        => EnqueueAsync<CausalToken>(BatchStatementKind.Commit, BatchOpKind.Commit, request, slotIndex, ct);

    public async Task EnqueueRollbackAsync(SqlRequest request, int slotIndex, CancellationToken ct)
        => await EnqueueAsync<object?>(BatchStatementKind.Rollback, BatchOpKind.Rollback, request, slotIndex, ct)
            .ConfigureAwait(false);

    private async Task<T> EnqueueAsync<T>(
        BatchStatementKind kind, BatchOpKind opKind, SqlRequest request, int? slotIndex, CancellationToken ct,
        long? expectedTransportId = null)
        => (await EnqueueTrackedAsync<T>(kind, opKind, request, slotIndex, ct, expectedTransportId).ConfigureAwait(false)).Result;

    /// <summary>
    /// Enqueues an op and also reports the transport it was written to. PREPARE needs that: the id it
    /// mints is only valid on the stream that carried the PREPARE, so caching the id we <em>hoped</em>
    /// to write to — rather than the one we did — would make every entry a guess.
    /// </summary>
    private async Task<(T Result, long TransportId)> EnqueueTrackedAsync<T>(
        BatchStatementKind kind, BatchOpKind opKind, SqlRequest request, int? slotIndex, CancellationToken ct,
        long? expectedTransportId = null)
    {
        int slot = slotIndex ?? NextRoundRobin();
        int id = Interlocked.Increment(ref requestIdSeq);

        // Apply a default deadline when the caller gave no cancellable token, so a wedged stream cannot
        // hang the caller forever. The linked source is disposed when the op completes.
        CancellationTokenSource? deadline = null;
        if (!ct.CanBeCanceled && options.OperationTimeout > TimeSpan.Zero)
        {
            deadline = new CancellationTokenSource(options.OperationTimeout);
            ct = deadline.Token;
        }

        PendingOp op = new(id, opKind, deadline);

        if (ct.CanBeCanceled)
            op.Registration = ct.Register(static state =>
            {
                PendingOp o = (PendingOp)state!;
                o.Owner!.Fault(o, new OperationCanceledException());
            }, op);
        op.Owner = this;

        pending[id] = op;

        BatchExecuteRequest wire = new() { RequestId = id, Kind = kind, Request = request };
        inbox.Enqueue(new QueuedItem(wire, slot, op, expectedTransportId));
        TryStartPump();

        object? result = await op.Promise.Task.ConfigureAwait(false);
        return ((T)result!, op.TransportId);
    }

    // ─── Prepared statements ──────────────────────────────────────────────────

    /// <summary>
    /// Returns this slot's registration for the statement, preparing it first if the slot has none
    /// for its <b>current</b> transport.
    ///
    /// <para>This lives on the batcher rather than on the caller because only the batcher can read a
    /// slot's current transport id and write the follow-up op through the same path; a cache kept
    /// anywhere else would be comparing against an id it cannot keep in step.</para>
    ///
    /// <para>Concurrent callers racing to prepare the same statement share one in-flight registration
    /// (the dictionary holds the task, not the result), so the server is not asked to register the
    /// same SQL twice — harmless if it happened, but it would waste a handle from the caller's cap.</para>
    /// </summary>
    public async Task<PreparedSlotEntry> EnsurePreparedAsync(
        int slotIndex, PreparedStatementKey key, CancellationToken ct)
    {
        Slot slot = slots[slotIndex];

        while (true)
        {
            if (slot.Prepared.TryGetValue(key, out Task<PreparedSlotEntry>? existing))
            {
                PreparedSlotEntry entry;
                try
                {
                    entry = await existing.ConfigureAwait(false);
                }
                catch
                {
                    // Whoever created it already reported the failure to its own caller; drop the
                    // poisoned entry and take a fresh turn rather than failing every later execution.
                    slot.Prepared.TryRemove(new KeyValuePair<PreparedStatementKey, Task<PreparedSlotEntry>>(key, existing));
                    continue;
                }

                if (entry.TransportId == slot.Transport?.Id)
                    return entry;

                // The slot's stream was rebuilt since this was registered — the handle died with it.
                slot.Prepared.TryRemove(new KeyValuePair<PreparedStatementKey, Task<PreparedSlotEntry>>(key, existing));
                continue;
            }

            TaskCompletionSource<PreparedSlotEntry> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!slot.Prepared.TryAdd(key, promise.Task))
                continue;   // lost the race; the winner's registration is the one to use.

            try
            {
                (PrepareReply reply, long transportId) = await EnqueueTrackedAsync<PrepareReply>(
                    BatchStatementKind.Prepare, BatchOpKind.Prepare,
                    new SqlRequest { Database = key.Database, Sql = key.Sql }, slotIndex, ct).ConfigureAwait(false);

                PreparedSlotEntry entry = new(transportId, reply.StatementId, reply.ParameterNames.ToArray());
                promise.SetResult(entry);
                return entry;
            }
            catch (Exception ex)
            {
                slot.Prepared.TryRemove(new KeyValuePair<PreparedStatementKey, Task<PreparedSlotEntry>>(key, promise.Task));
                promise.TrySetException(ex);
                _ = promise.Task.Exception;   // observed here; the throw below is what callers see.
                throw;
            }
        }
    }

    /// <summary>
    /// Forgets a slot's registration for a statement, but only if it is still the one the caller was
    /// using — so a concurrent re-prepare that already succeeded is not thrown away by a straggler
    /// reacting to the old entry.
    /// </summary>
    public void InvalidatePrepared(int slotIndex, PreparedStatementKey key, PreparedSlotEntry stale)
    {
        Slot slot = slots[slotIndex];

        if (slot.Prepared.TryGetValue(key, out Task<PreparedSlotEntry>? existing)
            && existing.IsCompletedSuccessfully
            && existing.Result.StatementId == stale.StatementId
            && existing.Result.TransportId == stale.TransportId)
        {
            slot.Prepared.TryRemove(new KeyValuePair<PreparedStatementKey, Task<PreparedSlotEntry>>(key, existing));
        }
    }

    /// <summary>
    /// Removes every slot registration for a statement and returns them <b>as tasks</b>, for a caller
    /// that is disposing it and must release the server-side handles.
    ///
    /// <para>In-flight registrations are returned too, not just finished ones. That is what makes
    /// disposal complete: a registration still in progress would otherwise mint a handle after
    /// disposal had already removed the only reference to it, leaking it until the stream ends. The
    /// caller awaits each task and closes whatever id it produced.</para>
    /// </summary>
    public IReadOnlyList<(int SlotIndex, Task<PreparedSlotEntry> Registration)> TakePrepared(PreparedStatementKey key)
    {
        List<(int, Task<PreparedSlotEntry>)> taken = new();

        foreach (Slot slot in slots)
        {
            if (slot.Prepared.TryRemove(key, out Task<PreparedSlotEntry>? registration))
                taken.Add((slot.Index, registration));
        }

        return taken;
    }

    /// <summary>
    /// Releases a prepared statement on one slot. Best-effort by contract: the stream may already be
    /// gone, in which case the server has freed the handle anyway, so a failure here is not worth
    /// reporting to the caller.
    /// </summary>
    public async Task ClosePreparedAsync(int slotIndex, PreparedSlotEntry entry, CancellationToken ct)
    {
        try
        {
            await EnqueueAsync<object?>(
                BatchStatementKind.Close, BatchOpKind.Close,
                new SqlRequest { StatementId = entry.StatementId }, slotIndex, ct, entry.TransportId).ConfigureAwait(false);
        }
        catch
        {
            // Stream already gone, or the handle was released with it — nothing to do.
        }
    }

    // ─── Pump ─────────────────────────────────────────────────────────────────

    private void TryStartPump()
    {
        if (Interlocked.CompareExchange(ref processing, 1, 0) == 0)
            _ = DeliverMessagesAsync();
    }

    private async Task DeliverMessagesAsync()
    {
        try
        {
            while (true)
            {
                int drained = 0;
                while (inbox.TryDequeue(out QueuedItem item))
                {
                    await WriteItemAsync(item).ConfigureAwait(false);
                    drained++;
                }

                // Coalesce: after writing a small batch, pause briefly so more ops accumulate before the
                // next drain writes them together.
                if (drained > 0 && options.CoalescingThreshold > 1
                    && drained < options.CoalescingThreshold && options.CoalescingDelayMs > 0)
                {
                    try { await Task.Delay(options.CoalescingDelayMs, shutdown.Token).ConfigureAwait(false); }
                    catch (OperationCanceledException) { return; }
                }

                Interlocked.Exchange(ref processing, 0);   // mark idle
                if (inbox.IsEmpty)
                    return;
                // Items arrived between drain and idle — re-acquire, or bail if another pump took over.
                if (Interlocked.CompareExchange(ref processing, 1, 0) != 0)
                    return;
            }
        }
        catch
        {
            Interlocked.Exchange(ref processing, 0);
        }
    }

    private async Task WriteItemAsync(QueuedItem item)
    {
        try
        {
            Slot slot = slots[item.SlotIndex];
            IBatchTransport transport = slot.Transport
                ?? throw new InvalidOperationException("Transport slot is not connected");

            // A prepared execution names a handle that exists only on the stream it was registered
            // on. This is the last moment the two can be compared — check any earlier and the stream
            // could still be rebuilt in between — so refuse here rather than send an op that the
            // server can only answer with "unknown statement".
            if (item.ExpectedTransportId is long expected && transport.Id != expected)
                throw new PreparedStatementStaleException();

            item.Op.TransportId = transport.Id;

            await slot.WriteLock.WaitAsync(shutdown.Token).ConfigureAwait(false);
            try { await transport.SendAsync(item.Request, shutdown.Token).ConfigureAwait(false); }
            finally { slot.WriteLock.Release(); }
        }
        catch (Exception ex)
        {
            Fault(item.Op, ex);
        }
    }

    // ─── Reader / demux ───────────────────────────────────────────────────────

    private void StartReaderLoop(Slot slot, Func<long, IBatchTransport> factory)
    {
        _ = Task.Run(async () =>
        {
            while (!shutdown.IsCancellationRequested)
            {
                IBatchTransport transport = slot.Transport!;
                Exception fault = new IOException("gRPC batch stream closed");
                try
                {
                    await foreach (BatchExecuteResponse resp in transport.ReadAllAsync(shutdown.Token).ConfigureAwait(false))
                        Demux(resp);
                }
                catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    fault = ex;
                }
                finally
                {
                    try { await transport.DisposeAsync().ConfigureAwait(false); } catch { /* best effort */ }
                }

                // Fail this transport's still-pending ops so callers see the fault and can replay.
                slot.Transport = null;
                FailTransportPending(transport.Id, fault);
                if (shutdown.IsCancellationRequested)
                    break;

                // Rebuild the slot with a fresh transport for subsequent ops.
                slot.Transport = factory(Interlocked.Increment(ref transportIdSeq));
            }
        });
    }

    private void Demux(BatchExecuteResponse resp)
    {
        if (!pending.TryGetValue(resp.RequestId, out PendingOp? op))
            return;   // cancelled, timed out, or already completed — drop.

        switch (resp.PayloadCase)
        {
            case BatchExecuteResponse.PayloadOneofCase.Schema:
                op.Schema = resp.Schema;
                break;
            case BatchExecuteResponse.PayloadOneofCase.Row:
                op.Rows.Add(resp.Row);
                break;
            case BatchExecuteResponse.PayloadOneofCase.QueryComplete:
                Complete(op, new QueryResult(
                    op.Schema ?? new ResultSchema(), op.Rows,
                    new CausalToken(resp.QueryComplete.CausalTokenN, resp.QueryComplete.CausalTokenL, resp.QueryComplete.CausalTokenC)));
                break;
            case BatchExecuteResponse.PayloadOneofCase.NonQuery:
                Complete(op, new NonQueryResult(
                    resp.NonQuery.AffectedRows,
                    new CausalToken(resp.NonQuery.CausalTokenN, resp.NonQuery.CausalTokenL, resp.NonQuery.CausalTokenC)));
                break;
            case BatchExecuteResponse.PayloadOneofCase.StartReply:
                Complete(op, resp.StartReply);
                break;
            case BatchExecuteResponse.PayloadOneofCase.CommitReply:
                Complete(op, new CausalToken(
                    resp.CommitReply.CausalTokenN, resp.CommitReply.CausalTokenL, resp.CommitReply.CausalTokenC));
                break;
            case BatchExecuteResponse.PayloadOneofCase.RollbackReply:
                Complete(op, null);
                break;
            case BatchExecuteResponse.PayloadOneofCase.PrepareReply:
                Complete(op, resp.PrepareReply);
                break;
            case BatchExecuteResponse.PayloadOneofCase.CloseReply:
                Complete(op, null);
                break;
            case BatchExecuteResponse.PayloadOneofCase.Error:
                Fault(op, new CamusGrpcException(resp.Error.Code, resp.Error.Message));
                break;
        }
    }

    private void Complete(PendingOp op, object? result)
    {
        if (!pending.TryRemove(op.RequestId, out _))
            return;
        op.Dispose();
        op.Promise.TrySetResult(result);
    }

    private void Fault(PendingOp op, Exception ex)
    {
        if (!pending.TryRemove(op.RequestId, out _))
            return;
        op.Dispose();
        if (ex is OperationCanceledException oce)
            op.Promise.TrySetCanceled(oce.CancellationToken);
        else
            op.Promise.TrySetException(ex);
    }

    private void FailTransportPending(long transportId, Exception ex)
    {
        foreach (KeyValuePair<int, PendingOp> entry in pending.ToArray())
            if (entry.Value.TransportId == transportId)
                Fault(entry.Value, ex);
    }

    public async ValueTask DisposeAsync()
    {
        shutdown.Cancel();
        foreach (Slot slot in slots)
        {
            IBatchTransport? t = slot.Transport;
            if (t is not null)
            {
                try { await t.DisposeAsync().ConfigureAwait(false); } catch { /* best effort */ }
            }
        }
        foreach (KeyValuePair<int, PendingOp> entry in pending.ToArray())
            Fault(entry.Value, new ObjectDisposedException(nameof(GrpcBatcher)));
        shutdown.Dispose();
    }

    // ─── Nested state ─────────────────────────────────────────────────────────

    private sealed class Slot
    {
        public readonly int Index;
        public readonly SemaphoreSlim WriteLock = new(1, 1);
        public volatile IBatchTransport? Transport;

        /// <summary>
        /// Statements registered on this slot, keyed by (database, sql). The value is the in-flight
        /// or finished registration rather than the result, so concurrent first-executions of the
        /// same statement await one PREPARE instead of each sending their own.
        /// </summary>
        public readonly ConcurrentDictionary<PreparedStatementKey, Task<PreparedSlotEntry>> Prepared = new();

        public Slot(int index) => Index = index;
    }

    private readonly record struct QueuedItem(
        BatchExecuteRequest Request, int SlotIndex, PendingOp Op, long? ExpectedTransportId);

    /// <summary>One in-flight op awaiting its terminal response, plus the accumulator a QUERY needs.</summary>
    private sealed class PendingOp
    {
        public readonly int RequestId;
        public readonly BatchOpKind Kind;
        public readonly TaskCompletionSource<object?> Promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ResultSchema? Schema;
        public readonly List<ResultRow> Rows = new();
        public long TransportId;
        public GrpcBatcher? Owner;
        public CancellationTokenRegistration Registration;
        private readonly CancellationTokenSource? deadline;

        public PendingOp(int requestId, BatchOpKind kind, CancellationTokenSource? deadline)
        {
            RequestId = requestId;
            Kind = kind;
            this.deadline = deadline;
        }

        public void Dispose()
        {
            Registration.Dispose();
            deadline?.Dispose();
        }
    }
}
