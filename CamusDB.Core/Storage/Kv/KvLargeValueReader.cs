/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using Kahuna;
using Kommander.Time;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Resolves the compressed and out-of-line cells of rows a store read returned, before the bytes
/// reach a decoder.
///
/// <para><b>Why resolution is eager and batched.</b> A decoded row may be backed by a borrowed
/// <see cref="RowView"/> whose accessors are synchronous, so no cell can fetch its value when it is
/// first read. Every cell a caller needs is therefore resolved here, at the batch boundary, and the
/// out-of-line values of a whole batch of rows are fetched in one
/// <see cref="KvBranchReader.ProbeManyRaw"/> round trip plus one per ancestry level for the values a
/// branch inherits. A row with no storage-form trailer is returned untouched and costs one byte
/// test.</para>
///
/// <para><b>Snapshot and folding.</b> A value is read with the same transaction identity, read
/// timestamp and read-set folding as the row read, so an optimistic transaction validates it at commit
/// exactly as it validates the row. A branch walks its ancestry nearest level first at each fork
/// timestamp; a tombstone ends the walk, the same rule a row read follows.</para>
///
/// <para><b>Torn reads.</b> When the transaction has no read timestamp, the row and its values are
/// read at two different moments, and a concurrent update may commit between them. Each pointer
/// records the length and XxHash64 of the value it names, so that case is detected: the row is read
/// again and resolved again, a bounded number of times, and the caller then gets a retryable
/// <see cref="CamusDBErrorCodes.TransactionMustRetry"/>. With a read timestamp both reads see one
/// snapshot, so a mismatch there means damaged data and raises
/// <see cref="CamusDBErrorCodes.LargeValueCorrupt"/>.</para>
///
/// <para><b>Memory bound.</b> A compressed cell decodes to up to 255 times its stored size, so the rows
/// of a batch say little about the memory their values need. The rows to resolve are split into chunks
/// whose recorded decoded sizes (<see cref="RowStorageForms.MarkedRawBytes"/>) stay within
/// <see cref="CamusDBOptions.LargeValueResolveBatchBytes"/>, and each chunk is rebuilt before the next
/// one is resolved, so the per-cell buffers of only one chunk are alive at a time. A row larger than the
/// bound is a chunk of its own. The caller still holds every rebuilt row it asked for, so a scan also
/// closes its window on the same bound.</para>
///
/// <para><b>Locks.</b> This reader takes no lock of its own. Every write of a large value also writes
/// its row key, because the pointer in the row changes with it, so the shared lock a Serializable read
/// holds on the row key already blocks any writer of the row's values.</para>
/// </summary>
internal sealed class KvLargeValueReader
{
    /// <summary>How many times a row whose values changed under a read-committed read is read again.</summary>
    internal const int MaxTornReadAttempts = 8;

    private readonly KvKeyBuilder keys;
    private readonly KvBranchReader branch;

    private long fetchCalls;

    private long resolveBatchBytes;

    /// <summary>
    /// The current <see cref="CamusDBOptions.LargeValueResolveBatchBytes"/>. Read once per operation; a
    /// new value from <see cref="ApplyOptions"/> applies to the next one.
    /// </summary>
    internal long ResolveBatchBytes => Interlocked.Read(ref resolveBatchBytes);

    /// <summary>
    /// Batched out-of-line fetches issued since the store was built: one per resolution that needed at
    /// least one value, however many values it fetched. An observability counter that also lets tests
    /// prove a query that does not read a large column fetches nothing.
    /// </summary>
    internal long FetchCalls => Interlocked.Read(ref fetchCalls);

    internal KvLargeValueReader(KvKeyBuilder keys, KvBranchReader branch, CamusDBOptions options)
    {
        this.keys = keys;
        this.branch = branch;
        resolveBatchBytes = options.LargeValueResolveBatchBytes;
    }

    /// <summary>Takes the resolution byte bound of a newly published configuration snapshot.</summary>
    internal void ApplyOptions(CamusDBOptions next) => Interlocked.Exchange(ref resolveBatchBytes, next.LargeValueResolveBatchBytes);

    /// <summary>
    /// Replaces each entry of <paramref name="rows"/> that carries marked cells with a copy in which
    /// the cells <paramref name="fetch"/> requires are resolved. An entry may become null when a
    /// read-committed re-read finds the row deleted. <paramref name="rowIds"/> is aligned with
    /// <paramref name="rows"/>.
    /// </summary>
    internal async Task ResolveAsync(
        KvTransaction tx,
        IReadOnlyList<ObjectIdValue> rowIds,
        ReadOnlyMemory<byte>?[] rows,
        LargeValueFetch? fetch,
        CancellationToken cancellationToken)
    {
        if (fetch is { IsRaw: true })
            return;

        List<int>? pending = null;
        for (int i = 0; i < rows.Length; i++)
        {
            if (rows[i] is { } row && RowStorageForms.HasTrailer(row.Span))
                (pending ??= []).Add(i);
        }

        if (pending is null)
            return;

        long budget = ResolveBatchBytes;
        if (budget <= 0 || pending.Count == 1)
        {
            await ResolveChunkAsync(tx, rowIds, rows, pending, fetch, cancellationToken).ConfigureAwait(false);
            return;
        }

        List<int> chunk = [];
        long chunkBytes = 0;
        foreach (int i in pending)
        {
            long rowBytes = RowStorageForms.MarkedRawBytes(rows[i]!.Value.Span);
            if (chunk.Count > 0 && chunkBytes + rowBytes > budget)
            {
                await ResolveChunkAsync(tx, rowIds, rows, chunk, fetch, cancellationToken).ConfigureAwait(false);
                chunk = [];
                chunkBytes = 0;
            }

            chunk.Add(i);
            chunkBytes += rowBytes;
        }

        await ResolveChunkAsync(tx, rowIds, rows, chunk, fetch, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves the rows at <paramref name="pending"/>, reading again the rows whose values changed under
    /// a read-committed read, a bounded number of times.
    /// </summary>
    private async Task ResolveChunkAsync(
        KvTransaction tx,
        IReadOnlyList<ObjectIdValue> rowIds,
        ReadOnlyMemory<byte>?[] rows,
        List<int> pending,
        LargeValueFetch? fetch,
        CancellationToken cancellationToken)
    {
        for (int attempt = 0; ; attempt++)
        {
            List<int>? torn = await ResolvePassAsync(tx, rowIds, rows, pending, fetch, cancellationToken).ConfigureAwait(false);
            if (torn is null)
                return;

            if (!tx.ReadTimestamp.IsNull())
                throw new CamusDBException(
                    CamusDBErrorCodes.LargeValueCorrupt,
                    $"An out-of-line value of table {keys.DisplayTableName} does not match its row at a fixed snapshot");

            if (attempt + 1 >= MaxTornReadAttempts)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    $"Rows of table {keys.DisplayTableName} kept changing while their large values were read — retry the operation");

            await RereadRowsAsync(tx, rowIds, rows, torn, cancellationToken).ConfigureAwait(false);

            List<int>? next = null;
            foreach (int i in torn)
            {
                if (rows[i] is { } row && RowStorageForms.HasTrailer(row.Span))
                    (next ??= []).Add(i);
            }

            if (next is null)
                return;

            pending = next;
        }
    }

    /// <summary>
    /// One resolution pass over <paramref name="positions"/>. Returns the positions whose fetched
    /// value did not match the pointer (or was absent), or null when every row resolved.
    /// </summary>
    private async Task<List<int>?> ResolvePassAsync(
        KvTransaction tx,
        IReadOnlyList<ObjectIdValue> rowIds,
        ReadOnlyMemory<byte>?[] rows,
        List<int> positions,
        LargeValueFetch? fetch,
        CancellationToken cancellationToken)
    {
        // Per pending row: the replacement array (one entry per variable ordinal), and the out-of-line
        // cells still to fetch as (row position, ordinal, key index).
        byte[]?[]?[] replacements = new byte[]?[]?[positions.Count];
        List<(int slot, int ordinal)> outOfLine = [];
        List<string> fetchKeys = [];
        List<ObjectIdValue> fetchRowIds = [];

        for (int p = 0; p < positions.Count; p++)
        {
            ReadOnlyMemory<byte> row = rows[positions[p]]!.Value;
            RowStorageForms.TrailerLayout layout = RowStorageForms.ReadLayout(row.Span);

            bool[]? required = null;
            if (fetch is not null)
            {
                int storedVersion = RowStorageForms.StoredVersion(BinaryPrimitives.ReadUInt32LittleEndian(row.Span));
                required = await fetch.RequiredVariableOrdinalsAsync(tx.TransactionId, storedVersion).ConfigureAwait(false);
            }

            byte[]?[]? rowReplacements = null;
            for (int v = 0; v < layout.VariableCount; v++)
            {
                if (required is not null && (v >= required.Length || !required[v]))
                    continue;

                ReadOnlySpan<byte> span = row.Span;
                if (!layout.IsMarked(span, v))
                    continue;

                rowReplacements ??= new byte[]?[layout.VariableCount];

                if (layout.IsOutOfLine(span, v))
                {
                    outOfLine.Add((p, v));
                    fetchKeys.Add(keys.BuildLargeValueKey(rowIds[positions[p]], v));
                    fetchRowIds.Add(rowIds[positions[p]]);
                    continue;
                }

                // Compressed inline: resolved here, no I/O.
                rowReplacements[v] = RowStorageForms.ResolveCell(span, layout, v, default);
            }

            replacements[p] = rowReplacements;
        }

        List<int>? torn = null;

        if (fetchKeys.Count > 0)
        {
            Interlocked.Increment(ref fetchCalls);
            ReadOnlyMemory<byte>?[] fetched = await FetchAsync(tx, fetchKeys, fetchRowIds, outOfLine, cancellationToken).ConfigureAwait(false);

            HashSet<int>? tornSlots = null;
            for (int k = 0; k < outOfLine.Count; k++)
            {
                (int slot, int ordinal) = outOfLine[k];
                if (tornSlots is not null && tornSlots.Contains(slot))
                    continue;

                ReadOnlyMemory<byte> row = rows[positions[slot]]!.Value;
                RowStorageForms.TrailerLayout layout = RowStorageForms.ReadLayout(row.Span);

                byte[]? raw = fetched[k] is { } stored
                    ? RowStorageForms.ResolveCell(row.Span, layout, ordinal, stored.Span)
                    : null;

                if (raw is null)
                {
                    (tornSlots ??= []).Add(slot);
                    continue;
                }

                replacements[slot]![ordinal] = raw;
            }

            if (tornSlots is not null)
            {
                torn = [];
                foreach (int slot in tornSlots)
                {
                    torn.Add(positions[slot]);
                    replacements[slot] = null;
                }
            }
        }

        for (int p = 0; p < positions.Count; p++)
        {
            if (replacements[p] is not { } rowReplacements)
                continue;

            ReadOnlyMemory<byte> row = rows[positions[p]]!.Value;
            RowStorageForms.TrailerLayout layout = RowStorageForms.ReadLayout(row.Span);
            rows[positions[p]] = RowStorageForms.Rebuild(row.Span, layout, rowReplacements);
        }

        return torn;
    }

    /// <summary>
    /// Fetches stored large values: one batched probe at level 0, then one per ancestry level for
    /// the keys still unanswered. A tombstone at any level ends the walk for that key, which then
    /// reads as absent. Returns the stored bytes (envelope stripped) aligned with <paramref name="fetchKeys"/>.
    /// </summary>
    private async Task<ReadOnlyMemory<byte>?[]> FetchAsync(
        KvTransaction tx,
        List<string> fetchKeys,
        List<ObjectIdValue> fetchRowIds,
        List<(int slot, int ordinal)> cells,
        CancellationToken cancellationToken)
    {
        BranchKvValue[] level0 = await branch.ProbeManyRaw(
            tx.TransactionId,
            tx.ReadTimestamp,
            fetchKeys,
            tx.FoldReads ? tx.CoordinatorKey : "",
            "large_values_batch",
            cancellationToken).ConfigureAwait(false);

        ReadOnlyMemory<byte>?[] output = new ReadOnlyMemory<byte>?[fetchKeys.Count];
        List<int>? unresolved = null;

        for (int i = 0; i < level0.Length; i++)
        {
            BranchKvValue value = level0[i];
            if (value.Kind == BranchKvKind.Tombstone)
                continue;

            if (value.HasPayload)
            {
                output[i] = (ReadOnlyMemory<byte>?)value.Payload;
                continue;
            }

            if (branch.IsBranch)
                (unresolved ??= []).Add(i);
        }

        if (unresolved is null)
            return output;

        foreach ((KvKeyBuilder ancestorKeys, KvBranchReader ancestorReader, HLCTimestamp forkTimestamp) in branch.Levels)
        {
            string[] ancestorProbeKeys = new string[unresolved.Count];
            for (int i = 0; i < unresolved.Count; i++)
            {
                ancestorProbeKeys[i] = ancestorKeys.BuildLargeValueKey(fetchRowIds[unresolved[i]], cells[unresolved[i]].ordinal);
                BranchMetrics.RecordAncestorProbe();
            }

            BranchKvValue[] probed = await ancestorReader.ProbeManyRaw(
                HLCTimestamp.Zero,
                forkTimestamp,
                ancestorProbeKeys,
                coordinatorKey: "",
                "ancestor_large_values_batch",
                cancellationToken).ConfigureAwait(false);

            List<int>? next = null;
            for (int i = 0; i < unresolved.Count; i++)
            {
                BranchKvValue value = probed[i];
                if (value.Kind == BranchKvKind.Tombstone)
                    continue;

                if (value.HasPayload)
                {
                    output[unresolved[i]] = (ReadOnlyMemory<byte>?)value.Payload;
                    continue;
                }

                (next ??= []).Add(unresolved[i]);
            }

            if (next is null)
                break;

            unresolved = next;
        }

        return output;
    }

    /// <summary>
    /// Reads the current row bytes again for rows whose values changed under a read-committed read.
    /// The re-read walks the ancestry like any row read, and a row that is now gone becomes null.
    /// </summary>
    private async Task RereadRowsAsync(
        KvTransaction tx,
        IReadOnlyList<ObjectIdValue> rowIds,
        ReadOnlyMemory<byte>?[] rows,
        List<int> positions,
        CancellationToken cancellationToken)
    {
        string[] rowKeys = new string[positions.Count];
        for (int i = 0; i < positions.Count; i++)
            rowKeys[i] = keys.BuildRowKey(rowIds[positions[i]]);

        BranchKvValue[] level0 = await branch.ProbeManyRaw(
            tx.TransactionId,
            tx.ReadTimestamp,
            rowKeys,
            tx.FoldReads ? tx.CoordinatorKey : "",
            "large_values_reread",
            cancellationToken).ConfigureAwait(false);

        List<ObjectIdValue>? ancestorIds = null;
        List<int>? ancestorPositions = null;

        for (int i = 0; i < positions.Count; i++)
        {
            BranchKvValue value = level0[i];
            if (value.Kind == BranchKvKind.Tombstone)
            {
                rows[positions[i]] = null;
                continue;
            }

            if (value.HasPayload)
            {
                rows[positions[i]] = (ReadOnlyMemory<byte>?)value.Payload;
                continue;
            }

            if (!branch.IsBranch)
            {
                rows[positions[i]] = null;
                continue;
            }

            (ancestorIds ??= []).Add(rowIds[positions[i]]);
            (ancestorPositions ??= []).Add(positions[i]);
        }

        if (ancestorIds is null)
            return;

        ReadOnlyMemory<byte>?[] resolved = new ReadOnlyMemory<byte>?[ancestorIds.Count];
        List<int> all = new(ancestorIds.Count);
        for (int i = 0; i < ancestorIds.Count; i++)
            all.Add(i);

        await branch.ResolveRowsFromAncestorsAsync(ancestorIds, all, resolved, cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < ancestorIds.Count; i++)
            rows[ancestorPositions![i]] = resolved[i];
    }
}
