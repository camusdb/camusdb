/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// The outcome of one <c>ALTER TABLE ... REWRITE STORAGE</c> run, also written to the log.
/// <see cref="RowsDeferred"/> counts rows the run gave up on because live traffic kept changing them;
/// running the statement again picks them up.
/// </summary>
public readonly record struct StorageRewriteReport(
    long RowsScanned,
    long RowsConverted,
    long RowsAlreadyConverted,
    long RowsDeferred,
    long BytesMovedOutOfLine,
    long BytesSavedByCompression);

/// <summary>
/// <c>ALTER TABLE t REWRITE STORAGE [INLINE]</c>: converts the rows of one table, in bounded
/// transactions, to the form the current storage rules would write — or, with <c>INLINE</c>, back to
/// fully inline and uncompressed rows, which a binary that predates compressed and out-of-line values
/// can read. Existing rows never need this to stay readable; it only lets old data reach the new form
/// without waiting for writes.
///
/// <para><b>Rules, each preventing a specific failure.</b></para>
/// <list type="bullet">
///   <item><b>The stored schema version is kept.</b> A row is decoded and re-encoded with the codec of
///   the version it was written under. A version bump would append schema history and retire every
///   cached decode plan for a change that alters no column layout.</item>
///   <item><b>No index entry is touched.</b> Column values do not change, so neither do index keys.
///   Only the row key and its out-of-line keys are written.</item>
///   <item><b>Bounded transactions.</b> <see cref="CamusDBOptions.LargeValueRewriteBatchRows"/> rows per
///   transaction, in row-id order. A batch that hits the mutation limit is halved and retried. A batch
///   also closes before a row that would take the recorded decoded size of its compressed and
///   out-of-line cells past <see cref="CamusDBOptions.LargeValueResolveBatchBytes"/>, because a batch
///   holds every resolved and re-encoded row until it commits.</item>
///   <item><b>Idempotent.</b> A row already in the target form produces identical bytes and is not
///   written. A row with no marked cell and no value large enough to change is skipped without being
///   decoded. A second run over a converted table writes nothing.</item>
///   <item><b>Resumable.</b> The last committed row id is written in the same transaction as each
///   batch (<see cref="MetaKeys.StorageRewriteKey"/>), so a run that stops resumes after the last
///   committed batch. The cursor moves only over a contiguous prefix of finished rows: once a batch is
///   deferred, later batches of the run leave the cursor where it is. A run that stops before its
///   deferred rows are retried therefore resumes before them, not after them.</item>
///   <item><b>One policy per cursor.</b> A run takes the column strategies and the large-value settings
///   once, when it starts, and applies them to every batch. The cursor records a fingerprint of that
///   target policy. A later run whose policy differs (a <c>SET STORAGE</c>, a changed threshold or
///   compression setting, or the other mode) ignores the cursor and starts from the beginning, because
///   the rows before the cursor were converted to a different target.</item>
///   <item><b>Live traffic is not lost.</b> A batch runs as an optimistic read-committed transaction with
///   read validation, so it takes no lock while it reads. A user write that commits to a row the batch
///   read makes the batch fail at commit, and the rewrite never overwrites that write. The rows of such a
///   batch are retried at the end of the run and reported as deferred if they keep changing. The rewrite
///   cannot fully yield: from the moment a batch stages its writes until it commits, those keys carry its
///   write intents, and a user transaction that asks for a lock on one of them fails at once with a
///   retryable conflict. Kahuna has no lock priority that would make the maintenance transaction give way
///   instead. Small batches keep that window short.</item>
/// </list>
///
/// <para><b>Why this is safe under MVCC, branches and time travel.</b> A rewrite writes a new version
/// of each row and of its out-of-line values; it never modifies an old version, and an old version keeps
/// its own form. A read at an earlier timestamp, a branch forked before the rewrite (whose ancestor
/// reads are pinned at the fork timestamp), and a restore to an earlier point therefore all read
/// consistent rows. That holds only because every historical read and ancestor probe is
/// timestamped.</para>
/// </summary>
internal sealed class StorageRewriter
{
    /// <summary>How many extra passes retry the rows of batches that lost to live traffic.</summary>
    private const int DeferredRetryPasses = 3;

    private readonly ILogger<ICamusDB> logger;

    public StorageRewriter(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    internal async Task<StorageRewriteReport> RewriteAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        bool inline,
        CancellationToken cancellationToken = default)
    {
        RewriteTarget target = RewriteTarget.Capture(table, database.Options, inline);
        string mode = inline ? "inline" : "current";
        string cursorKey = MetaKeys.StorageRewriteKey(database.Id, table.Schema.EffectiveStorageId);
        ObjectIdValue? after = await ReadCursorAsync(database, cursorKey, target.Fingerprint).ConfigureAwait(false);

        long scanned = 0, converted = 0, alreadyConverted = 0, movedOut = 0, saved = 0;
        List<(ObjectIdValue rowId, long bytes)> deferred = [];
        int batchRows = Math.Max(1, target.Options.LargeValueRewriteBatchRows);
        long budget = target.Options.LargeValueResolveBatchBytes;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            KvTransaction tx = await BeginBatchTransactionAsync(database, batchRows).ConfigureAwait(false);
            List<(ObjectIdValue rowId, ReadOnlyMemory<byte> data)> rows = [];
            List<long> rowBytes = [];

            try
            {
                long batchBytes = 0;
                await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in table.Store.ScanRows(
                    tx, maxRows: batchRows, afterRowId: after, cancellationToken: cancellationToken, largeValues: LargeValueFetch.Raw).ConfigureAwait(false))
                {
                    long bytes = RowStorageForms.MarkedRawBytes(data.Span);

                    // The row that would cross the byte bound starts the next batch instead.
                    if (budget > 0 && rows.Count > 0 && batchBytes + bytes > budget)
                        break;

                    rows.Add((rowId, data));
                    rowBytes.Add(bytes);
                    batchBytes += bytes;
                }

                if (rows.Count == 0)
                {
                    await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                    break;
                }

                BatchOutcome outcome = await RewriteBatchAsync(table, tx, rows, target, cancellationToken).ConfigureAwait(false);

                ObjectIdValue last = rows[^1].rowId;

                // Once a row is deferred, the rows after it are not a contiguous finished prefix: a cursor
                // written past the deferred row would make a resumed run skip it.
                if (deferred.Count == 0)
                    await MetaKeyWriter.WriteMetaKey(database.Kahuna.Kahuna, tx, cursorKey, Encoding.UTF8.GetBytes(target.Fingerprint + ":" + last)).ConfigureAwait(false);

                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);

                scanned += rows.Count;
                converted += outcome.Converted;
                alreadyConverted += rows.Count - outcome.Converted;
                movedOut += outcome.BytesMovedOutOfLine;
                saved += outcome.BytesSavedByCompression;
                after = last;
                batchRows = Math.Max(1, target.Options.LargeValueRewriteBatchRows);

                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "Storage rewrite of table {Table}: {Scanned} rows scanned, {Converted} converted, {Moved} bytes moved out of line, {Saved} bytes saved by compression",
                        table.Name, scanned, converted, movedOut, saved);
            }
            catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TransactionMutationLimitExceeded && batchRows > 1)
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                batchRows = Math.Max(1, batchRows / 2);
            }
            catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex) && rows.Count > 0)
            {
                // A user transaction changed a row this batch read. The batch yields: its rows are
                // retried after the main pass, and the scan moves past them so the pass continues. The
                // durable cursor stays before them.
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                for (int i = 0; i < rows.Count; i++)
                    deferred.Add((rows[i].rowId, rowBytes[i]));
                scanned += rows.Count;
                after = rows[^1].rowId;
            }
            catch
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                throw;
            }
        }

        for (int pass = 0; pass < DeferredRetryPasses && deferred.Count > 0; pass++)
        {
            List<(ObjectIdValue rowId, long bytes)> stillDeferred = [];
            for (int start = 0; start < deferred.Count;)
            {
                // Grouped by row count and by the decoded size each row recorded when it was deferred. A row
                // that changed since then may differ, but only by what one user write changed.
                List<(ObjectIdValue rowId, long bytes)> group = [];
                long groupBytes = 0;
                while (start < deferred.Count && group.Count < batchRows
                    && (budget <= 0 || group.Count == 0 || groupBytes + deferred[start].bytes <= budget))
                {
                    group.Add(deferred[start]);
                    groupBytes += deferred[start].bytes;
                    start++;
                }

                List<ObjectIdValue> ids = new(group.Count);
                foreach ((ObjectIdValue rowId, long _) in group)
                    ids.Add(rowId);

                KvTransaction tx = await BeginBatchTransactionAsync(database, batchRows).ConfigureAwait(false);
                try
                {
                    ReadOnlyMemory<byte>?[] data = await table.Store.GetRowsBatch(tx, ids, cancellationToken, LargeValueFetch.Raw).ConfigureAwait(false);
                    List<(ObjectIdValue, ReadOnlyMemory<byte>)> rows = [];
                    for (int i = 0; i < ids.Count; i++)
                    {
                        if (data[i] is { } row)
                            rows.Add((ids[i], row));
                    }

                    BatchOutcome outcome = await RewriteBatchAsync(table, tx, rows, target, cancellationToken).ConfigureAwait(false);
                    await database.Transactions.CommitAsync(tx).ConfigureAwait(false);

                    converted += outcome.Converted;
                    alreadyConverted += rows.Count - outcome.Converted;
                    movedOut += outcome.BytesMovedOutOfLine;
                    saved += outcome.BytesSavedByCompression;
                }
                catch (CamusDBException ex) when (SerializableRetryHelper.IsRetryable(ex))
                {
                    await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                    stillDeferred.AddRange(group);
                }
                catch
                {
                    await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
                    throw;
                }
            }

            deferred = stillDeferred;
        }

        // The run reached the end of the table: the cursor has served its purpose. Deferred rows are not
        // tracked by the cursor, so a later run over them starts from the beginning and skips the rows
        // that are already converted without writing them.
        await DeleteCursorAsync(database, cursorKey).ConfigureAwait(false);

        StorageRewriteReport report = new(scanned, converted, alreadyConverted, deferred.Count, movedOut, saved);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "Storage rewrite of table {Table} finished ({Mode}): {Scanned} rows scanned, {Converted} converted, {Already} already converted, {Deferred} deferred, {Moved} bytes moved out of line, {Saved} bytes saved by compression",
                table.Name, mode, report.RowsScanned, report.RowsConverted, report.RowsAlreadyConverted, report.RowsDeferred, report.BytesMovedOutOfLine, report.BytesSavedByCompression);

        return report;
    }

    private readonly record struct BatchOutcome(int Converted, long BytesMovedOutOfLine, long BytesSavedByCompression);

    /// <summary>
    /// Rewrites the rows of one batch that are not yet in the target form, inside <paramref name="tx"/>.
    /// </summary>
    private static async Task<BatchOutcome> RewriteBatchAsync(
        TableDescriptor table,
        KvTransaction tx,
        List<(ObjectIdValue rowId, ReadOnlyMemory<byte> data)> rows,
        RewriteTarget target,
        CancellationToken cancellationToken)
    {
        bool inline = target.Inline;
        Dictionary<int, LargeValuePolicy> policies = [];
        List<int> candidates = [];

        for (int i = 0; i < rows.Count; i++)
        {
            ReadOnlySpan<byte> payload = rows[i].data.Span;
            if (RowStorageForms.HasTrailer(payload))
            {
                candidates.Add(i);
                continue;
            }

            if (inline)
                continue;

            int version = RowStorageForms.StoredVersion(BinaryPrimitives.ReadUInt32LittleEndian(payload));
            CompiledRowCodec codec = await table.GetRowCodecAsync(tx.TransactionId, version).ConfigureAwait(false);
            LargeValuePolicy policy = await PolicyForVersionAsync(table, tx, version, target, policies).ConfigureAwait(false);

            codec.ValidateFrame(rows[i].data.Span);
            if (codec.AnyStoredCellMayChange(rows[i].data.Span, policy))
                candidates.Add(i);
        }

        if (candidates.Count == 0)
            return default;

        List<ObjectIdValue> ids = new(candidates.Count);
        ReadOnlyMemory<byte>?[] resolved = new ReadOnlyMemory<byte>?[candidates.Count];
        for (int c = 0; c < candidates.Count; c++)
        {
            ids.Add(rows[candidates[c]].rowId);
            resolved[c] = rows[candidates[c]].data;
        }

        await table.Store.ResolveLargeValuesAsync(tx, ids, resolved, null, cancellationToken).ConfigureAwait(false);

        List<KvTableStore.RowUpdate> updates = [];
        long movedOut = 0, saved = 0;

        for (int c = 0; c < candidates.Count; c++)
        {
            if (resolved[c] is not { } plain)
                continue;

            ReadOnlyMemory<byte> original = rows[candidates[c]].data;
            int version = RowStorageForms.StoredVersion(BinaryPrimitives.ReadUInt32LittleEndian(plain.Span));

            CompiledRowCodec codec = await table.GetRowCodecAsync(tx.TransactionId, version).ConfigureAwait(false);
            LargeValuePolicy policy = inline
                ? LargeValuePolicy.Inline
                : await PolicyForVersionAsync(table, tx, version, target, policies).ConfigureAwait(false);

            codec.ValidateFrame(plain.Span);
            EncodedRow encoded = codec.EncodeStorageValue(codec.DecodeToSlots(plain.Span), policy);

            if (encoded.StorageValue.AsSpan(1).SequenceEqual(original.Span))
                continue;

            updates.Add(new KvTableStore.RowUpdate
            {
                RowId = ids[c],
                NewRowData = encoded.StorageValue,
                LargeValues = encoded.OutOfLine,
                LargeValueDeletes = StoredRowRewriter.OrdinalsNotRewritten(RowStorageForms.OutOfLineOrdinals(original.Span), encoded),
            });

            (long rowMoved, long rowSaved) = MeasureForms(encoded);
            movedOut += rowMoved;
            saved += rowSaved;
        }

        if (updates.Count > 0)
            await table.Store.UpdateRowsBatch(tx, updates, cancellationToken).ConfigureAwait(false);

        return new BatchOutcome(updates.Count, movedOut, saved);
    }

    /// <summary>
    /// The write policy for rows stored under <paramref name="version"/>: that version's column layout,
    /// with each column's strategy taken, by column id, from the strategies the run captured when it
    /// started. A strategy set after the version was written therefore governs the rewrite, while the
    /// layout stays the stored one.
    /// </summary>
    private static async ValueTask<LargeValuePolicy> PolicyForVersionAsync(
        TableDescriptor table,
        KvTransaction tx,
        int version,
        RewriteTarget target,
        Dictionary<int, LargeValuePolicy> cache)
    {
        if (cache.TryGetValue(version, out LargeValuePolicy? cached))
            return cached;

        TableSchemaHistory history = await table.Schema.GetSchemaHistoryAsync(tx.TransactionId, version).ConfigureAwait(false);
        List<TableColumnSchema> stored = history.Columns ?? [];

        List<TableColumnSchema> withCurrentStrategy = new(stored.Count);
        foreach (TableColumnSchema column in stored)
        {
            bool known = !string.IsNullOrEmpty(column.Id) && target.Strategies.ContainsKey(column.Id);
            ColumnStorageStrategy? now = known ? target.Strategies[column.Id] : null;
            withCurrentStrategy.Add(known && now != column.Storage
                ? new TableColumnSchema(column.Id, column.Name, column.Type, column.NotNull, column.DefaultValue, column.State,
                    column.MaxLength, column.ArrayElementType, column.DefaultFunction, column.NotNullConstraintName, column.Comment, now,
                    column.DefaultSequenceId, column.IdentityAlways)
                : column);
        }

        LargeValuePolicy policy = LargeValuePolicy.For(withCurrentStrategy, target.Options);
        cache[version] = policy;
        return policy;
    }

    /// <summary>
    /// The target of one run, captured once when the run starts: the mode, the options snapshot, and the
    /// storage strategy of each current column by column id. Every batch of the run converts rows to this
    /// target, so a <c>SET STORAGE</c> or a settings change during the run cannot split the table between
    /// two targets under one cursor.
    ///
    /// <para><see cref="Fingerprint"/> names the target in the cursor. It covers every input that decides
    /// the form of a converted row: the mode, the threshold, the compression switch and its minimum
    /// saving, and each variable column's effective strategy. A run resumes a cursor only when the
    /// fingerprints are equal.</para>
    /// </summary>
    private sealed record RewriteTarget(
        bool Inline,
        CamusDBOptions Options,
        IReadOnlyDictionary<string, ColumnStorageStrategy?> Strategies,
        string Fingerprint)
    {
        public static RewriteTarget Capture(TableDescriptor table, CamusDBOptions options, bool inline)
        {
            Dictionary<string, ColumnStorageStrategy?> strategies = new(StringComparer.Ordinal);
            List<(string id, ColumnStorageStrategy strategy)> variable = [];

            foreach (TableColumnSchema column in table.Schema.Columns ?? [])
            {
                if (string.IsNullOrEmpty(column.Id))
                    continue;

                strategies[column.Id] = column.Storage;
                if (TableColumnSchema.SupportsStorageStrategy(column.Type))
                    variable.Add((column.Id, column.EffectiveStorage));
            }

            StringBuilder description = new();
            if (inline)
            {
                // An inline run writes every row fully inline and uncompressed, whatever the policy says.
                description.Append("inline");
            }
            else
            {
                description.Append("current|").Append(options.LargeValueThresholdBytes)
                    .Append('|').Append(options.LargeValueCompressionEnabled ? 1 : 0)
                    .Append('|').Append(options.LargeValueCompressionMinSavingPercent);

                variable.Sort((a, b) => string.CompareOrdinal(a.id, b.id));
                foreach ((string id, ColumnStorageStrategy strategy) in variable)
                    description.Append('|').Append(id).Append('=').Append((int)strategy);
            }

            ulong hash = System.IO.Hashing.XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(description.ToString()));
            string fingerprint = (inline ? "inline-" : "current-") + hash.ToString("x16");

            return new RewriteTarget(inline, options, strategies, fingerprint);
        }
    }

    /// <summary>Bytes the new row stores out of line, and bytes its compressed cells save over raw.</summary>
    private static (long movedOut, long saved) MeasureForms(EncodedRow encoded)
    {
        long movedOut = 0, saved = 0;
        ReadOnlySpan<byte> payload = encoded.StorageValue.AsSpan(1);

        if (encoded.OutOfLine is { } writes)
        {
            foreach (LargeValueWrite write in writes)
                movedOut += write.StorageValue.Length - 1;
        }

        if (!RowStorageForms.HasTrailer(payload))
            return (movedOut, saved);

        RowStorageForms.TrailerLayout layout = RowStorageForms.ReadLayout(payload);
        for (int v = 0; v < layout.VariableCount; v++)
        {
            if (!layout.IsCompressed(payload, v))
                continue;

            ReadOnlySpan<byte> cell = layout.Cell(payload, v);
            if (layout.IsOutOfLine(payload, v))
            {
                RowStorageForms.Pointer pointer = RowStorageForms.ReadPointer(cell);
                saved += pointer.RawLength - pointer.StoredLength;
            }
            else
            {
                saved += BinaryPrimitives.ReadUInt32LittleEndian(cell) - (cell.Length - RowStorageForms.CompressedPrefixSize);
            }
        }

        return (movedOut, saved);
    }

    private static Task<KvTransaction> BeginBatchTransactionAsync(DatabaseDescriptor database, int batchRows) =>
        database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted,
            CamusTransactionMode.ReadWrite,
            // A single row that still exceeds the limit after halving would stall the run forever; that
            // one-row batch runs without the limit instead.
            mutationLimitOverride: batchRows == 1 ? 0 : null,
            locking: KeyValueTransactionLocking.Optimistic,
            readValidation: ReadValidation.TrackAndValidate);

    /// <summary>
    /// Reads the cursor <c>{fingerprint}:{rowIdHex24}</c>. Returns null, which starts the run from the
    /// beginning, when there is no cursor or when it names a different target.
    /// </summary>
    private static async Task<ObjectIdValue?> ReadCursorAsync(DatabaseDescriptor database, string cursorKey, string fingerprint)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await database.Kahuna.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, cursorKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry?.Value is not { Length: > 0 } value)
            return null;

        string text = Encoding.UTF8.GetString(value);
        int colon = text.LastIndexOf(':');

        // A cursor left by a run with another target says nothing about this one: the rows before it
        // were converted to a different form. Start from the beginning.
        if (colon < 0 || !string.Equals(text[..colon], fingerprint, StringComparison.Ordinal))
            return null;

        return ObjectId.ToValue(text[(colon + 1)..]);
    }

    private static async Task DeleteCursorAsync(DatabaseDescriptor database, string cursorKey)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite).ConfigureAwait(false);
        try
        {
            await MetaKeyWriter.DeleteMetaKey(database.Kahuna.Kahuna, tx, cursorKey).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }
}
