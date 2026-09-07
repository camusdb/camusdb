/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// One level of a branched database's lineage, as seen by a single table.
///
/// <para>An ancestor owns its own key namespace, so a probe for an inherited row must be composed
/// and issued <em>there</em>: <see cref="Keys"/> builds the key and <see cref="Reader"/> issues the
/// read. <see cref="ForkTimestamp"/> is the HLC instant this database was forked from that ancestor,
/// and it is the read timestamp every probe of the level uses — never the reading transaction's own
/// timestamp, so a branch observes its parent frozen at the fork rather than as it is today.</para>
/// </summary>
internal readonly record struct BranchLevel(KvKeyBuilder Keys, KvBranchReader Reader, HLCTimestamp ForkTimestamp);

/// <summary>
/// The raw, lineage-aware read layer for one table: single and batched key probes, unfiltered row
/// and index iterators, and the unique-key resolution a branch write needs.
///
/// <para><b>Nearest-wins with tombstone suppression</b> is the rule every method here implements.
/// Levels are visited nearest parent first. A level that answers with a value <em>or</em> with a
/// tombstone ends the resolution for that key: the tombstone is the "deleted in this branch" signal,
/// so a value at an older level can never resurrect a row a nearer level deleted. This is why a
/// branch delete writes a tombstone instead of issuing a physical delete — a physical delete leaves
/// no level-0 entry, and the merge would surface the ancestor's row again.</para>
///
/// <para><b>An unknown is never decoded as an absence.</b> A read that ends in <c>MustRetry</c>,
/// <c>WaitingForReplication</c>, <c>Errored</c>, or an exhausted retry budget means the key's state
/// is unknown, not that the key is missing. Every probe here throws
/// <see cref="CamusDBErrorCodes.TransactionMustRetry"/> in that case. Returning a miss instead once
/// made an UPDATE's locate phase match zero rows and report success, while the transient read folded
/// no observation for commit validation to catch — a committed transaction with the write silently
/// dropped.</para>
///
/// <para>A root database has no levels, and every method here degenerates to a single level-0
/// operation with no added cost.</para>
/// </summary>
internal sealed class KvBranchReader
{
    private readonly IKahuna kahuna;
    private readonly KvKeyBuilder keys;
    private readonly BranchLevel[] levels;

    internal KvBranchReader(IKahuna kahuna, KvKeyBuilder keys, BranchLevel[] levels)
    {
        this.kahuna = kahuna;
        this.keys = keys;
        this.levels = levels;
    }

    /// <summary>
    /// The ancestry levels, nearest parent first. Treat as read-only: the array is shared, and the
    /// merge iterators in <see cref="KvTableStore.ScanRows"/> and <see cref="KvTableStore.ScanIndex"/>
    /// depend on its order for nearest-wins.
    /// </summary>
    internal BranchLevel[] Levels => levels;

    /// <summary>
    /// Number of ancestor levels a read miss walks. Zero for root databases. This is the per-read
    /// amplification factor: a point read may probe up to this many extra levels, and a range scan
    /// opens this many extra iterators.
    /// </summary>
    internal int Depth => levels.Length;

    /// <summary>True when this table belongs to a branch database and has inherited state to merge.</summary>
    internal bool IsBranch => levels.Length > 0;

    /// <summary>
    /// Probes a single Kahuna key at the given transaction identity and read timestamp, returning the
    /// raw (kind, payload) pair decoded by <see cref="BranchKvCodec"/>. A Kahuna miss returns
    /// <see cref="BranchKvValue.Miss"/> — the same signal as decoding a null value — so callers treat
    /// a null payload as "not here, continue walking ancestry". A Tombstone payload means "deleted at
    /// this level; stop walking".
    ///
    /// <para>When <paramref name="coordinatorKey"/> is non-empty the read is registered with the
    /// transaction coordinator so its existence/base-revision observation folds into the working set
    /// and is validated at commit — the optimistic / TrackAndValidate read path. An empty coordinator
    /// key (the default, and every ancestor snapshot probe) issues an unregistered read, identical to
    /// the pessimistic behavior. The operation id is minted once, outside the retry, so a transient
    /// MustRetry replays under the same id rather than registering a second read.</para>
    /// </summary>
    internal async Task<BranchKvValue> ProbeRaw(
        HLCTimestamp txId,
        HLCTimestamp readTimestamp,
        string key,
        CancellationToken cancellationToken,
        string coordinatorKey = "")
    {
        TransactionOperationId operationId = coordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await KahunaRetryPolicy.RetryOnMustRetry(
            () => kahuna.LocateAndTryGetValue(txId, key, -1, readTimestamp, KeyValueDurability.Persistent, cancellationToken, coordinatorKey: coordinatorKey, operationId: operationId),
            cancellationToken
        ).ConfigureAwait(false);

        if (type == KeyValueResponseType.Aborted)
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                $"Read of key {key} was aborted by Kahuna — retry the operation from BeginAsync");

        // Only a confirmed answer may shape the result. Anything else — an exhausted MustRetry /
        // WaitingForReplication (a foreign 2PC intent that stalled past the whole retry budget),
        // Errored, or any future non-confirmed type — means the key's state is UNKNOWN, not absent.
        // See the class summary for the atomicity violation that decoding it as a miss produced.
        if (type is not (KeyValueResponseType.Get or KeyValueResponseType.DoesNotExist))
            throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                $"Read of key {key} did not return a confirmed result ({type}) — retry the operation from BeginAsync");

        if (entry is null || type == KeyValueResponseType.DoesNotExist)
            return BranchKvValue.Miss;  // confirmed Kahuna miss — continue ancestry walk

        return BranchKvCodec.Decode(entry.Value);
    }

    /// <summary>
    /// Batch counterpart of <see cref="ProbeRaw"/>: probes every key at one transaction identity and
    /// one read timestamp, and returns one decoded <see cref="BranchKvValue"/> per input position.
    /// A key Kahuna does not hold decodes to <see cref="BranchKvValue.Miss"/>, the same signal the
    /// single-key probe returns, so a caller treats it as "not at this level, keep walking".
    ///
    /// <para>
    /// <c>LocateAndTryGetManyValues</c> fans keys out across leader nodes and returns results in
    /// leader-group order, not input order, so results are matched by key and then projected back
    /// positionally. A repeated key therefore resolves once and answers every position that holds it.
    /// </para>
    ///
    /// <para>
    /// MustRetry / WaitingForReplication mean a partition is not yet ready: only the affected subset is
    /// retried, with exponential back-off, up to <see cref="KahunaRetryPolicy.MaxKahunaRetries"/>. Any
    /// other non-confirmed response, and an exhausted retry budget, throw
    /// <see cref="CamusDBErrorCodes.TransactionMustRetry"/> rather than decoding as an absence.
    /// </para>
    ///
    /// <para>
    /// A non-empty <paramref name="coordinatorKey"/> registers the read for commit-time validation. One
    /// operation id is bound to the pending declaration: it is reused while the pending set is unchanged
    /// (an ack-loss resend of the identical batch, which the coordinator can only replay idempotently
    /// under the same id) and a fresh id is minted only when the set shrinks. Unregistered reads — every
    /// ancestor snapshot probe — carry the default id, so the identity is immaterial there.
    /// </para>
    /// </summary>
    internal async Task<BranchKvValue[]> ProbeManyRaw(
        HLCTimestamp txId,
        HLCTimestamp readTimestamp,
        IReadOnlyList<string> probeKeys,
        string coordinatorKey,
        string retryDiagnosticLabel,
        CancellationToken cancellationToken)
    {
        if (probeKeys.Count == 0)
            return [];

        List<(string key, long revision, KeyValueDurability durability)> requested = new(probeKeys.Count);
        for (int i = 0; i < probeKeys.Count; i++)
            requested.Add((probeKeys[i], -1, KeyValueDurability.Persistent));

        Dictionary<string, (KeyValueResponseType responseType, ReadOnlyKeyValueEntry? entry)> byKey =
            new(probeKeys.Count, StringComparer.Ordinal);

        List<(string key, long revision, KeyValueDurability durability)> pending = requested;
        int retries = 0;

        TransactionOperationId operationId = coordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

        while (pending.Count > 0)
        {
            List<(KeyValueResponseType responseType, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> results =
                await kahuna.LocateAndTryGetManyValues(
                    txId,
                    readTimestamp,
                    pending,
                    cancellationToken,
                    coordinatorKey,
                    operationId
                ).ConfigureAwait(false);

            List<(string key, long revision, KeyValueDurability durability)>? nextPending = null;

            foreach ((KeyValueResponseType responseType, string key, _, ReadOnlyKeyValueEntry? entry) in results)
            {
                if (responseType == KeyValueResponseType.Aborted)
                    throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                        $"Batch read of key {key} was aborted by Kahuna — retry the operation from BeginAsync");

                if (responseType is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                {
                    nextPending ??= [];
                    nextPending.Add((key, -1, KeyValueDurability.Persistent));
                    continue;
                }

                // Same rule as ProbeRaw: only a confirmed answer may reach the decode loop below,
                // which treats every unmatched key as an absent row. A per-key Errored (a dropped
                // actor response) that lands in byKey decodes as Miss — an unknown converted into a
                // definitive absence, with no folded observation for commit validation to catch.
                if (responseType is not (KeyValueResponseType.Get or KeyValueResponseType.DoesNotExist))
                    throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                        $"Batch read of key {key} returned {responseType} — retry the operation from BeginAsync");

                byKey[key] = (responseType, entry);
            }

            if (nextPending is null)
                break;

            if (++retries >= KahunaRetryPolicy.MaxKahunaRetries)
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                    $"Batch read was not ready after {KahunaRetryPolicy.MaxKahunaRetries} retries — retry the operation from BeginAsync");

            ServerDiagnostics.AddKvRetryWait(retryDiagnosticLabel);
            await Task.Delay(KahunaRetryPolicy.RetryDelayMs(retries), cancellationToken).ConfigureAwait(false);

            // Shrinking set (some reads confirmed) → new, smaller declaration under a fresh id on the
            // registered path. Unchanged set (every key transient) → identical resend of a lost ack →
            // keep the same id so the coordinator replays the read observation idempotently.
            if (coordinatorKey.Length != 0 && nextPending.Count != pending.Count)
                operationId = TransactionOperationId.NewRandom();

            pending = nextPending;
        }

        BranchKvValue[] decoded = new BranchKvValue[probeKeys.Count];

        for (int i = 0; i < probeKeys.Count; i++)
        {
            decoded[i] = byKey.TryGetValue(probeKeys[i], out (KeyValueResponseType responseType, ReadOnlyKeyValueEntry? entry) res)
                         && res is { responseType: KeyValueResponseType.Get, entry: not null }
                ? BranchKvCodec.Decode(res.entry.Value)
                : BranchKvValue.Miss;
        }

        return decoded;
    }

    /// <summary>
    /// Resolves the input positions a level-0 batch read left unanswered by walking the branch
    /// ancestry, one bounded batch per level instead of one round trip per row per level.
    ///
    /// <para>
    /// A page produced by a branch index scan is full of inherited rows by construction — the scan
    /// merges the ancestors' index levels, so an entry for a row that was never written into the branch
    /// namespace misses level 0 every time. Probing those one at a time cost <c>page × depth</c>
    /// serial round trips.
    /// </para>
    ///
    /// <para><b>Ordering is part of the contract.</b> Levels are visited nearest parent first, and a
    /// position that a level answers — with a value <em>or</em> with a tombstone — is removed before
    /// the next level runs. That is what makes a level-k tombstone suppress a level-k+1 value, exactly
    /// as a per-row walk does by breaking out of its loop. Each level's batch is built and issued on
    /// the store that owns that level's keyspace, at <see cref="HLCTimestamp.Zero"/> (the transaction
    /// identity) and that level's fork timestamp (the read timestamp) — never the child transaction's
    /// own timestamp — and stays unregistered, so no ancestor read folds into the read set.
    /// </para>
    /// </summary>
    internal async Task ResolveRowsFromAncestorsAsync(
        IReadOnlyList<ObjectIdValue> rowIds,
        List<int> unresolvedPositions,
        ReadOnlyMemory<byte>?[] output,
        CancellationToken cancellationToken)
    {
        List<int> pending = unresolvedPositions;

        foreach ((KvKeyBuilder ancestorKeys, KvBranchReader ancestorReader, HLCTimestamp forkTimestamp) in levels)
        {
            string[] ancestorProbeKeys = new string[pending.Count];

            for (int i = 0; i < pending.Count; i++)
            {
                // Each ancestor owns its own key namespace, so the key must be built on that store.
                ancestorProbeKeys[i] = ancestorKeys.BuildRowKey(rowIds[pending[i]]);
                BranchMetrics.RecordAncestorProbe();
            }

            BranchKvValue[] probed = await ancestorReader.ProbeManyRaw(
                HLCTimestamp.Zero,
                forkTimestamp,
                ancestorProbeKeys,
                coordinatorKey: "",
                "ancestor_rows_batch",
                cancellationToken).ConfigureAwait(false);

            List<int>? next = null;

            for (int i = 0; i < pending.Count; i++)
            {
                int position = pending[i];
                BranchKvValue decoded = probed[i];

                if (decoded.Kind == BranchKvKind.Tombstone)
                {
                    // Deleted at this level: resolution stops here permanently, so the position is not
                    // carried forward and a value at an older level can never resurrect the row.
                    output[position] = null;
                    continue;
                }

                if (decoded.HasPayload)
                {
                    output[position] = (ReadOnlyMemory<byte>?)decoded.Payload;
                    continue;
                }

                (next ??= []).Add(position);
            }

            if (next is null)
                return;

            pending = next;
        }

        // Absent at every level: the output slot keeps its null, stated explicitly.
        for (int i = 0; i < pending.Count; i++)
            output[pending[i]] = null;
    }

    // Yields every row entry from this store's namespace at the given snapshot, including tombstones,
    // so the branch-merge caller can apply the nearest-wins/tombstone-suppression rule across levels.
    // txId is the live transaction id for level-0 reads (use HLCTimestamp.Zero for ancestor snapshots).
    // startHex/endHex are bare row-id hex bounds (no prefix) so each store in a lineage builds the
    // bound keys in its own namespace; endHex is always exclusive.
    internal async IAsyncEnumerable<(string rowIdHex, BranchKvKind kind, ReadOnlyMemory<byte>? payload)> ScanRowsRawAsync(
        HLCTimestamp txId,
        HLCTimestamp readTimestamp,
        string? startHex,
        bool startInclusive,
        string? endHex,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        string coordinatorKey = "")
    {
        string rowKeyPrefix = keys.RowKeyPrefix;
        int prefixLen = rowKeyPrefix.Length;

        string? startKey = startHex is not null ? rowKeyPrefix + startHex : null;
        string? endKey = endHex is not null ? rowKeyPrefix + endHex : null;

        // Non-empty coordinatorKey registers the scan so every row it returns folds as a read
        // observation for commit-time validation; the coordinator derives a distinct operation id per
        // page from this base id. Empty (default, and all ancestor snapshots) scans unregistered.
        TransactionOperationId operationId = coordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            txId, keys.RowBucketPrefix, startKey, startInclusive, endKey, endKey is null, KvStoreConstants.DefaultPageSize,
            readTimestamp, KeyValueDurability.Persistent, cancellationToken, coordinatorKey, operationId).ConfigureAwait(false))
        {
            if (entry.Value is null) continue;
            string rowIdHex = key.AsSpan(prefixLen).ToString();
            BranchKvValue decoded = BranchKvCodec.Decode(entry.Value);
            yield return (rowIdHex, decoded.Kind, decoded.HasPayload ? (ReadOnlyMemory<byte>?)decoded.Payload : null);
        }
    }

    // Yields every index entry within optional encoded bounds from this store's namespace at the
    // given snapshot.  txId is the live transaction id for level-0 reads (HLCTimestamp.Zero for
    // ancestor snapshots).  fromEncoded/toEncoded are the raw encoded key strings (no prefix) so
    // this store builds its own start/end keys in its own keyspace ({dbId}:{tableId}:i:{indexId}/…).
    internal async IAsyncEnumerable<(string suffix, BranchKvKind kind, ReadOnlyMemory<byte>? payload)> ScanIndexRawAsync(
        HLCTimestamp txId,
        HLCTimestamp readTimestamp,
        string indexId,
        string? fromEncoded, bool fromInclusive,
        string? toEncoded, bool toInclusive,
        bool unique,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        string coordinatorKey = "",
        bool toIsPrefixBound = false,
        bool fromIsPrefixBound = false)
    {
        string bucketPrefix = keys.BuildIndexBucketPrefix(indexId);
        string keyPrefix = bucketPrefix + "/";
        int prefixLen = keyPrefix.Length;

        // See KvTableStore.ScanIndex for why a prefix bound needs the sentinel even on a unique index,
        // and why an exclusive lower bound must clear the bound value's stored-key extensions with it.
        string? startKey = fromEncoded is not null
            ? ((unique && !fromIsPrefixBound) || fromInclusive ? keyPrefix + fromEncoded : keyPrefix + fromEncoded + KvStoreConstants.IndexKeySentinel)
            : null;
        string? endKey   = toEncoded   is not null
            ? (unique && !toIsPrefixBound ? keyPrefix + toEncoded : keyPrefix + toEncoded + KvStoreConstants.IndexKeySentinel)
            : null;

        // See ScanRowsRawAsync: non-empty coordinatorKey registers the index scan for read-set folding.
        TransactionOperationId operationId = coordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

        await foreach ((string kvKey, ReadOnlyKeyValueEntry entry) in kahuna.LocateAndScanRange(
            txId, bucketPrefix, startKey, fromInclusive, endKey, toInclusive,
            KvStoreConstants.DefaultPageSize, readTimestamp, KeyValueDurability.Persistent, cancellationToken, coordinatorKey, operationId).ConfigureAwait(false))
        {
            if (entry.Value is null || !kvKey.StartsWith(keyPrefix, StringComparison.Ordinal)) continue;
            string suffix = kvKey.Substring(prefixLen);
            BranchKvValue decoded = BranchKvCodec.Decode(entry.Value);
            yield return (suffix, decoded.Kind, decoded.HasPayload ? (ReadOnlyMemory<byte>?)decoded.Payload : null);
        }
    }

    /// <summary>
    /// One unique-index entry a branch batch write needs write flags for. <see cref="Key"/> is the
    /// decoded composite key, kept because each ancestry level builds the probe key in its own
    /// keyspace; <see cref="KvKey"/> is that key encoded for this (level-0) store.
    /// </summary>
    internal readonly record struct BranchUniqueFlagRequest(
        string IndexId,
        CompositeColumnValue Key,
        string KvKey,
        ObjectIdValue RowId);

    /// <summary>
    /// Batch form of <see cref="ResolveBranchUniqueFlagsAsync"/>: resolves the write flags for every
    /// unique index entry of a branch batch write with one level-0 probe and one probe per ancestry
    /// level, instead of one probe per entry per level. Returns the flags keyed by the level-0 KV key.
    ///
    /// <para>
    /// The per-entry decisions are unchanged — level-0 tombstone → <see cref="KeyValueFlags.Set"/>,
    /// level-0 live value for this same row → <see cref="KeyValueFlags.Set"/>, live value for another
    /// row → <see cref="CamusDBErrorCodes.DuplicateUniqueKeyValue"/>, and otherwise a nearest-first
    /// ancestry walk that stops at the first tombstone or value and ends in
    /// <see cref="KeyValueFlags.SetIfNotExists"/> as the concurrent-insert fence.
    /// </para>
    ///
    /// <para><b>Call this after every lock of the batch is held</b>, for the same reason the per-entry
    /// resolver documents: Kahuna builds the transaction's MVCC snapshot on first access, so a
    /// post-lock probe reflects the state a competing writer left behind rather than a pre-contention
    /// one. Both batch writers reject a repeated new unique key before reaching here, so every request
    /// carries a distinct <see cref="BranchUniqueFlagRequest.KvKey"/> and no entry can be shadowed by
    /// another entry of the same batch.</para>
    ///
    /// <para>A conflict is recorded per position and raised only after every position is resolved, so
    /// the reported duplicate is the first one in write order — the entry the sequential resolver would
    /// have failed on.</para>
    /// </summary>
    internal async Task<Dictionary<string, KeyValueFlags>> ResolveBranchUniqueFlagsBatchAsync(
        KvTransaction tx,
        List<BranchUniqueFlagRequest> requests,
        CancellationToken cancellationToken)
    {
        Dictionary<string, KeyValueFlags> resolved = new(requests.Count, StringComparer.Ordinal);

        if (requests.Count == 0)
            return resolved;

        string[] level0Keys = new string[requests.Count];
        for (int i = 0; i < requests.Count; i++)
            level0Keys[i] = requests[i].KvKey;

        BranchKvValue[] level0 = await ProbeManyRaw(
            tx.TransactionId,
            HLCTimestamp.Zero,
            level0Keys,
            coordinatorKey: "",
            "branch_unique_flags",
            cancellationToken).ConfigureAwait(false);

        // Index id of the conflicting entry, per request position; null when the position resolved.
        string?[] conflicts = new string?[requests.Count];
        List<int> pending = [];

        for (int i = 0; i < requests.Count; i++)
        {
            BranchUniqueFlagRequest request = requests[i];
            BranchKvValue existing = level0[i];

            if (existing.Kind == BranchKvKind.Tombstone)
            {
                // Slot was explicitly cleared in this branch; replace the tombstone with the new value.
                resolved[request.KvKey] = KeyValueFlags.Set;
                continue;
            }

            if (existing.Kind == BranchKvKind.Value && existing.HasPayload)
            {
                // Live value at level-0: an idempotent same-row write is allowed, another row is a conflict.
                if (Encoding.UTF8.GetString(existing.Payload.Span) != request.RowId.ToString())
                    conflicts[i] = request.IndexId;
                else
                    resolved[request.KvKey] = KeyValueFlags.Set;

                continue;
            }

            if (levels.Length == 0)
            {
                resolved[request.KvKey] = KeyValueFlags.SetIfNotExists;
                continue;
            }

            pending.Add(i);
        }

        foreach ((KvKeyBuilder ancestorKeys, KvBranchReader ancestorReader, HLCTimestamp forkTimestamp) in levels)
        {
            if (pending.Count == 0)
                break;

            string[] ancestorProbeKeys = new string[pending.Count];

            for (int i = 0; i < pending.Count; i++)
            {
                BranchUniqueFlagRequest request = requests[pending[i]];
                ancestorProbeKeys[i] = ancestorKeys.BuildUniqueIndexKey(request.IndexId, request.Key);
                BranchMetrics.RecordAncestorProbe();
            }

            BranchKvValue[] probed = await ancestorReader.ProbeManyRaw(
                HLCTimestamp.Zero,
                forkTimestamp,
                ancestorProbeKeys,
                coordinatorKey: "",
                "branch_unique_flags_ancestor",
                cancellationToken).ConfigureAwait(false);

            List<int> next = [];

            for (int i = 0; i < pending.Count; i++)
            {
                int position = pending[i];
                BranchUniqueFlagRequest request = requests[position];
                BranchKvValue ancestor = probed[i];

                if (ancestor.Kind == BranchKvKind.Tombstone)
                {
                    // An ancestor branch cleared this slot: treat as available and stop the walk.
                    resolved[request.KvKey] = KeyValueFlags.SetIfNotExists;
                    continue;
                }

                if (ancestor.Kind == BranchKvKind.Value && ancestor.HasPayload)
                {
                    // A live ancestor entry for another row is a conflict; for this same row the branch
                    // simply shadows its own inherited entry. Either way the walk stops here.
                    if (Encoding.UTF8.GetString(ancestor.Payload.Span) != request.RowId.ToString())
                        conflicts[position] = request.IndexId;
                    else
                        resolved[request.KvKey] = KeyValueFlags.SetIfNotExists;

                    continue;
                }

                next.Add(position);
            }

            pending = next;
        }

        // Absent from level-0 and every ancestor: SetIfNotExists is the concurrent-insert fence.
        for (int i = 0; i < pending.Count; i++)
            resolved[requests[pending[i]].KvKey] = KeyValueFlags.SetIfNotExists;

        for (int i = 0; i < requests.Count; i++)
        {
            if (conflicts[i] is string conflictingIndexId)
                throw new CamusDBException(
                    CamusDBErrorCodes.DuplicateUniqueKeyValue,
                    $"Duplicate entry for key '{keys.DuplicateKeyLabel(conflictingIndexId)}'");
        }

        return resolved;
    }

    /// <summary>
    /// Resolves the <see cref="KeyValueFlags"/> to use when writing a unique index entry on a
    /// branch database.
    ///
    /// <para>
    /// This method MUST be called AFTER the exclusive lock for <paramref name="kvKey"/> has been
    /// acquired.  Because Kahuna creates each transaction's MVCC snapshot lazily on first
    /// access, probing <paramref name="kvKey"/> here — the first access for this transaction —
    /// reflects the committed state that exists once the lock was acquired, not a stale snapshot
    /// captured before lock contention resolved.
    /// </para>
    ///
    /// Three cases:
    /// <list type="bullet">
    ///   <item>Level-0 tombstone → <see cref="KeyValueFlags.Set"/> (tombstone-replace: the slot was
    ///     explicitly cleared in this branch and is available for re-use).</item>
    ///   <item>Level-0 live value, same rowId → <see cref="KeyValueFlags.Set"/> (idempotent write).</item>
    ///   <item>Level-0 live value, different rowId → throws <see cref="CamusDBErrorCodes.DuplicateUniqueKeyValue"/>.</item>
    ///   <item>Level-0 miss → walks ancestor chain; ancestor live value for a different rowId → throws
    ///     <see cref="CamusDBErrorCodes.DuplicateUniqueKeyValue"/>; otherwise → <see cref="KeyValueFlags.SetIfNotExists"/>
    ///     (concurrent-insert fence for same-branch races).</item>
    /// </list>
    /// </summary>
    internal async Task<KeyValueFlags> ResolveBranchUniqueFlagsAsync(
        KvTransaction tx,
        string indexId,
        CompositeColumnValue key,
        string kvKey,
        ObjectIdValue rowId,
        CancellationToken cancellationToken)
    {
        // Post-lock probe: first access to this key in the transaction, so Kahuna creates the
        // MVCC snapshot from the current committed state — after any competing writer released
        // the lock by committing or rolling back.
        BranchKvValue existing = await ProbeRaw(
            tx.TransactionId, HLCTimestamp.Zero, kvKey, cancellationToken).ConfigureAwait(false);

        if (existing.Kind == BranchKvKind.Tombstone)
            // Slot was explicitly cleared in this branch; replace the tombstone with the new value.
            return KeyValueFlags.Set;

        if (existing.Kind == BranchKvKind.Value && existing.HasPayload)
        {
            // Live value at level-0: idempotent same-row write (e.g. backfill resume or an UPDATE
            // that touches non-indexed columns) is allowed; a different rowId is a conflict.
            if (Encoding.UTF8.GetString(existing.Payload.Span) != rowId.ToString())
                throw new CamusDBException(
                    CamusDBErrorCodes.DuplicateUniqueKeyValue,
                    $"Duplicate entry for key '{keys.DuplicateKeyLabel(indexId)}'");
            return KeyValueFlags.Set;
        }

        // Miss at level-0: walk ancestry to enforce uniqueness over the full union.
        // Nearest ancestor first; stop on the first hit (tombstone or live value).
        foreach ((KvKeyBuilder ancestorKeys, KvBranchReader ancestorReader, HLCTimestamp forkTimestamp) in levels)
        {
            string ancestorKvKey = ancestorKeys.BuildUniqueIndexKey(indexId, key);
            BranchKvValue ancestor = await ancestorReader.ProbeRaw(
                HLCTimestamp.Zero, forkTimestamp, ancestorKvKey, cancellationToken).ConfigureAwait(false);

            if (ancestor.Kind == BranchKvKind.Tombstone)
                break;   // an ancestor branch cleared this slot; treat as available

            if (ancestor.Kind == BranchKvKind.Value && ancestor.HasPayload)
            {
                if (Encoding.UTF8.GetString(ancestor.Payload.Span) != rowId.ToString())
                    throw new CamusDBException(
                        CamusDBErrorCodes.DuplicateUniqueKeyValue,
                        $"Duplicate entry for key '{keys.DuplicateKeyLabel(indexId)}'");
                break;   // same rowId in ancestor (branch shadows its own inherited entry); allow
            }
        }

        // Slot absent from level-0 and all ancestors.  SetIfNotExists is a concurrent-insert
        // fence: if two branch transactions race to insert the same unique key, one wins the
        // lock and writes; the other, when it retries the lock and probes, sees the winner's
        // committed value and throws DuplicateUniqueKeyValue above.  As an additional safety net,
        // SetIfNotExists here ensures the second writer's set also fails if the MVCC snapshot
        // somehow missed the intermediate commit.
        return KeyValueFlags.SetIfNotExists;
    }
}
