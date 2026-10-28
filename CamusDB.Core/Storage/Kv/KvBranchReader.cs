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

    /// <summary>
    /// Set only on a reader whose store serves as a frozen <em>ancestor level</em> of some branch:
    /// every read this reader answers is then an as-of-fork snapshot read whose completeness
    /// depends on that branch's snapshot-hold chain staying alive. Each such read verifies the
    /// chain through this guard after the Kahuna round-trip and before its result — value,
    /// tombstone, or absence — is surfaced, so a lapsed hold produces a hard error instead of a
    /// silently incomplete answer. Null on level-0 readers and on root databases, where reads use
    /// live MVCC snapshots that need no retention pin.
    /// </summary>
    private readonly BranchSnapshotHoldGuard? snapshotGuard;

    internal KvBranchReader(IKahuna kahuna, KvKeyBuilder keys, BranchLevel[] levels, BranchSnapshotHoldGuard? snapshotGuard = null)
    {
        this.kahuna = kahuna;
        this.keys = keys;
        this.levels = levels;
        this.snapshotGuard = snapshotGuard;
    }

    /// <summary>
    /// Verifies the owning branch's snapshot protection when this reader is an ancestor level;
    /// no-op otherwise. Called after a Kahuna read completes so the confirmation covers it — a
    /// renew confirmed at time T proves the hold chain was live continuously until T, which
    /// validates every read that finished before T.
    /// </summary>
    private ValueTask GuardSnapshotAsync(CancellationToken cancellationToken) =>
        snapshotGuard?.EnsureProtectedAsync(cancellationToken) ?? ValueTask.CompletedTask;

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
    ///
    /// <para>With <paramref name="aborts"/>, a read Kahuna aborted or never confirmed is recorded there
    /// and answered as <see cref="BranchKvValue.Miss"/>, which the caller must not use: it tests
    /// <see cref="RetryableAbortSink.HasAbort"/> first. See <see cref="RetryableAbortSink"/>.</para>
    ///
    /// <para><paramref name="readMode"/> is the Kahuna read mode that goes with
    /// <paramref name="readTimestamp"/>; a caller takes both from one <see cref="KvReadView"/>. The
    /// same parameter on the batch and scan methods of this class has the same meaning. An ancestor
    /// snapshot read keeps the default mode.</para>
    /// </summary>
    internal async ValueTask<BranchKvValue> ProbeRaw(
        HLCTimestamp txId,
        HLCTimestamp readTimestamp,
        string key,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        RetryableAbortSink? aborts = null,
        KeyValueReadMode readMode = KeyValueReadMode.Default)
    {
        TransactionOperationId operationId = coordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

        // The arguments travel as a tuple state to a static lambda: a probe is the inner step of every
        // unique index lookup and every single-row read, and a capturing lambda cost a closure and a
        // delegate per call.
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await KahunaRetryPolicy.RetryOnMustRetry(
            static state => state.kahuna.LocateAndTryGetValue(
                state.txId, state.key, -1, state.readTimestamp, KeyValueDurability.Persistent, state.cancellationToken,
                coordinatorKey: state.coordinatorKey, operationId: state.operationId, readMode: state.readMode),
            (kahuna, txId, key, readTimestamp, cancellationToken, coordinatorKey, operationId, readMode),
            cancellationToken
        ).ConfigureAwait(false);

        // Kahuna's early conflict check answers Aborted when another transaction committed the key after
        // this one first read it: the transaction has lost, and only the client can restart it.
        if (type == KeyValueResponseType.Aborted)
        {
            RetryableAbortSink.Raise(aborts,
                new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                    $"Read of key {key} was aborted by Kahuna — retry the operation from BeginAsync"),
                ServerDiagnostics.Tags.AbortSite.Read);
            return BranchKvValue.Miss;
        }

        // Only a confirmed answer may shape the result. Anything else — an exhausted MustRetry /
        // WaitingForReplication (a foreign 2PC intent that stalled past the whole retry budget),
        // Errored, or any future non-confirmed type — means the key's state is UNKNOWN, not absent.
        // See the class summary for the atomicity violation that decoding it as a miss produced.
        if (type is not (KeyValueResponseType.Get or KeyValueResponseType.DoesNotExist))
        {
            RetryableAbortSink.Raise(aborts,
                new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                    $"Read of key {key} did not return a confirmed result ({type}) — retry the operation from BeginAsync"),
                ServerDiagnostics.Tags.AbortSite.Read);
            return BranchKvValue.Miss;
        }

        // On an ancestor level a "confirmed" answer is only as good as the snapshot pin behind it:
        // a reclaimed revision reads as a confirmed miss. Verify the pin before the answer is used.
        await GuardSnapshotAsync(cancellationToken).ConfigureAwait(false);

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
    /// The read is <c>LocateAndTryProbeMany</c>: the keys of one key space are answered in one actor
    /// turn on their partition, in input order, and the keys the actor does not hold are loaded from the
    /// backend as one batch. Kahuna takes one key space per call and at most
    /// <see cref="KvStoreConstants.ProbeBatchMaxKeys"/> keys, so the distinct keys are grouped by key
    /// space (the text before the last <c>/</c>) and split to that size. Every caller today passes keys
    /// of one key space, so the grouping is one comparison per key that allocates nothing. A repeated
    /// key is sent once and answers every position that holds it.
    /// </para>
    ///
    /// <para>
    /// MustRetry / WaitingForReplication mean a partition is not yet ready: only the affected subset is
    /// retried, with exponential back-off, up to <see cref="KahunaRetryPolicy.MaxKahunaRetries"/> times
    /// and for at most <see cref="KahunaRetryPolicy.TransientRetryWindowMs"/>. Any
    /// other non-confirmed response, and an exhausted retry budget, throw
    /// <see cref="CamusDBErrorCodes.TransactionMustRetry"/> rather than decoding as an absence. A batch
    /// Kahuna refuses as malformed is a bug in the caller, not a transient, and throws
    /// <see cref="CamusDBErrorCodes.InvalidInternalOperation"/>.
    /// </para>
    ///
    /// <para>
    /// A non-empty <paramref name="coordinatorKey"/> registers the read for commit-time validation. Each
    /// batch Kahuna receives is one operation: its id is reused while its pending set is unchanged (an
    /// ack-loss resend of the identical batch, which the coordinator can only replay idempotently under
    /// the same id) and a fresh id is minted only when the set shrinks. Unregistered reads — every
    /// ancestor snapshot probe — carry the default id, so the identity is immaterial there.
    /// </para>
    ///
    /// <para>
    /// With <paramref name="aborts"/>, an aborted or never-confirmed batch is recorded there and answered
    /// as all misses, which the caller must not use: it tests <see cref="RetryableAbortSink.HasAbort"/>
    /// first. See <see cref="RetryableAbortSink"/>.
    /// </para>
    /// </summary>
    internal async Task<BranchKvValue[]> ProbeManyRaw(
        HLCTimestamp txId,
        HLCTimestamp readTimestamp,
        IReadOnlyList<string> probeKeys,
        string coordinatorKey,
        string retryDiagnosticLabel,
        CancellationToken cancellationToken,
        RetryableAbortSink? aborts = null,
        KeyValueReadMode readMode = KeyValueReadMode.Default)
    {
        if (probeKeys.Count == 0)
            return [];

        // The distinct keys in first-seen order, and the distinct slot of every input position.
        int[] slotOf = new int[probeKeys.Count];
        List<string> distinct = new(probeKeys.Count);
        Dictionary<string, int> slots = new(probeKeys.Count, StringComparer.Ordinal);

        for (int i = 0; i < probeKeys.Count; i++)
        {
            string key = probeKeys[i];

            if (!slots.TryGetValue(key, out int slot))
            {
                slot = distinct.Count;
                slots[key] = slot;
                distinct.Add(key);
            }

            slotOf[i] = slot;
        }

        KeyValueProbeResult[] answers = new KeyValueProbeResult[distinct.Count];

        foreach ((int[]? members, int offset, int count) in ProbeBatches(distinct))
        {
            bool confirmed = await ProbeBatchAsync(
                txId, readTimestamp, distinct, members, offset, count, answers,
                coordinatorKey, retryDiagnosticLabel, cancellationToken, aborts, readMode).ConfigureAwait(false);

            if (!confirmed)
                return AllMisses(probeKeys.Count);
        }

        // Same rule as ProbeRaw: on an ancestor level, a batch full of confirmed misses is exactly
        // what reclaimed history looks like. Verify the snapshot pin before decoding any of it.
        await GuardSnapshotAsync(cancellationToken).ConfigureAwait(false);

        BranchKvValue[] decoded = new BranchKvValue[probeKeys.Count];

        for (int i = 0; i < probeKeys.Count; i++)
        {
            KeyValueProbeResult answer = answers[slotOf[i]];
            decoded[i] = answer is { Type: KeyValueResponseType.Get, Entry: not null }
                ? BranchKvCodec.Decode(answer.Entry.Value)
                : BranchKvValue.Miss;
        }

        return decoded;
    }

    /// <summary>The key space of a key: the text before its last <c>/</c>, or the whole key when it has none.</summary>
    private static ReadOnlySpan<char> KeySpaceOf(string key)
    {
        int separator = key.LastIndexOf('/');
        return separator < 0 ? key.AsSpan() : key.AsSpan(0, separator);
    }

    /// <summary>
    /// Splits the distinct keys into the batches one Kahuna call accepts: one key space per batch and at
    /// most <see cref="KvStoreConstants.ProbeBatchMaxKeys"/> keys, each key once, in input order. A batch
    /// is the slots at <c>members</c>, or, when <c>members</c> is null, the <c>count</c> slots from
    /// <c>offset</c>. The common call, one key space and a page of keys, is one batch of every key that
    /// allocates only the list.
    /// </summary>
    private static List<(int[]? Members, int Offset, int Count)> ProbeBatches(List<string> keys)
    {
        List<(int[]? Members, int Offset, int Count)> batches = [];
        Dictionary<string, List<int>>? bySpace = null;

        for (int i = 1; i < keys.Count; i++)
        {
            if (bySpace is null)
            {
                if (KeySpaceOf(keys[i]).SequenceEqual(KeySpaceOf(keys[0])))
                    continue;

                // The first key of another space: from here on, every key goes to its space's group. The
                // keys before it are all of the first key's space.
                bySpace = new(StringComparer.Ordinal);
                List<int> first = new(i);
                for (int j = 0; j < i; j++)
                    first.Add(j);
                bySpace[KeySpaceOf(keys[0]).ToString()] = first;
            }

            string space = KeySpaceOf(keys[i]).ToString();
            if (!bySpace.TryGetValue(space, out List<int>? group))
                bySpace[space] = group = [];
            group.Add(i);
        }

        if (bySpace is null)
        {
            for (int offset = 0; offset < keys.Count; offset += KvStoreConstants.ProbeBatchMaxKeys)
                batches.Add((null, offset, Math.Min(KvStoreConstants.ProbeBatchMaxKeys, keys.Count - offset)));

            return batches;
        }

        foreach (List<int> group in bySpace.Values)
        {
            int[] members = [.. group];
            for (int offset = 0; offset < members.Length; offset += KvStoreConstants.ProbeBatchMaxKeys)
                batches.Add((members, offset, Math.Min(KvStoreConstants.ProbeBatchMaxKeys, members.Length - offset)));
        }

        return batches;
    }

    /// <summary>
    /// One batch of <see cref="ProbeManyRaw"/>: asks Kahuna for its keys, resends the keys that answered
    /// a transient until every key is confirmed, and writes each confirmed answer to the key's slot of
    /// <paramref name="answers"/>. False when the batch was aborted or never confirmed; the abort is then
    /// recorded in <paramref name="aborts"/> (or thrown) and no slot of the batch may be read.
    /// </summary>
    private async Task<bool> ProbeBatchAsync(
        HLCTimestamp txId,
        HLCTimestamp readTimestamp,
        List<string> keys,
        int[]? members,
        int offset,
        int count,
        KeyValueProbeResult[] answers,
        string coordinatorKey,
        string retryDiagnosticLabel,
        CancellationToken cancellationToken,
        RetryableAbortSink? aborts,
        KeyValueReadMode readMode)
    {
        // The slots still to ask. Null on the first attempt: the whole batch.
        List<int>? pending = null;
        int pendingCount = count;
        int retries = 0;
        long deadline = KahunaRetryPolicy.TransientRetryDeadlineTicks();

        TransactionOperationId operationId = coordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

        while (true)
        {
            string[] request = new string[pendingCount];
            for (int j = 0; j < pendingCount; j++)
                request[j] = keys[SlotAt(j)];

            // null: the forward was refused by the dead partition leader, so no key was answered — every
            // pending key stays pending and the unchanged batch is resent under the same id.
            KeyValueProbeResult[]? results = await KahunaRetryPolicy.InvokeOrUnanswered(
                () => kahuna.LocateAndTryProbeMany(
                    txId,
                    readTimestamp,
                    request,
                    KeyValueProbeLock.None,
                    0,
                    KeyValueDurability.Persistent,
                    cancellationToken,
                    coordinatorKey,
                    operationId,
                    readMode),
                cancellationToken).ConfigureAwait(false);

            List<int>? next = null;

            if (results is null)
            {
                next = new(pendingCount);
                for (int j = 0; j < pendingCount; j++)
                    next.Add(SlotAt(j));
            }
            else
            {
                if (results.Length != pendingCount)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation,
                        $"Batched probe of {pendingCount} keys answered {results.Length} results");

                for (int j = 0; j < pendingCount; j++)
                {
                    KeyValueProbeResult result = results[j];
                    int slot = SlotAt(j);

                    switch (result.Type)
                    {
                        case KeyValueResponseType.Get:
                        case KeyValueResponseType.DoesNotExist:
                            answers[slot] = result;
                            break;

                        case KeyValueResponseType.MustRetry:
                        case KeyValueResponseType.WaitingForReplication:
                            (next ??= []).Add(slot);
                            break;

                        case KeyValueResponseType.Aborted:
                            RetryableAbortSink.Raise(aborts,
                                new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                                    $"Batch read of key {keys[slot]} was aborted by Kahuna — retry the operation from BeginAsync"),
                                ServerDiagnostics.Tags.AbortSite.ReadMany);
                            return false;

                        case KeyValueResponseType.InvalidInput:
                            // The batch was malformed (too many keys, or keys of several key spaces): a
                            // caller bug that a retry cannot fix.
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation,
                                $"Kahuna refused a batched probe of {pendingCount} keys starting at {keys[SlotAt(0)]}: " +
                                $"a batch holds keys of one key space, at most {KvStoreConstants.ProbeBatchMaxKeys} of them");

                        default:
                            // Same rule as ProbeRaw: only a confirmed answer may reach the decode loop,
                            // which treats every unanswered key as an absent row. A per-key Errored (a
                            // dropped actor response) that reached a slot would decode as Miss — an unknown
                            // converted into a definitive absence, with no folded observation for commit
                            // validation to catch.
                            RetryableAbortSink.Raise(aborts,
                                new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                                    $"Batch read of key {keys[slot]} returned {result.Type} — retry the operation from BeginAsync"),
                                ServerDiagnostics.Tags.AbortSite.ReadMany);
                            return false;
                    }
                }
            }

            if (next is null)
                return true;

            if (!KahunaRetryPolicy.MayRetryTransient(ref retries, deadline))
            {
                RetryableAbortSink.Raise(aborts,
                    new CamusDBException(CamusDBErrorCodes.TransactionMustRetry,
                        $"Batch read was not ready after {retries} attempts or {KahunaRetryPolicy.TransientRetryWindowMs} ms — retry the operation from BeginAsync"),
                    ServerDiagnostics.Tags.AbortSite.ReadMany);
                return false;
            }

            ServerDiagnostics.AddKvRetryWait(retryDiagnosticLabel);
            await Task.Delay(KahunaRetryPolicy.RetryDelayMs(retries), cancellationToken).ConfigureAwait(false);

            // Shrinking set (some reads confirmed) → new, smaller declaration under a fresh id on the
            // registered path. Unchanged set (every key transient) → identical resend of a lost ack →
            // keep the same id so the coordinator replays the read observation idempotently.
            if (coordinatorKey.Length != 0 && next.Count != pendingCount)
                operationId = TransactionOperationId.NewRandom();

            pending = next;
            pendingCount = next.Count;
        }

        int SlotAt(int j) => pending is not null ? pending[j] : members is not null ? members[offset + j] : offset + j;
    }

    /// <summary>The neutral answer a batch read leaves behind when it records an abort: one miss per position.</summary>
    private static BranchKvValue[] AllMisses(int count)
    {
        BranchKvValue[] misses = new BranchKvValue[count];
        Array.Fill(misses, BranchKvValue.Miss);
        return misses;
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
        string coordinatorKey = "",
        KeyValueReadMode readMode = KeyValueReadMode.Default)
    {
        string rowKeyPrefix = keys.RowKeyPrefix;
        int prefixLen = rowKeyPrefix.Length;

        string? startKey = startHex is not null ? rowKeyPrefix + startHex : null;
        string? endKey = endHex is not null ? rowKeyPrefix + endHex : null;

        // Non-empty coordinatorKey registers the scan so every row it returns folds as a read
        // observation for commit-time validation; the coordinator derives a distinct operation id per
        // page from this base id. Empty (default, and all ancestor snapshots) scans unregistered.
        TransactionOperationId operationId = coordinatorKey.Length == 0 ? default : TransactionOperationId.NewRandom();

        // Counted in a finally block: a caller that stops early disposes the iterator, and the entries
        // read until then still count.
        long entriesRead = 0;

        try
        {
            await foreach ((string key, ReadOnlyKeyValueEntry entry) in KvScanFailure.Translate(kahuna.LocateAndScanRange(
                txId, keys.RowBucketPrefix, startKey, startInclusive, endKey, endKey is null, KvStoreConstants.DefaultPageSize,
                readTimestamp, KeyValueDurability.Persistent, cancellationToken, coordinatorKey, operationId, readMode: readMode),
                $"row scan of table {keys.DisplayTableName}", cancellationToken).ConfigureAwait(false))
            {
                entriesRead++;

                // Per-entry on the fast path this is one volatile read and one tick comparison; the
                // renew round-trip happens at most once per refresh window. Checking as the scan
                // streams keeps every yielded page covered by a confirmation that postdates it.
                await GuardSnapshotAsync(cancellationToken).ConfigureAwait(false);

                if (entry.Value is null) continue;
                string rowIdHex = key.AsSpan(prefixLen).ToString();
                BranchKvValue decoded = BranchKvCodec.Decode(entry.Value);
                yield return (rowIdHex, decoded.Kind, decoded.HasPayload ? (ReadOnlyMemory<byte>?)decoded.Payload : null);
            }
        }
        finally
        {
            ServerDiagnostics.AddKvScanEntries(index: false, entriesRead);
        }

        // End-of-stream is information too: a scan over reclaimed history simply ends early, so the
        // absence of further rows must be validated exactly like a returned row.
        await GuardSnapshotAsync(cancellationToken).ConfigureAwait(false);
    }

    // Yields every index entry within optional encoded bounds from this store's namespace at the
    // given snapshot.  txId is the live transaction id for level-0 reads (HLCTimestamp.Zero for
    // ancestor snapshots).  fromEncoded/toEncoded are the raw encoded key strings (no prefix) so
    // this store builds its own start/end keys in its own keyspace ({dbId}:{tableId}|i:{indexId}/…).
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
        bool fromIsPrefixBound = false,
        int pageSize = KvStoreConstants.DefaultPageSize,
        KeyValueReadMode readMode = KeyValueReadMode.Default)
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

        ConfiguredCancelableAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> cursor = KvScanFailure.Translate(kahuna.LocateAndScanRange(
            txId, 
            bucketPrefix, 
            startKey, 
            fromInclusive, 
            endKey, 
            toInclusive,
            pageSize, 
            readTimestamp, 
            KeyValueDurability.Persistent, 
            cancellationToken, 
            coordinatorKey, 
            operationId,
            readMode: readMode
        ),
        $"index scan of {keys.DisplayTableName}.{indexId}", cancellationToken).ConfigureAwait(false);

        // See ScanRowsRawAsync: counted in a finally block so an early stop still counts.
        long entriesRead = 0;

        try
        {
            await foreach ((string kvKey, ReadOnlyKeyValueEntry entry) in cursor)
            {
                entriesRead++;

                // See ScanRowsRawAsync: cheap per-entry check keeping every streamed page covered by a
                // snapshot-pin confirmation that postdates it.
                await GuardSnapshotAsync(cancellationToken).ConfigureAwait(false);

                if (entry.Value is null || !kvKey.StartsWith(keyPrefix, StringComparison.Ordinal)) continue;
                string suffix = kvKey.Substring(prefixLen);
                BranchKvValue decoded = BranchKvCodec.Decode(entry.Value);
                yield return (suffix, decoded.Kind, decoded.HasPayload ? (ReadOnlyMemory<byte>?)decoded.Payload : null);
            }
        }
        finally
        {
            ServerDiagnostics.AddKvScanEntries(index: true, entriesRead);
        }

        // See ScanRowsRawAsync: an early end over reclaimed history must not read as a normal end.
        await GuardSnapshotAsync(cancellationToken).ConfigureAwait(false);
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
        CancellationToken cancellationToken
    )
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
            cancellationToken
        ).ConfigureAwait(false);

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
                cancellationToken
            ).ConfigureAwait(false);

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
            tx.TransactionId,
            HLCTimestamp.Zero, 
            kvKey,
            cancellationToken
        ).ConfigureAwait(false);

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
                HLCTimestamp.Zero, 
                forkTimestamp, 
                ancestorKvKey, 
                cancellationToken
            ).ConfigureAwait(false);

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
