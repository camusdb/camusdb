
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Ttl;

/// <summary>
/// Owns the durable state of a TTL sweep — the run manifest, the per-span claim leases, and the per-span
/// checkpoints — and the protocol by which workers on different nodes divide a table's keyspace without
/// stepping on each other.
///
/// <para><b>This is the one genuinely new mechanism in row-level TTL.</b> Every other background job in
/// the engine is leader-only: it checks leadership and does all the work itself. Nothing previously
/// handed work to other nodes. Spans are claimed rather than assigned, because assignment would require
/// the planner to know which nodes are alive and to reassign when one dies — a membership problem.
/// A claim, by contrast, needs no membership knowledge at all: a worker takes what is free, and a dead
/// worker's claim frees itself when its lease lapses.</para>
///
/// <para><b>Keyspace</b> (following the <c>{db}/meta/…</c> convention):</para>
/// <code>
/// {db}/meta/ttl/{tableId}              run manifest
/// {db}/meta/ttl/{tableId}/{span}       span claim — leased, so a dead owner releases it
/// {db}/meta/ttl/{tableId}/{span}/ck    span checkpoint — NOT leased, so progress outlives the owner
/// </code>
///
/// <para>The split between the last two is the important one. If progress lived in the claim value it
/// would expire with the claim, and a reclaiming worker would rescan a span from the start — turning a
/// crashed worker from a small delay into repeated work on every failure.</para>
/// </summary>
internal sealed class TtlSpanCoordinator : IAsyncDisposable
{
    private const int MaxRetries = 10;

    private readonly IKahuna kahuna;
    private readonly TtlSpanLease spanLease;

    public TtlSpanCoordinator(IKahuna kahuna, string ownerPrefix, int spanLeaseMs, int renewIntervalMs)
    {
        this.kahuna = kahuna;
        spanLease = new TtlSpanLease(kahuna, ownerPrefix, spanLeaseMs, renewIntervalMs);
    }

    // ── Key layout ────────────────────────────────────────────────────────────

    internal static string ManifestKey(string dbId, string tableId) => $"{dbId}/meta/ttl/{tableId}";

    internal static string SpanClaimKey(string dbId, string tableId, int spanIndex) =>
        $"{dbId}/meta/ttl/{tableId}/{spanIndex}";

    internal static string SpanCheckpointKey(string dbId, string tableId, int spanIndex) =>
        $"{dbId}/meta/ttl/{tableId}/{spanIndex}/ck";

    // ── Run manifest ──────────────────────────────────────────────────────────

    /// <summary>
    /// Reads the open run for a table, or null when there is none (or the stored record is unreadable,
    /// which is treated as "no run" so a corrupt manifest causes a fresh run rather than a stuck table).
    /// </summary>
    public async Task<TtlRunManifest?> ReadManifestAsync(string dbId, string tableId, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, ManifestKey(dbId, tableId), -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, ct
        ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry?.Value is null)
            return null;

        try
        {
            return MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.TtlRunManifest);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Publishes a run manifest, conditional on what the caller believed was there.
    ///
    /// <para><b>Never an unconditional write.</b> Leadership can be lost between reading the manifest and
    /// publishing a new one, and a former leader that publishes anyway would overwrite the run its
    /// successor had already minted — two horizons for one table, each half-swept, with spans
    /// checkpointed under the wrong one. Passing <paramref name="expected"/> as null requires the key to
    /// be absent (minting a first run); passing a manifest requires the stored bytes to match it exactly
    /// (completing or replacing a run the caller has actually seen).</para>
    ///
    /// <para>Returns false when the record moved underneath the caller. That is not an error — it means
    /// another node got there first, and the correct response is to leave the run alone.</para>
    /// </summary>
    public async Task<bool> TryWriteManifestAsync(
        string dbId, TtlRunManifest? expected, TtlRunManifest manifest, CancellationToken ct)
    {
        byte[] value = MetaJsonSerializer.Serialize(manifest, MetaJsonContext.Default.TtlRunManifest);
        byte[]? compare = expected is null
            ? null
            : MetaJsonSerializer.Serialize(expected, MetaJsonContext.Default.TtlRunManifest);

        KeyValueFlags flags = expected is null ? KeyValueFlags.SetIfNotExists : KeyValueFlags.SetIfEqualToValue;

        int retries = 0;
        while (!ct.IsCancellationRequested)
        {
            (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, ManifestKey(dbId, manifest.TableId),
                value, compare, -1, flags, 0, KeyValueDurability.Persistent, ct
            ).ConfigureAwait(false);

            if (type == KeyValueResponseType.Set)
                return true;

            if (type == KeyValueResponseType.NotSet)
                return false; // someone else published first, or the record has since changed

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
                && ++retries < MaxRetries)
            {
                await Task.Delay(retries * 10, ct).ConfigureAwait(false);
                continue;
            }

            // Any other status, or exhausted retries, is an unresolved write. Report failure so the
            // caller does not proceed as though its manifest were published.
            return false;
        }

        return false;
    }

    /// <summary>
    /// Deletes a run and every span record belonging to it. Called when a run completes, and when a
    /// manifest is found to belong to a table id that no longer exists — a run left behind by a
    /// <c>DROP</c> must be discarded, never driven, or it would delete rows in whatever table now
    /// carries that name.
    /// </summary>
    public async Task DeleteRunAsync(string dbId, string tableId, int spanCount, CancellationToken ct)
    {
        for (int i = 0; i < spanCount; i++)
        {
            await TryDeleteAsync(SpanCheckpointKey(dbId, tableId, i), ct).ConfigureAwait(false);
            await TryDeleteAsync(SpanClaimKey(dbId, tableId, i), ct).ConfigureAwait(false);
        }

        await TryDeleteAsync(ManifestKey(dbId, tableId), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Lists the table ids that currently have TTL run metadata in a database, by scanning the
    /// <c>{db}/meta/ttl/</c> namespace.
    ///
    /// <para>Exists because discovery only ever visits tables whose TTL is <em>active</em>: the moment a
    /// table is paused, reset, or dropped it stops being visited, and any manifest, claim, or checkpoint
    /// it left behind becomes unreachable — stranded forever in a namespace nothing revisits. Finding
    /// them requires asking what metadata exists rather than what tables exist.</para>
    /// </summary>
    public async Task<IReadOnlyList<string>> ListRunTableIdsAsync(string dbId, CancellationToken ct)
    {
        string prefix = $"{dbId}/meta/ttl";
        HashSet<string> tableIds = new(StringComparer.Ordinal);

        await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, prefix, null, true, null, true, 512,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, ct).ConfigureAwait(false))
        {
            // Keys are "{db}/meta/ttl/{tableId}" plus optional "/{span}" and "/{span}/ck" suffixes. The
            // first segment after the prefix is the table id in all three shapes.
            if (!key.StartsWith(prefix + "/", StringComparison.Ordinal))
                continue;

            ReadOnlySpan<char> rest = key.AsSpan(prefix.Length + 1);
            int slash = rest.IndexOf('/');
            string tableId = slash < 0 ? rest.ToString() : rest[..slash].ToString();

            if (tableId.Length > 0)
                tableIds.Add(tableId);
        }

        return [.. tableIds];
    }

    // ── Span claims ───────────────────────────────────────────────────────────

    /// <summary>
    /// Attempts to claim a span for this node, returning the claim's fencing token or null if another
    /// worker holds it. Exactly one of any number of racing workers wins; the losers move to the next
    /// span rather than waiting, so a contended run still makes progress at full width instead of
    /// serializing on one span.
    ///
    /// <para>The token must be carried through every subsequent operation on this span. It is what ties
    /// a checkpoint write to the claim that authorized it — without it, a worker that stalled past its
    /// lease could write progress into a span another worker now owns.</para>
    /// </summary>
    public Task<long?> TryClaimSpanAsync(string dbId, string tableId, int spanIndex, CancellationToken ct)
        => spanLease.TryAcquireAsync(SpanClaimKey(dbId, tableId, spanIndex), ct);

    /// <summary>
    /// Releases a span claim so another worker can take it immediately rather than after the lease
    /// lapses. A no-op unless <paramref name="token"/> is still the live claim, so a late release from a
    /// worker whose lease already lapsed cannot free a span its successor is working.
    /// </summary>
    public Task ReleaseSpanAsync(string dbId, string tableId, int spanIndex, long token)
        => spanLease.ReleaseAsync(SpanClaimKey(dbId, tableId, spanIndex), token);

    /// <summary>
    /// Whether this node's renewer still believes it holds the span under <paramref name="token"/>.
    /// Checked between batches so a worker whose lease lapsed during a stall stops deleting instead of
    /// racing the worker that reclaimed it. A local view by design — it is a cheap early-out, not proof
    /// of ownership; the compare-and-set on the checkpoint write is what actually fences.
    /// </summary>
    public bool StillOwnsSpan(string dbId, string tableId, int spanIndex, long token)
        => spanLease.StillHeldLocally(SpanClaimKey(dbId, tableId, spanIndex), token);

    // ── Span checkpoints ──────────────────────────────────────────────────────

    /// <summary>
    /// Reads a span's checkpoint. A checkpoint stamped with a different run id is treated as absent: it
    /// describes progress through a previous run's horizon and must not be used to skip rows in this one.
    /// </summary>
    public async Task<TtlSpanCheckpoint?> ReadCheckpointAsync(
        string dbId, string tableId, int spanIndex, string runId, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, SpanCheckpointKey(dbId, tableId, spanIndex), -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, ct
        ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry?.Value is null)
            return null;

        TtlSpanCheckpoint checkpoint;
        try
        {
            checkpoint = MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.TtlSpanCheckpoint);
        }
        catch
        {
            return null;
        }

        return string.Equals(checkpoint.RunId, runId, StringComparison.Ordinal) ? checkpoint : null;
    }

    /// <summary>
    /// Persists a span's progress, refusing any write that is not a forward step by the current claim
    /// holder. Returns false when the write was declined, which the caller must treat as having lost the
    /// span.
    ///
    /// <para>Written after the delete transaction commits, never before: a checkpoint that ran ahead of
    /// its deletes would skip rows permanently on the next resume, whereas one that lags merely re-scans
    /// a batch whose rows are already gone, which is harmless.</para>
    ///
    /// <para><b>Three guards, each covering what the one before it cannot.</b> The local claim check
    /// stops a worker that has already observed losing the span. The <b>fencing token</b> is the one that
    /// actually decides: because Kahuna hands out a strictly greater token on every grant, a record
    /// already carrying a higher token proves a later owner exists, no matter what this worker still
    /// believes — local knowledge always trails reality, and a lease can lapse between the check and the
    /// write. Finally the value is refused if it would move <c>LastRowIdHex</c> backwards or clear
    /// <c>Done</c>, so progress cannot regress even between writes of one owner. The compare-and-set on
    /// the stored bytes then makes the whole thing atomic against a concurrent writer.</para>
    /// </summary>
    public async Task<bool> TryWriteCheckpointAsync(
        string dbId, string tableId, int spanIndex, long claimToken,
        TtlSpanCheckpoint? previous, TtlSpanCheckpoint checkpoint, CancellationToken ct)
    {
        if (!spanLease.StillHeldLocally(SpanClaimKey(dbId, tableId, spanIndex), claimToken))
            return false;

        // A stored token greater than ours means the span was granted to someone else after us. This is
        // the guard a self-minted identifier cannot provide: equality can only say "not mine", while
        // ordering says "mine, and older" — which is what makes the decision safe without a live read of
        // the lock.
        if (previous is not null && previous.OwnerFencingToken > claimToken)
            return false;

        if (!IsForwardStep(previous, checkpoint))
            return false;

        checkpoint.OwnerFencingToken = claimToken;

        byte[] value = MetaJsonSerializer.Serialize(checkpoint, MetaJsonContext.Default.TtlSpanCheckpoint);

        // No previous record means this span has not been checkpointed in this run, so the write is
        // conditional on the key still being absent. Otherwise it is conditional on the exact bytes we
        // read — either way a concurrent writer's record is never silently overwritten.
        byte[]? compare = previous is null
            ? null
            : MetaJsonSerializer.Serialize(previous, MetaJsonContext.Default.TtlSpanCheckpoint);

        KeyValueFlags flags = previous is null ? KeyValueFlags.SetIfNotExists : KeyValueFlags.SetIfEqualToValue;

        int retries = 0;
        while (!ct.IsCancellationRequested)
        {
            (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, SpanCheckpointKey(dbId, tableId, spanIndex),
                value, compare, -1, flags, 0, KeyValueDurability.Persistent, ct
            ).ConfigureAwait(false);

            if (type == KeyValueResponseType.Set)
                return true;

            // NotSet means the stored record is not what we based this write on: another worker owns the
            // span and has advanced it. Ours is stale and must not be forced.
            if (type == KeyValueResponseType.NotSet)
                return false;

            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication
                && ++retries < MaxRetries)
            {
                await Task.Delay(retries * 10, ct).ConfigureAwait(false);
                continue;
            }

            return false;
        }

        return false;
    }

    /// <summary>
    /// Whether <paramref name="next"/> is a legal forward step from <paramref name="previous"/>: the
    /// resume point may only advance in ordinal row-id order, and a span that has been marked done may
    /// never be un-marked.
    /// </summary>
    private static bool IsForwardStep(TtlSpanCheckpoint? previous, TtlSpanCheckpoint next)
    {
        if (previous is null)
            return true;

        if (previous.Done && !next.Done)
            return false;

        if (previous.LastRowIdHex.Length == 0)
            return true;

        if (next.LastRowIdHex.Length == 0)
            return false;

        return string.CompareOrdinal(next.LastRowIdHex, previous.LastRowIdHex) >= 0;
    }

    // ── Span bounds ───────────────────────────────────────────────────────────

    /// <summary>
    /// Computes the half-open row-id range <c>[start, end)</c> of a span, dividing the 24-hex row-id
    /// space into <paramref name="spanCount"/> equal slices. Span 0 has no lower bound and the last span
    /// no upper bound, so the spans together cover the whole keyspace with no gap and no overlap.
    ///
    /// <para><b>The start is inclusive and the end exclusive, and that asymmetry is the whole point.</b>
    /// Adjacent spans share a boundary value: it is span N's <c>end</c> and span N+1's <c>start</c>. If
    /// both were exclusive — as an <c>(after, until)</c> pair reads — a row sitting exactly on that
    /// boundary would be excluded by the span below it <em>and</em> by the span above it, and no worker
    /// would ever visit it. It would simply never expire, silently and forever.</para>
    ///
    /// <para>Resume-after-checkpoint is a <em>separate</em>, exclusive bound. It describes progress
    /// within a span, not the span's extent, and overloading the inclusive start with it is what
    /// reintroduces the same off-by-one.</para>
    ///
    /// <para>The division is uniform over the whole 32-bit ObjectId timestamp word, which is
    /// <b>not</b> balanced in practice: that word is Unix seconds, so 64 spans are roughly two years
    /// wide each and an active table lands almost entirely in one of them. Better split points are
    /// tracked separately; correctness here does not depend on the distribution, only throughput does.</para>
    /// </summary>
    internal static (ObjectIdValue? start, ObjectIdValue? end) SpanBounds(int spanIndex, int spanCount)
    {
        ObjectIdValue? start = spanIndex == 0 ? null : BoundaryAt(spanIndex, spanCount);
        ObjectIdValue? end = spanIndex == spanCount - 1 ? null : BoundaryAt(spanIndex + 1, spanCount);
        return (start, end);
    }

    // The boundary is derived from the top 4 bytes of the row id (its high-order timestamp word), which
    // is what determines ordinal hex order first. Lower words are zero, so a boundary is the smallest id
    // in its slice — which is exactly why it must be INCLUSIVE as a start: it is a row id a real row can
    // hold, and the span above it is the only span that can claim it.
    private static ObjectIdValue BoundaryAt(int index, int spanCount)
    {
        // Scale over the unsigned 32-bit range without overflowing: (index / spanCount) * 2^32.
        ulong scaled = (ulong)index * 0x1_0000_0000UL / (ulong)spanCount;
        return new ObjectIdValue(unchecked((int)(uint)scaled), 0, 0);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task TryDeleteAsync(string key, CancellationToken ct)
    {
        int retries = 0;
        while (retries < MaxRetries)
        {
            try
            {
                (KeyValueResponseType type, _, _) = await kahuna.LocateAndTryDeleteKeyValue(
                    HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct
                ).ConfigureAwait(false);

                if (type is not (KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication))
                    return;
            }
            catch
            {
                return; // best-effort cleanup; a leftover record is re-evaluated on the next sweep
            }

            await Task.Delay(++retries * 10, ct).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync() => spanLease.DisposeAsync();
}
