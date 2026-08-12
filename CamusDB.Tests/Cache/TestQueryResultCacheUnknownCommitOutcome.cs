/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cache;

/// <summary>
/// Cache behavior when a commit's outcome is <b>unknown</b>.
///
/// A coordinator can apply a commit and still fail to report it — a leadership flip, a dropped
/// response, an expired handle. The write is then readable while the finalize call reports
/// <c>MustRetry</c> or <c>Errored</c>. Treating that as "nothing committed" would leave every
/// cached entry for the touched keyspace in place, so a query starting after the data became
/// visible could be served rows that predate it. These tests drive real commits through a wrapper
/// that hides the response after the write has landed, and assert the entries are evicted anyway.
///
/// The counter-case matters just as much: a <em>definite</em> abort must not evict, or every
/// conflict would needlessly cold-start the cache.
///
/// These run against <see cref="KvTransactionsManager"/> directly rather than through SQL because
/// the fault has to be injected at the <see cref="IKahuna"/> boundary, and the command executor
/// takes a concrete embedded node. The SQL-level counterpart — a real Optimistic / Read Committed
/// write evicting a hinted SELECT's entry — lives in
/// <c>TestQueryResultCacheOptimisticReadCommitted</c>.
/// </summary>
[TestFixture]
public sealed class TestQueryResultCacheUnknownCommitOutcome
{
    private const string DatabaseId = "testdb";

    // These tests are about what the cache does with an unknown outcome, not about how long a finalize
    // is retried, so they disable the retry budget: the finalize is attempted exactly once and a single
    // hidden response is the whole loop. An injection count tied to however many attempts a budget
    // happens to allow would stop exhausting the loop the moment that budget changed, and the tests
    // would pass by committing cleanly instead of by leaving the outcome unknown.
    private static readonly CamusDBOptions NoFinalizeRetries =
        CamusDBOptions.Default with { TransactionFinalizeRetryBudgetMs = 0 };

    private const int PersistentHiddenResponses = 1;

    /// <summary>
    /// Forwards the commit to the real node — so the write actually applies — and then replaces the
    /// response with <paramref name="hideAs"/>. This is the committed-but-lost response Kahuna's own
    /// fault seam models; a wrapper that returned a fault <em>before</em> delegating would leave the
    /// write uncommitted and could not reproduce the defect.
    /// </summary>
    private sealed class AppliedThenHiddenCommitKahuna(IKahuna inner, KeyValueResponseType hideAs, int hideCount)
        : CamusDB.Tests.Storage.DelegatingKahuna(inner)
    {
        private int hidesRemaining = hideCount;

        /// <summary>Outcomes the real node returned, before hiding. Proves the commit landed.</summary>
        public List<KeyValueResponseType> InnerOutcomes { get; } = [];

        public override async Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(
            TransactionHandle handle, CancellationToken cancellationToken)
        {
            (KeyValueResponseType type, string? anchor) =
                await inner.LocateAndCommitTransaction(handle, cancellationToken);

            lock (InnerOutcomes)
                InnerOutcomes.Add(type);

            if (Interlocked.Decrement(ref hidesRemaining) >= 0)
                return (hideAs, anchor);

            return (type, anchor);
        }
    }

    /// <summary>
    /// Returns a definite abort without ever asking the node to commit, so nothing can have landed.
    /// </summary>
    private sealed class AbortingCommitKahuna(IKahuna inner)
        : CamusDB.Tests.Storage.DelegatingKahuna(inner)
    {
        public override Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(
            TransactionHandle handle, CancellationToken cancellationToken)
            => Task.FromResult<(KeyValueResponseType, string?)>((KeyValueResponseType.Aborted, null));
    }

    private static async Task<EmbeddedKahuna> StartNodeAsync(string warmupKey)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{warmupKey}/warmup", CancellationToken.None);
        return node;
    }

    private static TableSchema SingleColumnSchema()
    {
        List<TableColumnSchema> cols = [new("v", "v", ColumnType.Integer64, false, null)];
        return new TableSchema
        {
            Id = "t", Name = "t", Version = 0,
            Columns = cols,
            SchemaHistory = [new TableSchemaHistory { Version = 0, Columns = cols }]
        };
    }

    private static byte[] EncodeRow(ObjectIdValue rowId, long value) =>
        RowEncoder.Encode(SingleColumnSchema(),
            new Dictionary<string, ColumnValue> { ["v"] = new(ColumnType.Integer64, value) }, rowId);

    /// <summary>
    /// Publishes an entry that depends on <paramref name="keyspace"/> as a range dep, which is what
    /// a scan of that table produces and what a committed row write must invalidate.
    /// </summary>
    private static async Task<string> PublishEntryAsync(QueryResultCache cache, string keyspace, string fingerprint)
    {
        CacheGenerationToken token = cache.PublishGate.SnapshotGenerations([keyspace]);
        CachedQueryResult result = new(
            CacheName: "c",
            DatabaseId: DatabaseId,
            Rows: [],
            ResultFingerprint: fingerprint,
            CachedAt: default(HLCTimestamp),
            Status: QueryCacheStatus.Miss);

        await cache.TryPublishAsync(result, token, new QueryDependencySet([keyspace], [], []));
        return fingerprint;
    }

    [Test]
    public async Task UnresolvedCommitOutcome_EvictsEntriesForTheWrittenKeyspace()
    {
        await using EmbeddedKahuna node = await StartNodeAsync("unknown-a");
        using QueryResultCache cache = new(CamusDBOptions.Default, sweepIntervalMs: -1);

        AppliedThenHiddenCommitKahuna hiddenCommit =
            new(node.Kahuna, KeyValueResponseType.MustRetry, PersistentHiddenResponses);

        KvTransactionsManager mgr = new(hiddenCommit, NoFinalizeRetries, cache: cache);
        KvTableStore store = new(node.Kahuna, CamusDBOptions.Default, DatabaseId, "unknown-a");

        string fp = await PublishEntryAsync(cache, store.RowKeySpace, "fp-unresolved");
        Assert.That(await cache.TryGetAsync(DatabaseId, "c", fp), Is.Not.Null,
            "The entry must be live before the write, or the assertion below proves nothing");

        ObjectIdValue rowId = new(300, 0, 0);
        byte[] data = EncodeRow(rowId, 42L);

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            locking: Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Optimistic);
        await store.InsertRow(tx, rowId, data);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionFinalizeUnresolved),
            "Hiding every commit response must surface the non-terminal unresolved-finalize error");

        Assert.That(hiddenCommit.InnerOutcomes, Does.Contain(KeyValueResponseType.Committed),
            "The real node must have committed at least once — otherwise the hidden-response " +
            "scenario was not reproduced and the eviction assertion is vacuous");

        ReadOnlyMemory<byte>? visible = await store.GetRow(KvTransaction.CreateReadOnly(), rowId);
        Assert.That(visible, Is.Not.Null,
            "The write landed despite the hidden response — this is exactly why the cache cannot " +
            "treat an unresolved finalize as a non-commit");

        Assert.That(await cache.TryGetAsync(DatabaseId, "c", fp), Is.Null,
            "An unresolved finalize must evict entries for the written keyspace; leaving them would " +
            "serve rows that predate a write already readable on this node");
    }

    [Test]
    public async Task UnresolvedCommitOutcome_FencesLaterPublishesUnderAPreWriteToken()
    {
        await using EmbeddedKahuna node = await StartNodeAsync("unknown-b");
        using QueryResultCache cache = new(CamusDBOptions.Default, sweepIntervalMs: -1);

        AppliedThenHiddenCommitKahuna hiddenCommit =
            new(node.Kahuna, KeyValueResponseType.MustRetry, PersistentHiddenResponses);

        KvTransactionsManager mgr = new(hiddenCommit, NoFinalizeRetries, cache: cache);
        KvTableStore store = new(node.Kahuna, CamusDBOptions.Default, DatabaseId, "unknown-b");

        // A query in flight since before the write holds a token minted at the old generation.
        CacheGenerationToken preWriteToken = cache.PublishGate.SnapshotGenerations([store.RowKeySpace]);

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            locking: Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Optimistic);
        await store.InsertRow(tx, new ObjectIdValue(301, 0, 0), EncodeRow(new ObjectIdValue(301, 0, 0), 7L));

        Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));

        CachedQueryResult racing = new(
            CacheName: "c", DatabaseId: DatabaseId, Rows: [],
            ResultFingerprint: "fp-racing", CachedAt: default(HLCTimestamp),
            Status: QueryCacheStatus.Miss);

        (QueryCacheStatus status, _) = await cache.TryPublishAsync(
            racing, preWriteToken, new QueryDependencySet([store.RowKeySpace], [], []));

        Assert.That(status, Is.EqualTo(QueryCacheStatus.EvictedBeforePublish),
            "The generation must advance on an unresolved finalize too, so a query that scanned " +
            "before the write cannot publish its now-stale rows afterwards");
    }

    [Test]
    public async Task ErroredCommitOutcome_EvictsEntriesForTheWrittenKeyspace()
    {
        await using EmbeddedKahuna node = await StartNodeAsync("unknown-c");
        using QueryResultCache cache = new(CamusDBOptions.Default, sweepIntervalMs: -1);

        // One hidden response is enough: Errored is terminal, so there is no retry loop.
        AppliedThenHiddenCommitKahuna hiddenCommit =
            new(node.Kahuna, KeyValueResponseType.Errored, hideCount: 1);

        KvTransactionsManager mgr = new(hiddenCommit, NoFinalizeRetries, cache: cache);
        KvTableStore store = new(node.Kahuna, CamusDBOptions.Default, DatabaseId, "unknown-c");

        string fp = await PublishEntryAsync(cache, store.RowKeySpace, "fp-errored");

        ObjectIdValue rowId = new(302, 0, 0);
        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            locking: Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Optimistic);
        await store.InsertRow(tx, rowId, EncodeRow(rowId, 13L));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionAlreadyCompleted),
            "An Errored finalize means the outcome is unavailable, surfaced as the non-retryable error");

        Assert.That(await cache.TryGetAsync(DatabaseId, "c", fp), Is.Null,
            "Errored is an unknown outcome, not a definite abort — the entry must be evicted");
    }

    [Test]
    public async Task DefiniteAbort_PreservesCachedEntries()
    {
        await using EmbeddedKahuna node = await StartNodeAsync("unknown-d");
        using QueryResultCache cache = new(CamusDBOptions.Default, sweepIntervalMs: -1);

        KvTransactionsManager mgr = new(new AbortingCommitKahuna(node.Kahuna), CamusDBOptions.Default, cache: cache);
        KvTableStore store = new(node.Kahuna, CamusDBOptions.Default, DatabaseId, "unknown-d");

        string fp = await PublishEntryAsync(cache, store.RowKeySpace, "fp-aborted");

        ObjectIdValue rowId = new(303, 0, 0);
        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            locking: Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Optimistic);
        await store.InsertRow(tx, rowId, EncodeRow(rowId, 5L));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionConflict),
            "A coordinator abort is a definite non-commit and surfaces as the retryable conflict error");

        Assert.That(await cache.TryGetAsync(DatabaseId, "c", fp), Is.Not.Null,
            "Nothing committed, so cached entries stay valid — evicting here would cold-start the " +
            "cache on every conflict");
    }

    [Test]
    public async Task ResolvingAnUnresolvedCommitLater_RepeatsTheEvictionHarmlessly()
    {
        await using EmbeddedKahuna node = await StartNodeAsync("unknown-e");
        using QueryResultCache cache = new(CamusDBOptions.Default, sweepIntervalMs: -1);

        // Hide only the first round of responses; the retry that follows resolves normally.
        AppliedThenHiddenCommitKahuna hiddenCommit =
            new(node.Kahuna, KeyValueResponseType.MustRetry, PersistentHiddenResponses);

        KvTransactionsManager mgr = new(hiddenCommit, NoFinalizeRetries, cache: cache);
        KvTableStore store = new(node.Kahuna, CamusDBOptions.Default, DatabaseId, "unknown-e");

        string fp = await PublishEntryAsync(cache, store.RowKeySpace, "fp-resolved-later");

        ObjectIdValue rowId = new(304, 0, 0);
        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            locking: Kahuna.Shared.KeyValue.KeyValueTransactionLocking.Optimistic);
        await store.InsertRow(tx, rowId, EncodeRow(rowId, 21L));

        Assert.ThrowsAsync<CamusDBException>(() => mgr.CommitAsync(tx));
        Assert.That(await cache.TryGetAsync(DatabaseId, "c", fp), Is.Null);

        // The caller retries the SAME handle, as the unresolved-finalize error instructs. The second
        // fence covers the same frozen key set: generations only move forward and the entries are
        // already gone, so repeating it changes nothing.
        await mgr.CommitAsync(tx);

        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Committed),
            "Retrying the same handle after an unresolved finalize must resolve it");
        Assert.That(await cache.TryGetAsync(DatabaseId, "c", fp), Is.Null,
            "The repeated fence must be a no-op, not a resurrection or a failure");
    }
}
