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
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Tests.Storage;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// What a stale registry cache hit <em>costs</em> on the foreground resolve path.
///
/// <para>The generation stamp that keeps nodes coherent is deliberately coarse: any registration,
/// drop, or rename anywhere invalidates every cached name on every other node. That is fine for
/// correctness and was fine at a handful of databases — but it decides what happens next, and
/// rebuilding the whole cache in response would put a scan whose cost grows with the number of
/// registered databases directly underneath a user's statement, every time any DDL moved the
/// generation. These tests pin the resolve path to a single point read, and pin the full rebuild to
/// the background sweep where its cost is nobody's latency.</para>
///
/// <para>Coherence itself is covered by <c>TestRegistryCoherence</c>; nothing here may come at its
/// expense, which is why each case also asserts the resolved value.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestRegistryRevalidationCost : BaseTest
{
    /// <summary>
    /// Counts the registry-bucket operations a registry issues, so a test can distinguish "re-read one
    /// key" from "rescanned the bucket" — which the resolved value alone cannot show.
    /// </summary>
    private sealed class CountingKahuna : DelegatingKahuna
    {
        private readonly string registryBucket;

        public CountingKahuna(IKahuna inner, string registryBucket) : base(inner)
            => this.registryBucket = registryBucket;

        public int RegistryScans;
        public int RegistryPointReads;

        public void Reset()
        {
            Volatile.Write(ref RegistryScans, 0);
            Volatile.Write(ref RegistryPointReads, 0);
        }

        public override IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(
            HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey,
            bool endInclusive, int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability,
            CancellationToken ct, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (string.Equals(prefix, registryBucket, StringComparison.Ordinal))
                Interlocked.Increment(ref RegistryScans);

            return base.LocateAndScanRange(
                txId, prefix, startKey, startInclusive, endKey, endInclusive, pageSize,
                readTimestamp, durability, ct, coordinatorKey, operationId);
        }

        public override Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
            HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp,
            KeyValueDurability durability, CancellationToken cancellationToken,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (key.StartsWith(registryBucket + "/db:", StringComparison.Ordinal))
                Interlocked.Increment(ref RegistryPointReads);

            return base.LocateAndTryGetValue(
                transactionId, key, revision, readTimestamp, durability, cancellationToken,
                coordinatorKey, operationId);
        }
    }

    private const string RegistryBucket = "_system/dbregistry";

    /// <summary>
    /// Two registries over one shared node stand in for two cluster nodes: independent in-memory caches,
    /// one replicated store. The observed one is wrapped so its KV traffic can be counted.
    /// </summary>
    private async Task<(DatabaseRegistry mutator, DatabaseRegistry observer, CountingKahuna counter)> TwoNodesAsync()
    {
        DatabaseRegistry mutator = await DatabaseRegistry.OpenAsync(TestNode!, Options, isClusterMode: true);

        CountingKahuna counter = new(TestNode!.Kahuna, RegistryBucket);
        DatabaseRegistry observer = await DatabaseRegistry.OpenForTestingAsync(
            TestNode!, counter, Options, isClusterMode: true);

        return (mutator, observer, counter);
    }

    private static string NewName() => "rev_" + Guid.NewGuid().ToString("n");

    /// <summary>
    /// A resolve whose cached hit has been invalidated re-reads exactly the one name it was asked
    /// about, and does not rescan the registry bucket.
    /// </summary>
    [Test]
    public async Task AStaleHitReReadsOneKeyRatherThanScanningTheBucket()
    {
        (DatabaseRegistry mutator, DatabaseRegistry observer, CountingKahuna counter) = await TwoNodesAsync();
        try
        {
            string target = NewName();
            string targetId = await mutator.AllocateIdAsync();
            await mutator.RegisterAsync(target, targetId);

            // Several other databases, so a rebuild would be visibly more work than a point read.
            for (int i = 0; i < 5; i++)
                await mutator.RegisterAsync(NewName(), await mutator.AllocateIdAsync());

            // Warm the observer's cache so the next resolve is a hit.
            Assert.AreEqual(targetId, (await observer.TryResolveEntryAsync(target))!.Id);

            // A mutation elsewhere invalidates every cached name on the observer.
            await mutator.RegisterAsync(NewName(), await mutator.AllocateIdAsync());

            counter.Reset();
            DatabaseRegistryEntry? resolved = await observer.TryResolveEntryAsync(target);

            Assert.AreEqual(targetId, resolved!.Id, "the stale hit must still resolve correctly");
            Assert.AreEqual(
                0, Volatile.Read(ref counter.RegistryScans),
                "a foreground resolve must never rescan the registry bucket");
            Assert.AreEqual(
                1, Volatile.Read(ref counter.RegistryPointReads),
                "it must re-read exactly the one name it was asked about");
        }
        finally
        {
            await mutator.DisposeAsync();
            await observer.DisposeAsync();
        }
    }

    /// <summary>
    /// The cost of a stale-hit resolve does not grow with the registry. Same assertion as above, with an
    /// order of magnitude more databases registered — if the rebuild ever came back, this is where it
    /// would show up as the registry grows.
    /// </summary>
    [Test]
    public async Task StaleHitCostDoesNotGrowWithTheNumberOfDatabases()
    {
        (DatabaseRegistry mutator, DatabaseRegistry observer, CountingKahuna counter) = await TwoNodesAsync();
        try
        {
            string target = NewName();
            string targetId = await mutator.AllocateIdAsync();
            await mutator.RegisterAsync(target, targetId);

            for (int i = 0; i < 40; i++)
                await mutator.RegisterAsync(NewName(), await mutator.AllocateIdAsync());

            Assert.AreEqual(targetId, (await observer.TryResolveEntryAsync(target))!.Id);

            await mutator.RegisterAsync(NewName(), await mutator.AllocateIdAsync());

            counter.Reset();
            Assert.AreEqual(targetId, (await observer.TryResolveEntryAsync(target))!.Id);

            Assert.AreEqual(0, Volatile.Read(ref counter.RegistryScans));
            Assert.AreEqual(
                1, Volatile.Read(ref counter.RegistryPointReads),
                "one point read regardless of how many databases are registered");
        }
        finally
        {
            await mutator.DisposeAsync();
            await observer.DisposeAsync();
        }
    }

    /// <summary>
    /// Coherence still holds through the cheaper path: a name dropped on another node resolves to
    /// nothing, and the eviction costs one point read rather than a rebuild.
    /// </summary>
    [Test]
    public async Task ADroppedNameIsEvictedByTheTargetedReadWithoutAScan()
    {
        (DatabaseRegistry mutator, DatabaseRegistry observer, CountingKahuna counter) = await TwoNodesAsync();
        try
        {
            string name = NewName();
            string id = await mutator.AllocateIdAsync();
            await mutator.RegisterAsync(name, id);

            Assert.AreEqual(id, (await observer.TryResolveEntryAsync(name))!.Id, "warm the observer's cache");

            await mutator.UnregisterAsync(name);

            counter.Reset();

            Assert.IsNull(
                await observer.TryResolveEntryAsync(name),
                "a name dropped on another node must stop resolving");
            Assert.AreEqual(
                0, Volatile.Read(ref counter.RegistryScans),
                "evicting a dropped name must not require a bucket scan");
        }
        finally
        {
            await mutator.DisposeAsync();
            await observer.DisposeAsync();
        }
    }

    /// <summary>
    /// The background sweep is what brings a node's whole cache back to the current generation, so a
    /// node that only reads stops paying per-resolve revalidation.
    ///
    /// <para>This is the other half of the trade. A targeted re-read deliberately does not claim the
    /// cache is current — it checked one key — so without something that does, every resolve on a
    /// read-only node would re-read forever after the first mutation anywhere in the cluster.</para>
    /// </summary>
    [Test]
    public async Task TheBackgroundSweepRestoresFreeCacheHits()
    {
        (DatabaseRegistry mutator, DatabaseRegistry observer, CountingKahuna counter) = await TwoNodesAsync();
        try
        {
            string name = NewName();
            string id = await mutator.AllocateIdAsync();
            await mutator.RegisterAsync(name, id);

            Assert.AreEqual(id, (await observer.TryResolveEntryAsync(name))!.Id, "warm the observer's cache");

            // A mutation elsewhere leaves the observer's cache stale.
            await mutator.RegisterAsync(NewName(), await mutator.AllocateIdAsync());

            counter.Reset();
            Assert.AreEqual(id, (await observer.TryResolveEntryAsync(name))!.Id);
            Assert.AreEqual(
                1, Volatile.Read(ref counter.RegistryPointReads),
                "precondition: while the cache is stale, resolving re-reads the name");

            // The background sweep rebuilds and adopts the generation.
            await observer.GetBackgroundSnapshotAsync();

            counter.Reset();
            Assert.AreEqual(id, (await observer.TryResolveEntryAsync(name))!.Id);

            Assert.AreEqual(
                0, Volatile.Read(ref counter.RegistryPointReads),
                "after the sweep has brought the cache current, a hit must cost no key read at all");
            Assert.AreEqual(0, Volatile.Read(ref counter.RegistryScans));
        }
        finally
        {
            await mutator.DisposeAsync();
            await observer.DisposeAsync();
        }
    }

    /// <summary>
    /// The registry scan's cache backfill must key entries the way lookups ask for them.
    ///
    /// <para>Database names are case-insensitive but case-preserving: the entry keeps the case it was
    /// created with, while every lookup normalizes first. A backfill written under the raw name
    /// therefore lands under a key nothing will ever ask for — the cache looks populated and is never
    /// once hit, which costs a KV read per resolve and is invisible in every result.</para>
    ///
    /// <para>Deliberately drives <c>ScanAllEntriesAsync</c> directly rather than through the background
    /// snapshot, and observes a standalone registry: the snapshot's reconcile writes normalized keys of
    /// its own, and a cluster-mode resolve re-reads the key anyway, so either would mask exactly the
    /// defect under test.</para>
    /// </summary>
    [Test]
    public async Task TheScanBackfillsUnderTheKeyLookupsActuallyUse()
    {
        DatabaseRegistry mutator = await DatabaseRegistry.OpenAsync(TestNode!, Options, isClusterMode: true);

        CountingKahuna counter = new(TestNode!.Kahuna, RegistryBucket);
        DatabaseRegistry observer = await DatabaseRegistry.OpenForTestingAsync(
            TestNode!, counter, Options, isClusterMode: false);

        try
        {
            string name = "Rev_MixedCase_" + Guid.NewGuid().ToString("n");
            string id = await mutator.AllocateIdAsync();
            await mutator.RegisterAsync(name, id);

            // The observer learns about it only through the scan's backfill.
            await observer.ScanAllEntriesAsync();

            counter.Reset();
            DatabaseRegistryEntry? resolved = await observer.TryResolveEntryAsync(name);

            Assert.IsNotNull(resolved, "the backfilled entry must be reachable by name");
            Assert.AreEqual(id, resolved!.Id);
            Assert.AreEqual(
                0, Volatile.Read(ref counter.RegistryPointReads),
                "a mixed-case name must be served from the backfilled cache, not re-read from KV");
        }
        finally
        {
            await mutator.DisposeAsync();
            await observer.DisposeAsync();
        }
    }
}
