/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor.Controllers.Queries;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Dependency-validation tests for <see cref="PlanCache"/>, driven straight through
/// <c>TryGet</c>/<c>Put</c>.
///
/// <para>
/// The validation compares one dependency directly and same-order lists positionally, falling back to
/// a dictionary only for a genuine reordering. These tests pin the outcome of each of those routes:
/// an unchanged set hits, a reordered set still hits, and a change to any single field of any
/// dependency — schema version, index-set generation, analyze generation, table identity — misses and
/// evicts the entry.
/// </para>
/// </summary>
public sealed class TestPlanCacheDependencyMatching
{
    private static PlanCacheDep Dep(string tableId, int schemaVersion = 1, long indexGen = 10, long analyzeGen = 20)
        => new(tableId, schemaVersion, indexGen, analyzeGen);

    private static PlanCacheEntry Entry(params PlanCacheDep[] deps)
        => new(deps, SingleTable: new SingleTableDecision("year_idx"), JoinAliasOrder: null);

    private static PlanCache Cache() => new(maxEntries: 8);

    [Test]
    public void UnchangedDeps_Hit()
    {
        PlanCache cache = Cache();
        cache.Put("db", "shape", Entry(Dep("t1")));

        Assert.IsTrue(cache.TryGet("db", "shape", [Dep("t1")], out PlanCacheEntry? entry));
        Assert.AreEqual("year_idx", entry!.SingleTable!.IndexName);
    }

    [Test]
    public void ReorderedDeps_StillHit()
    {
        PlanCache cache = Cache();
        cache.Put("db", "shape", Entry(Dep("t1"), Dep("t2", schemaVersion: 3), Dep("t3", indexGen: 99)));

        Assert.IsTrue(cache.TryGet("db", "shape",
            [Dep("t3", indexGen: 99), Dep("t1"), Dep("t2", schemaVersion: 3)], out _),
            "a reordering of identical dependencies must not evict a usable entry");
    }

    [Test]
    public void DuplicateTableDeps_Hit_InEitherOrder()
    {
        // A self-join contributes the same table twice; both entries carry the same descriptor.
        PlanCache cache = Cache();
        cache.Put("db", "shape", Entry(Dep("t1"), Dep("t1"), Dep("t2")));

        Assert.IsTrue(cache.TryGet("db", "shape", [Dep("t1"), Dep("t1"), Dep("t2")], out _));
        Assert.IsTrue(cache.TryGet("db", "shape", [Dep("t2"), Dep("t1"), Dep("t1")], out _));
    }

    [Test]
    public void ChangedSchemaVersion_MissesAndEvicts()
    {
        PlanCache cache = Cache();
        cache.Put("db", "shape", Entry(Dep("t1")));

        Assert.IsFalse(cache.TryGet("db", "shape", [Dep("t1", schemaVersion: 2)], out _));

        // The stale entry is dropped, so the original dependency set no longer hits either.
        Assert.IsFalse(cache.TryGet("db", "shape", [Dep("t1")], out _), "a stale entry must be evicted on the miss");
    }

    [Test]
    public void ChangedIndexSetGeneration_Misses()
    {
        PlanCache cache = Cache();
        cache.Put("db", "shape", Entry(Dep("t1")));

        Assert.IsFalse(cache.TryGet("db", "shape", [Dep("t1", indexGen: 11)], out _),
            "index DDL must invalidate a cached access-path decision");
    }

    [Test]
    public void ChangedAnalyzeGeneration_Misses()
    {
        PlanCache cache = Cache();
        cache.Put("db", "shape", Entry(Dep("t1")));

        Assert.IsFalse(cache.TryGet("db", "shape", [Dep("t1", analyzeGen: 21)], out _),
            "a statistics refresh must invalidate a cached access-path decision");
    }

    [Test]
    public void DifferentTableIdentity_Misses()
    {
        PlanCache cache = Cache();
        cache.Put("db", "shape", Entry(Dep("t1")));

        Assert.IsFalse(cache.TryGet("db", "shape", [Dep("t9")], out _));
    }

    [Test]
    public void ChangeInASingleDepOfAReorderedSet_Misses()
    {
        PlanCache cache = Cache();
        cache.Put("db", "shape", Entry(Dep("t1"), Dep("t2"), Dep("t3")));

        Assert.IsFalse(cache.TryGet("db", "shape",
            [Dep("t3"), Dep("t1"), Dep("t2", analyzeGen: 999)], out _),
            "the reordering fallback must still compare every field of every dependency");
    }

    [Test]
    public void DifferentDepCount_Misses()
    {
        PlanCache cache = Cache();
        cache.Put("db", "shape", Entry(Dep("t1"), Dep("t2")));

        Assert.IsFalse(cache.TryGet("db", "shape", [Dep("t1")], out _));
    }

    [Test]
    public void CrossDatabaseAndCrossShape_DoNotCollide()
    {
        PlanCache cache = Cache();
        cache.Put("db1", "shape", Entry(Dep("t1")));

        Assert.IsFalse(cache.TryGet("db2", "shape", [Dep("t1")], out _));
        Assert.IsFalse(cache.TryGet("db1", "other-shape", [Dep("t1")], out _));
        Assert.IsTrue(cache.TryGet("db1", "shape", [Dep("t1")], out _));
    }

    [Test]
    public async Task ConcurrentHitReplaceAndResize_StayConsistent()
    {
        PlanCache cache = Cache();

        for (int i = 0; i < 8; i++)
            cache.Put("db", "shape-" + i, Entry(Dep("t" + i)));

        List<Task> workers = [];

        for (int w = 0; w < 8; w++)
        {
            int worker = w;

            workers.Add(Task.Run(() =>
            {
                for (int i = 0; i < 500; i++)
                {
                    int slot = (worker + i) % 8;

                    // A lookup with matching deps must never return an entry from another shape.
                    if (cache.TryGet("db", "shape-" + slot, [Dep("t" + slot)], out PlanCacheEntry? entry))
                        Assert.AreEqual("year_idx", entry!.SingleTable!.IndexName);

                    cache.Put("db", "shape-" + slot, Entry(Dep("t" + slot)));

                    if (i % 97 == 0)
                        cache.SetMaxEntries(4 + (i % 5));
                }
            }));
        }

        await Task.WhenAll(workers);

        cache.SetMaxEntries(8);
        cache.Put("db", "shape-final", Entry(Dep("tf")));

        Assert.IsTrue(cache.TryGet("db", "shape-final", [Dep("tf")], out _));
    }
}
