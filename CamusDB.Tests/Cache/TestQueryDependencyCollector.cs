
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.Cache;

namespace CamusDB.Tests.Cache;

/// <summary>
/// Unit tests for <see cref="QueryDependencyCollector"/> and <see cref="DependencyIndex"/>.
///
/// Covers the accumulator contract: correct recording, deduplication, cap enforcement,
/// <see cref="QueryDependencySet"/> construction, and the empty-set cleanup invariant on
/// <see cref="DependencyIndex.Remove"/>.
///
/// <para><b>Deferral note:</b> acceptance criteria that require an end-to-end wire-up
/// ("update a projected non-indexed column invalidates the entry", "phantom insert into an
/// index range invalidates", "residual-filter row flip invalidates") are not covered here.
/// <see cref="QueryPlan.DepCollector"/> is never assigned on the production path until the
/// cached-read path is installed, so all <c>deps?.Record*</c> call sites in the scanner and
/// join executor are currently no-ops. The cached-read implementation must include real
/// end-to-end assertions for all three scenarios — not just structural checks.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestQueryDependencyCollector
{
    // ─────────────────────────────────────────────────────────────────────────
    // Basic record and build
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void EmptyCollector_BuildsEmptySet()
    {
        var col = new QueryDependencyCollector(CamusDBOptions.Default);
        Assert.That(col.CapExceeded, Is.False);

        QueryDependencySet deps = col.Build();
        Assert.That(deps.RangeDeps.Count, Is.EqualTo(0));
        Assert.That(deps.PointDeps.Count, Is.EqualTo(0));
        Assert.That(deps.SchemaDeps.Count, Is.EqualTo(0));
    }

    [Test]
    public void RecordRange_AppearsInBuild()
    {
        var col = new QueryDependencyCollector(CamusDBOptions.Default);
        col.RecordRange("db1:tbl1:r");
        col.RecordRange("db1:tbl1:i:idx-abc");

        QueryDependencySet deps = col.Build();
        Assert.That(deps.RangeDeps, Contains.Item("db1:tbl1:r"));
        Assert.That(deps.RangeDeps, Contains.Item("db1:tbl1:i:idx-abc"));
    }

    [Test]
    public void RecordRange_DedupesIdentical()
    {
        var col = new QueryDependencyCollector(CamusDBOptions.Default);
        col.RecordRange("db1:tbl1:r");
        col.RecordRange("db1:tbl1:r");

        QueryDependencySet deps = col.Build();
        Assert.That(deps.RangeDeps.Count, Is.EqualTo(1));
    }

    [Test]
    public void RecordPoint_AppearsInBuild()
    {
        var col = new QueryDependencyCollector(CamusDBOptions.Default);
        col.RecordRange("db1:tbl1:r");
        col.RecordPoint("db1:tbl1:r/000000000000000000000001");
        col.RecordPoint("db1:tbl1:r/000000000000000000000002");

        QueryDependencySet deps = col.Build();
        Assert.That(deps.PointDeps.Count, Is.EqualTo(2));
        Assert.That(deps.PointDeps, Contains.Item("db1:tbl1:r/000000000000000000000001"));
    }

    [Test]
    public void RecordSchema_AppearsInBuild()
    {
        var col = new QueryDependencyCollector(CamusDBOptions.Default);
        col.RecordSchema("tbl-id-1", 3);

        QueryDependencySet deps = col.Build();
        Assert.That(deps.SchemaDeps.Count, Is.EqualTo(1));
        Assert.That(deps.SchemaDeps[0].TableId, Is.EqualTo("tbl-id-1"));
        Assert.That(deps.SchemaDeps[0].SchemaVersion, Is.EqualTo(3));
    }

    [Test]
    public void RecordSchema_DedupesForSameTableId()
    {
        var col = new QueryDependencyCollector(CamusDBOptions.Default);
        col.RecordSchema("tbl-id-1", 3);
        col.RecordSchema("tbl-id-1", 3);  // duplicate
        col.RecordSchema("tbl-id-2", 1);

        QueryDependencySet deps = col.Build();
        Assert.That(deps.SchemaDeps.Count, Is.EqualTo(2));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Total dep cap
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void TotalDepCap_SetsCapExceeded()
    {
        // The collector fixes its caps when constructed, so the cap under test is stated as options
        // rather than assigned to a global and restored afterwards.
        var col = new QueryDependencyCollector(CamusDBOptions.Default with { QueryResultCacheMaxDeps = 3 });
        {
            col.RecordRange("r1");
            col.RecordRange("r2");
            col.RecordRange("r3");
            col.RecordRange("r4");  // exceeds cap of 3

            Assert.That(col.CapExceeded, Is.True);
        }
    }

    [Test]
    public void WhenCapExceeded_BuildReturnsEmpty()
    {
        // The collector fixes its caps when constructed, so the cap under test is stated as options
        // rather than assigned to a global and restored afterwards.
        var col = new QueryDependencyCollector(CamusDBOptions.Default with { QueryResultCacheMaxDeps = 1 });
        {
            col.RecordRange("r1");
            col.RecordRange("r2");

            Assert.That(col.CapExceeded, Is.True);

            QueryDependencySet deps = col.Build();
            Assert.That(deps.RangeDeps.Count, Is.EqualTo(0),
                "Build() must return QueryDependencySet.Empty when total cap is exceeded");
        }
    }

    [Test]
    public void AfterCapExceeded_FurtherRecordCallsAreNoOps()
    {
        // The collector fixes its caps when constructed, so the cap under test is stated as options
        // rather than assigned to a global and restored afterwards.
        var col = new QueryDependencyCollector(CamusDBOptions.Default with { QueryResultCacheMaxDeps = 1 });
        {
            col.RecordRange("r1");
            col.RecordRange("r2");  // triggers cap

            Assert.DoesNotThrow(() => col.RecordRange("r3"));
            Assert.DoesNotThrow(() => col.RecordPoint("p1"));
            Assert.DoesNotThrow(() => col.RecordSchema("t1", 1));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Point dep cap — silently drops excess; does NOT set CapExceeded
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void PointDepCap_SilentlyDropsExcess_RangeDepStillPresent()
    {
        // The collector fixes its caps when constructed, so the cap under test is stated as options
        // rather than assigned to a global and restored afterwards.
        var col = new QueryDependencyCollector(CamusDBOptions.Default with { QueryResultCacheMaxPointDeps = 2 });
        {
            col.RecordRange("db:tbl:r");
            col.RecordPoint("db:tbl:r/aaaa");
            col.RecordPoint("db:tbl:r/bbbb");
            col.RecordPoint("db:tbl:r/cccc");  // silently dropped

            Assert.That(col.CapExceeded, Is.False,
                "Exceeding the point-dep cap alone must not set CapExceeded");

            QueryDependencySet deps = col.Build();
            Assert.That(deps.PointDeps.Count, Is.EqualTo(2),
                "Excess point deps are dropped; the range dep still covers the bucket");
            Assert.That(deps.RangeDeps, Contains.Item("db:tbl:r"),
                "Range dep must survive point-dep cap overflow");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Range dep cap — overflow fails closed (CapExceeded), not truncated
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void RangeDepCap_SetsCapExceeded()
    {
        // The collector fixes its caps when constructed, so the cap under test is stated as options
        // rather than assigned to a global and restored afterwards.
        var col = new QueryDependencyCollector(CamusDBOptions.Default with { QueryResultCacheMaxRanges = 2 });
        {
            col.RecordRange("db:tbl1:r");
            col.RecordRange("db:tbl2:r");
            col.RecordRange("db:tbl3:r");  // exceeds cap

            Assert.That(col.CapExceeded, Is.True,
                "Exceeding the range-dep cap must set CapExceeded — a truncated range set is not safe to publish");
        }
    }

    [Test]
    public void RangeDepCap_BuildReturnsEmpty()
    {
        // The collector fixes its caps when constructed, so the cap under test is stated as options
        // rather than assigned to a global and restored afterwards.
        var col = new QueryDependencyCollector(CamusDBOptions.Default with { QueryResultCacheMaxRanges = 1 });
        {
            col.RecordRange("db:tbl1:r");
            col.RecordRange("db:tbl2:r");  // triggers cap

            QueryDependencySet deps = col.Build();
            Assert.That(deps.RangeDeps.Count, Is.EqualTo(0),
                "Build() must return QueryDependencySet.Empty when the range cap is exceeded");
        }
    }

    [Test]
    public void RangeDepCap_DuplicatesDoNotCountTowardCap()
    {
        // The collector fixes its caps when constructed, so the cap under test is stated as options
        // rather than assigned to a global and restored afterwards.
        var col = new QueryDependencyCollector(CamusDBOptions.Default with { QueryResultCacheMaxRanges = 1 });
        {
            col.RecordRange("db:tbl1:r");
            col.RecordRange("db:tbl1:r");  // duplicate — must not count
            col.RecordRange("db:tbl1:r");  // duplicate — must not count

            // Only one distinct bucket was recorded; cap is 1 so it must not be exceeded.
            Assert.That(col.CapExceeded, Is.False,
                "Duplicate range buckets must not count toward the range-dep cap");

            QueryDependencySet deps = col.Build();
            Assert.That(deps.RangeDeps.Count, Is.EqualTo(1));
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Round-trip through QueryDependencySet
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void Build_RoundTrip_AllThreeKinds()
    {
        var col = new QueryDependencyCollector(CamusDBOptions.Default);
        col.RecordRange("db:tbl:r");
        col.RecordRange("db:tbl:i:idx1");
        col.RecordPoint("db:tbl:r/000000000000000000000001");
        col.RecordSchema("tbl-uuid", 5);

        QueryDependencySet deps = col.Build();

        Assert.That(deps.RangeDeps.Count, Is.EqualTo(2));
        Assert.That(deps.PointDeps.Count, Is.EqualTo(1));
        Assert.That(deps.SchemaDeps.Count, Is.EqualTo(1));
        Assert.That(deps.SchemaDeps[0], Is.EqualTo(("tbl-uuid", 5)));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DependencyIndex.Remove — empty-set leak prevention
    //
    // Point keys are per-row (one per fetched row × per cached entry). After an entry
    // is evicted, Remove must delete the dictionary key when its set empties — not just
    // clear the set — otherwise a long-running process accumulates one empty HashSet per
    // row ever cached.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public void DependencyIndex_Remove_DeletesEmptyPointEntry()
    {
        var index = new DependencyIndex();

        var deps = new QueryDependencySet(
            ["db:tbl:r"],
            ["db:tbl:r/000000000000000000000001"],
            []);

        index.Add("entry-1", deps, "db");

        // Verify the point key is findable before removal
        Assert.That(index.FindByPoint("db:tbl:r/000000000000000000000001").Any(), Is.True);

        index.Remove("entry-1", deps, "db");

        // After removal the key must no longer be findable (empty set deleted, not retained)
        Assert.That(index.FindByPoint("db:tbl:r/000000000000000000000001").Any(), Is.False,
            "Empty point-index bucket must be removed from the dictionary, not left as an empty set");
    }

    [Test]
    public void DependencyIndex_Remove_RetainsPointEntryWhenOtherEntriesStillReferenceIt()
    {
        var index = new DependencyIndex();

        string pointKey = "db:tbl:r/000000000000000000000001";
        var deps = new QueryDependencySet(["db:tbl:r"], [pointKey], []);

        index.Add("entry-1", deps, "db");
        index.Add("entry-2", deps, "db");

        index.Remove("entry-1", deps, "db");

        // entry-2 still depends on the same key — the bucket must remain
        Assert.That(index.FindByPoint(pointKey).Any(), Is.True,
            "Point-index bucket must survive while another entry still references it");
        Assert.That(index.FindByPoint(pointKey), Contains.Item("entry-2"));
    }

    [Test]
    public void DependencyIndex_Remove_DeletesEmptyRangeAndSchemaEntries()
    {
        var index = new DependencyIndex();

        var deps = new QueryDependencySet(
            ["db:tbl:r"],
            [],
            [("tbl-id", 1)]);

        index.Add("entry-1", deps, "db");
        index.Remove("entry-1", deps, "db");

        Assert.That(index.FindByKeyspace("db:tbl:r").Any(), Is.False,
            "Empty range bucket must be removed");
        Assert.That(index.FindByTableSchema("db", "tbl-id").Any(), Is.False,
            "Empty schema bucket must be removed");
    }
}
