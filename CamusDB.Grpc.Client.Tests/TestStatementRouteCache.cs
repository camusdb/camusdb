
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using CamusDB.Grpc.Client.Routing;

namespace CamusDB.Grpc.Client.Tests;

/// <summary>
/// Unit tests for the learned-route cache: TTL expiry against a caller-supplied monotonic clock,
/// revision-guarded conditional replacement (late responses and late clears must lose), and the
/// entry/byte bounds that keep high-cardinality SQL from retaining unbounded memory.
/// </summary>
[TestFixture]
public sealed class TestStatementRouteCache
{
    private static StatementRouteKey Key(string sql, RouteOpKind kind = RouteOpKind.Query)
        => new("db1", sql, kind);

    [Test]
    public void LearnThenGet_ReturnsNode_AndExpires()
    {
        StatementRouteCache cache = new(maxEntries: 16, maxBytes: 1 << 20);
        StatementRouteKey key = Key("select 1");

        cache.Learn(key, "node-b", "tok", expiresAt: 1_000, observedRevision: 0, now: 0);

        Assert.That(cache.TryGet(key, now: 999, out long revision), Is.EqualTo("node-b"));
        Assert.That(revision, Is.EqualTo(1));

        // At the deadline the entry is expired and dropped.
        Assert.That(cache.TryGet(key, now: 1_000, out _), Is.Null);
        Assert.That(cache.Count, Is.EqualTo(0));
    }

    [Test]
    public void QueryAndNonQuery_AreDistinctIdentities()
    {
        StatementRouteCache cache = new(16, 1 << 20);
        cache.Learn(Key("update t set a=1"), "node-a", null, 1_000, 0, 0);

        Assert.That(cache.TryGet(Key("update t set a=1", RouteOpKind.NonQuery), 1, out _), Is.Null);
        Assert.That(cache.TryGet(Key("update t set a=1"), 1, out _), Is.EqualTo("node-a"));
    }

    [Test]
    public void LateLearn_WithStaleRevision_IsDiscarded()
    {
        StatementRouteCache cache = new(16, 1 << 20);
        StatementRouteKey key = Key("select 1");

        // A fast response learned node-b (revision becomes 1).
        cache.Learn(key, "node-b", null, 10_000, observedRevision: 0, now: 0);

        // A slower response that dispatched before that learn (it observed revision 0) must not win.
        cache.Learn(key, "node-a", null, 10_000, observedRevision: 0, now: 1);

        Assert.That(cache.TryGet(key, 2, out _), Is.EqualTo("node-b"));
    }

    [Test]
    public void Refresh_WithCurrentRevision_Wins()
    {
        StatementRouteCache cache = new(16, 1 << 20);
        StatementRouteKey key = Key("select 1");

        cache.Learn(key, "node-b", null, 10_000, observedRevision: 0, now: 0);
        Assert.That(cache.TryGet(key, 1, out long observed), Is.EqualTo("node-b"));

        cache.Learn(key, "node-c", null, 10_000, observedRevision: observed, now: 2);
        Assert.That(cache.TryGet(key, 3, out _), Is.EqualTo("node-c"));
    }

    [Test]
    public void LateClear_WithStaleRevision_IsDiscarded()
    {
        StatementRouteCache cache = new(16, 1 << 20);
        StatementRouteKey key = Key("select 1");

        cache.Learn(key, "node-b", null, 10_000, observedRevision: 0, now: 0);

        // A clear whose request observed the pre-learn state (revision 0) must not delete the newer route.
        cache.Clear(key, observedRevision: 0);
        Assert.That(cache.TryGet(key, 1, out long current), Is.EqualTo("node-b"));

        // A clear that observed the current revision removes it.
        cache.Clear(key, observedRevision: current);
        Assert.That(cache.TryGet(key, 2, out _), Is.Null);
    }

    [Test]
    public void EntryCap_EvictsLeastRecentlyTouched()
    {
        StatementRouteCache cache = new(maxEntries: 2, maxBytes: 1 << 20);

        cache.Learn(Key("s1"), "node-a", null, 100_000, 0, now: 0);
        cache.Learn(Key("s2"), "node-a", null, 100_000, 0, now: 1);

        // Touch s1 so s2 is the least recently used.
        Assert.That(cache.TryGet(Key("s1"), 5, out _), Is.EqualTo("node-a"));

        cache.Learn(Key("s3"), "node-a", null, 100_000, 0, now: 6);

        Assert.That(cache.Count, Is.EqualTo(2));
        Assert.That(cache.TryGet(Key("s2"), 7, out _), Is.Null);
        Assert.That(cache.TryGet(Key("s1"), 7, out _), Is.EqualTo("node-a"));
        Assert.That(cache.TryGet(Key("s3"), 7, out _), Is.EqualTo("node-a"));
    }

    [Test]
    public void ByteBudget_Bounds_RetainedMemory()
    {
        // Each entry costs well over 64 bytes, so a tiny budget forces eviction down to one entry.
        StatementRouteCache cache = new(maxEntries: 1_000, maxBytes: 200);

        cache.Learn(Key("select * from t where a = @a"), "node-a", "tok", 100_000, 0, 0);
        cache.Learn(Key("select * from t where b = @b"), "node-a", "tok", 100_000, 0, 1);
        cache.Learn(Key("select * from t where c = @c"), "node-a", "tok", 100_000, 0, 2);

        Assert.That(cache.RetainedBytes, Is.LessThanOrEqualTo(200));
        Assert.That(cache.Count, Is.LessThan(3));
    }

    [Test]
    public void ExpiredEntries_AreEvictedBeforeLiveOnes()
    {
        StatementRouteCache cache = new(maxEntries: 2, maxBytes: 1 << 20);

        cache.Learn(Key("s1"), "node-a", null, expiresAt: 10, observedRevision: 0, now: 0);
        cache.Learn(Key("s2"), "node-a", null, expiresAt: 100_000, observedRevision: 0, now: 20);

        // s1 is expired by now=20; inserting s3 at the cap drops it and keeps both live routes.
        cache.Learn(Key("s3"), "node-a", null, expiresAt: 100_000, observedRevision: 0, now: 21);

        Assert.That(cache.TryGet(Key("s2"), 22, out _), Is.EqualTo("node-a"));
        Assert.That(cache.TryGet(Key("s3"), 22, out _), Is.EqualTo("node-a"));
    }
}
