
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using Kommander.Time;

namespace CamusDB.Tests.Cache;

/// <summary>
/// Unit tests for SQL cache-hint parsing and binding.
///
/// These tests verify that <c>{cache=name}</c> round-trips through SQL parse,
/// <see cref="SelectQueryCreator"/>, and the resulting <see cref="SelectQuery"/>
/// and <see cref="CamusDB.Core.CommandsExecutor.Models.Tickets.QueryTicket"/> without
/// touching any database or storage layer.
/// </summary>
[TestFixture]
public class TestSqlCacheHint
{
    private static SelectQuery ParseSelect(string sql)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);
        return new SelectQueryCreator().CreateSelectQuery(ast);
    }

    // ---------------------------------------------------------------------
    // Queries without a hint remain on the uncached path
    // ---------------------------------------------------------------------

    [Test]
    public void NoHint_SelectQuery_CacheHintIsNull()
    {
        SelectQuery q = ParseSelect("SELECT * FROM orders");
        Assert.That(q.CacheHint, Is.Null, "A query without a hint must not carry a CacheHint");
    }

    [Test]
    public void NoHint_ForceIndex_CacheHintIsNull()
    {
        // FORCE_INDEX is an index hint, not a cache hint — CacheHint must remain null.
        SelectQuery q = ParseSelect("SELECT * FROM orders @{FORCE_INDEX=idx_created_at}");
        Assert.That(q.CacheHint, Is.Null);
        Assert.That((q.Source as TableSource)?.ForcedIndexName, Is.EqualTo("idx_created_at"));
    }

    // ---------------------------------------------------------------------
    // Basic {cache=name} round-trip
    // ---------------------------------------------------------------------

    [Test]
    public void BasicHint_ParsesAndBubblesToSelectQuery()
    {
        SelectQuery q = ParseSelect("SELECT id, total FROM orders {cache=recent_orders}");
        Assert.That(q.CacheHint, Is.Not.Null);
        Assert.That(q.CacheHint!.CacheName, Is.EqualTo("recent_orders"));
        Assert.That(q.CacheHint.TtlMs, Is.Null, "No ttl= specified");
        Assert.That(q.CacheHint.IsStrict, Is.False, "No strict flag");
    }

    [Test]
    public void BasicHint_CacheName_IsLowercased()
    {
        // Cache names are lowercased at parse time by the grammar's identifier production
        // (ToLowerInvariant), so comparisons are case-insensitive and locale-independent.
        SelectQuery q = ParseSelect("SELECT * FROM orders {cache=RecentOrders}");
        Assert.That(q.CacheHint!.CacheName, Is.EqualTo("recentorders"),
            "Cache names are lowercased at parse time");
    }

    [Test]
    public void BasicHint_TableSourceAlsoCarriesHint()
    {
        SelectQuery q = ParseSelect("SELECT * FROM orders {cache=order_list}");
        TableSource? ts = q.Source as TableSource;
        Assert.That(ts, Is.Not.Null);
        Assert.That(ts!.CacheHint, Is.Not.Null);
        Assert.That(ts.CacheHint!.CacheName, Is.EqualTo("order_list"));
    }

    [Test]
    public void BasicHint_WithAlias_ParsesCorrectly()
    {
        SelectQuery q = ParseSelect("SELECT o.id FROM orders o {cache=aliased_cache}");
        Assert.That(q.CacheHint!.CacheName, Is.EqualTo("aliased_cache"));
        TableSource? ts = q.Source as TableSource;
        Assert.That(ts!.Alias, Is.EqualTo("o"));
    }

    // ---------------------------------------------------------------------
    // Optional ttl= and strict options
    // ---------------------------------------------------------------------

    [Test]
    public void HintWithTtlSeconds_ParsesAsMs()
    {
        // {cache=name, ttl=30s} must convert to 30000 ms.
        SelectQuery q = ParseSelect("SELECT * FROM orders {cache=hot_orders, ttl=30s}");
        Assert.That(q.CacheHint!.TtlMs, Is.EqualTo(30_000));
        Assert.That(q.CacheHint.IsStrict, Is.False);
    }

    [Test]
    public void HintWithTtlMilliseconds_ParsesDirectly()
    {
        SelectQuery q = ParseSelect("SELECT * FROM orders {cache=hot_orders, ttl=3000ms}");
        Assert.That(q.CacheHint!.TtlMs, Is.EqualTo(3000));
    }

    [Test]
    public void HintWithTtlMinutes_ParsesAsMs()
    {
        SelectQuery q = ParseSelect("SELECT * FROM orders {cache=hot_orders, ttl=2m}");
        Assert.That(q.CacheHint!.TtlMs, Is.EqualTo(120_000));
    }

    [Test]
    public void HintWithTtlHours_ParsesAsMs()
    {
        SelectQuery q = ParseSelect("SELECT * FROM orders {cache=hot_orders, ttl=1h}");
        Assert.That(q.CacheHint!.TtlMs, Is.EqualTo(3_600_000));
    }

    [Test]
    public void HintWithTtlBareInteger_ParsesAsMs()
    {
        // Bare integer without unit suffix is treated as milliseconds.
        SelectQuery q = ParseSelect("SELECT * FROM orders {cache=hot_orders, ttl=5000}");
        Assert.That(q.CacheHint!.TtlMs, Is.EqualTo(5000));
    }

    [Test]
    public void HintWithStrict_ParsesStrictFlag()
    {
        SelectQuery q = ParseSelect("SELECT * FROM orders {cache=strict_orders, strict}");
        Assert.That(q.CacheHint!.IsStrict, Is.True);
        Assert.That(q.CacheHint.TtlMs, Is.Null);
    }

    [Test]
    public void HintWithTtlAndStrict_ParsesBothOptions()
    {
        SelectQuery q = ParseSelect("SELECT * FROM orders {cache=full_opts, ttl=5s, strict}");
        Assert.That(q.CacheHint!.CacheName, Is.EqualTo("full_opts"));
        Assert.That(q.CacheHint.TtlMs, Is.EqualTo(5000));
        Assert.That(q.CacheHint.IsStrict, Is.True);
    }

    [Test]
    public void HintWithStrictAndTtl_OptionOrderDoesNotMatter()
    {
        // strict before ttl
        SelectQuery q = ParseSelect("SELECT * FROM orders {cache=opts_reversed, strict, ttl=1s}");
        Assert.That(q.CacheHint!.TtlMs, Is.EqualTo(1000));
        Assert.That(q.CacheHint.IsStrict, Is.True);
    }

    // ---------------------------------------------------------------------
    // Invalid or duplicate hints produce deterministic errors
    // ---------------------------------------------------------------------

    [Test]
    public void UnknownHintKey_Throws()
    {
        // {flavor=vanilla} is not a recognised hint.
        var ex = Assert.Throws<CamusDBException>(() =>
            ParseSelect("SELECT * FROM orders {flavor=vanilla}"));
        Assert.That(ex!.Message, Does.Contain("flavor"),
            "Error should name the unknown hint key");
    }

    [Test]
    public void UnknownCacheHintOption_Throws()
    {
        var ex = Assert.Throws<CamusDBException>(() =>
            ParseSelect("SELECT * FROM orders {cache=c, freshness=high}"));
        Assert.That(ex!.Message, Does.Contain("freshness"),
            "Error should name the unknown option");
    }

    [Test]
    public void TtlZero_Throws()
    {
        var ex = Assert.Throws<CamusDBException>(() =>
            ParseSelect("SELECT * FROM orders {cache=c, ttl=0}"));
        Assert.That(ex!.Message, Does.Contain("ttl"),
            "Zero TTL must be rejected with a clear error");
    }

    [Test]
    public void TtlOverflow_Throws()
    {
        // 9999999999 > int.MaxValue — must be rejected, not silently dropped.
        var ex = Assert.Throws<CamusDBException>(() =>
            ParseSelect("SELECT * FROM orders {cache=c, ttl=9999999999}"));
        Assert.That(ex!.Message, Does.Contain("ttl"),
            "Out-of-range TTL must be rejected with a clear error");
    }

    [Test]
    public void TtlUnknownUnit_Throws()
    {
        var ex = Assert.Throws<CamusDBException>(() =>
            ParseSelect("SELECT * FROM orders {cache=c, ttl=30y}"));
        Assert.That(ex!.Message, Does.Contain("y"),
            "Unknown TTL unit must be named in the error");
    }

    [Test]
    public void TtlNonNumericValue_ThrowsHelpfulMessage()
    {
        // 'ttl' is a known option — a non-numeric value must report the value problem,
        // not "unknown option 'ttl'".
        var ex = Assert.Throws<CamusDBException>(() =>
            ParseSelect("SELECT * FROM orders {cache=c, ttl=soon}"));
        Assert.That(ex!.Message, Does.Contain("ttl").And.Contain("integer"),
            "A bad ttl value must name ttl and say it needs an integer, not call ttl unknown");
    }

    [Test]
    public void TtlUnitOverflow_Throws()
    {
        // A value that overflows int.MaxValue even before applying the unit multiplier
        // must be rejected, not silently wrapped by the internal long multiplication.
        var ex = Assert.Throws<CamusDBException>(() =>
            ParseSelect("SELECT * FROM orders {cache=c, ttl=99999999999h}"));
        Assert.That(ex!.Message, Does.Contain("ttl"),
            "An out-of-range unit TTL must be rejected with a clear error");
    }

    [Test]
    public void MultipleCacheHints_OnJoin_Throws()
    {
        // Two table sources in a join, each with a cache hint — must be rejected.
        var ex = Assert.Throws<CamusDBException>(() =>
            ParseSelect(
                "SELECT * FROM orders {cache=c1} INNER JOIN items {cache=c2} ON orders.id = items.order_id"));
        Assert.That(ex!.Message, Does.Contain("at most one"),
            "A SELECT with multiple cache hints must throw with a clear message");
    }

    // ---------------------------------------------------------------------
    // WHERE-clause subquery hint scoping
    //
    // Contract: hint collection only walks the FROM tree (TableSource /
    // JoinSource / DerivedTableSource). A {cache=...} inside a WHERE-clause
    // subquery (scalar, IN, EXISTS) lives on that subquery's own SelectQuery,
    // so it never reaches the outer query's hint extraction. The multi-hint
    // guard therefore never sees it, and the outer query parses cleanly with
    // only its own FROM hint. Only the top-level SELECT is wrapped for caching;
    // subquery hints are inert. Detection/rejection of inner hints can be added
    // later if inner-hint semantics are defined.
    // ---------------------------------------------------------------------

    [Test]
    public void InnerHint_InWhereSubquery_DoesNotConflictWithOuterHint()
    {
        // The outer SELECT has {cache=c1}; the IN-subquery has {cache=c2}.
        // Hint collection only walks the FROM tree, so it sees only c1.
        // The outer SelectQuery must parse without throwing and carry only c1.
        SelectQuery q = ParseSelect(
            "SELECT * FROM orders {cache=c1} WHERE id IN (SELECT id FROM items {cache=c2})");

        Assert.That(q.CacheHint, Is.Not.Null,
            "Outer FROM hint must be captured");
        Assert.That(q.CacheHint!.CacheName, Is.EqualTo("c1"),
            "Outer CacheHint must name the FROM-clause hint, not the subquery hint");
    }

    [Test]
    public void InnerHint_Only_InWhereSubquery_OuterHintIsNull()
    {
        // No hint on the outer FROM clause; hint only on the subquery.
        // The outer SelectQuery must have a null CacheHint — the subquery hint
        // does not bubble up and the outer query follows the uncached path.
        SelectQuery q = ParseSelect(
            "SELECT * FROM orders WHERE id IN (SELECT id FROM items {cache=inner_only})");

        Assert.That(q.CacheHint, Is.Null,
            "A hint confined to a WHERE subquery must not be treated as a query-level hint");
    }

    // ---------------------------------------------------------------------
    // Cache hint coexists with WHERE / ORDER BY / LIMIT
    // ---------------------------------------------------------------------

    [Test]
    public void HintWithWhereOrderByLimit_ParsesCompletely()
    {
        SelectQuery q = ParseSelect(
            "SELECT id, total FROM orders {cache=paged} WHERE status = 1 ORDER BY total DESC LIMIT 20");
        Assert.That(q.CacheHint!.CacheName, Is.EqualTo("paged"));
        Assert.That(q.Where, Is.Not.Null);
        Assert.That(q.OrderBy, Is.Not.Null);
        Assert.That(q.Limit, Is.Not.Null);
    }

    // ---------------------------------------------------------------------
    // Hint round-trips to QueryTicket
    //
    // The hint must survive ticket creation: QueryTicketAdapter.ToQueryTicket
    // reads query.CacheHint and passes it to the QueryTicket constructor. A stub
    // KvTransaction (default HLC timestamp, fake unique-id) avoids needing a node.
    // ---------------------------------------------------------------------

    [Test]
    public void CacheHint_RoundTrips_ToQueryTicket()
    {
        SelectQuery q = ParseSelect("SELECT id, total FROM orders {cache=ticket_test, ttl=10s, strict}");

        var stubTxn = new KvTransaction(default(HLCTimestamp), "stub-txn-id");
        var sqlTicket = new ExecuteSQLTicket(
            stubTxn, "testdb",
            "SELECT id, total FROM orders {cache=ticket_test, ttl=10s, strict}",
            parameters: null);

        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(q, sqlTicket);

        Assert.That(ticket.CacheHint, Is.Not.Null,
            "CacheHint must survive QueryTicketAdapter.ToQueryTicket");
        Assert.That(ticket.CacheHint!.CacheName, Is.EqualTo("ticket_test"));
        Assert.That(ticket.CacheHint.TtlMs, Is.EqualTo(10_000));
        Assert.That(ticket.CacheHint.IsStrict, Is.True);
    }

    [Test]
    public void NoCacheHint_QueryTicket_CacheHintIsNull()
    {
        SelectQuery q = ParseSelect("SELECT id FROM orders");

        var stubTxn = new KvTransaction(default(HLCTimestamp), "stub-txn-id");
        var sqlTicket = new ExecuteSQLTicket(stubTxn, "testdb", "SELECT id FROM orders", null);

        QueryTicket ticket = QueryTicketAdapter.ToQueryTicket(q, sqlTicket);

        Assert.That(ticket.CacheHint, Is.Null,
            "A query without a hint must produce a ticket with null CacheHint");
    }

    // ---------------------------------------------------------------------
    // EVICT keyword is reserved; CACHE and ALL remain valid identifiers
    // ---------------------------------------------------------------------

    [Test]
    public void CacheUsedAsTableName_ParsesWithoutError()
    {
        // 'cache' is not a reserved word — schemas using it as an identifier must not break.
        NodeAst ast = SQLParserProcessor.Parse("SELECT * FROM cache");
        Assert.That(ast.nodeType, Is.EqualTo(NodeType.Select));
    }

    [Test]
    public void AllUsedAsColumnName_ParsesWithoutError()
    {
        // 'all' is not a reserved word — schemas using it as an identifier must not break.
        NodeAst ast = SQLParserProcessor.Parse("SELECT all FROM perms");
        Assert.That(ast.nodeType, Is.EqualTo(NodeType.Select));
    }

    [Test]
    public void EvictCacheByName_ParsesAsEvictCacheNode()
    {
        NodeAst ast = SQLParserProcessor.Parse("EVICT CACHE 'robots-by-date'");
        Assert.That(ast.nodeType, Is.EqualTo(NodeType.EvictCache));
        Assert.That(ast.yytext, Is.EqualTo("'robots-by-date'"),
            "yytext carries the raw quoted string; executor strips quotes");
    }

    [Test]
    public void EvictCacheAll_ParsesAsEvictCacheAllNode()
    {
        NodeAst ast = SQLParserProcessor.Parse("EVICT CACHE ALL");
        Assert.That(ast.nodeType, Is.EqualTo(NodeType.EvictCacheAll));
    }

    [Test]
    public void EvictCacheAll_CaseInsensitive()
    {
        NodeAst ast = SQLParserProcessor.Parse("evict cache all");
        Assert.That(ast.nodeType, Is.EqualTo(NodeType.EvictCacheAll));
    }

    [Test]
    public void EvictWrongKeyword_ThrowsHelpfulError()
    {
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("EVICT NOTHING 'x'"),
            "A wrong contextual keyword after EVICT should throw a parse error");
    }

    // ---------------------------------------------------------------------
    // The @{cache=name} form is an accepted alias of {cache=name}
    // ---------------------------------------------------------------------

    [Test]
    public void AtBraceCacheHint_ParsesAsCacheHint()
    {
        // @{cache=name} must produce the same CacheHint as {cache=name}, not a forced-index hint.
        SelectQuery q = ParseSelect("SELECT * FROM orders @{cache=recent_orders}");
        Assert.That(q.CacheHint, Is.Not.Null, "@{cache=...} must be recognised as a cache hint");
        Assert.That(q.CacheHint!.CacheName, Is.EqualTo("recent_orders"));
        Assert.That(q.CacheHint.TtlMs, Is.Null);
        Assert.That(q.CacheHint.IsStrict, Is.False);
        Assert.That((q.Source as TableSource)?.ForcedIndexName, Is.Null,
            "A cache hint must not be mistaken for a forced index");
    }

    [Test]
    public void AtBraceCacheHint_WithOptions_ParsesOptions()
    {
        SelectQuery q = ParseSelect("SELECT * FROM orders @{cache=hot, ttl=30s, strict}");
        Assert.That(q.CacheHint!.CacheName, Is.EqualTo("hot"));
        Assert.That(q.CacheHint.TtlMs, Is.EqualTo(30_000));
        Assert.That(q.CacheHint.IsStrict, Is.True);
    }

    [Test]
    public void AtBraceCacheHint_CaseInsensitiveKeyAndLowercasedName()
    {
        SelectQuery q = ParseSelect("SELECT * FROM orders @{CACHE=RecentOrders}");
        Assert.That(q.CacheHint!.CacheName, Is.EqualTo("recentorders"),
            "The cache key is case-insensitive and the name is lowercased, matching the bare form");
    }

    [Test]
    public void AtBraceForceIndex_StillWorks_NoCacheHint()
    {
        // Regression: the non-cache key path (FORCE_INDEX) must be unchanged.
        SelectQuery q = ParseSelect("SELECT * FROM orders @{FORCE_INDEX=idx_created_at}");
        Assert.That(q.CacheHint, Is.Null);
        Assert.That((q.Source as TableSource)?.ForcedIndexName, Is.EqualTo("idx_created_at"));
    }

    [Test]
    public void AtBraceForceIndex_WithOptions_Throws()
    {
        // Options are only meaningful for the cache hint; a forced index with ttl/strict must error.
        var ex = Assert.Throws<CamusDBException>(() =>
            ParseSelect("SELECT * FROM orders @{FORCE_INDEX=idx, ttl=1s}"));
        Assert.That(ex!.Message, Does.Contain("cache").And.Contain("FORCE_INDEX").IgnoreCase);
    }
}
