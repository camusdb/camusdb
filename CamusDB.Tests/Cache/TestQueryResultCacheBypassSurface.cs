/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Cache;

/// <summary>
/// Verifies that every hinted-but-ineligible SELECT surfaces explicit bypass metadata
/// rather than producing a response that looks identical to an un-hinted query.
///
/// Three cases covered:
/// <list type="number">
///   <item><description>
///     <b>Join</b> — a <c>{cache=name}</c> hint on a multi-source JOIN. The cache fences
///     only one table's row keyspace per entry; joining requires all tables to be fenced.
///     Until multi-keyspace fencing is implemented the hint executes live and reports
///     <c>cacheStatus = bypass, reason = join</c>.
///   </description></item>
///   <item><description>
///     <b>Explicit transaction</b> — a <c>{cache=name}</c> hint inside an explicit
///     read-write (or explicit serializable RO) transaction. The cache only admits implicit
///     single-statement autocommit reads; an explicit transaction must read live storage.
///     Reports <c>cacheStatus = bypass, reason = in-transaction</c>.
///   </description></item>
///   <item><description>
///     <b>Inner subquery hint</b> — a <c>{cache=name}</c> hint on a subquery inside the
///     WHERE clause when the outer SELECT has no hint. SubqueryRewriter executes inner
///     subqueries live and discards their cache hints. The outer response now carries an
///     explicit <c>cacheStatus = bypass, reason = inner-hint</c> so the hint is visible.
///   </description></item>
/// </list>
/// </summary>
[TestFixture]
public sealed class TestQueryResultCacheBypassSurface : CommandsExecutor.BaseTest
{
    private QueryResultCache? _cache;

    protected override CommandExecutor CreateCommandExecutor()
    {
        _cache = new QueryResultCache(CamusDBConfig.Ambient, sweepIntervalMs: -1);
        CommandValidator validator = new();
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, CamusDBConfig.Ambient,
                   sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false,
                   cache: _cache);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task SetupTables(string dbname, DatabaseDescriptor db, CommandExecutor exec)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await exec.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE orders (id STRING NOT NULL PRIMARY KEY, amount int64 NOT NULL)", null));
        await exec.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE items (id STRING NOT NULL PRIMARY KEY, label STRING NOT NULL)", null));

        KvTransaction ins = await db.Transactions.BeginAsync();
        await exec.ExecuteNonSQLQuery(new ExecuteSQLTicket(ins, dbname,
            "INSERT INTO orders (id, amount) VALUES (\"o1\", 100)", null));
        await exec.ExecuteNonSQLQuery(new ExecuteSQLTicket(ins, dbname,
            "INSERT INTO items (id, label) VALUES (\"o1\", \"widget\")", null));
        await db.Transactions.CommitAsync(ins);
    }

    private static async Task<CacheMetadataHolder> RunQuery(
        string dbname, CommandExecutor exec, string sql)
    {
        var meta = new CacheMetadataHolder();
        KvTransaction readTx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await exec.ExecuteSQLQuery(new ExecuteSQLTicket(readTx, dbname, sql, null), meta);
        await foreach (QueryResultRow _ in cursor) { }
        return meta;
    }

    // ── 1. Hinted JOIN surfaces bypass with reason "join" ─────────────────────
    [Test]
    public async Task HintedJoin_BypassesWithJoinReason()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor exec) = await CreateDatabase();
        await SetupTables(dbname, db, exec);

        CacheMetadataHolder meta = await RunQuery(dbname, exec,
            "SELECT o.id, i.label FROM orders o{cache=join_test} JOIN items i ON o.id = i.id");

        Assert.That(meta.CacheName, Is.EqualTo("join_test"),
            "cacheName must be set so the hint is visible in the response");
        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Bypass),
            "a hinted JOIN must bypass the result cache");
        Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.Join),
            "bypass reason must be Join, not a generic ineligible reason");
        Assert.That(CacheMetadataHolder.ToBypassReasonString(meta.BypassReason), Is.EqualTo("join"),
            "the wire string surfaced in the JSON envelope must be \"join\"");
    }

    // ── 2. Hinted query inside explicit transaction surfaces in-transaction ───
    [Test]
    public async Task HintedExplicitTransaction_BypassesWithInTransactionReason()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor exec) = await CreateDatabase();
        await SetupTables(dbname, db, exec);

        // Use an explicit read-write transaction (not an autocommit read).
        var meta = new CacheMetadataHolder();
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await exec.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, dbname,
                    "SELECT * FROM orders{cache=tx_test}", null), meta);
            await foreach (QueryResultRow _ in cursor) { }
        }
        finally
        {
            await db.Transactions.RollbackAsync(tx);
        }

        Assert.That(meta.CacheName, Is.EqualTo("tx_test"),
            "cacheName must be set so the hint is visible in the response");
        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Bypass),
            "a hinted query inside an explicit transaction must bypass the result cache");
        Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.InTransaction),
            "bypass reason must be InTransaction");
        Assert.That(CacheMetadataHolder.ToBypassReasonString(meta.BypassReason), Is.EqualTo("in-transaction"),
            "the wire string surfaced in the JSON envelope must be \"in-transaction\"");
    }

    // ── 3. Inner subquery hint surfaces bypass with reason "inner-hint" ────────
    //
    // The outer SELECT has no {cache=...} hint; the hint is on the inner SELECT
    // inside the WHERE clause. SubqueryRewriter materialises the inner subquery live
    // and discards its cache hint. The outer response must carry an explicit bypass
    // (not silence, which would look identical to an un-hinted query).
    [Test]
    public async Task InnerSubqueryHint_OuterResponseBypassesWithInnerHintReason()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor exec) = await CreateDatabase();
        await SetupTables(dbname, db, exec);

        // The outer SELECT has no {cache=...}. Only the inner subquery does.
        CacheMetadataHolder meta = await RunQuery(dbname, exec,
            "SELECT id FROM orders WHERE id IN (SELECT id FROM items{cache=inner_test})");

        Assert.That(meta.CacheName, Is.EqualTo("inner_test"),
            "cacheName should be populated from the inner hint so it is traceable");
        Assert.That(meta.Status, Is.EqualTo(QueryCacheStatus.Bypass),
            "the outer response must bypass when only an inner subquery has a hint");
        Assert.That(meta.BypassReason, Is.EqualTo(QueryCacheBypassReason.InnerHint),
            "bypass reason must be InnerHint to distinguish from other bypass causes");
        Assert.That(CacheMetadataHolder.ToBypassReasonString(meta.BypassReason), Is.EqualTo("inner-hint"),
            "the wire string surfaced in the JSON envelope must be \"inner-hint\"");
    }
}
