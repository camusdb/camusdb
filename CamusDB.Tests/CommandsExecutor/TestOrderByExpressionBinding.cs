
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance tests for ORDER BY binding: which shapes bind, which are rejected and with what error,
/// and the single alias-precedence rule shared by the plain and grouped sort paths.
/// </summary>
internal sealed class TestOrderByExpressionBinding : SharedNodeBaseTest
{
    private async Task<(DatabaseDescriptor db, CommandExecutor executor)> SetupRows()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddl = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname,
            "CREATE TABLE t (id OID NOT NULL, a int64, b int64, name string, PRIMARY KEY (id))", null));

        // a and b rank the rows in opposite directions, so a test that resolves the wrong one
        // gets the reverse order rather than a coincidentally equal one.
        await Insert(executor, db, a: 1, b: 3, name: "ccc");
        await Insert(executor, db, a: 2, b: 2, name: "bb");
        await Insert(executor, db, a: 3, b: 1, name: "a");

        return (db, executor);
    }

    private static async Task Insert(CommandExecutor executor, DatabaseDescriptor db, long a, long b, string name)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name,
            "INSERT INTO t (id, a, b, name) VALUES (@id, @a, @b, @name)",
            new()
            {
                { "@id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "@a", new(ColumnType.Integer64, a) },
                { "@b", new(ColumnType.Integer64, b) },
                { "@name", new(ColumnType.String, name) },
            }));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Select(
        CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, sql, null));
        return await cursor.ToListAsync();
    }

    // ── Rejected shapes: a caller error must never read as an engine fault ────

    [Test]
    [NonParallelizable]
    public async Task ComputedOrdering_IsRejectedAsInvalidInput()
    {
        // Binding accepts the shape and preserves it; the comparer cannot evaluate it yet. The
        // important part is the error class: this used to surface as InvalidInternalOperation,
        // which reports a caller's valid SQL as an engine fault.
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupRows();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Select(executor, db, "SELECT id FROM t ORDER BY length(name)"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        Assert.AreNotEqual(CamusDBErrorCodes.InvalidInternalOperation, ex.Code);
        StringAssert.Contains("length", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task AggregateOrdering_WithoutGroupBy_IsRejected()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupRows();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Select(executor, db, "SELECT id FROM t ORDER BY count(a)"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task ComputedOrdering_WithGroupBy_IsRejectedWithAGuidingMessage()
    {
        // A grouped query sorts after projection, so a base column the expression names no longer
        // exists by then. The message must say what to do instead.
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupRows();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await Select(executor, db, "SELECT a FROM t GROUP BY a ORDER BY length(name)"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("SELECT projection", ex.Message);
    }

    // ── One alias-precedence rule, proven on both sort paths ─────────────────

    [Test]
    [NonParallelizable]
    public async Task PlainSelect_AliasOutranksABaseColumnOfTheSameName()
    {
        // "a" is both a real column and the alias of b. Standard SQL gives the select-list alias
        // precedence in ORDER BY, so this must order by b — descending in terms of column a.
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupRows();

        List<QueryResultRow> rows = await Select(executor, db, "SELECT b AS a FROM t ORDER BY a");

        CollectionAssert.AreEqual(new[] { 1L, 2L, 3L }, rows.Select(r => r.Row["a"].LongValue).ToArray());
    }

    [Test]
    [NonParallelizable]
    public async Task GroupedSelect_AliasOutranksABaseColumnOfTheSameName()
    {
        // The same rule, reached through the post-projection resolver. If the two paths disagreed,
        // one of these two tests would order the rows the other way.
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupRows();

        List<QueryResultRow> rows = await Select(executor, db, "SELECT b AS a FROM t GROUP BY b ORDER BY a");

        CollectionAssert.AreEqual(new[] { 1L, 2L, 3L }, rows.Select(r => r.Row["a"].LongValue).ToArray());
    }

    [Test]
    [NonParallelizable]
    public async Task PlainSelect_AliasWithNoMatchingColumn_NowOrders()
    {
        // Before the alias was resolved, this looked up a column "n" that no row carries and failed
        // as an internal error. It is ordinary SQL and must simply work.
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupRows();

        List<QueryResultRow> rows = await Select(executor, db, "SELECT name AS n FROM t ORDER BY n");

        CollectionAssert.AreEqual(new[] { "a", "bb", "ccc" }, rows.Select(r => r.Row["n"].StrValue).ToArray());
    }

    // ── Unchanged behavior ───────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task PlainColumnOrdering_IsUnchanged()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupRows();

        List<QueryResultRow> ascending = await Select(executor, db, "SELECT a FROM t ORDER BY a");
        List<QueryResultRow> descending = await Select(executor, db, "SELECT a FROM t ORDER BY a DESC");

        CollectionAssert.AreEqual(new[] { 1L, 2L, 3L }, ascending.Select(r => r.Row["a"].LongValue).ToArray());
        CollectionAssert.AreEqual(new[] { 3L, 2L, 1L }, descending.Select(r => r.Row["a"].LongValue).ToArray());
    }
}
