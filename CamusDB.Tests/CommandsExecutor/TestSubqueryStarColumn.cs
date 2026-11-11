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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A subquery that must give one column (scalar, <c>IN</c>, <c>ANY</c>/<c>ALL</c>, correlated or not) accepts a
/// <c>*</c> that expands to one column, as in PostgreSQL, and refuses a <c>*</c> that expands to more. The column
/// of the <c>*</c> keeps its static type, so a statement check that the type of a named column fails also fails
/// when the subquery gives no row.
///
/// <para>Every expected result was measured on PostgreSQL 16.11 over the same rows.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSubqueryStarColumn : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    private async Task<Db> SetupAsync()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE d (id INT64 PRIMARY KEY, v INT64)");
        await Ddl(db, "CREATE TABLE one (id INT64 PRIMARY KEY)");
        await Ddl(db, "CREATE TABLE onestr (id STRING PRIMARY KEY)");

        await Dml(db, "INSERT INTO d (id, v) VALUES (1, 10), (2, 20)");
        await Dml(db, "INSERT INTO one (id) VALUES (1), (2)");

        return db;
    }

    [Test]
    public async Task AScalarSubquery_AcceptsAStarOfOneColumn()
    {
        Db db = await SetupAsync();

        await Expect(db, "SELECT (SELECT * FROM (SELECT v FROM d WHERE id = 1) s)", "10");
        await Expect(db, "SELECT (SELECT * FROM one WHERE id = 2)", "2");

        // The column keeps its type, so it takes part in arithmetic.
        await Expect(db, "SELECT (SELECT * FROM (SELECT v FROM d WHERE id = 1) s) + 1", "11");

        // A window call in the ORDER BY adds a hidden column, which the * does not count.
        await Expect(db, "SELECT (SELECT * FROM one ORDER BY row_number() OVER (ORDER BY id DESC) LIMIT 1)", "2");

        // A set operation is a * over its result, and was accepted before.
        await Expect(db, "SELECT (SELECT * FROM (SELECT 1 AS x UNION SELECT 1) u)", "1");
    }

    /// <summary>
    /// A scalar subquery column takes its name from the parse tree, before the sources are bound, so a * has no
    /// column name there and the column has its position as its name, as an expression column has. PostgreSQL
    /// names it <c>v</c> and <c>id</c>; the difference is listed with the names of expression columns.
    /// </summary>
    [Test]
    public async Task TheOutputColumn_HasItsPositionAsItsName()
    {
        Db db = await SetupAsync();

        CollectionAssert.AreEqual(new[] { "0" }, await Names(db, "SELECT (SELECT * FROM (SELECT v FROM d WHERE id = 1) s)"));
        CollectionAssert.AreEqual(new[] { "id", "1" }, await Names(db, "SELECT id, (SELECT * FROM one WHERE one.id = d.id) FROM d"));
    }

    [Test]
    public async Task ACorrelatedSubquery_AcceptsAStarOfOneColumn()
    {
        Db db = await SetupAsync();

        await Expect(db, "SELECT id, (SELECT * FROM one WHERE one.id = d.id) AS o FROM d ORDER BY 1", "1|1", "2|2");
        await Expect(db, "SELECT id FROM d WHERE id + 1 IN (SELECT * FROM one WHERE one.id = d.id + 1)", "1");
        await Expect(db, "SELECT id FROM d WHERE id IN (SELECT * FROM one WHERE one.id = d.id) ORDER BY id", "1", "2");
        await Expect(db, "SELECT id FROM d WHERE id = ANY (SELECT * FROM one WHERE one.id = d.id) ORDER BY id", "1", "2");
    }

    [Test]
    public async Task AnInOrQuantifiedSubquery_AcceptsAStarOfOneColumn()
    {
        Db db = await SetupAsync();

        await Expect(db, "SELECT id FROM d WHERE v IN (SELECT * FROM (SELECT v FROM d WHERE id = 2) s)", "2");
        await Expect(db, "SELECT id FROM d WHERE id NOT IN (SELECT * FROM one WHERE id = 1)", "2");
        await Expect(db, "SELECT id FROM d WHERE v > ALL (SELECT * FROM (SELECT v FROM d WHERE id = 1) s)", "2");
    }

    [Test]
    public async Task AViewBody_KeepsAStarOfOneColumn()
    {
        Db db = await SetupAsync();

        await Ddl(db, "CREATE VIEW vs AS SELECT id, (SELECT * FROM one WHERE one.id = d.id) AS o FROM d");
        await Expect(db, "SELECT * FROM vs ORDER BY id", "1|1", "2|2");
    }

    /// <summary>
    /// PostgreSQL refuses <c>bigint = text</c> also when the subquery gives no row. The column of a <c>*</c> has
    /// the type of the column it names, so the check gives the same error as for the named column.
    /// </summary>
    [Test]
    public async Task TheColumnOfAStar_KeepsItsTypeWhenNoRowComes()
    {
        Db db = await SetupAsync();

        CamusDBException named = await Refused(db, "SELECT id FROM d WHERE id = (SELECT id FROM onestr)");
        CamusDBException star = await Refused(db, "SELECT id FROM d WHERE id = (SELECT * FROM onestr)");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, named.Code, named.Message);
        Assert.AreEqual(named.Code, star.Code, star.Message);
        Assert.AreEqual(named.Message, star.Message);
    }

    [Test]
    public async Task AStarOfMoreColumns_IsRefused()
    {
        Db db = await SetupAsync();

        foreach (string sql in new[]
        {
            "SELECT (SELECT * FROM d)",
            "SELECT id FROM d WHERE id IN (SELECT * FROM d)",
            "SELECT (SELECT * FROM d d2 WHERE d2.id = d.id) FROM d",
            "SELECT id FROM d WHERE id IN (SELECT 1, 2 UNION SELECT 3, 4)",
        })
        {
            CamusDBException ex = await Refused(db, sql);

            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, $"{sql}: {ex.Message}");
            Assert.AreEqual("Subquery must return exactly one column", ex.Message, sql);
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static async Task Expect(Db db, string sql, params string[] rows) =>
        CollectionAssert.AreEqual(rows, await Query(db, sql), sql);

    private static async Task<CamusDBException> Refused(Db db, string sql) =>
        Assert.ThrowsAsync<CamusDBException>(async () => await Query(db, sql), sql)!;

    private static async Task<string[]> Names(Db db, string sql)
    {
        QuerySchemaHolder holder = new();
        KvTransaction tx = await db.Database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db.Name, sql, null), schemaOut: holder);
            await cursor.ToListAsync();

            return holder.Schema.Select(column => column.Name).ToArray();
        }
        finally
        {
            await db.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Each row as its cells in schema order, each cell read through the row key of its column.</summary>
    private static async Task<List<string>> Query(Db db, string sql)
    {
        QuerySchemaHolder holder = new();
        KvTransaction tx = await db.Database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db.Name, sql, null), schemaOut: holder);
            List<QueryResultRow> rows = await cursor.ToListAsync();

            return rows
                .Select(row => string.Join("|", holder.Schema.Select(c => TestJoinOutputNames.Cell(row.Row[c.RowKey]))))
                .ToList();
        }
        finally
        {
            await db.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Ddl(Db db, string sql)
    {
        KvTransaction tx = await db.Database.Transactions.BeginAsync();

        try
        {
            await db.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
            await db.Database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await db.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Dml(Db db, string sql)
    {
        KvTransaction tx = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Database.Transactions.CommitAsync(tx);
    }
}
