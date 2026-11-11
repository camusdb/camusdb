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
/// An ORDER BY position counts output columns, as in PostgreSQL: a <c>*</c> counts once for each column
/// it puts out, so <c>SELECT * FROM j1 ORDER BY 2</c> sorts by <c>a</c>, and a position after the <c>*</c>
/// names the item after it. A position that names a column of a <c>*</c> sorts on the cell of that column,
/// so two columns of one name (a <c>*</c> over a join) do not make it ambiguous.
///
/// <para>Every expected result was measured on PostgreSQL 16.11 over the same rows. NULL sorts first in
/// ascending order and last in descending order in CamusDB (a decided difference), so the PostgreSQL
/// measurement wrote NULLS FIRST or NULLS LAST where a NULL takes part in the order.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestOrderByOutputPositions : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    private async Task<Db> SetupAsync()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE j1 (id INT64 PRIMARY KEY, a INT64)");
        await Ddl(db, "CREATE TABLE j2 (id INT64 PRIMARY KEY, a INT64)");

        await Dml(db, "INSERT INTO j1 (id, a) VALUES (1, 10), (2, 20), (3, NULL)");
        await Dml(db, "INSERT INTO j2 (id, a) VALUES (1, 100), (2, 10), (4, 20)");

        return db;
    }

    [Test]
    public async Task APositionCountsTheColumnsOfAStar()
    {
        Db db = await SetupAsync();

        await Expect(db, "SELECT * FROM j1 ORDER BY 2", ["id", "a"], "3|NULL", "1|10", "2|20");
        await Expect(db, "SELECT * FROM j1 ORDER BY 2 DESC", ["id", "a"], "2|20", "1|10", "3|NULL");
        await Expect(db, "SELECT * FROM j1 ORDER BY 2 DESC LIMIT 1", ["id", "a"], "2|20");
    }

    [Test]
    public async Task APositionBesideAStar_NamesTheItemAtThatPlace()
    {
        Db db = await SetupAsync();

        // After the *: position 3 is the item b.
        await Expect(db, "SELECT *, a + 1 AS b FROM j1 ORDER BY 3 DESC", ["id", "a", "b"],
            "2|20|21", "1|10|11", "3|NULL|NULL");

        // Before the *: position 1 is x, and position 3 is the column a of the *.
        await Expect(db, "SELECT id AS x, * FROM j1 ORDER BY 3, 1", ["x", "id", "a"],
            "3|3|NULL", "1|1|10", "2|2|20");
        await Expect(db, "SELECT id AS x, * FROM j1 ORDER BY 1 DESC", ["x", "id", "a"],
            "3|3|NULL", "2|2|20", "1|1|10");
    }

    [Test]
    public async Task APositionOverAJoin_SortsOnTheCellOfThatColumn()
    {
        Db db = await SetupAsync();

        // Position 4 is j2.a, one of two columns named a.
        await Expect(db, "SELECT * FROM j1 JOIN j2 ON j1.id = j2.id ORDER BY 4 DESC", ["id", "a", "id", "a"],
            "1|10|1|100", "2|20|2|10");

        // The swap of a RIGHT JOIN does not move the columns: position 3 is j2.id.
        await Expect(db, "SELECT * FROM j1 RIGHT JOIN j2 ON j1.id = j2.id ORDER BY 3 DESC", ["id", "a", "id", "a"],
            "NULL|NULL|4|20", "2|20|2|10", "1|10|1|100");

        // Over a derived table with two columns named a, the name d.a is ambiguous, but the position is not.
        await Expect(db, "SELECT * FROM (SELECT * FROM j1 JOIN j2 ON j1.id = j2.id) d ORDER BY 4", ["id", "a", "id", "a"],
            "2|20|2|10", "1|10|1|100");
    }

    [Test]
    public async Task APositionWorksInEveryQueryThatSorts()
    {
        Db db = await SetupAsync();

        // DISTINCT sorts after it projects; position 2 is the column id of the *.
        await Expect(db, "SELECT DISTINCT a, * FROM j1 ORDER BY 2 DESC", ["a", "id", "a"],
            "NULL|3|NULL", "20|2|20", "10|1|10");

        // A window column moves the query onto a derived table; the position still counts the *.
        await Expect(db, "SELECT *, row_number() OVER (ORDER BY id) AS rn FROM j1 ORDER BY 2 DESC", ["id", "a", "rn"],
            "2|20|2", "1|10|1", "3|NULL|3");

        // The inner query of a derived table and an EXISTS body.
        await Expect(db, "SELECT x.id FROM (SELECT * FROM j1 ORDER BY 2 DESC LIMIT 1) x", ["id"], "2");
        await Expect(db, "SELECT id FROM j2 WHERE EXISTS (SELECT * FROM j1 WHERE j1.id = j2.id ORDER BY 2) ORDER BY id",
            ["id"], "1", "2");

        // A view keeps the position in its stored body and resolves it again on each read.
        await Ddl(db, "CREATE VIEW top2 AS SELECT * FROM j1 ORDER BY 2 DESC LIMIT 2");
        await Expect(db, "SELECT * FROM top2 ORDER BY 1", ["id", "a"], "1|10", "2|20");
    }

    [Test]
    public async Task APositionOnAnIndexedColumn_GetsThePlanOfItsName()
    {
        Db db = await SetupAsync();

        CollectionAssert.AreEqual(
            await PlanNodes(db, "SELECT * FROM j1 ORDER BY id"),
            await PlanNodes(db, "SELECT * FROM j1 ORDER BY 1"),
            "ORDER BY 1 names the primary key, so it must read the same plan as ORDER BY id");
    }

    [Test]
    public async Task APositionPastTheLastColumn_IsOutOfRange()
    {
        Db db = await SetupAsync();

        await ExpectRefused(db, "SELECT * FROM j1 ORDER BY 3", "ORDER BY position 3 is out of range");
        await ExpectRefused(db, "SELECT *, a FROM j1 ORDER BY 4", "ORDER BY position 4 is out of range");
        await ExpectRefused(db, "SELECT * FROM j1 ORDER BY 0", "ORDER BY position 0 is out of range");
    }

    /// <summary>
    /// GROUP BY refuses every <c>*</c>, because a <c>*</c> is never a group key. PostgreSQL accepts
    /// <c>SELECT * FROM j1 GROUP BY 1</c> because <c>id</c> is the primary key; CamusDB does not have that form
    /// yet (it is listed with <c>SELECT id, v FROM d GROUP BY id</c>), so the statement stays an error.
    /// </summary>
    [Test]
    public async Task GroupByPositionOverAStar_StaysRefused()
    {
        Db db = await SetupAsync();

        await ExpectRefused(db, "SELECT * FROM j1 GROUP BY 1", "GROUP BY position 1 cannot reference *");
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static async Task Expect(Db db, string sql, string[] names, params string[] rows)
    {
        (IReadOnlyList<DerivedColumnSchema> schema, List<string> actual) = await Query(db, sql);

        CollectionAssert.AreEqual(names, schema.Select(column => column.Name).ToArray(), $"{sql}: names");
        CollectionAssert.AreEqual(rows, actual, sql);
    }

    private static async Task ExpectRefused(Db db, string sql, string message)
    {
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await Query(db, sql), sql)!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, $"{sql}: {ex.Message}");
        Assert.AreEqual(message, ex.Message, sql);
    }

    private static async Task<List<string>> PlanNodes(Db db, string sql)
    {
        (_, List<string> rows) = await Query(db, "EXPLAIN " + sql, column: "node");
        return rows;
    }

    /// <summary>Each row as its cells in schema order, each cell read through the row key of its column.</summary>
    private static async Task<(IReadOnlyList<DerivedColumnSchema> Schema, List<string> Rows)> Query(
        Db db, string sql, string? column = null)
    {
        QuerySchemaHolder holder = new();
        KvTransaction tx = await db.Database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db.Name, sql, null), schemaOut: holder);
            List<QueryResultRow> rows = await cursor.ToListAsync();

            List<string> rendered = rows
                .Select(row => column is not null
                    ? TestJoinOutputNames.Cell(row.Row[column])
                    : string.Join("|", holder.Schema.Select(c => TestJoinOutputNames.Cell(row.Row[c.RowKey]))))
                .ToList();

            return (holder.Schema, rendered);
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
