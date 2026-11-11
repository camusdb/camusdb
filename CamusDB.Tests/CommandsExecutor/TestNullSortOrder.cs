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

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// NULL sorts first in ascending order and last in descending order, on every path that orders rows. This
/// is the GoogleSQL rule and a decided difference from PostgreSQL, which does the opposite by default. An
/// index keeps its keys in the same order (a NULL key sorts before every value of an ascending column and
/// after every value of a descending one), so an ORDER BY that an index supplies needs no sort, and it must
/// give the same rows as the sort.
///
/// <para>Each expected result was measured on PostgreSQL 16.11 with <c>NULLS FIRST</c> for an ascending key
/// and <c>NULLS LAST</c> for a descending one, which is the CamusDB rule written out.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestNullSortOrder : BaseTest
{
    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    private async Task<Db> SetupAsync()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Db db = new(name, database, executor);

        await Ddl(db, "CREATE TABLE n (id INT64 PRIMARY KEY, v INT64, w INT64)");
        await Ddl(db, "CREATE INDEX n_v ON n (v)");
        await Ddl(db, "CREATE INDEX n_w ON n (w DESC)");
        await Dml(db, "INSERT INTO n (id, v, w) VALUES (1, 30, NULL), (2, NULL, 5), (3, 10, 7), (4, NULL, NULL), (5, 20, 6)");

        return db;
    }

    [Test]
    public async Task TheSort_PutsNullFirstAscending_AndLastDescending()
    {
        Db db = await SetupAsync();

        // v + 0 is an expression, so no index can supply the order and the rows are sorted.
        await Expect(db, "SELECT v FROM n ORDER BY v + 0", "NULL", "NULL", "10", "20", "30");
        await Expect(db, "SELECT v FROM n ORDER BY v + 0 DESC", "30", "20", "10", "NULL", "NULL");
        await Expect(db, "SELECT id FROM n ORDER BY v, id", "2", "4", "3", "5", "1");
        await Expect(db, "SELECT id FROM n ORDER BY v DESC, id", "1", "5", "3", "2", "4");
    }

    [Test]
    public async Task TopK_KeepsTheSameRowsAsTheFullSort()
    {
        Db db = await SetupAsync();

        await Expect(db, "SELECT v FROM n ORDER BY v + 0 LIMIT 3", "NULL", "NULL", "10");
        await Expect(db, "SELECT v FROM n ORDER BY v + 0 DESC LIMIT 4", "30", "20", "10", "NULL");
        await Expect(db, "SELECT v FROM n ORDER BY v + 0 DESC LIMIT 2 OFFSET 2", "10", "NULL");
    }

    [Test]
    public async Task AnIndexOrder_GivesTheRowsOfTheSort()
    {
        Db db = await SetupAsync();

        await ExpectIndexOrder(db, "SELECT v FROM n@{FORCE_INDEX=n_v} ORDER BY v", "NULL", "NULL", "10", "20", "30");
        await ExpectIndexOrder(db, "SELECT v FROM n@{FORCE_INDEX=n_v} ORDER BY v LIMIT 3", "NULL", "NULL", "10");
        await ExpectIndexOrder(db, "SELECT w FROM n@{FORCE_INDEX=n_w} ORDER BY w DESC", "7", "6", "5", "NULL", "NULL");
        await ExpectIndexOrder(db, "SELECT w FROM n@{FORCE_INDEX=n_w} ORDER BY w DESC LIMIT 4", "7", "6", "5", "NULL");
    }

    [Test]
    public async Task WindowsGroupsDistinctAndSetOperations_FollowTheSameRule()
    {
        Db db = await SetupAsync();

        await Expect(db, "SELECT id, row_number() OVER (ORDER BY v, id) AS rn FROM n ORDER BY id",
            "1|5", "2|1", "3|3", "4|2", "5|4");
        await Expect(db, "SELECT id, row_number() OVER (ORDER BY v DESC, id) AS rn FROM n ORDER BY id",
            "1|1", "2|4", "3|3", "4|5", "5|2");
        await Expect(db, "SELECT v, COUNT(*) AS c FROM n GROUP BY v ORDER BY v",
            "NULL|2", "10|1", "20|1", "30|1");
        await Expect(db, "SELECT DISTINCT v FROM n ORDER BY v DESC", "30", "20", "10", "NULL");
        await Expect(db, "SELECT v FROM n UNION SELECT w FROM n ORDER BY 1",
            "NULL", "5", "6", "7", "10", "20", "30");
        await Expect(db, "SELECT v FROM (SELECT v FROM n) d ORDER BY v DESC", "30", "20", "10", "NULL", "NULL");
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static async Task Expect(Db db, string sql, params string[] rows) =>
        CollectionAssert.AreEqual(rows, await Query(db, sql), sql);

    /// <summary>The rows, and a plan that reads the index in its order with no sort above it.</summary>
    private static async Task ExpectIndexOrder(Db db, string sql, params string[] rows)
    {
        await Expect(db, sql, rows);

        List<string> nodes = await Query(db, "EXPLAIN " + sql, column: "node");
        string plan = string.Join(" / ", nodes);

        Assert.IsFalse(nodes.Any(node => node.StartsWith("sort") || node.StartsWith("topk")), $"{sql}: no sort expected in {plan}");
        Assert.IsTrue(nodes.Any(node => node.Contains("index")), $"{sql}: an index read expected in {plan}");
    }

    /// <summary>Each row as its cells joined by '|', or the one named column.</summary>
    private static async Task<List<string>> Query(Db db, string sql, string? column = null)
    {
        QuerySchemaHolder holder = new();
        KvTransaction tx = await db.Database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db.Name, sql, null), schemaOut: holder);
            List<QueryResultRow> rows = await cursor.ToListAsync();

            return rows
                .Select(row => column is not null
                    ? TestJoinOutputNames.Cell(row.Row[column])
                    : string.Join("|", holder.Schema.Select(c => TestJoinOutputNames.Cell(row.Row[c.RowKey]))))
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
