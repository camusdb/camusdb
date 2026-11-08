/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A correlated EXISTS whose body needs more than the FROM and WHERE of one table runs the whole body for
/// each outer row: a join, a derived table, a set operation, a view, a series, no FROM, an aggregate,
/// GROUP BY, HAVING, OFFSET, a window function, and a subquery in the body. A derived table in the body
/// can read the outer row too. The WHERE of an UPDATE or a DELETE can hold such an EXISTS
/// too. Before, each form was refused with <c>CADB0533</c> or failed with <c>CADB0400</c> ("must read one
/// table"). Each expected value was measured on PostgreSQL 16.11 with the same rows.
///
/// <para>The tables: <c>r(id, v, s)</c> = (1, 0, 'a'), (2, 2, 'b'), (3, 0, NULL); <c>r2(id, w)</c> = (1, 10),
/// (2, 20); <c>d(id, v)</c> = (1, 1), (2, 2), (3, 3); <c>e(id, s)</c> is empty; the view <c>vw</c> is
/// <c>SELECT id, w FROM r2</c>.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestCorrelatedExistsBody : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE r (id INT64 PRIMARY KEY, v INT64, s STRING)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO r (id, v, s) VALUES (1, 0, 'a'), (2, 2, 'b'), (3, 0, NULL)");
        await Commit(database, executor, dbname, "CREATE TABLE r2 (id INT64 PRIMARY KEY, w INT64)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO r2 (id, w) VALUES (1, 10), (2, 20)");
        await Commit(database, executor, dbname, "CREATE TABLE d (id INT64 PRIMARY KEY, v INT64)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO d (id, v) VALUES (1, 1), (2, 2), (3, 3)");
        await Commit(database, executor, dbname, "CREATE TABLE e (id INT64 PRIMARY KEY, s STRING)", ddl: true);
        await Commit(database, executor, dbname, "CREATE VIEW vw AS SELECT id, w FROM r2", ddl: true);

        return (dbname, database, executor);
    }

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, bool ddl = false)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        if (ddl)
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        else
            _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

        await database.Transactions.CommitAsync(tx);
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        _ => value.StrValue ?? "",
    };

    /// <summary>Each row as its columns joined with <c>|</c>, the rows sorted and joined with <c> ; </c>.</summary>
    private static async Task<List<string>> ReadRows(CommandExecutor executor, KvTransaction tx, string dbname, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
        List<QueryResultRow> rows = await cursor.ToListAsync();

        return rows
            .Select(r => string.Join("|", r.Row.Values.Select(Render)))
            .OrderBy(r => r, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>The rows of <paramref name="sql"/>, or the code and message of the error.</summary>
    private static async Task<string> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            return string.Join(" ; ", await ReadRows(executor, tx, dbname, sql, parameters));
        }
        catch (CamusDBException e)
        {
            return e.Code + ": " + e.Message;
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    /// <summary>
    /// Runs <paramref name="write"/> and then <paramref name="read"/> in one transaction, and rolls back.
    /// Returns the rows of the read, or the code and message of the error.
    /// </summary>
    private static async Task<string> WriteThenRead(DatabaseDescriptor database, CommandExecutor executor, string dbname, string write, string read)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, write, null));
            return string.Join(" ; ", await ReadRows(executor, tx, dbname, read, null));
        }
        catch (CamusDBException e)
        {
            return e.Code + ": " + e.Message;
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    [TestCase("SELECT id FROM d o WHERE EXISTS (SELECT COUNT(*) FROM d i WHERE i.id = o.id + 100)", "1 ; 2 ; 3", TestName = "Aggregate_HasOneRowForEachOuterRow")]
    [TestCase("SELECT id FROM d o WHERE EXISTS (SELECT COUNT(*) FROM d i WHERE i.id = o.id HAVING COUNT(*) > 5)", "", TestName = "Having_RemovesTheOneRow")]
    [TestCase("SELECT id FROM d o WHERE EXISTS (SELECT 1 FROM d i WHERE i.id = o.id OFFSET 1)", "", TestName = "Offset_SkipsTheOnlyRow")]
    [TestCase("SELECT id FROM d o WHERE EXISTS (SELECT 1 FROM d i WHERE i.id >= o.id OFFSET 1)", "1 ; 2", TestName = "Offset_KeepsASecondRow")]
    [TestCase("SELECT id FROM d o WHERE NOT EXISTS (SELECT 1 FROM d i WHERE i.id >= o.id OFFSET 1)", "3", TestName = "Offset_InANotExists")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r2 a JOIN d b ON a.id = b.id WHERE b.v = o.id)", "1 ; 2", TestName = "Join_InTheBody")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2, d WHERE r2.id = d.id AND d.v = v + 1)", "", TestName = "Join_ABareNameIsTheNearestColumn")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM (SELECT id AS k FROM r2) x WHERE x.k = o.id)", "1 ; 2", TestName = "DerivedTable_InTheBody")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM (SELECT o.id AS k) x WHERE x.k = 2)", "2", TestName = "DerivedTable_ReadsTheOuterRow")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM (SELECT id FROM r2 WHERE r2.id = o.id) x)", "1 ; 2", TestName = "DerivedTable_FiltersOnTheOuterRow")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r2 a JOIN (SELECT d.id FROM d WHERE d.v = o.id) b ON a.id = b.id)", "1 ; 2", TestName = "DerivedTable_InAJoinReadsTheOuterRow")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT id FROM r2 WHERE r2.id = o.id UNION SELECT id FROM d WHERE d.id = o.id + 10)", "1 ; 2", TestName = "SetOperation_AsTheBody")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM (SELECT id FROM r2 WHERE r2.id = o.id UNION ALL SELECT id FROM d WHERE d.v = o.v) x)", "1 ; 2", TestName = "SetOperation_InADerivedTable")]
    [TestCase("SELECT id FROM d o WHERE EXISTS (SELECT 1 FROM generate_series(1, 2) AS g WHERE g = o.id)", "1 ; 2", TestName = "Series_InTheBody")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM vw WHERE vw.id = o.id)", "1 ; 2", TestName = "View_InTheBody")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 WHERE o.v = 0)", "1 ; 3", TestName = "NoFrom_InTheBody")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT SUM(w) FROM r2 HAVING SUM(w) > o.id * 10)", "1 ; 2", TestName = "Having_ReadsTheOuterRow")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT w FROM r2 GROUP BY w HAVING w = o.id * 10)", "1 ; 2", TestName = "GroupBy_ReadsTheOuterRow")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT COUNT(*) FROM r2 WHERE w = v * 10 HAVING COUNT(*) > 0)", "2", TestName = "Aggregate_ABareOuterName")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT rank() OVER (ORDER BY w + o.v) FROM r2 WHERE r2.id = o.id)", "1 ; 2", TestName = "Window_ReadsTheOuterRow")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT rank() OVER (ORDER BY w) FROM r2 WHERE w > o.v * 6 OFFSET 1)", "1 ; 3", TestName = "Window_WithAnOffset")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r2 WHERE r2.id = o.id AND w = (SELECT MAX(w) FROM r2))", "2", TestName = "Nested_UncorrelatedScalar")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r2 WHERE r2.id IN (SELECT d.id FROM d WHERE d.v = o.id))", "1 ; 2", TestName = "Nested_InReadsTheOuterRow")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r2 a WHERE a.id = o.id AND EXISTS (SELECT 1 FROM d WHERE d.id = a.id AND d.v = 2))", "2", TestName = "Nested_ExistsReadsTheBody")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r2 a WHERE EXISTS (SELECT 1 FROM d WHERE d.id = a.id AND d.v = o.id))", "1 ; 2", TestName = "Nested_ExistsReadsTheOuterRow")]
    [TestCase("SELECT a.id FROM r a JOIN d b ON a.id = b.id WHERE EXISTS (SELECT COUNT(*) FROM r2 WHERE r2.id = b.id HAVING COUNT(*) = 1)", "1 ; 2", TestName = "OuterJoin_GivesItsColumns")]
    [TestCase("SELECT id FROM r o WHERE o.id = 3 OR EXISTS (SELECT MAX(w) FROM r2 HAVING MAX(w) = o.id * 10)", "2 ; 3", TestName = "UnderAnOr")]
    [TestCase("SELECT id FROM r o WHERE CASE WHEN EXISTS (SELECT MAX(w) FROM r2 HAVING MAX(w) > o.id * 10) THEN TRUE ELSE FALSE END", "1", TestName = "UnderACase")]
    public async Task ACorrelatedBody_RunsForEachOuterRow(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await Query(database, executor, dbname, sql), sql);
    }

    /// <summary>An OFFSET parameter is read when the statement runs, as PostgreSQL reads <c>$1</c>.</summary>
    [TestCase(1L, "1 ; 2")]
    [TestCase(2L, "1")]
    public async Task AnOffsetParameter_IsRead(long offset, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Dictionary<string, ColumnValue> parameters = new() { ["@m"] = new ColumnValue(ColumnType.Integer64, offset) };

        Assert.AreEqual(
            expected,
            await Query(database, executor, dbname, "SELECT id FROM d o WHERE EXISTS (SELECT 1 FROM d i WHERE i.id >= o.id OFFSET @m)", parameters));
    }

    /// <summary>
    /// The body is checked when the statement binds, so each error appears over an empty outer table too,
    /// as in PostgreSQL. The type of an outer column takes part in that check.
    /// </summary>
    [TestCase("SELECT id FROM e o WHERE EXISTS (SELECT COUNT(*) FROM r2 WHERE w = o.s + 1)", "CADB0400", TestName = "Error_ATypeMistakeWithAnOuterColumn")]
    [TestCase("SELECT id FROM e o WHERE EXISTS (SELECT 1 FROM r2 JOIN d ON r2.id = d.id WHERE d.v = o.s)", "CADB0400", TestName = "Error_AComparisonWithAnOuterColumn")]
    [TestCase("SELECT id FROM e o WHERE EXISTS (SELECT 1 FROM r2 WHERE w IN (SELECT nope FROM d WHERE d.id = o.id))", "CADB0404", TestName = "Error_AnUnknownColumnInANestedSubquery")]
    [TestCase("SELECT id FROM e o WHERE EXISTS (SELECT COUNT(*) FROM r2 WHERE w = x.v)", "CADB0400", TestName = "Error_AnUnknownAlias")]
    public async Task AMistakeInTheBody_FailsWithNoOuterRow(string sql, string code)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        string result = await Query(database, executor, dbname, sql);

        Assert.That(result, Does.StartWith(code + ":"), sql);
    }

    /// <summary>
    /// The WHERE of a DELETE or an UPDATE evaluates a correlated EXISTS for each row of the target table.
    /// The body reads the rows as the statement found them, also when it reads the target table: a row that
    /// the statement deletes or changes is still there for the body of a later row.
    /// </summary>
    [TestCase("DELETE FROM r WHERE EXISTS (SELECT 1 FROM r2 WHERE w = v + 10)", "SELECT id FROM r", "2", TestName = "Delete_ABareOuterName")]
    [TestCase("DELETE FROM d WHERE EXISTS (SELECT 1 FROM d i WHERE i.id = d.id + 1)", "SELECT id FROM d", "3", TestName = "Delete_TheBodyReadsTheTargetAsItWas")]
    [TestCase("DELETE FROM r WHERE NOT EXISTS (SELECT 1 FROM r2 a JOIN d b ON a.id = b.id WHERE b.v = r.id)", "SELECT id FROM r", "1 ; 2", TestName = "Delete_AJoinInTheBody")]
    [TestCase("DELETE FROM r WHERE EXISTS (SELECT 1 FROM r2 WHERE w = 20) AND id = 1", "SELECT id FROM r", "2 ; 3", TestName = "Delete_AnUncorrelatedExistsStillFolds")]
    [TestCase("UPDATE r SET s = 'x' WHERE EXISTS (SELECT COUNT(*) FROM r2 WHERE r2.id = r.id HAVING COUNT(*) = 1)", "SELECT id, s FROM r", "1|x ; 2|x ; 3|", TestName = "Update_AHavingInTheBody")]
    [TestCase("UPDATE d SET v = v + 10 WHERE EXISTS (SELECT 1 FROM d i WHERE i.v = d.v + 1)", "SELECT id, v FROM d", "1|11 ; 2|12 ; 3|3", TestName = "Update_TheBodyReadsTheTargetAsItWas")]
    [TestCase("UPDATE r SET v = 9 WHERE EXISTS (SELECT 1 FROM r2 WHERE r2.id IN (SELECT d.id FROM d WHERE d.v = r.id))", "SELECT id, v FROM r", "1|9 ; 2|9 ; 3|0", TestName = "Update_ANestedSubqueryReadsTheTarget")]
    public async Task AWrite_EvaluatesTheExistsForEachRow(string write, string read, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await WriteThenRead(database, executor, dbname, write, read), write);
    }
}
