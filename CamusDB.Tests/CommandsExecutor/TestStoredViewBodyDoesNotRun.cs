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
/// CREATE VIEW and CREATE MATERIALIZED VIEW bind the body and run none of its subqueries, so the CREATE
/// does not depend on the rows. Before this rule <c>CREATE VIEW c AS SELECT id, (SELECT v / 0 FROM t
/// WHERE id = 1) AS x FROM t</c> failed with a division by zero, and a subquery with two rows failed the
/// CREATE too. PostgreSQL accepts both and fails the read. A mistake that no row decides, such as an
/// unknown column in a subquery, still fails the CREATE. Each expected result was measured on
/// PostgreSQL 16.11 with the same rows, unless the test says that CamusDB differs by decision.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestStoredViewBodyDoesNotRun : BaseTest
{
    private const string DivisionByZero = "CADB0400: Division by zero";
    private const string TwoRows = "CADB0400: Scalar subquery returned more than one row";

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, s STRING)");
        await Commit(database, executor, dbname, "INSERT INTO t (id, v, s) VALUES (1, 0, 'a'), (2, 2, 'b'), (3, 0, NULL)");
        await Commit(database, executor, dbname, "CREATE TABLE em (id INT64 PRIMARY KEY, v INT64)");

        return (dbname, database, executor);
    }

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        string outcome = await Write(database, executor, dbname, sql);
        if (outcome != "written")
            throw new InvalidOperationException(sql + " -> " + outcome);
    }

    /// <summary>Runs a DDL or a write and returns "written", or the code and message of the error.</summary>
    private static async Task<string> Write(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            if (sql.StartsWith("CREATE", StringComparison.Ordinal))
                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
            else
                _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

            await database.Transactions.CommitAsync(tx);
            return "written";
        }
        catch (CamusDBException e)
        {
            await database.Transactions.RollbackAsync(tx);
            return e.Code + ": " + e.Message;
        }
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "",
        ColumnType.String => value.StrValue!,
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    /// <summary>
    /// The rows of <paramref name="sql"/>, sorted: cells join with <c>|</c> and rows with <c> ; </c>, or the
    /// code and message of the error. The type of each cell of the first row goes to <paramref name="types"/>.
    /// </summary>
    private static async Task<string> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, List<ColumnType>? types = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();

            if (rows.Count > 0)
                types?.AddRange(rows[0].Row.Values.Select(c => c.Type));

            return string.Join(" ; ", rows.Select(r => string.Join("|", r.Row.Values.Select(Render))).OrderBy(r => r, StringComparer.Ordinal));
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

    [TestCase("CREATE VIEW c AS SELECT id, (SELECT v / 0 FROM t WHERE id = 1) AS x FROM t", "SELECT x FROM c", DivisionByZero)]
    [TestCase("CREATE VIEW c AS SELECT id, (SELECT id FROM t) AS x FROM t", "SELECT x FROM c", TwoRows)]
    [TestCase("CREATE VIEW c AS SELECT (SELECT 1 / 0) AS x", "SELECT x FROM c", DivisionByZero)]
    [TestCase("CREATE VIEW c AS SELECT id FROM t WHERE id IN (SELECT 1 / v FROM t)", "SELECT id FROM c", DivisionByZero)]
    [TestCase("CREATE VIEW c AS SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t WHERE 1 / v > 0)", "SELECT id FROM c", DivisionByZero)]
    [TestCase("CREATE VIEW c AS SELECT id, 1 / 0 AS x FROM t", "SELECT x FROM c", DivisionByZero)]
    public async Task ABodyThatFailsOnTheRows_IsAccepted_AndTheReadFails(string create, string read, string readError)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual("written", await Write(database, executor, dbname, create), create);
        Assert.AreEqual(readError, await Query(database, executor, dbname, read), read);
    }

    [Test]
    public async Task AViewOverAFailingView_IsAccepted_AndTheReadFails()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW c AS SELECT id, (SELECT id FROM t) AS x FROM t");
        await Commit(database, executor, dbname, "CREATE VIEW c2 AS SELECT id, x FROM c");

        Assert.AreEqual(TwoRows, await Query(database, executor, dbname, "SELECT x FROM c2"));
    }

    [TestCase("CREATE VIEW c AS SELECT id, (SELECT nope FROM t) AS m FROM t", "CADB0404: Unknown column: nope")]
    [TestCase("CREATE VIEW c AS SELECT id FROM t WHERE id IN (SELECT s FROM t)", "CADB0400: No matching signature for operator IN for argument types: Integer64, String")]
    public async Task AMistakeThatNoRowDecides_FailsTheCreate(string create, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await Write(database, executor, dbname, create), create);
    }

    [Test]
    public async Task ASubqueryOfTheView_GivesItsValue_WhenAStatementReadsIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW c AS SELECT id, (SELECT v FROM t WHERE id = 2) AS m FROM t");

        Assert.AreEqual("1|2 ; 2|2 ; 3|2", await Query(database, executor, dbname, "SELECT id, m FROM c"));
    }

    /// <summary>
    /// The column of a scalar subquery has the declared type of the subquery column, also when the
    /// subquery returns no value. Before, the type came from the value, so a view over an empty table had
    /// a column with no type, and a materialized view of it was refused.
    /// </summary>
    [Test]
    public async Task ASubqueryColumn_HasItsDeclaredType_OverAnEmptyTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE VIEW c AS SELECT id, (SELECT MAX(v) FROM em) AS m FROM t");

        string columns = await Query(database, executor, dbname, "SHOW COLUMNS FROM c");

        Assert.AreEqual("id|integer64 ; m|integer64",
            string.Join(" ; ", columns.Split(" ; ").Select(row => string.Join("|", row.Split('|').Take(2)))));
    }

    [Test]
    public async Task AMaterializedView_FillsWithRealValues_AndRefreshesThem()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE MATERIALIZED VIEW m AS SELECT id, (SELECT MAX(v) FROM t) AS x FROM t");
        await Commit(database, executor, dbname, "CREATE MATERIALIZED VIEW me AS SELECT id, (SELECT MAX(v) FROM em) AS x FROM t");

        List<ColumnType> types = [];
        Assert.AreEqual("1|2 ; 2|2 ; 3|2", await Query(database, executor, dbname, "SELECT id, x FROM m", types));
        Assert.AreEqual(ColumnType.Integer64, types[1]);
        Assert.AreEqual("1| ; 2| ; 3|", await Query(database, executor, dbname, "SELECT id, x FROM me"));

        await Commit(database, executor, dbname, "INSERT INTO em (id, v) VALUES (1, 7)");
        await Commit(database, executor, dbname, "REFRESH MATERIALIZED VIEW me");

        Assert.AreEqual("1|7 ; 2|7 ; 3|7", await Query(database, executor, dbname, "SELECT id, x FROM me"));
    }
}
