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
/// A name in a subquery resolves to the nearest query that has it: the subquery first, then the query
/// around it. So a bare name that only the outer query has is an outer reference, and a name that both
/// queries have is the column of the subquery. Before this rule a bare outer name failed with
/// <c>CADB0404</c>, a name in both scopes failed as ambiguous, and <c>IN (SELECT o.id FROM r2)</c> could
/// read <c>o.id</c> as the inner <c>id</c>. Each row result was measured on PostgreSQL 16.11 with the same
/// rows. A correlated scalar, <c>IN</c> or quantified subquery reads the outer name by the same rule.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestSubqueryNameScope : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE r (id INT64 PRIMARY KEY, v INT64, s STRING)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO r (id, v, s) VALUES (1, 0, 'a'), (2, 2, 'b'), (3, 0, NULL)");
        await Commit(database, executor, dbname, "CREATE TABLE r2 (id INT64 PRIMARY KEY, w INT64)", ddl: true);
        await Commit(database, executor, dbname, "INSERT INTO r2 (id, w) VALUES (1, 10), (2, 20)");

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

    /// <summary>The first column of each row, sorted and joined with <c> ; </c>, or the code and message of the error.</summary>
    private static async Task<string> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            return await ReadFirstColumn(executor, tx, dbname, sql);
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

    private static async Task<string> ReadFirstColumn(CommandExecutor executor, KvTransaction tx, string dbname, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();

        return string.Join(" ; ", rows
            .Select(r => r.Row.First().Value.LongValue.ToString(CultureInfo.InvariantCulture))
            .OrderBy(r => r, StringComparer.Ordinal));
    }

    /// <summary>
    /// Runs a write and then a read in one transaction, and rolls back. Returns the first column of the read,
    /// as <see cref="Query"/> does, or the code and message of the error.
    /// </summary>
    private static async Task<string> WriteThenRead(DatabaseDescriptor database, CommandExecutor executor, string dbname, string write, string read)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, write, null));
            return await ReadFirstColumn(executor, tx, dbname, read);
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

    /// <summary>Runs a write and rolls it back. Returns "written", or the code and message of the error.</summary>
    private static async Task<string> Write(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            return "written";
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

    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 WHERE w = v + 10)", "1 ; 3", TestName = "BareOuterName_InTheWhere")]
    [TestCase("SELECT id FROM r WHERE NOT EXISTS (SELECT 1 FROM r2 WHERE w = v + 10)", "2", TestName = "BareOuterName_InANotExists")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 WHERE id = v)", "2", TestName = "BareOuterName_KeysTheInnerIndex")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 i WHERE i.id = r.id AND w = v * 10 + 10)", "1", TestName = "BareOuterName_WithAQualifiedOne")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 WHERE id = 2 AND s = 'b')", "2", TestName = "BareOuterName_NextToABareInnerName")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT s FROM r2 i)", "1 ; 2 ; 3", TestName = "BareOuterName_InTheDiscardedSelectList")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 WHERE w = 10 ORDER BY s)", "1 ; 2 ; 3", TestName = "BareOuterName_InTheDiscardedOrderBy")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r i WHERE i.id = o.id + 1 AND v = 0)", "2", TestName = "NameInBothScopes_IsTheInnerColumn")]
    [TestCase("SELECT id FROM r o WHERE EXISTS (SELECT 1 FROM r2 i WHERE i.id = id)", "1 ; 2 ; 3", TestName = "NameInBothScopes_MakesNoCorrelation")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 r WHERE r.id = 1)", "1 ; 2 ; 3", TestName = "InnerAlias_HidesTheOuterAlias")]
    [TestCase("SELECT id FROM r x WHERE EXISTS (SELECT 1 FROM r2 x WHERE x.w = 10 AND s = 'a')", "1", TestName = "HiddenOuterAlias_StillGivesItsBareColumns")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 WHERE r2.id = r.id AND w = 20)", "2", TestName = "OuterTableName_AsAQualifier")]
    [TestCase("SELECT id FROM r WHERE id IN (SELECT id FROM r2)", "1 ; 2", TestName = "In_NameInBothScopes_IsUncorrelated")]
    [TestCase("SELECT id FROM r WHERE v IN (SELECT w - 10 FROM r2)", "1 ; 3", TestName = "In_InnerNamesOnly_IsUncorrelated")]
    public async Task ANameResolvesToTheNearestQuery(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await Query(database, executor, dbname, sql), sql);
    }

    /// <summary>A name that neither query has is still an unknown column, not an outer reference.</summary>
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 WHERE nope = 1)")]
    [TestCase("SELECT id FROM r WHERE id IN (SELECT id FROM r2 WHERE nope = 1)")]
    [TestCase("SELECT id, (SELECT nope FROM r2 WHERE id = 1) FROM r")]
    public async Task ANameThatNoQueryHas_IsAnUnknownColumn(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual("CADB0404: Unknown column: nope", await Query(database, executor, dbname, sql), sql);
    }

    /// <summary>
    /// A correlated <c>IN</c>, <c>NOT IN</c>, scalar or quantified subquery reads the outer name by the nearest
    /// rule, and runs for each outer row. Before the rule each form failed with an unknown alias or column, and
    /// the <c>IN</c> form over a table with an index could read <c>o.id</c> as the inner <c>id</c>.
    /// </summary>
    [TestCase("SELECT id FROM r o WHERE id IN (SELECT o.id FROM r2)", "1 ; 2 ; 3")]
    [TestCase("SELECT x.id FROM r x WHERE x.id IN (SELECT x.id)", "1 ; 2 ; 3")]
    [TestCase("SELECT id FROM r WHERE id NOT IN (SELECT id FROM r2 WHERE w = v + 10)", "2 ; 3")]
    [TestCase("SELECT id FROM r WHERE (SELECT v) = 0", "1 ; 3")]
    [TestCase("SELECT id FROM r WHERE v < ANY (SELECT w FROM r2 WHERE w = v + 10)", "1 ; 3")]
    public async Task ACorrelatedForm_ReadsTheOuterName(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await Query(database, executor, dbname, sql), sql);
    }

    /// <summary>A subquery of an UPDATE reads the target table by its bare names too, in the WHERE and in a SET value.</summary>
    [TestCase("UPDATE r SET v = CASE WHEN EXISTS (SELECT 1 FROM r2 WHERE w = v + 10) THEN 1 ELSE 0 END WHERE id > 0", "0 ; 1 ; 1")]
    [TestCase("UPDATE r SET v = (SELECT w FROM r2 WHERE r2.id = r.id) WHERE id = 1", "0 ; 10 ; 2")]
    [TestCase("UPDATE r SET v = 5 WHERE id IN (SELECT id FROM r2 WHERE w = v + 10)", "0 ; 2 ; 5")]
    public async Task ACorrelatedSubqueryOfAWrite_ReadsTheTargetRow(string write, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await WriteThenRead(database, executor, dbname, write, "SELECT v FROM r"), write);
    }

    /// <summary>An uncorrelated subquery that writes a name of the outer query only in its own scope still runs once.</summary>
    [Test]
    public async Task AnUncorrelatedSubquery_InAWrite_StillRuns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual("written", await Write(database, executor, dbname, "UPDATE r SET v = (SELECT w FROM r2 WHERE id = 2) WHERE id IN (SELECT id FROM r2)"));
    }
    /// <summary>
    /// A correlated EXISTS with a subquery in the WHERE of its body runs the body for each outer row. The
    /// first form reads the bare outer name <c>v</c> inside the nested subquery. Before, each form failed
    /// with the internal error <c>CADB0099</c>. The rows were measured on PostgreSQL 16.11.
    /// </summary>
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 WHERE w IN (SELECT w FROM r2 WHERE w = v + 10))", "1 ; 3")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 i WHERE i.id = r.id AND i.w IN (SELECT 10))", "1")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 i WHERE i.id = r.id AND i.w = (SELECT 10))", "1")]
    public async Task ACorrelatedExistsWithASubqueryInItsWhere_Runs(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(expected, await Query(database, executor, dbname, sql), sql);
    }
}
