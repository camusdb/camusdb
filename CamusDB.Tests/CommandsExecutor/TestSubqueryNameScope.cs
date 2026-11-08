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
/// rows. A correlated scalar, <c>IN</c> or quantified subquery, and a correlated EXISTS outside the WHERE
/// of a SELECT, are refused with <c>CADB0533</c>: PostgreSQL runs them, and CamusDB does not run them yet.
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
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();

            return string.Join(" ; ", rows
                .Select(r => r.Row.First().Value.LongValue.ToString(CultureInfo.InvariantCulture))
                .OrderBy(r => r, StringComparer.Ordinal));
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
    /// PostgreSQL runs each of these forms. CamusDB finds the outer name now and refuses the form with
    /// <c>CADB0533</c>, and the message names the outer reference. Before, each one failed with an unknown
    /// alias or column, and the <c>IN</c> form over a table with an index could read <c>o.id</c> as the
    /// inner <c>id</c>. PostgreSQL 16.11 gives <c>1 ; 2 ; 3</c> for that form.
    /// </summary>
    [TestCase("SELECT id FROM r o WHERE id IN (SELECT o.id FROM r2)", "A correlated IN or NOT IN subquery is not supported yet: it reads 'o.id' of the outer query")]
    [TestCase("SELECT x.id FROM r x WHERE x.id IN (SELECT x.id)", "A correlated IN or NOT IN subquery is not supported yet: it reads 'x.id' of the outer query")]
    [TestCase("SELECT id FROM r WHERE id NOT IN (SELECT id FROM r2 WHERE w = v + 10)", "A correlated IN or NOT IN subquery is not supported yet: it reads 'v' of the outer query")]
    [TestCase("SELECT id, (SELECT w FROM r2 WHERE r2.id = r.id) FROM r", "A correlated scalar subquery is not supported yet: it reads 'r.id' of the outer query")]
    [TestCase("SELECT id FROM r WHERE (SELECT v) = 0", "A correlated scalar subquery is not supported yet: it reads 'v' of the outer query")]
    [TestCase("SELECT id FROM r WHERE v < ANY (SELECT w FROM r2 WHERE w = v + 10)", "A correlated subquery in an ANY, SOME or ALL comparison is not supported yet: it reads 'v' of the outer query")]
    [TestCase("SELECT id, EXISTS (SELECT 1 FROM r2 WHERE w = v + 10) FROM r", "A correlated EXISTS subquery is not supported in the select list; it is supported in the WHERE clause of a SELECT")]
    public async Task ACorrelatedFormThatDoesNotRunYet_IsRefused_AndNamesTheOuterName(string sql, string message)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported + ": " + message, await Query(database, executor, dbname, sql), sql);
    }

    /// <summary>A subquery of an UPDATE or a DELETE reads the target table by its bare names too.</summary>
    [TestCase("DELETE FROM r WHERE EXISTS (SELECT 1 FROM r2 WHERE w = v + 10)", "A correlated EXISTS subquery is not supported in the WHERE of a DELETE; it is supported in the WHERE clause of a SELECT")]
    [TestCase("UPDATE r SET v = (SELECT w FROM r2 WHERE r2.id = r.id) WHERE id = 1", "A correlated scalar subquery is not supported yet: it reads 'r.id' of the outer query")]
    [TestCase("UPDATE r SET v = 5 WHERE id IN (SELECT id FROM r2 WHERE w = v + 10)", "A correlated IN or NOT IN subquery is not supported yet: it reads 'v' of the outer query")]
    public async Task ACorrelatedSubqueryOfAWrite_IsRefused_AndWritesNothing(string sql, string message)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported + ": " + message, await Write(database, executor, dbname, sql), sql);
        Assert.AreEqual("1 ; 2 ; 3", await Query(database, executor, dbname, "SELECT id FROM r WHERE v IN (0, 2)"));
    }

    /// <summary>An uncorrelated subquery that writes a name of the outer query only in its own scope still runs once.</summary>
    [Test]
    public async Task AnUncorrelatedSubquery_InAWrite_StillRuns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual("written", await Write(database, executor, dbname, "UPDATE r SET v = (SELECT w FROM r2 WHERE id = 2) WHERE id IN (SELECT id FROM r2)"));
    }
    /// <summary>
    /// The correlated evaluator reads the WHERE of the body row by row, and it cannot run a subquery in it.
    /// Before, each form failed with the internal error <c>CADB0099</c>. PostgreSQL 16.11 gives
    /// <c>1 ; 3</c>, <c>1</c> and <c>1</c>.
    /// </summary>
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 WHERE w IN (SELECT w FROM r2 WHERE w = v + 10))")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 i WHERE i.id = r.id AND i.w IN (SELECT 10))")]
    [TestCase("SELECT id FROM r WHERE EXISTS (SELECT 1 FROM r2 i WHERE i.id = r.id AND i.w = (SELECT 10))")]
    public async Task ACorrelatedExistsWithASubqueryInItsWhere_IsRefused(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.AreEqual(
            CamusDBErrorCodes.FeatureNotSupported + ": A correlated EXISTS subquery with a subquery in its WHERE is not supported yet",
            await Query(database, executor, dbname, sql),
            sql);
    }
}
