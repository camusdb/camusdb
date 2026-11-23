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
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An UPDATE, a DELETE and an upsert DO UPDATE have no binding, so before they had a statement record
/// they typed CASE, COALESCE and <c>ARRAY[…]</c> from the values of each row. A NULL hid the type of its
/// column: <c>COALESCE(i, f)</c> was an INT64 in a row where <c>f</c> is NULL, so <c>/ 4</c> divided as
/// integers, and <c>ARRAY[i, f]</c> was an <c>ARRAY&lt;Integer64&gt;</c> that did not compare with an
/// <c>ARRAY&lt;Float64&gt;</c>. A SELECT typed both from the columns. Each test here gives an answer
/// that only the column types give.
/// </summary>
[NonParallelizable]
public sealed class TestUpdateDeleteExpressionTypes : SharedNodeBaseTest
{
    // ── ARRAY[...] ──────────────────────────────────────────────────────────

    [Test]
    public async Task UpdateWhere_AnArrayOverTwoColumnTypes_HasOneElementTypeInEveryRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecNonQuery(database, executor, dbname, "UPDATE nums SET n = 1 WHERE ARRAY[i, f] = ARRAY[1.0, 2.5]");

        CollectionAssert.AreEqual(new long[] { 1 }, await Ids(database, executor, dbname, "WHERE n = 1"));
    }

    [Test]
    public async Task DeleteWhere_AnArrayOverTwoColumnTypes_HasOneElementTypeInEveryRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecNonQuery(database, executor, dbname, "DELETE FROM nums WHERE ARRAY[i, f] = ARRAY[1.0, 2.5]");

        CollectionAssert.AreEqual(new long[] { 2, 3 }, await Ids(database, executor, dbname, ""));
    }

    // ── CASE and COALESCE ───────────────────────────────────────────────────

    [Test]
    public async Task UpdateSet_Coalesce_IsFloat64InARowWhereTheFloatIsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecNonQuery(database, executor, dbname, "UPDATE nums SET g = COALESCE(i, f) / 4 WHERE TRUE");

        CollectionAssert.AreEqual(new double?[] { 0.25, 0.5, null }, await Gs(database, executor, dbname));
    }

    [Test]
    public async Task UpdateWhere_Case_IsFloat64InEveryRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecNonQuery(database, executor, dbname,
            "UPDATE nums SET n = 2 WHERE CASE WHEN id > 0 THEN i ELSE f END / 4 > 0.2");

        CollectionAssert.AreEqual(new long[] { 1, 2 }, await Ids(database, executor, dbname, "WHERE n = 2"));
    }

    [Test]
    public async Task DeleteWhere_Coalesce_IsFloat64InARowWhereTheFloatIsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecNonQuery(database, executor, dbname, "DELETE FROM nums WHERE COALESCE(i, f) / 4 = 0.5");

        CollectionAssert.AreEqual(new long[] { 1, 3 }, await Ids(database, executor, dbname, ""));
    }

    [Test]
    public async Task UpsertDoUpdate_Coalesce_IsFloat64()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO nums (id, i, f) VALUES (2, 7, NULL) ON CONFLICT (id) DO UPDATE SET g = COALESCE(excluded.i, nums.f) / 4");

        CollectionAssert.AreEqual(new double?[] { null, 1.75, null }, await Gs(database, executor, dbname));
    }

    // ── UPDATE ... FROM and DELETE ... USING ────────────────────────────────

    [Test]
    public async Task UpdateFrom_SetValue_IsFloat64()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);
        await SeedPicks(database, executor, dbname);

        await ExecNonQuery(database, executor, dbname,
            "UPDATE nums SET g = COALESCE(nums.i, nums.f) / 4 FROM picks p WHERE p.id = nums.id");

        CollectionAssert.AreEqual(new double?[] { 0.25, 0.5, null }, await Gs(database, executor, dbname));
    }

    /// <summary>
    /// The read phase is a SELECT and finds row 2 with the column types. The re-check of the locked row
    /// has no binding, and before the statement record it divided as integers there, so it judged that
    /// the row no longer matched and kept it.
    /// </summary>
    [Test]
    public async Task DeleteUsing_TheReCheck_AgreesWithTheReadPhase()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);
        await SeedPicks(database, executor, dbname);

        await ExecNonQuery(database, executor, dbname,
            "DELETE FROM nums USING picks p WHERE p.id = nums.id AND COALESCE(nums.i, nums.f) / 4 = 0.5");

        CollectionAssert.AreEqual(new long[] { 1, 3 }, await Ids(database, executor, dbname, ""));
    }

    // ── The record is current only while its statement runs ──────────────────

    [Test]
    public void ARecord_CannotBeWrittenAfterItIsEntered()
    {
        StatementExpressionTypes types = new();
        types.RecordConditionalResultType(new(Core.SQLParser.NodeType.Null, null, null, null, null, null, null, null, null), new ExpressionType(ColumnType.Float64));

        using (StatementExpressionTypes.Enter(types))
        {
            Assert.AreSame(types, StatementExpressionTypes.Current);

            CamusDBException ex = Assert.Throws<CamusDBException>(() =>
                types.RecordArrayElementType(new(Core.SQLParser.NodeType.Null, null, null, null, null, null, null, null, null), new ExpressionType(ColumnType.Float64)))!;
            Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, ex.Code);

            // A nested statement with no record hides the outer one until it ends.
            using (StatementExpressionTypes.Enter(new StatementExpressionTypes()))
                Assert.IsNull(StatementExpressionTypes.Current);

            Assert.AreSame(types, StatementExpressionTypes.Current);
        }

        Assert.IsNull(StatementExpressionTypes.Current);
    }

    // ── Harness ─────────────────────────────────────────────────────────────

    /// <summary>
    /// <c>nums (id, i int64, f float64, n int64, g float64)</c> with rows (1, 1, 2.5), (2, 2, NULL) and
    /// (3, NULL, NULL); <c>n</c> and <c>g</c> start NULL.
    /// </summary>
    private static async Task Seed(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE nums (id int64 PRIMARY KEY, i int64 NULL, f float64 NULL, n int64 NULL, g float64 NULL)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO nums (id, i, f) VALUES (1, 1, 2.5), (2, 2, NULL), (3, NULL, NULL)");
    }

    /// <summary><c>picks (id)</c> naming every row of <c>nums</c>.</summary>
    private static async Task SeedPicks(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname, "CREATE TABLE picks (id int64 PRIMARY KEY)");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO picks (id) VALUES (1), (2), (3)");
    }

    private static async Task<long[]> Ids(DatabaseDescriptor database, CommandExecutor executor, string dbname, string where)
    {
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, $"SELECT id FROM nums {where} ORDER BY id");
        return rows.Select(r => r.Row["id"].LongValue).ToArray();
    }

    private static async Task<double?[]> Gs(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT g FROM nums ORDER BY id");
        return rows.Select(r => r.Row["g"].Type == ColumnType.Null ? (double?)null : r.Row["g"].FloatValue).ToArray();
    }

    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            _ = await executor.ExecuteNonSQLQuery(new(txnState: tx, database: dbname, sql: sql, parameters: null));
            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<List<QueryResultRow>> ExecQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket, null, null);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }
}
