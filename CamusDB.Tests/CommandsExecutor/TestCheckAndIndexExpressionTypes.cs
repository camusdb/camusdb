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
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A CHECK condition and an expression-index key part are evaluated with no binding. Before they were
/// typed from their table's columns, a NULL hid the type of its column there, and the type came from
/// the values of each row: <c>COALESCE(i, f)</c> was an INT64 in a row where <c>f</c> is NULL, so
/// <c>/ 4</c> divided as integers. A CHECK then refused a row that a SELECT of the same condition
/// accepts, and an index stored a key that a SELECT of the same expression does not compute. Each test
/// here gives an answer that only the column types give.
/// </summary>
[NonParallelizable]
public sealed class TestCheckAndIndexExpressionTypes : SharedNodeBaseTest
{
    // ── CHECK constraints ───────────────────────────────────────────────────

    [TestCase("COALESCE(i, f) / 4 > 0.1", TestName = "Check_Coalesce_IsFloat64WhereTheFloatIsNull")]
    [TestCase("CASE WHEN i IS NOT NULL THEN i ELSE f END / 4 > 0.1", TestName = "Check_Case_IsFloat64WhereTheFloatIsNull")]
    [TestCase("IFNULL(i, f) / 4 > 0.1", TestName = "Check_Ifnull_IsFloat64WhereTheFloatIsNull")]
    [TestCase("GREATEST(i, f) / 4 > 0.1", TestName = "Check_Greatest_IsFloat64WhereTheFloatIsNull")]
    public async Task ACheck_UsesTheColumnTypes(string condition)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            $"CREATE TABLE checked (id int64 PRIMARY KEY, i int64 NULL, f float64 NULL, CHECK ({condition}))");

        // 2 / 4 is 0 as integers and 0.5 as FLOAT64.
        await ExecNonQuery(database, executor, dbname, "INSERT INTO checked (id, i, f) VALUES (1, 2, NULL)");
        await ExecNonQuery(database, executor, dbname, "UPDATE checked SET i = 3 WHERE id = 1");

        CamusDBException violation = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO checked (id, i, f) VALUES (2, 0, NULL)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, violation.Code);
    }

    [Test]
    public async Task AddConstraint_JudgesExistingRowsWithTheColumnTypes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE later (id int64 PRIMARY KEY, i int64 NULL, f float64 NULL)");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO later (id, i, f) VALUES (1, 2, NULL)");

        await ExecDdl(database, executor, dbname, "ALTER TABLE later ADD CONSTRAINT later_ratio CHECK (COALESCE(i, f) / 4 > 0.1)");

        CamusDBException violation = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO later (id, i, f) VALUES (2, 0, NULL)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, violation.Code);
    }

    /// <summary>
    /// The same condition on two tables whose <c>f</c> has another type: each constraint is typed from
    /// its own table's columns, so the integer table divides as integers and the float table does not.
    /// </summary>
    [Test]
    public async Task TheSameCheck_OnTwoTables_IsTypedFromEachTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE floats (id int64 PRIMARY KEY, i int64 NULL, f float64 NULL, CHECK (COALESCE(i, f) / 4 = 0))");
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE ints (id int64 PRIMARY KEY, i int64 NULL, f int64 NULL, CHECK (COALESCE(i, f) / 4 = 0))");

        await ExecNonQuery(database, executor, dbname, "INSERT INTO ints (id, i, f) VALUES (1, 1, NULL)");

        CamusDBException violation = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecNonQuery(database, executor, dbname, "INSERT INTO floats (id, i, f) VALUES (1, 1, NULL)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, violation.Code);
    }

    // ── Expression indexes ──────────────────────────────────────────────────

    /// <summary>
    /// The index declares NUMERIC for <c>COALESCE(i, p)</c>. A row with a NULL <c>p</c> gave an INT64,
    /// which the index refused, so the INSERT failed.
    /// </summary>
    [Test]
    public async Task AnIndex_AcceptsARowWhereTheWiderColumnIsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE prices (id int64 PRIMARY KEY, i int64 NULL, p numeric NULL)");
        await ExecDdl(database, executor, dbname, "CREATE INDEX prices_amount ON prices ((COALESCE(i, p)))");

        await ExecNonQuery(database, executor, dbname, "INSERT INTO prices (id, i, p) VALUES (1, 5, NULL), (2, NULL, NUMERIC '7.5')");

        CollectionAssert.AreEqual(new long[] { 1 }, await Ids(database, executor, dbname, "prices", "COALESCE(i, p) = 5"));
    }

    /// <summary>
    /// The key of row 2 must be 0.5, the value a SELECT computes, so a seek on the index finds it. Before,
    /// the key was 0 and the seek missed the row. An UPDATE then moves the entry, which needs the old key
    /// to be computed the same way.
    /// </summary>
    [Test]
    public async Task AnIndex_StoresTheKeyASelectComputes_AndAnUpdateMovesIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE ratios (id int64 PRIMARY KEY, i int64 NULL, f float64 NULL)");
        await ExecDdl(database, executor, dbname, "CREATE INDEX ratios_quarter ON ratios ((COALESCE(i, f) / 4))");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO ratios (id, i, f) VALUES (1, 1, 2.5), (2, 2, NULL), (3, NULL, NULL)");

        const string seek = "SELECT id FROM ratios WHERE COALESCE(i, f) / 4 = 0.5";
        StringAssert.Contains("ratios_quarter", await PlanOf(database, executor, dbname, seek));

        CollectionAssert.AreEqual(new long[] { 2 }, await Ids(database, executor, dbname, "ratios", "COALESCE(i, f) / 4 = 0.5"));

        await ExecNonQuery(database, executor, dbname, "UPDATE ratios SET i = 4 WHERE id = 2");

        CollectionAssert.IsEmpty(await Ids(database, executor, dbname, "ratios", "COALESCE(i, f) / 4 = 0.5"));
        CollectionAssert.AreEqual(new long[] { 2 }, await Ids(database, executor, dbname, "ratios", "COALESCE(i, f) / 4 = 1.0"));

        await ExecNonQuery(database, executor, dbname, "DELETE FROM ratios WHERE id = 2");
        CollectionAssert.IsEmpty(await Ids(database, executor, dbname, "ratios", "COALESCE(i, f) / 4 = 1.0"));
    }

    [Test]
    public async Task AnIndexBuiltOverExistingRows_StoresTheKeyASelectComputes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE ratios (id int64 PRIMARY KEY, i int64 NULL, f float64 NULL)");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO ratios (id, i, f) VALUES (1, 1, 2.5), (2, 2, NULL), (3, NULL, NULL)");
        await ExecDdl(database, executor, dbname, "CREATE INDEX ratios_quarter ON ratios ((CASE WHEN i IS NOT NULL THEN i ELSE f END / 4))");

        const string where = "CASE WHEN i IS NOT NULL THEN i ELSE f END / 4 = 0.5";
        StringAssert.Contains("ratios_quarter", await PlanOf(database, executor, dbname, $"SELECT id FROM ratios WHERE {where}"));

        CollectionAssert.AreEqual(new long[] { 2 }, await Ids(database, executor, dbname, "ratios", where));
    }

    // ── WITH CHECK OPTION ───────────────────────────────────────────────────

    /// <summary>
    /// A row written through the view must be one the view shows. A SELECT through the view computes
    /// <c>2 / 4</c> as 0.5 for a row where <c>f</c> is NULL, so the check option must too, also for a
    /// cascaded view that checks the condition of the view beneath it.
    /// </summary>
    [Test]
    public async Task ACheckOption_UsesTheColumnTypes_DirectlyAndCascaded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE base (id int64 PRIMARY KEY, i int64 NULL, f float64 NULL)");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW quarter AS SELECT id, i, f FROM base WHERE COALESCE(i, f) / 4 > 0.1 WITH CHECK OPTION");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW quarter_all AS SELECT id, i, f FROM quarter WITH CASCADED CHECK OPTION");

        await ExecNonQuery(database, executor, dbname, "INSERT INTO quarter (id, i, f) VALUES (1, 2, NULL)");
        await ExecNonQuery(database, executor, dbname, "UPDATE quarter SET i = 3 WHERE id = 1");
        await ExecNonQuery(database, executor, dbname, "INSERT INTO quarter_all (id, i, f) VALUES (2, 2, NULL)");

        CollectionAssert.AreEqual(new long[] { 1, 2 }, await Ids(database, executor, dbname, "quarter", "TRUE"));

        foreach (string sql in new[]
                 {
                     "INSERT INTO quarter (id, i, f) VALUES (3, 0, NULL)",
                     "INSERT INTO quarter_all (id, i, f) VALUES (3, 0, NULL)",
                     "UPDATE quarter SET i = 0 WHERE id = 1",
                 })
        {
            CamusDBException violation = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(database, executor, dbname, sql), sql)!;
            Assert.AreEqual(CamusDBErrorCodes.ViewCheckOptionViolated, violation.Code, sql);
        }
    }

    /// <summary>
    /// The same view over a base table whose <c>f</c> is FLOAT64 in one database and INT64 in the other:
    /// each write types its condition from its own base table, in either order.
    /// </summary>
    [Test]
    public async Task TheSameView_InTwoDatabases_IsTypedFromEachBaseTable()
    {
        (string floatsDb, DatabaseDescriptor floats, CommandExecutor executor) = await CreateDatabase();
        (string intsDb, DatabaseDescriptor ints, _) = await CreateDatabase();

        await ExecDdl(floats, executor, floatsDb, "CREATE TABLE base (id int64 PRIMARY KEY, i int64 NULL, f float64 NULL)");
        await ExecDdl(ints, executor, intsDb, "CREATE TABLE base (id int64 PRIMARY KEY, i int64 NULL, f int64 NULL)");

        const string view = "CREATE VIEW zero AS SELECT id, i, f FROM base WHERE COALESCE(i, f) / 4 = 0 WITH CHECK OPTION";
        await ExecDdl(floats, executor, floatsDb, view);
        await ExecDdl(ints, executor, intsDb, view);

        for (int round = 0; round < 2; round++)
        {
            // 1 / 4 is 0.25 as FLOAT64, so the row is not visible through the view.
            CamusDBException violation = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(floats, executor, floatsDb, $"INSERT INTO zero (id, i, f) VALUES ({round + 1}, 1, NULL)"))!;
            Assert.AreEqual(CamusDBErrorCodes.ViewCheckOptionViolated, violation.Code);

            // 1 / 4 is 0 as INT64, so it is.
            await ExecNonQuery(ints, executor, intsDb, $"INSERT INTO zero (id, i, f) VALUES ({round + 1}, 1, NULL)");
        }

        CollectionAssert.AreEqual(new long[] { 1, 2 }, await Ids(ints, executor, intsDb, "zero", "TRUE"));
    }

    /// <summary>
    /// A view body stored before relations were named by id can have the same text, and so the same
    /// cached tree, in two databases. Each write types its own copy of the condition, so typing it for
    /// a FLOAT64 column leaves the shared tree untyped and does not reach a copy typed for an INT64
    /// column. Driven directly: two databases on one node never share a body text today.
    /// </summary>
    [Test]
    public void ACheckOptionCopy_KeepsItsTypesOffTheSharedTree()
    {
        Core.SQLParser.NodeAst shared = Core.SQLParser.SQLParserProcessor.ParseCondition("COALESCE(i, f) / 4 = 0");

        Core.SQLParser.NodeAst floats = Core.CommandsExecutor.Controllers.Queries.OwnedExpressionTypes.TypedCopy(
            shared, name => name == "f" ? new ExpressionType(Core.Catalogs.Models.ColumnType.Float64) : new ExpressionType(Core.Catalogs.Models.ColumnType.Integer64));
        Core.SQLParser.NodeAst ints = Core.CommandsExecutor.Controllers.Queries.OwnedExpressionTypes.TypedCopy(
            shared, static _ => new ExpressionType(Core.Catalogs.Models.ColumnType.Integer64));

        Assert.AreNotSame(shared, floats);
        Assert.AreNotSame(shared, ints);

        Assert.AreEqual(Core.Catalogs.Models.ColumnType.Float64, ResultTypeOfCoalesce(floats));
        Assert.AreEqual(Core.Catalogs.Models.ColumnType.Integer64, ResultTypeOfCoalesce(ints));
        Assert.IsNull(ResultTypeOfCoalesce(shared));
    }

    /// <summary>The type stored for the COALESCE node of <c>COALESCE(i, f) / 4 = 0</c>, or null.</summary>
    private static Core.Catalogs.Models.ColumnType? ResultTypeOfCoalesce(Core.SQLParser.NodeAst condition)
    {
        Core.SQLParser.NodeAst coalesce = condition.leftAst!.leftAst!;
        Assert.AreEqual(Core.SQLParser.NodeType.ExprFuncCall, coalesce.nodeType);

        return Core.CommandsExecutor.Controllers.Queries.OwnedExpressionTypes.TryGetConditionalResultType(coalesce, out ExpressionType type)
            ? type.Type
            : null;
    }

    // ── Harness ─────────────────────────────────────────────────────────────

    private static async Task<long[]> Ids(DatabaseDescriptor database, CommandExecutor executor, string dbname, string table, string where)
    {
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, $"SELECT id FROM {table} WHERE {where} ORDER BY id");
        return rows.Select(r => r.Row["id"].LongValue).ToArray();
    }

    private static async Task<string> PlanOf(DatabaseDescriptor database, CommandExecutor executor, string dbname, string select)
    {
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "EXPLAIN " + select);
        return string.Join("\n", rows.Select(r => $"{r.Row["node"].StrValue} {r.Row["detail"].StrValue}"));
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
