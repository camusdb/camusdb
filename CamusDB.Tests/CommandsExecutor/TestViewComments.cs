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

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>COMMENT ON VIEW</c> and <c>COMMENT ON MATERIALIZED VIEW</c>, read back through
/// <c>SHOW VIEWS</c> and <c>SHOW MATERIALIZED VIEWS</c>. A view stored a comment field that no
/// statement could set.
/// </summary>
[NonParallelizable]
public sealed class TestViewComments : SharedNodeBaseTest
{
    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<ColumnValue> ShownComment(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string statement, string nameColumn, string name)
    {
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, statement);
        return rows.Single(r => r.Row[nameColumn].StrValue == name).Row["comment"];
    }

    private static async Task Seed(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname, "CREATE TABLE orders (id int64 PRIMARY KEY, total int64)");
        await ExecDdl(database, executor, dbname, "CREATE VIEW big AS SELECT id FROM orders WHERE total > 100");
        await ExecDdl(database, executor, dbname, "CREATE MATERIALIZED VIEW totals AS SELECT SUM(total) AS spent FROM orders");
    }

    [Test]
    public async Task ACommentOnAViewIsSetRemovedAndKeptDistinctFromAnEmptyOne()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        Assert.AreEqual(ColumnType.Null, (await ShownComment(database, executor, dbname, "SHOW VIEWS", "views", "big")).Type);

        await ExecDdl(database, executor, dbname, "COMMENT ON VIEW big IS 'orders over 100'");
        Assert.AreEqual("orders over 100", (await ShownComment(database, executor, dbname, "SHOW VIEWS", "views", "big")).StrValue);

        // The non-query entry point runs the same statement.
        await ExecNonQuery(database, executor, dbname, "COMMENT ON VIEW big IS ''");
        ColumnValue empty = await ShownComment(database, executor, dbname, "SHOW VIEWS", "views", "big");
        Assert.AreEqual(ColumnType.String, empty.Type);
        Assert.AreEqual("", empty.StrValue, "IS '' stores a present but empty comment");

        await ExecDdl(database, executor, dbname, "COMMENT ON VIEW big IS NULL");
        Assert.AreEqual(ColumnType.Null, (await ShownComment(database, executor, dbname, "SHOW VIEWS", "views", "big")).Type);
    }

    /// <summary>
    /// A comment describes the view, not one body of it, and it is persisted with the view: a replace
    /// and a rename keep it, and so does a reopen that reads the schema back from storage.
    /// </summary>
    [Test]
    public async Task AViewCommentSurvivesReplaceRenameAndReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "COMMENT ON VIEW big IS 'kept'");
        await ExecDdl(database, executor, dbname, "CREATE OR REPLACE VIEW big AS SELECT id FROM orders WHERE total > 200");
        await ExecDdl(database, executor, dbname, "ALTER VIEW big RENAME TO bigger");

        Assert.AreEqual("kept", (await ShownComment(database, executor, dbname, "SHOW VIEWS", "views", "bigger")).StrValue);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.AreEqual("kept", (await ShownComment(reopened, executor, dbname, "SHOW VIEWS", "views", "bigger")).StrValue);
        Assert.AreEqual("kept", reopened.Schema.Views["bigger"].Comment);
    }

    [Test]
    public async Task ACommentOnAMaterializedViewIsTheRelationsOwn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        await ExecDdl(database, executor, dbname, "COMMENT ON MATERIALIZED VIEW totals IS 'nightly rollup'");
        Assert.AreEqual("nightly rollup",
            (await ShownComment(database, executor, dbname, "SHOW MATERIALIZED VIEWS", "materialized_views", "totals")).StrValue);

        // A refresh replaces the contents, not the relation, so the comment stays.
        await ExecNonQuery(database, executor, dbname, "REFRESH MATERIALIZED VIEW totals");
        Assert.AreEqual("nightly rollup",
            (await ShownComment(database, executor, dbname, "SHOW MATERIALIZED VIEWS", "materialized_views", "totals")).StrValue);

        await ExecNonQuery(database, executor, dbname, "COMMENT ON MATERIALIZED VIEW totals IS NULL");
        Assert.AreEqual(ColumnType.Null,
            (await ShownComment(database, executor, dbname, "SHOW MATERIALIZED VIEWS", "materialized_views", "totals")).Type);
    }

    /// <summary>Each kind of object takes its own statement, and the refusal names the right one.</summary>
    [Test]
    public async Task EachStatementRefusesTheOtherKind()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Seed(database, executor, dbname);

        CamusDBException? onMaterialized = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "COMMENT ON VIEW totals IS 'x'"));
        Assert.AreEqual(CamusDBErrorCodes.ViewDoesntExist, onMaterialized!.Code);
        StringAssert.Contains("COMMENT ON MATERIALIZED VIEW", onMaterialized.Message);

        CamusDBException? onTable = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "COMMENT ON VIEW orders IS 'x'"));
        StringAssert.Contains("COMMENT ON TABLE", onTable!.Message);

        CamusDBException? onView = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "COMMENT ON MATERIALIZED VIEW big IS 'x'"));
        Assert.AreEqual(CamusDBErrorCodes.ViewDoesntExist, onView!.Code);

        CamusDBException? missing = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "COMMENT ON VIEW nothing_here IS 'x'"));
        Assert.AreEqual(CamusDBErrorCodes.ViewDoesntExist, missing!.Code);
    }
}
