/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Views and materialized views whose body is a set operation. A view body is stored as rendered
/// text with its relations and columns bound by id, so each case checks what a stored body must
/// keep: the rows, the frozen columns of every operand, the published names, and the column
/// dependencies through both operands, across a reopen and across renames.
///
/// <para>Cluster mode here; <see cref="TestSetOperationViewsStandalone"/> runs every case in
/// standalone mode.</para>
/// </summary>
[NonParallelizable]
public class TestSetOperationViews : SetOperationsTestBase
{
    private static async Task<CamusDBException> DdlFails(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql, string code)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null)), sql)!;
        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(code, ex.Code, sql + ": " + ex.Message);
        return ex;
    }

    private static async Task<DatabaseDescriptor> Reopen(CommandExecutor executor, string dbname)
    {
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        return await executor.OpenDatabase(dbname);
    }

    [Test]
    public async Task AViewOverEachForm_ReturnsTheRowsOfTheQuery()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        string[] operators = ["UNION ALL", "UNION", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"];

        for (int i = 0; i < operators.Length; i++)
        {
            string body = $"SELECT x FROM a {operators[i]} SELECT y FROM b";
            await ExecDdl(executor, database, dbname, $"CREATE VIEW v{i} AS {body}");

            CollectionAssert.AreEqual(
                await SortedLongs(executor, database, dbname, body, "x"),
                await SortedLongs(executor, database, dbname, $"SELECT x FROM v{i}", "x"),
                operators[i]);
        }
    }

    [Test]
    public async Task AStoredBody_SurvivesAReopen_AndShowCreateViewReparses()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        await ExecDdl(executor, database, dbname,
            "CREATE VIEW top3 AS (SELECT x AS n FROM a WHERE x > 1 ORDER BY n LIMIT 1) " +
            "UNION SELECT y FROM b WHERE y IS NOT NULL EXCEPT SELECT 4 ORDER BY n DESC LIMIT 3");

        List<long?> expected = [2, 1];
        CollectionAssert.AreEqual(expected, await Longs(executor, database, dbname, "SELECT n FROM top3", "n"));

        database = await Reopen(executor, dbname);
        CollectionAssert.AreEqual(expected, await Longs(executor, database, dbname, "SELECT n FROM top3", "n"));

        List<QueryResultRow> shown = await ExecQuery(executor, database, dbname, "SHOW CREATE VIEW top3");
        string text = shown[0].Row["create view"].StrValue!;

        StringAssert.Contains("UNION", text);
        StringAssert.Contains("EXCEPT", text);
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse(text), text);

        // A view over the view reads it like any relation.
        await ExecDdl(executor, database, dbname, "CREATE VIEW over_top3 AS SELECT n FROM top3 WHERE n > 1");
        CollectionAssert.AreEqual(new long?[] { 2 }, await Longs(executor, database, dbname, "SELECT n FROM over_top3", "n"));
    }

    [Test]
    public async Task TheStarOfEveryOperand_IsFrozenAtCreation()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        await ExecDdl(executor, database, dbname, "CREATE VIEW both_tables AS SELECT * FROM a UNION ALL SELECT * FROM b");

        await ExecDdl(executor, database, dbname, "ALTER TABLE a ADD COLUMN z int64 NULL");

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT * FROM both_tables");
        Assert.AreEqual(12, rows.Count, "a column added to one operand's table must not change the view");
        CollectionAssert.AreEquivalent(new[] { "id", "x" }, rows[0].Row.Keys);
    }

    [Test]
    public async Task AColumnAliasList_RenamesTheOutput_AndTheOrderByStillApplies()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        await ExecDdl(executor, database, dbname,
            "CREATE VIEW pairs (n, k) AS SELECT x, id FROM a WHERE x IS NOT NULL " +
            "UNION SELECT y, id FROM b WHERE y IS NOT NULL ORDER BY x, id LIMIT 3");

        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT n, k FROM pairs");

        CollectionAssert.AreEqual(new long[] { 1, 1, 1 }, rows.Select(r => r.Row["n"].LongValue));
        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, rows.Select(r => r.Row["k"].LongValue));
    }

    [Test]
    public async Task RenamingABaseColumn_IsTransparent()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        await ExecDdl(executor, database, dbname, "CREATE VIEW merged AS SELECT x FROM a UNION SELECT y FROM b");

        await ExecDdl(executor, database, dbname, "ALTER TABLE a RENAME COLUMN x TO xx");
        await ExecDdl(executor, database, dbname, "ALTER TABLE b RENAME COLUMN y TO yy");
        await ExecDdl(executor, database, dbname, "ALTER TABLE b RENAME TO bb");

        CollectionAssert.AreEqual(new long?[] { null, 1, 2, 3, 4 },
            await SortedLongs(executor, database, dbname, "SELECT x FROM merged", "x"),
            "the view keeps its column name and reads both operands through the new names");

        List<QueryResultRow> shown = await ExecQuery(executor, database, dbname, "SHOW CREATE VIEW merged");
        StringAssert.Contains("SELECT xx AS x FROM a UNION SELECT yy FROM bb AS b", shown[0].Row["create view"].StrValue!);
    }

    [Test]
    public async Task ColumnDependencies_CoverBothOperands()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        await ExecDdl(executor, database, dbname,
            "CREATE VIEW deps AS SELECT x FROM a INTERSECT SELECT y FROM b WHERE id > 0");

        await DdlFails(executor, database, dbname, "ALTER TABLE a DROP COLUMN x", CamusDBErrorCodes.DependentObjectsExist);
        await DdlFails(executor, database, dbname, "ALTER TABLE b DROP COLUMN y", CamusDBErrorCodes.DependentObjectsExist);
        await DdlFails(executor, database, dbname, "DROP TABLE b", CamusDBErrorCodes.DependentObjectsExist);

        // A column that no operand reads is free to go.
        await ExecDdl(executor, database, dbname, "ALTER TABLE a ADD COLUMN spare int64 NULL");
        await ExecDdl(executor, database, dbname, "ALTER TABLE a DROP COLUMN spare");
    }

    [Test]
    public async Task AMaterializedView_IsCreatedAndRefreshed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        await ExecDdl(executor, database, dbname, "CREATE MATERIALIZED VIEW mv AS SELECT x AS v FROM a UNION SELECT y FROM b");

        CollectionAssert.AreEqual(new long?[] { null, 1, 2, 3, 4 },
            await SortedLongs(executor, database, dbname, "SELECT v FROM mv", "v"));

        await ExecNonQuery(executor, database, dbname, "INSERT INTO a (id, x) VALUES (7, 9)");
        await ExecNonQuery(executor, database, dbname, "REFRESH MATERIALIZED VIEW mv");

        CollectionAssert.AreEqual(new long?[] { null, 1, 2, 3, 4, 9 },
            await SortedLongs(executor, database, dbname, "SELECT v FROM mv", "v"));

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "mv"));
        Assert.AreEqual(ColumnType.Integer64, table.Schema.Columns!.First(c => c.Name == "v").Type);
    }

    [Test]
    public async Task ASetOperationInsideTheBody_IsStoredToo()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        await ExecDdl(executor, database, dbname,
            "CREATE VIEW inner_set AS SELECT s.x AS v FROM (SELECT x FROM a UNION SELECT y FROM b) AS s WHERE s.x IS NOT NULL");
        CollectionAssert.AreEqual(new long?[] { 1, 2, 3, 4 },
            await SortedLongs(executor, database, dbname, "SELECT v FROM inner_set", "v"));

        database = await Reopen(executor, dbname);
        CollectionAssert.AreEqual(new long?[] { 1, 2, 3, 4 },
            await SortedLongs(executor, database, dbname, "SELECT v FROM inner_set", "v"));
    }

    [Test]
    public async Task ABodyThatCannotBeFrozen_OrNamed_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedTwoTables(executor, database, dbname);

        // A '*' inside a derived table cannot be expanded at creation.
        await DdlFails(executor, database, dbname,
            "CREATE VIEW nested_star AS SELECT s.x FROM (SELECT * FROM a UNION SELECT * FROM b) AS s",
            CamusDBErrorCodes.InvalidInput);

        // The leftmost operand names the columns, so an unnamed expression there needs an alias.
        await DdlFails(executor, database, dbname,
            "CREATE VIEW unnamed AS SELECT x + 1 FROM a UNION SELECT y FROM b",
            CamusDBErrorCodes.InvalidInput);

        // An alias on a later operand names nothing.
        await ExecDdl(executor, database, dbname, "CREATE VIEW named AS SELECT x + 1 AS n FROM a UNION SELECT y + 1 FROM b");
        CollectionAssert.AreEqual(new long?[] { null, 2, 3, 4, 5 },
            await SortedLongs(executor, database, dbname, "SELECT n FROM named", "n"));
    }
}

/// <summary>Every <see cref="TestSetOperationViews"/> case in standalone mode.</summary>
[NonParallelizable]
public sealed class TestSetOperationViewsStandalone : TestSetOperationViews
{
    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);
    }
}
