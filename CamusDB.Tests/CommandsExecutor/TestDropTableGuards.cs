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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The rules that refuse a <c>DROP TABLE</c> — a view reads the table, or the relation is a
/// materialized view — hold on every entry point: the SQL statement, with and without FORCE, and the
/// ticket API that a forwarded statement runs through on the schema leader. A refused drop, even one
/// refused after the dropper purged the index entries, leaves the table whole: its indexes, its
/// unique enforcement and its rows.
/// </summary>
internal static class DropTableGuardScenarios
{
    private const string CreateCities =
        "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, population int64, UNIQUE KEY cities_name (name))";

    public static async Task DropOfATableAViewReadsIsRefusedOnEveryEntryPoint(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);
        await Ddl(executor, database, dbname, "CREATE VIEW big_cities AS SELECT id, name FROM cities WHERE population > 5");
        await Ddl(executor, database, dbname, "CREATE MATERIALIZED VIEW city_names AS SELECT name FROM cities");

        int indexes = database.Schema.Tables["cities"].Indexes!.Count;

        foreach (string sql in new[] { "DROP TABLE cities", "DROP TABLE cities FORCE", "DROP TABLE IF EXISTS cities FORCE" })
        {
            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, database, dbname, sql))!;

            Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, exception.Code, sql);
            Assert.That(exception.Message, Does.Contain("big_cities"), sql);
            Assert.That(exception.Message, Does.Contain("city_names"), sql);
        }

        // The ticket API is what a forwarded statement runs through on the leader, and what the
        // engine's own callers use. It meets the same rule.
        foreach (bool force in new[] { false, true })
        {
            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.DropTable(new DropTableTicket(dbname, "cities", ifExists: false, force: force)))!;

            Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, exception.Code, $"ticket force={force}");
        }

        await AssertTableIsWhole(executor, database, dbname, indexes);

        // Once nothing reads the table, it can go.
        await Ddl(executor, database, dbname, "DROP VIEW big_cities");
        await Ddl(executor, database, dbname, "DROP MATERIALIZED VIEW city_names");
        await Ddl(executor, database, dbname, "DROP TABLE cities FORCE");
        Assert.IsFalse(database.Schema.Tables.ContainsKey("cities"));
    }

    public static async Task DropTableOnAMaterializedViewIsRefusedOnEveryEntryPoint(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);
        await Ddl(executor, database, dbname, "CREATE MATERIALIZED VIEW city_names AS SELECT id, name FROM cities");

        foreach (string sql in new[] { "DROP TABLE city_names", "DROP TABLE city_names FORCE" })
        {
            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, database, dbname, sql))!;

            Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, exception.Code, sql);
            Assert.That(exception.Message, Does.Contain("use DROP MATERIALIZED VIEW"), sql);
        }

        foreach (bool force in new[] { false, true })
        {
            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.DropTable(new DropTableTicket(dbname, "city_names", ifExists: false, force: force)))!;

            Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, exception.Code, $"ticket force={force}");
            Assert.That(exception.Message, Does.Contain("use DROP MATERIALIZED VIEW"), $"ticket force={force}");
        }

        Assert.IsTrue(database.Schema.Tables.ContainsKey("city_names"));
        Assert.AreEqual(1, await Count(executor, database, dbname, "city_names"), "A refused drop must not have deleted any row");

        // The statement made for it still works, with its CASCADE over a dependent view, and so does
        // the ticket that says the caller knows it is dropping a materialized view.
        await Ddl(executor, database, dbname, "CREATE VIEW city_name_list AS SELECT name FROM city_names");
        await Ddl(executor, database, dbname, "DROP MATERIALIZED VIEW city_names CASCADE");
        Assert.IsFalse(database.Schema.Tables.ContainsKey("city_names"));
        Assert.IsFalse(database.Schema.Views.ContainsKey("city_name_list"));

        await Ddl(executor, database, dbname, "CREATE MATERIALIZED VIEW city_names AS SELECT id, name FROM cities");
        Assert.IsTrue(await executor.DropTable(new DropTableTicket(dbname, "city_names", ifExists: false, force: true, allowMaterializedView: true)));
        Assert.IsFalse(database.Schema.Tables.ContainsKey("city_names"));
    }

    /// <summary>
    /// The refusal arrives late: a view is created while the immediate drop is already running,
    /// after it purged the index entries and before it proposed the table's drop delta. The delta's
    /// validation refuses the drop in log order, the transaction rolls back, and the table keeps every
    /// index in memory — the drop used to remove them index by index before the delta, and a refusal
    /// then left the table with no primary key and no unique enforcement until the schema reloaded.
    /// </summary>
    public static async Task ADropRefusedAfterItsIndexPurgeKeepsTheIndexes(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);
        int indexes = database.Schema.Tables["cities"].Indexes!.Count;

        RowDeleter deleter = executor.RowDeleterForTests;
        bool hookRan = false;

        // Fires inside the drop, between the index purge and the row deletes. CREATE VIEW does not
        // take the DDL semaphore the drop holds, so it can land in that window.
        deleter.TestBeforeWriteHook = async () =>
        {
            deleter.TestBeforeWriteHook = null;
            hookRan = true;
            await Ddl(executor, database, dbname, "CREATE VIEW big_cities AS SELECT id, name FROM cities WHERE population > 5");
        };

        try
        {
            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
                await Ddl(executor, database, dbname, "DROP TABLE cities FORCE"))!;

            Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, exception.Code);
            Assert.That(exception.Message, Does.Contain("big_cities"));
        }
        finally
        {
            deleter.TestBeforeWriteHook = null;
        }

        Assert.IsTrue(hookRan, "the view was never created inside the drop's window");
        Assert.IsTrue(database.Schema.Views.ContainsKey("big_cities"));

        await AssertTableIsWhole(executor, database, dbname, indexes);

        await Ddl(executor, database, dbname, "DROP VIEW big_cities");
        await Ddl(executor, database, dbname, "DROP TABLE cities FORCE");
        Assert.IsFalse(database.Schema.Tables.ContainsKey("cities"));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>cities with one row.</summary>
    private static async Task Setup(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, database, dbname, CreateCities);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name, population) VALUES (1, 'lima', 10)");
    }

    /// <summary>
    /// The table is still in the schema with every index it had, the unique index still refuses a
    /// duplicate, the primary key still answers a point lookup, and the row is still there.
    /// </summary>
    private static async Task AssertTableIsWhole(CommandExecutor executor, DatabaseDescriptor database, string dbname, int indexes)
    {
        Assert.IsTrue(database.Schema.Tables.ContainsKey("cities"));
        Assert.AreEqual(indexes, database.Schema.Tables["cities"].Indexes!.Count, "A refused drop must not have dropped any index");

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "cities"));
        Assert.AreEqual(indexes, table.Indexes.Count, "A refused drop must not have removed an index from the descriptor");

        CamusDBException duplicate = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, database, dbname, "INSERT INTO cities (id, name, population) VALUES (2, 'lima', 3)"))!;
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, duplicate.Code, "The unique index must still be enforced");

        Assert.AreEqual(1, await Count(executor, database, dbname, "cities"), "A refused drop must not have deleted any row");
        Assert.AreEqual(1, await Count(executor, database, dbname, "cities", "WHERE id = 1"), "The primary key must still answer a point lookup");
        Assert.AreEqual(1, await Count(executor, database, dbname, "cities", "WHERE name = 'lima'"), "The unique index must still answer a point lookup");
    }

    private static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Dml(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> Count(CommandExecutor executor, DatabaseDescriptor database, string dbname, string table, string where = "")
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> rows) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, $"SELECT id FROM {table} {where}", null));

            int count = 0;
            await foreach (QueryResultRow _ in rows)
                count++;

            return count;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }
}

/// <summary>DROP TABLE guards on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestDropTableGuards : BaseTest
{
    [Test] public async Task DropOfATableAViewReadsIsRefusedOnEveryEntryPoint() => await Run(DropTableGuardScenarios.DropOfATableAViewReadsIsRefusedOnEveryEntryPoint);
    [Test] public async Task DropTableOnAMaterializedViewIsRefusedOnEveryEntryPoint() => await Run(DropTableGuardScenarios.DropTableOnAMaterializedViewIsRefusedOnEveryEntryPoint);
    [Test] public async Task ADropRefusedAfterItsIndexPurgeKeepsTheIndexes() => await Run(DropTableGuardScenarios.ADropRefusedAfterItsIndexPurgeKeepsTheIndexes);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}

/// <summary>DROP TABLE guards on a cluster-mode engine, where the drop is a replicated delta.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestDropTableGuardsCluster : SharedNodeBaseTest
{
    [Test] public async Task DropOfATableAViewReadsIsRefusedOnEveryEntryPoint() => await Run(DropTableGuardScenarios.DropOfATableAViewReadsIsRefusedOnEveryEntryPoint);
    [Test] public async Task DropTableOnAMaterializedViewIsRefusedOnEveryEntryPoint() => await Run(DropTableGuardScenarios.DropTableOnAMaterializedViewIsRefusedOnEveryEntryPoint);
    [Test] public async Task ADropRefusedAfterItsIndexPurgeKeepsTheIndexes() => await Run(DropTableGuardScenarios.ADropRefusedAfterItsIndexPurgeKeepsTheIndexes);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}
