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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// DDL that would remove what a foreign key depends on: the parent table, its contents, a column of
/// either side, the referenced or the backing index, and row-level TTL on a parent. Each refusal leaves
/// the schema unchanged. RELINK restores a child without its constraints and names them.
/// </summary>
internal static class ForeignKeyDdlGuardScenarios
{
    private const string CreateCities =
        "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, population int64, UNIQUE KEY cities_name (name))";

    private const string CreateWeather =
        "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name))";

    public static async Task DropOfAReferencedParentIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);
        int indexes = database.Schema.Tables["cities"].Indexes!.Count;

        foreach (string sql in new[] { "DROP TABLE cities", "DROP TABLE cities FORCE" })
        {
            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, database, dbname, sql))!;

            Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, exception.Code, sql);
            Assert.That(exception.Message, Does.Contain("weather_city_fkey"));
            Assert.That(exception.Message, Does.Contain("'weather'"));
        }

        Assert.IsTrue(database.Schema.Tables.ContainsKey("cities"));
        Assert.AreEqual(indexes, database.Schema.Tables["cities"].Indexes!.Count, "A refused drop must not have dropped any index");
        Assert.AreEqual(1, await Count(executor, database, dbname, "cities"), "A refused drop must not have deleted any row");

        // Once the child is gone, the parent can go.
        await Ddl(executor, database, dbname, "DROP TABLE weather");
        await Ddl(executor, database, dbname, "DROP TABLE cities");
        Assert.IsTrue(database.Schema.ForeignKeys.IsEmpty);
    }

    public static async Task TruncateOfAReferencedParentIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.TruncateTable(new TruncateTableTicket(dbname, "cities")))!;

        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, exception.Code);
        Assert.AreEqual(1, await Count(executor, database, dbname, "cities"));

        // The child itself can be truncated, and so can a table that only references itself.
        await executor.TruncateTable(new TruncateTableTicket(dbname, "weather"));
        await Ddl(executor, database, dbname, "CREATE TABLE employees (id int64 PRIMARY KEY NOT NULL, manager_id int64 REFERENCES employees (id))");
        await Dml(executor, database, dbname, "INSERT INTO employees (id, manager_id) VALUES (1, NULL), (2, 1)");
        await executor.TruncateTable(new TruncateTableTicket(dbname, "employees"));
        Assert.AreEqual(0, await Count(executor, database, dbname, "employees"));
    }

    public static async Task DropOfAColumnInAForeignKeyIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);

        foreach (string sql in new[] { "ALTER TABLE weather DROP COLUMN city", "ALTER TABLE cities DROP COLUMN name" })
        {
            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, database, dbname, sql))!;
            Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, exception.Code, sql);
            Assert.That(exception.Message, Does.Contain("weather_city_fkey"));
        }

        // A column outside the constraint still drops.
        await Ddl(executor, database, dbname, "ALTER TABLE cities DROP COLUMN population");
    }

    public static async Task DropOfAnIndexAForeignKeyNeedsIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);
        await Ddl(executor, database, dbname,
            "CREATE TABLE trips (id int64 PRIMARY KEY NOT NULL, city string, KEY trips_city (city), FOREIGN KEY (city) REFERENCES cities (name))");

        foreach (string sql in new[]
        {
            "ALTER TABLE cities DROP INDEX cities_name",               // the referenced unique index
            "ALTER TABLE trips DROP INDEX trips_city",                 // a user index the constraint reuses
        })
        {
            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, database, dbname, sql))!;
            Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, exception.Code, sql);
        }

        // The index created for the constraint cannot be named in SQL at all: a name that starts with
        // '~' belongs to the engine. Through the ticket API it can be named, and the guard refuses it.
        CamusDBException sqlRefusal = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Ddl(executor, database, dbname, "ALTER TABLE weather DROP INDEX `~fk_weather_city_fkey`"))!;
        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, sqlRefusal.Code);

        CamusDBException ticketRefusal = Assert.ThrowsAsync<CamusDBException>(async () => await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "weather",
            indexName: "~fk_weather_city_fkey",
            columns: Array.Empty<ColumnIndexInfo>(),
            operation: AlterIndexOperation.DropIndex)))!;
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, ticketRefusal.Code);

        Assert.IsNotNull(database.Schema.Tables["cities"].Indexes!.SingleOrDefault(i => i.Name == "cities_name"));
        Assert.IsNotNull(database.Schema.Tables["weather"].Indexes!.SingleOrDefault(i => i.Name == "~fk_weather_city_fkey"));
        Assert.IsNotNull(database.Schema.Tables["trips"].Indexes!.SingleOrDefault(i => i.Name == "trips_city"));
        Assert.IsTrue(database.Schema.ForeignKeys.ChildPlansOf(database.Schema.Tables["weather"].Id!).Single().IsEnforced);
    }

    public static async Task RowLevelTtlOnAReferencedTableIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Ddl(executor, database, dbname, "ALTER TABLE cities SET (ttl_expiration_expression = 'population')"))!;

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, exception.Code);
        Assert.IsNull(database.Schema.Tables["cities"].Settings?.GetValueOrDefault(TableSettings.TtlExpirationExpressionKey));
    }

    /// <summary>
    /// A dropped child comes back without its constraint: while it was detached, no parent-side check
    /// saw its rows. The statement names the constraint, and the index created for it stays as an
    /// ordinary index.
    /// </summary>
    public static async Task RelinkedChildComesBackWithoutItsConstraint(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);
        string weatherId = database.Schema.Tables["weather"].Id!;

        await Ddl(executor, database, dbname, "DROP TABLE weather");

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, $"CREATE TABLE weather_back RELINK TO '{weatherId}'", null));
        await database.Transactions.CommitAsync(tx);

        TableSchema restored = database.Schema.Tables["weather_back"];
        Assert.IsNull(restored.ForeignKeys);
        Assert.IsTrue(database.Schema.ForeignKeys.IsEmpty);

        TableIndexSchema owned = restored.Indexes!.Single(i => i.Name == "~fk_weather_city_fkey");
        Assert.IsNull(owned.OwnerConstraintId, "The index must not name a constraint that is gone");

        Assert.That(result.Warning, Does.Contain("weather_city_fkey"));
        Assert.AreEqual(1, await Count(executor, database, dbname, "weather_back"));

        // The parent is no longer referenced, so it can be dropped.
        await Ddl(executor, database, dbname, "DROP TABLE cities");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>cities with one row, and weather referencing it.</summary>
    private static async Task Setup(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, database, dbname, CreateCities);
        await Ddl(executor, database, dbname, CreateWeather);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name, population) VALUES (1, 'lima', 10)");
        await Dml(executor, database, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima')");
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

    private static async Task<int> Count(CommandExecutor executor, DatabaseDescriptor database, string dbname, string table)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> rows) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, $"SELECT id FROM {table}", null));

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

/// <summary>Foreign-key DDL guards on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyDdlGuards : BaseTest
{
    [Test] public async Task DropOfAReferencedParentIsRefused() => await Run(ForeignKeyDdlGuardScenarios.DropOfAReferencedParentIsRefused);
    [Test] public async Task TruncateOfAReferencedParentIsRefused() => await Run(ForeignKeyDdlGuardScenarios.TruncateOfAReferencedParentIsRefused);
    [Test] public async Task DropOfAColumnInAForeignKeyIsRefused() => await Run(ForeignKeyDdlGuardScenarios.DropOfAColumnInAForeignKeyIsRefused);
    [Test] public async Task DropOfAnIndexAForeignKeyNeedsIsRefused() => await Run(ForeignKeyDdlGuardScenarios.DropOfAnIndexAForeignKeyNeedsIsRefused);
    [Test] public async Task RowLevelTtlOnAReferencedTableIsRefused() => await Run(ForeignKeyDdlGuardScenarios.RowLevelTtlOnAReferencedTableIsRefused);
    [Test] public async Task RelinkedChildComesBackWithoutItsConstraint() => await Run(ForeignKeyDdlGuardScenarios.RelinkedChildComesBackWithoutItsConstraint);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}

/// <summary>Foreign-key DDL guards on a cluster-mode engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyDdlGuardsCluster : SharedNodeBaseTest
{
    [Test] public async Task DropOfAReferencedParentIsRefused() => await Run(ForeignKeyDdlGuardScenarios.DropOfAReferencedParentIsRefused);
    [Test] public async Task TruncateOfAReferencedParentIsRefused() => await Run(ForeignKeyDdlGuardScenarios.TruncateOfAReferencedParentIsRefused);
    [Test] public async Task DropOfAColumnInAForeignKeyIsRefused() => await Run(ForeignKeyDdlGuardScenarios.DropOfAColumnInAForeignKeyIsRefused);
    [Test] public async Task DropOfAnIndexAForeignKeyNeedsIsRefused() => await Run(ForeignKeyDdlGuardScenarios.DropOfAnIndexAForeignKeyNeedsIsRefused);
    [Test] public async Task RowLevelTtlOnAReferencedTableIsRefused() => await Run(ForeignKeyDdlGuardScenarios.RowLevelTtlOnAReferencedTableIsRefused);
    [Test] public async Task RelinkedChildComesBackWithoutItsConstraint() => await Run(ForeignKeyDdlGuardScenarios.RelinkedChildComesBackWithoutItsConstraint);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}
