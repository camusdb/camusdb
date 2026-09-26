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

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The published foreign-key graph against a real database: it follows DDL that renames or drops either
/// table, and a persisted constraint survives a reopen. No DDL creates a constraint yet, so each scenario
/// places one directly into the child's schema under the schema lock. Every later DDL on the child then
/// persists it with the rest of the table.
/// </summary>
internal static class ForeignKeyGraphScenarios
{
    private const string CreateCities =
        "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))";

    private const string CreateWeather =
        "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string NULL, KEY weather_city (city))";

    public static async Task GraphFollowsRenamesOfTheParent(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateTablesWithConstraint(executor, database, dbname);
        string parentId = database.Schema.Tables["cities"].Id!;

        await Ddl(executor, dbname, "ALTER TABLE cities RENAME TO towns");
        await Ddl(executor, dbname, "ALTER TABLE towns RENAME COLUMN name TO title");

        ForeignKeyPlan plan = database.Schema.ForeignKeys.ParentPlansOf(parentId).Single();
        Assert.IsTrue(plan.IsResolved, plan.UnresolvedReason);
        Assert.AreEqual(new[] { "title" }, plan.ParentColumnNames, "The graph must resolve the new column name");
        Assert.AreEqual(new[] { "city" }, plan.ChildColumnNames);
    }

    public static async Task GraphFollowsRenamesOfTheChild(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateTablesWithConstraint(executor, database, dbname);
        string childId = database.Schema.Tables["weather"].Id!;

        await Ddl(executor, dbname, "ALTER TABLE weather RENAME COLUMN city TO town");

        ForeignKeyPlan plan = database.Schema.ForeignKeys.ChildPlansOf(childId).Single();
        Assert.IsTrue(plan.IsResolved, plan.UnresolvedReason);
        Assert.AreEqual(new[] { "town" }, plan.ChildColumnNames);
    }

    public static async Task DroppingTheChildRemovesItsConstraints(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateTablesWithConstraint(executor, database, dbname);
        string parentId = database.Schema.Tables["cities"].Id!;

        await Ddl(executor, dbname, "DROP TABLE weather");

        Assert.IsTrue(database.Schema.ForeignKeys.IsEmpty);
        Assert.IsEmpty(database.Schema.ForeignKeys.ParentPlansOf(parentId));
    }

    /// <summary>
    /// No guard stops this DROP yet. The plan must become unresolved and unenforced, and it must stay
    /// indexed under the old parent id, without an exception anywhere.
    /// </summary>
    public static async Task DroppingTheParentLeavesAnUnenforcedPlan(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateTablesWithConstraint(executor, database, dbname);
        string parentId = database.Schema.Tables["cities"].Id!;

        await Ddl(executor, dbname, "DROP TABLE cities");

        ForeignKeyPlan plan = database.Schema.ForeignKeys.ParentPlansOf(parentId).Single();
        Assert.IsFalse(plan.IsResolved);
        Assert.IsFalse(plan.IsEnforced);
    }

    public static async Task ConstraintSurvivesReopen(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        ForeignKeySchema placed = await CreateTablesWithConstraint(executor, database, dbname);

        // Any DDL on the child persists its whole table record, and with it the constraint.
        await Ddl(executor, dbname, "ALTER TABLE weather SET (sql_stats_automatic_collection_enabled = false)");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        TableSchema weather = reopened.Schema.Tables["weather"];
        ForeignKeySchema loaded = weather.ForeignKeys!.Single();
        Assert.AreEqual(placed.Id, loaded.Id);
        Assert.AreEqual(placed.Name, loaded.Name);
        Assert.AreEqual(placed.ColumnIds, loaded.ColumnIds);
        Assert.AreEqual(placed.ReferencedTableId, loaded.ReferencedTableId);
        Assert.AreEqual(placed.ReferencedColumnIds, loaded.ReferencedColumnIds);
        Assert.AreEqual(placed.ReferencedIndexId, loaded.ReferencedIndexId);
        Assert.AreEqual(placed.BackingIndexId, loaded.BackingIndexId);
        Assert.AreEqual(placed.OnDelete, loaded.OnDelete);
        Assert.AreEqual(placed.OnUpdate, loaded.OnUpdate);
        Assert.AreEqual(placed.Match, loaded.Match);
        Assert.AreEqual(placed.State, loaded.State);

        ForeignKeyPlan plan = reopened.Schema.ForeignKeys.ChildPlansOf(weather.Id!).Single();
        Assert.IsTrue(plan.IsEnforced, plan.UnresolvedReason);
        Assert.AreSame(plan, reopened.Schema.ForeignKeys.ParentPlansOf(placed.ReferencedTableId).Single());
    }

    /// <summary>A stale constraint on disk must never stop a database from opening.</summary>
    public static async Task DatabaseWithAnUnresolvableConstraintStillOpens(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateTablesWithConstraint(executor, database, dbname, referencedTableId: "no-such-table");
        await Ddl(executor, dbname, "ALTER TABLE weather SET (sql_stats_automatic_collection_enabled = false)");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        TableSchema weather = reopened.Schema.Tables["weather"];
        ForeignKeyPlan plan = reopened.Schema.ForeignKeys.ChildPlansOf(weather.Id!).Single();
        Assert.IsFalse(plan.IsEnforced);
        Assert.That(plan.UnresolvedReason, Does.Contain("no-such-table"));
    }

    /// <summary>
    /// Creates cities and weather, then places weather(city) → cities(name) in the child's schema, backed
    /// by the user index weather_city. Returns the placed constraint.
    /// </summary>
    private static async Task<ForeignKeySchema> CreateTablesWithConstraint(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string? referencedTableId = null)
    {
        await Ddl(executor, dbname, CreateCities);
        await Ddl(executor, dbname, CreateWeather);

        await database.Schema.AcquireLockAsync();
        try
        {
            TableSchema cities = database.Schema.Tables["cities"];
            TableSchema weather = database.Schema.Tables["weather"];

            ForeignKeySchema constraint = new(
                id: "fk-weather-city",
                name: "weather_city_fkey",
                columnIds: [ColumnId(weather, "city")],
                referencedTableId: referencedTableId ?? cities.Id!,
                referencedColumnIds: [ColumnId(cities, "name")],
                referencedIndexId: IndexKvId(cities, "cities_name"),
                backingIndexId: IndexKvId(weather, "weather_city"),
                onDelete: ForeignKeyAction.NoAction,
                onUpdate: ForeignKeyAction.Restrict,
                match: ForeignKeyMatch.Simple,
                state: SchemaElementState.Public);

            weather.ForeignKeys = [constraint];
            database.Schema.RebuildForeignKeyGraph();
            return constraint;
        }
        finally
        {
            database.Schema.ReleaseLock();
        }
    }

    private static string ColumnId(TableSchema table, string name) =>
        table.Columns!.Single(c => c.Name == name).Id;

    private static string IndexKvId(TableSchema table, string name) =>
        table.Indexes!.Single(i => i.Name == name).KvId;

    private static async Task Ddl(CommandExecutor executor, string dbname, string sql) =>
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: dbname, sql: sql, parameters: null));
}

/// <summary>The graph lifecycle on a standalone engine (<c>isClusterMode: false</c>).</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyGraphLifecycle : BaseTest
{
    [Test]
    public async Task GraphFollowsRenamesOfTheParent() => await Run(ForeignKeyGraphScenarios.GraphFollowsRenamesOfTheParent);

    [Test]
    public async Task GraphFollowsRenamesOfTheChild() => await Run(ForeignKeyGraphScenarios.GraphFollowsRenamesOfTheChild);

    [Test]
    public async Task DroppingTheChildRemovesItsConstraints() => await Run(ForeignKeyGraphScenarios.DroppingTheChildRemovesItsConstraints);

    [Test]
    public async Task DroppingTheParentLeavesAnUnenforcedPlan() => await Run(ForeignKeyGraphScenarios.DroppingTheParentLeavesAnUnenforcedPlan);

    [Test]
    public async Task ConstraintSurvivesReopen() => await Run(ForeignKeyGraphScenarios.ConstraintSurvivesReopen);

    [Test]
    public async Task DatabaseWithAnUnresolvableConstraintStillOpens() => await Run(ForeignKeyGraphScenarios.DatabaseWithAnUnresolvableConstraintStillOpens);

    private async Task Run(System.Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}

/// <summary>The graph lifecycle on a cluster-mode engine, where DDL is a replicated schema delta.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyGraphLifecycleCluster : SharedNodeBaseTest
{
    [Test]
    public async Task GraphFollowsRenamesOfTheParent() => await Run(ForeignKeyGraphScenarios.GraphFollowsRenamesOfTheParent);

    [Test]
    public async Task GraphFollowsRenamesOfTheChild() => await Run(ForeignKeyGraphScenarios.GraphFollowsRenamesOfTheChild);

    [Test]
    public async Task DroppingTheChildRemovesItsConstraints() => await Run(ForeignKeyGraphScenarios.DroppingTheChildRemovesItsConstraints);

    [Test]
    public async Task DroppingTheParentLeavesAnUnenforcedPlan() => await Run(ForeignKeyGraphScenarios.DroppingTheParentLeavesAnUnenforcedPlan);

    [Test]
    public async Task ConstraintSurvivesReopen() => await Run(ForeignKeyGraphScenarios.ConstraintSurvivesReopen);

    [Test]
    public async Task DatabaseWithAnUnresolvableConstraintStillOpens() => await Run(ForeignKeyGraphScenarios.DatabaseWithAnUnresolvableConstraintStillOpens);

    private async Task Run(System.Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}
