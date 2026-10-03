/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text;
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
/// CREATE TABLE with a foreign key, through the real SQL entry point: what is persisted, which index
/// backs the constraint, which statements are refused with which code, and the validation pass that
/// publishes a constraint in a cluster. Every scenario runs on a standalone engine and on a cluster-mode
/// engine, because the two take different rollout paths.
/// </summary>
internal static class ForeignKeyCreateTableScenarios
{
    private const string CreateCities =
        "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))";

    public static async Task ColumnReferenceIsPersistedAndSurvivesReopen(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);
        await Ddl(executor, dbname, "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities(name))");

        TableSchema cities = database.Schema.Tables["cities"];
        TableSchema weather = database.Schema.Tables["weather"];
        ForeignKeySchema constraint = weather.ForeignKeys!.Single();

        Assert.AreEqual("weather_city_fkey", constraint.Name);
        Assert.AreEqual(SchemaElementState.Public, constraint.State, "CREATE TABLE must return with the constraint public");
        Assert.AreEqual(new[] { ColumnId(weather, "city") }, constraint.ColumnIds);
        Assert.AreEqual(cities.Id, constraint.ReferencedTableId);
        Assert.AreEqual(new[] { ColumnId(cities, "name") }, constraint.ReferencedColumnIds);
        Assert.AreEqual(IndexKvId(cities, "cities_name"), constraint.ReferencedIndexId);
        Assert.AreEqual(ForeignKeyAction.NoAction, constraint.OnDelete);
        Assert.AreEqual(ForeignKeyMatch.Simple, constraint.Match);

        // No index on weather(city) was declared, so one is created for the constraint and owned by it.
        TableIndexSchema owned = weather.Indexes!.Single(i => i.Name == "~fk_weather_city_fkey");
        Assert.AreEqual(owned.KvId, constraint.BackingIndexId);
        Assert.AreEqual(constraint.Id, owned.OwnerConstraintId);
        Assert.AreEqual(IndexType.Multi, owned.Type);
        Assert.AreEqual(SchemaElementState.Public, owned.State);
        Assert.AreEqual(new[] { ColumnId(weather, "city") }, owned.ColumnIds);

        ForeignKeyPlan plan = database.Schema.ForeignKeys.ChildPlansOf(weather.Id!).Single();
        Assert.IsTrue(plan.IsEnforced, plan.UnresolvedReason);
        Assert.AreSame(plan, database.Schema.ForeignKeys.ParentPlansOf(cities.Id!).Single());

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        TableSchema reloaded = reopened.Schema.Tables["weather"];
        ForeignKeySchema loaded = reloaded.ForeignKeys!.Single();
        Assert.AreEqual(constraint.Id, loaded.Id);
        Assert.AreEqual(constraint.Name, loaded.Name);
        Assert.AreEqual(constraint.ColumnIds, loaded.ColumnIds);
        Assert.AreEqual(constraint.ReferencedTableId, loaded.ReferencedTableId);
        Assert.AreEqual(constraint.ReferencedColumnIds, loaded.ReferencedColumnIds);
        Assert.AreEqual(constraint.ReferencedIndexId, loaded.ReferencedIndexId);
        Assert.AreEqual(constraint.BackingIndexId, loaded.BackingIndexId);
        Assert.AreEqual(SchemaElementState.Public, loaded.State);
        Assert.AreEqual(loaded.Id, reloaded.Indexes!.Single(i => i.Name == "~fk_weather_city_fkey").OwnerConstraintId);
        Assert.IsTrue(reopened.Schema.ForeignKeys.ChildPlansOf(reloaded.Id!).Single().IsEnforced);
    }

    public static async Task ReferenceWithoutColumnsUsesThePrimaryKey(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);
        await Ddl(executor, dbname, "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city_id int64 REFERENCES cities)");

        TableSchema cities = database.Schema.Tables["cities"];
        ForeignKeySchema constraint = database.Schema.Tables["weather"].ForeignKeys!.Single();

        Assert.AreEqual(new[] { ColumnId(cities, "id") }, constraint.ReferencedColumnIds);
        Assert.AreEqual(IndexKvId(cities, "~pk"), constraint.ReferencedIndexId);
    }

    /// <summary>
    /// The constraint, the parent index and the child index each list the same two columns in a
    /// different order. The user index is reused, no index is added, and the plan maps both orders.
    /// </summary>
    public static async Task CompositeKeyReusesAnIndexWithTheSameColumnsInAnyOrder(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname,
            "CREATE TABLE regions (id int64 PRIMARY KEY NOT NULL, country string NOT NULL, code int64 NOT NULL, UNIQUE KEY regions_key (country, code))");
        await Ddl(executor, dbname,
            "CREATE TABLE sites (id int64 PRIMARY KEY NOT NULL, country string, code int64, KEY sites_code_country (code, country), " +
            "CONSTRAINT sites_region_fk FOREIGN KEY (code, country) REFERENCES regions (code, country))");

        TableSchema regions = database.Schema.Tables["regions"];
        TableSchema sites = database.Schema.Tables["sites"];
        ForeignKeySchema constraint = sites.ForeignKeys!.Single();

        Assert.AreEqual("sites_region_fk", constraint.Name);
        Assert.AreEqual(IndexKvId(regions, "regions_key"), constraint.ReferencedIndexId);
        Assert.AreEqual(IndexKvId(sites, "sites_code_country"), constraint.BackingIndexId);
        Assert.AreEqual(2, sites.Indexes!.Count, "A matching user index must be reused, not duplicated");
        Assert.IsTrue(sites.Indexes.All(i => i.OwnerConstraintId is null));

        ForeignKeyPlan plan = database.Schema.ForeignKeys.ChildPlansOf(sites.Id!).Single();
        Assert.IsTrue(plan.IsEnforced, plan.UnresolvedReason);
        Assert.AreEqual(new[] { 1, 0 }, plan.ParentKeyOrder, "Parent index (country, code) takes constraint columns (code, country) swapped");
        Assert.AreEqual(new[] { 0, 1 }, plan.BackingKeyOrder);
    }

    public static async Task TheNarrowestMatchingIndexIsPreferred(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);
        await Ddl(executor, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string, day int64, KEY weather_wide (city, day), KEY weather_narrow (city), " +
            "FOREIGN KEY (city) REFERENCES cities (name))");

        TableSchema weather = database.Schema.Tables["weather"];
        Assert.AreEqual(IndexKvId(weather, "weather_narrow"), weather.ForeignKeys!.Single().BackingIndexId);
        Assert.AreEqual(3, weather.Indexes!.Count);
    }

    public static async Task SelfReferenceResolvesAgainstTheNewTable(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, "CREATE TABLE employees (id int64 PRIMARY KEY NOT NULL, manager_id int64 REFERENCES employees (id))");

        TableSchema employees = database.Schema.Tables["employees"];
        ForeignKeySchema constraint = employees.ForeignKeys!.Single();

        Assert.AreEqual(employees.Id, constraint.ReferencedTableId);
        Assert.AreEqual(IndexKvId(employees, "~pk"), constraint.ReferencedIndexId);

        ForeignKeyPlan plan = database.Schema.ForeignKeys.ChildPlansOf(employees.Id!).Single();
        Assert.IsTrue(plan.IsSelfReference);
        Assert.IsTrue(plan.IsEnforced, plan.UnresolvedReason);
    }

    public static async Task TwoConstraintsOnOneTableEachGetAnIndex(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);
        await Ddl(executor, dbname,
            "CREATE TABLE trips (id int64 PRIMARY KEY NOT NULL, origin string REFERENCES cities (name), destination string REFERENCES cities (name))");

        TableSchema trips = database.Schema.Tables["trips"];
        Assert.AreEqual(new[] { "trips_origin_fkey", "trips_destination_fkey" }, trips.ForeignKeys!.Select(f => f.Name).ToArray());
        Assert.AreEqual(2, trips.Indexes!.Count(i => i.OwnerConstraintId is not null));
        Assert.AreEqual(2, database.Schema.ForeignKeys.ParentPlansOf(database.Schema.Tables["cities"].Id!).Length);
    }

    /// <summary>No rollout job may be left behind once CREATE TABLE returns, in either mode.</summary>
    public static async Task NoRolloutJobIsLeftBehind(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);
        await Ddl(executor, dbname, "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities(name))");

        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));
    }

    // ── Rejections ────────────────────────────────────────────────────────────

    public static async Task MissingParentIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname) =>
        await AssertRefused(executor, database, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES nowhere (name))",
            CamusDBErrorCodes.TableDoesntExist);

    public static async Task ViewAsParentIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);
        await Ddl(executor, dbname, "CREATE VIEW city_names AS SELECT id, name FROM cities");

        await AssertRefused(executor, database, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES city_names (name))",
            CamusDBErrorCodes.InvalidForeignKeyDefinition);
    }

    public static async Task MaterializedViewAsParentIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);
        await Ddl(executor, dbname, "CREATE MATERIALIZED VIEW city_copy AS SELECT id, name FROM cities");

        await AssertRefused(executor, database, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city_id int64 REFERENCES city_copy)",
            CamusDBErrorCodes.InvalidForeignKeyDefinition);
    }

    public static async Task ColumnsWithoutAUniqueIndexAreRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, KEY cities_name (name))");

        await AssertRefused(executor, database, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name))",
            CamusDBErrorCodes.InvalidForeignKeyDefinition);
    }

    public static async Task TypeMismatchIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);

        await AssertRefused(executor, database, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city int64 REFERENCES cities (name))",
            CamusDBErrorCodes.InvalidForeignKeyDefinition);
    }

    public static async Task ColumnCountMismatchIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);

        await AssertRefused(executor, database, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string, FOREIGN KEY (city) REFERENCES cities (id, name))",
            CamusDBErrorCodes.InvalidForeignKeyDefinition);
    }

    public static async Task MissingReferencedColumnIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);

        await AssertRefused(executor, database, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (title))",
            CamusDBErrorCodes.InvalidForeignKeyDefinition);
    }

    public static async Task RepeatedReferencedColumnIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);

        await AssertRefused(executor, database, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, a string, b string, FOREIGN KEY (a, b) REFERENCES cities (name, name))",
            CamusDBErrorCodes.InvalidForeignKeyDefinition);
    }

    public static async Task ParentWithRowLevelTtlIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname,
            "CREATE TABLE sessions (id int64 PRIMARY KEY NOT NULL, expires_at int64) WITH (ttl_expiration_expression = 'expires_at')");

        await AssertRefused(executor, database, dbname,
            "CREATE TABLE clicks (id int64 PRIMARY KEY NOT NULL, session_id int64 REFERENCES sessions)",
            CamusDBErrorCodes.FeatureNotSupported);
    }

    public static async Task NameTakenByACheckConstraintIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);

        await AssertRefused(executor, database, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string, CONSTRAINT weather_rule CHECK (id > 0), " +
            "CONSTRAINT weather_rule FOREIGN KEY (city) REFERENCES cities (name))",
            CamusDBErrorCodes.InvalidInput);
    }

    public static async Task UnsupportedActionIsRefusedByName(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);

        CamusDBException exception = await AssertRefused(executor, database, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string, FOREIGN KEY (city) REFERENCES cities (name) ON DELETE CASCADE)",
            CamusDBErrorCodes.FeatureNotSupported);

        Assert.That(exception.Message, Does.Contain("ON DELETE CASCADE"));
    }

    // ── The validation pass ───────────────────────────────────────────────────

    /// <summary>
    /// More child rows than one page of the pass, with repeated keys and NULL keys. Every non-NULL key has
    /// a parent, so the pass must succeed across the page boundary.
    /// </summary>
    public static async Task ValidationPassesWhenEveryRowHasAParent(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, "CREATE TABLE owners (id int64 PRIMARY KEY NOT NULL)");
        await Ddl(executor, dbname, "CREATE TABLE pets (id int64 PRIMARY KEY NOT NULL, owner_id int64 REFERENCES owners)");

        await InsertRange(executor, dbname, "owners", "id", 1, 700, i => $"({i})");
        int rows = ForeignKeyValidationPassRows;
        await InsertRange(executor, dbname, "pets", "id, owner_id", 1, rows, i => i % 10 == 0 ? $"({i}, NULL)" : $"({i}, {1 + i % 700})");

        Assert.DoesNotThrowAsync(async () => await executor.ValidateForeignKeyRowsAsync(database, "pets", "pets_owner_id_fkey"));
    }

    /// <summary>
    /// Two keys lose their parent: the rows are written while every parent exists, and then the parents'
    /// index entries are hidden with a test-only tombstone, as a parent DELETE on a node that did not know
    /// the constraint would leave them. The pass must report the first orphan in key order, and skip the
    /// NULL keys.
    /// </summary>
    public static async Task ValidationReportsTheFirstOrphanInKeyOrder(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, CreateCities);
        await Ddl(executor, dbname, "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities(name))");

        await Dml(executor, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima'), (2, 'quito'), (3, 'zurich'), (4, 'bogota')");
        await Dml(executor, dbname,
            "INSERT INTO weather (id, city) VALUES (1, 'quito'), (2, 'zurich'), (3, NULL), (4, 'bogota'), (5, 'lima'), (6, 'bogota')");
        await HideParentKey(database, "cities", "cities_name", new ColumnValue(ColumnType.String, "zurich"));
        await HideParentKey(database, "cities", "cities_name", new ColumnValue(ColumnType.String, "bogota"));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ValidateForeignKeyRowsAsync(database, "weather", "weather_city_fkey"))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code);
        Assert.That(exception.Message, Does.Contain("bogota"));
        Assert.That(exception.Message, Does.Not.Contain("zurich"), "Only the first orphan in key order is reported");
        Assert.That(exception.Message, Does.Contain("weather_city_fkey"));
    }

    /// <summary>
    /// The child index lists the columns in the other order from the parent index. A correct mapping
    /// passes the matching rows and still catches the one orphan.
    /// </summary>
    public static async Task ValidationMapsCompositeKeysAcrossColumnOrders(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname,
            "CREATE TABLE regions (id int64 PRIMARY KEY NOT NULL, country string NOT NULL, code int64 NOT NULL, UNIQUE KEY regions_key (country, code))");
        await Ddl(executor, dbname,
            "CREATE TABLE sites (id int64 PRIMARY KEY NOT NULL, country string, code int64, KEY sites_code_country (code, country), " +
            "CONSTRAINT sites_region_fk FOREIGN KEY (code, country) REFERENCES regions (code, country))");

        await Dml(executor, dbname, "INSERT INTO regions (id, country, code) VALUES (1, 'pe', 10), (2, 'ec', 20), (3, 'ec', 10)");
        await Dml(executor, dbname, "INSERT INTO sites (id, country, code) VALUES (1, 'pe', 10), (2, 'ec', 20), (3, 'pe', NULL), (4, 'ec', 10)");

        Assert.DoesNotThrowAsync(async () => await executor.ValidateForeignKeyRowsAsync(database, "sites", "sites_region_fk"));

        // The parent index is (country, code), so the hidden key is in that order.
        await HideParentKey(database, "regions", "regions_key",
            new ColumnValue(ColumnType.String, "ec"), new ColumnValue(ColumnType.Integer64, 10));

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ValidateForeignKeyRowsAsync(database, "sites", "sites_region_fk"))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code);
        Assert.That(exception.Message, Does.Contain("(code, country)=(10, ec)"));
    }

    /// <summary>Rows written by the pass's test; more than one page of the pass.</summary>
    internal const int ForeignKeyValidationPassRows = 1500;

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task<CamusDBException> AssertRefused(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql, string expectedCode)
    {
        long version = database.Schema.SchemaVersion;
        string tableName = sql.Split(' ')[2];

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, sql))!;

        Assert.AreEqual(expectedCode, exception.Code, exception.Message);
        Assert.IsFalse(database.Schema.Tables.ContainsKey(tableName), "A refused CREATE TABLE must not create the table");
        Assert.AreEqual(version, database.Schema.SchemaVersion, "A refused CREATE TABLE must not advance the schema");
        return exception;
    }

    private static async Task InsertRange(CommandExecutor executor, string dbname, string table, string columns, int from, int to, System.Func<int, string> row)
    {
        const int Chunk = 250;

        for (int start = from; start <= to; start += Chunk)
        {
            StringBuilder sql = new($"INSERT INTO {table} ({columns}) VALUES ");
            int end = System.Math.Min(to, start + Chunk - 1);

            for (int i = start; i <= end; i++)
            {
                if (i > start)
                    sql.Append(", ");
                sql.Append(row(i));
            }

            await Dml(executor, dbname, sql.ToString());
        }
    }

    /// <summary>
    /// Hides one entry of a parent's unique index behind a test-only tombstone. The parent row stays; only
    /// the key the child side looks up disappears. No DML can make an orphan once the constraint is
    /// enforced, so this stands in for the window before the constraint was enforced everywhere.
    /// </summary>
    internal static async Task HideParentKey(DatabaseDescriptor database, string table, string index, params ColumnValue[] key)
    {
        TableDescriptor parent = await database.TableDescriptors[table];
        string indexId = parent.Schema.Indexes!.Single(i => i.Name == index).KvId;

        KvTransaction tx = await database.Transactions.BeginAsync();
        await parent.Store.WriteUniqueIndexTombstoneForTesting(tx, indexId, new CompositeColumnValue(key));
        await database.Transactions.CommitAsync(tx);
    }

    private static string ColumnId(TableSchema table, string name) =>
        table.Columns!.Single(c => c.Name == name).Id;

    private static string IndexKvId(TableSchema table, string name) =>
        table.Indexes!.Single(i => i.Name == name).KvId;

    private static async Task Ddl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task Dml(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }
}

/// <summary>CREATE TABLE with a foreign key on a standalone engine, where the constraint is born public.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyCreateTable : BaseTest
{
    [Test] public async Task ColumnReferenceIsPersistedAndSurvivesReopen() => await Run(ForeignKeyCreateTableScenarios.ColumnReferenceIsPersistedAndSurvivesReopen);
    [Test] public async Task ReferenceWithoutColumnsUsesThePrimaryKey() => await Run(ForeignKeyCreateTableScenarios.ReferenceWithoutColumnsUsesThePrimaryKey);
    [Test] public async Task CompositeKeyReusesAnIndexWithTheSameColumnsInAnyOrder() => await Run(ForeignKeyCreateTableScenarios.CompositeKeyReusesAnIndexWithTheSameColumnsInAnyOrder);
    [Test] public async Task TheNarrowestMatchingIndexIsPreferred() => await Run(ForeignKeyCreateTableScenarios.TheNarrowestMatchingIndexIsPreferred);
    [Test] public async Task SelfReferenceResolvesAgainstTheNewTable() => await Run(ForeignKeyCreateTableScenarios.SelfReferenceResolvesAgainstTheNewTable);
    [Test] public async Task TwoConstraintsOnOneTableEachGetAnIndex() => await Run(ForeignKeyCreateTableScenarios.TwoConstraintsOnOneTableEachGetAnIndex);
    [Test] public async Task NoRolloutJobIsLeftBehind() => await Run(ForeignKeyCreateTableScenarios.NoRolloutJobIsLeftBehind);
    [Test] public async Task MissingParentIsRefused() => await Run(ForeignKeyCreateTableScenarios.MissingParentIsRefused);
    [Test] public async Task ViewAsParentIsRefused() => await Run(ForeignKeyCreateTableScenarios.ViewAsParentIsRefused);
    [Test] public async Task MaterializedViewAsParentIsRefused() => await Run(ForeignKeyCreateTableScenarios.MaterializedViewAsParentIsRefused);
    [Test] public async Task ColumnsWithoutAUniqueIndexAreRefused() => await Run(ForeignKeyCreateTableScenarios.ColumnsWithoutAUniqueIndexAreRefused);
    [Test] public async Task TypeMismatchIsRefused() => await Run(ForeignKeyCreateTableScenarios.TypeMismatchIsRefused);
    [Test] public async Task ColumnCountMismatchIsRefused() => await Run(ForeignKeyCreateTableScenarios.ColumnCountMismatchIsRefused);
    [Test] public async Task MissingReferencedColumnIsRefused() => await Run(ForeignKeyCreateTableScenarios.MissingReferencedColumnIsRefused);
    [Test] public async Task RepeatedReferencedColumnIsRefused() => await Run(ForeignKeyCreateTableScenarios.RepeatedReferencedColumnIsRefused);
    [Test] public async Task ParentWithRowLevelTtlIsRefused() => await Run(ForeignKeyCreateTableScenarios.ParentWithRowLevelTtlIsRefused);
    [Test] public async Task NameTakenByACheckConstraintIsRefused() => await Run(ForeignKeyCreateTableScenarios.NameTakenByACheckConstraintIsRefused);
    [Test] public async Task UnsupportedActionIsRefusedByName() => await Run(ForeignKeyCreateTableScenarios.UnsupportedActionIsRefusedByName);
    [Test] public async Task ValidationPassesWhenEveryRowHasAParent() => await Run(ForeignKeyCreateTableScenarios.ValidationPassesWhenEveryRowHasAParent);
    [Test] public async Task ValidationReportsTheFirstOrphanInKeyOrder() => await Run(ForeignKeyCreateTableScenarios.ValidationReportsTheFirstOrphanInKeyOrder);
    [Test] public async Task ValidationMapsCompositeKeysAcrossColumnOrders() => await Run(ForeignKeyCreateTableScenarios.ValidationMapsCompositeKeysAcrossColumnOrders);

    private async Task Run(System.Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}

/// <summary>
/// CREATE TABLE with a foreign key on a cluster-mode engine, where the constraint is born WriteOnly and
/// published by the coordinator after the validation pass.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyCreateTableCluster : SharedNodeBaseTest
{
    [Test] public async Task ColumnReferenceIsPersistedAndSurvivesReopen() => await Run(ForeignKeyCreateTableScenarios.ColumnReferenceIsPersistedAndSurvivesReopen);
    [Test] public async Task ReferenceWithoutColumnsUsesThePrimaryKey() => await Run(ForeignKeyCreateTableScenarios.ReferenceWithoutColumnsUsesThePrimaryKey);
    [Test] public async Task CompositeKeyReusesAnIndexWithTheSameColumnsInAnyOrder() => await Run(ForeignKeyCreateTableScenarios.CompositeKeyReusesAnIndexWithTheSameColumnsInAnyOrder);
    [Test] public async Task TheNarrowestMatchingIndexIsPreferred() => await Run(ForeignKeyCreateTableScenarios.TheNarrowestMatchingIndexIsPreferred);
    [Test] public async Task SelfReferenceResolvesAgainstTheNewTable() => await Run(ForeignKeyCreateTableScenarios.SelfReferenceResolvesAgainstTheNewTable);
    [Test] public async Task TwoConstraintsOnOneTableEachGetAnIndex() => await Run(ForeignKeyCreateTableScenarios.TwoConstraintsOnOneTableEachGetAnIndex);
    [Test] public async Task NoRolloutJobIsLeftBehind() => await Run(ForeignKeyCreateTableScenarios.NoRolloutJobIsLeftBehind);
    [Test] public async Task MissingParentIsRefused() => await Run(ForeignKeyCreateTableScenarios.MissingParentIsRefused);
    [Test] public async Task ViewAsParentIsRefused() => await Run(ForeignKeyCreateTableScenarios.ViewAsParentIsRefused);
    [Test] public async Task MaterializedViewAsParentIsRefused() => await Run(ForeignKeyCreateTableScenarios.MaterializedViewAsParentIsRefused);
    [Test] public async Task ColumnsWithoutAUniqueIndexAreRefused() => await Run(ForeignKeyCreateTableScenarios.ColumnsWithoutAUniqueIndexAreRefused);
    [Test] public async Task TypeMismatchIsRefused() => await Run(ForeignKeyCreateTableScenarios.TypeMismatchIsRefused);
    [Test] public async Task ColumnCountMismatchIsRefused() => await Run(ForeignKeyCreateTableScenarios.ColumnCountMismatchIsRefused);
    [Test] public async Task MissingReferencedColumnIsRefused() => await Run(ForeignKeyCreateTableScenarios.MissingReferencedColumnIsRefused);
    [Test] public async Task RepeatedReferencedColumnIsRefused() => await Run(ForeignKeyCreateTableScenarios.RepeatedReferencedColumnIsRefused);
    [Test] public async Task ParentWithRowLevelTtlIsRefused() => await Run(ForeignKeyCreateTableScenarios.ParentWithRowLevelTtlIsRefused);
    [Test] public async Task NameTakenByACheckConstraintIsRefused() => await Run(ForeignKeyCreateTableScenarios.NameTakenByACheckConstraintIsRefused);
    [Test] public async Task UnsupportedActionIsRefusedByName() => await Run(ForeignKeyCreateTableScenarios.UnsupportedActionIsRefusedByName);
    [Test] public async Task ValidationPassesWhenEveryRowHasAParent() => await Run(ForeignKeyCreateTableScenarios.ValidationPassesWhenEveryRowHasAParent);
    [Test] public async Task ValidationReportsTheFirstOrphanInKeyOrder() => await Run(ForeignKeyCreateTableScenarios.ValidationReportsTheFirstOrphanInKeyOrder);
    [Test] public async Task ValidationMapsCompositeKeysAcrossColumnOrders() => await Run(ForeignKeyCreateTableScenarios.ValidationMapsCompositeKeysAcrossColumnOrders);

    private async Task Run(System.Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}
