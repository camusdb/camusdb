/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
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

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance checks that cross the per-statement fixtures: a schema with every constraint shape
/// survives a reopen with the same ids and the same published graph, and the TTL rule holds on the
/// ALTER path too. Each statement goes through the SQL entry point.
/// </summary>
internal static class ForeignKeyAcceptanceScenarios
{
    /// <summary>
    /// One schema with a single-column constraint, a composite one with actions, a self-reference and a
    /// constraint added by ALTER. After a reopen, every constraint has the same ids, and the published
    /// graph holds the same plans: the same tables, names, key orders, directions and states. Then
    /// each constraint is still enforced.
    /// </summary>
    public static async Task EveryConstraintAndTheGraphSurviveAReopen(CommandExecutor executor, string dbname)
    {
        await ForeignKeyAlterScenarios.Ddl(executor, dbname,
            "CREATE TABLE regions (id int64 PRIMARY KEY NOT NULL, country string NOT NULL, code int64 NOT NULL, UNIQUE KEY regions_key (code DESC, country))");
        await ForeignKeyAlterScenarios.Ddl(executor, dbname,
            "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))");
        await ForeignKeyAlterScenarios.Ddl(executor, dbname,
            "CREATE TABLE sites (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name), country string, code int64, " +
            "CONSTRAINT sites_region_fk FOREIGN KEY (country, code) REFERENCES regions (country, code) ON DELETE RESTRICT ON UPDATE RESTRICT)");
        await ForeignKeyAlterScenarios.Ddl(executor, dbname,
            "CREATE TABLE employees (id int64 PRIMARY KEY NOT NULL, manager int64 REFERENCES employees (id), site int64)");
        await ForeignKeyAlterScenarios.Ddl(executor, dbname,
            "ALTER TABLE employees ADD CONSTRAINT employees_site_fk FOREIGN KEY (site) REFERENCES sites (id)");

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        string[] tables = ["regions", "cities", "sites", "employees"];

        Dictionary<string, string> constraintsBefore = DescribeConstraints(database.Schema, tables);
        string graphBefore = DescribeGraph(database.Schema, tables);
        Assert.AreEqual(4, constraintsBefore.Count);
        Assert.AreEqual(4, database.Schema.ForeignKeys.Count);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.AreEqual(constraintsBefore, DescribeConstraints(reopened.Schema, tables), "Every constraint must keep its ids");
        Assert.AreEqual(graphBefore, DescribeGraph(reopened.Schema, tables), "The published graph must be the same after the reload");
        Assert.IsEmpty(reopened.Schema.ForeignKeys.Unresolved);

        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO regions (id, country, code) VALUES (1, 'pe', 10)");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO sites (id, city, country, code) VALUES (1, 'lima', 'pe', 10)");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO employees (id, manager, site) VALUES (1, NULL, 1), (2, 1, 1)");

        await Expect(executor, dbname, "INSERT INTO sites (id, city) VALUES (2, 'quito')", CamusDBErrorCodes.ForeignKeyViolation);
        await Expect(executor, dbname, "INSERT INTO sites (id, country, code) VALUES (2, 'pe', 11)", CamusDBErrorCodes.ForeignKeyViolation);
        await Expect(executor, dbname, "UPDATE regions SET code = 11 WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictUpdate);
        await Expect(executor, dbname, "DELETE FROM employees WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictDelete);
        await Expect(executor, dbname, "DELETE FROM sites WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictDelete);
    }

    /// <summary>
    /// Row-level TTL deletes through a path that runs no parent-side check, so a TTL table cannot be a
    /// parent. ALTER ADD CONSTRAINT must refuse it as CREATE TABLE does, and leave nothing behind.
    /// </summary>
    public static async Task AddingAConstraintToATtlParentIsRefused(CommandExecutor executor, string dbname)
    {
        await ForeignKeyAlterScenarios.Ddl(executor, dbname,
            "CREATE TABLE sessions (id int64 PRIMARY KEY NOT NULL, expires_at int64) WITH (ttl_expiration_expression = 'expires_at')");
        await ForeignKeyAlterScenarios.Ddl(executor, dbname, "CREATE TABLE events (id int64 PRIMARY KEY NOT NULL, session_id int64)");

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ForeignKeyAlterScenarios.Ddl(executor, dbname,
                "ALTER TABLE events ADD CONSTRAINT events_session_fk FOREIGN KEY (session_id) REFERENCES sessions (id)"))!;
        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, refused.Code, refused.Message);

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        Assert.IsNull(database.Schema.Tables["events"].ForeignKeys);
        Assert.IsFalse(database.Schema.Tables["events"].Indexes?.Any(i => i.Name.StartsWith("~fk_", StringComparison.Ordinal)) == true,
            "A refused ADD leaves no owned index");
    }

    /// <summary>
    /// A parent DELETE finds its children through the published graph, not through the tables that
    /// happen to be open. After a reopen no table is open; the DELETE of a referenced parent must still
    /// be refused, and so must an UPDATE of its key.
    /// </summary>
    public static async Task ParentDeleteIsCheckedWhenTheChildTableIsNotOpen(CommandExecutor executor, string dbname)
    {
        await ForeignKeyAlterScenarios.Ddl(executor, dbname,
            "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))");
        await ForeignKeyAlterScenarios.Ddl(executor, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name))");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima')");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        Assert.IsFalse(reopened.TableDescriptors.ContainsKey("weather"), "Precondition: the child table is not open");

        await Expect(executor, dbname, "DELETE FROM cities WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictDelete);
        await Expect(executor, dbname, "UPDATE cities SET name = 'lima2' WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictUpdate);
        Assert.IsEmpty(await ForeignKeyAlterScenarios.Orphans(executor, dbname));
    }

    /// <summary>
    /// Every example of <c>docs/foreign-keys.md</c>, statement by statement, with the outcome the page
    /// gives for it. If this test changes, the page changes with it.
    /// </summary>
    public static async Task TheDocumentedExamplesBehaveAsDocumented(CommandExecutor executor, string dbname)
    {
        // Syntax: an inline reference to a named column.
        await Ddl(executor, dbname,
            "CREATE TABLE cities (name string(80) NOT NULL, population int64, PRIMARY KEY (name))");
        await Ddl(executor, dbname,
            "CREATE TABLE weather (id oid NOT NULL DEFAULT (gen_id()), city string(80) REFERENCES cities (name), " +
            "temp_lo int64, day date, PRIMARY KEY (id))");

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        Assert.AreEqual("weather_city_fkey", database.Schema.Tables["weather"].ForeignKeys!.Single().Name);

        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO cities (name, population) VALUES ('San Francisco', 808000)");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO weather (city, temp_lo, day) VALUES ('San Francisco', 46, '1994-11-27')");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO weather (city, temp_lo, day) VALUES (NULL, 50, '1994-11-29')");
        await Expect(executor, dbname, "INSERT INTO weather (city, temp_lo, day) VALUES ('Berkeley', 45, '1994-11-28')", CamusDBErrorCodes.ForeignKeyViolation);
        await Expect(executor, dbname, "DELETE FROM cities WHERE name = 'San Francisco'", CamusDBErrorCodes.ForeignKeyRestrictDelete);
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "UPDATE cities SET population = 815000 WHERE name = 'San Francisco'");

        // The owned index, in SHOW INDEXES, and the clause in SHOW CREATE TABLE.
        List<QueryResultRow> indexes = await ForeignKeyAlterScenarios.Query(executor, dbname, "SHOW INDEXES FROM weather");
        Assert.IsTrue(indexes.Any(r => r.Row["Key_name"].StrValue == "~fk_weather_city_fkey"), "SHOW INDEXES lists the owned index");

        string create = (await ForeignKeyAlterScenarios.Query(executor, dbname, "SHOW CREATE TABLE weather"))[0].Row["Create Table"].StrValue!;
        Assert.That(create, Does.Contain("CONSTRAINT `weather_city_fkey` FOREIGN KEY (`city`) REFERENCES `cities` (`name`)"));

        // DDL that would break the constraint.
        await ExpectDdl(executor, dbname, "DROP TABLE cities", CamusDBErrorCodes.DependentObjectsExist);
        await ExpectDdl(executor, dbname, "TRUNCATE TABLE cities", CamusDBErrorCodes.DependentObjectsExist);
        await ExpectDdl(executor, dbname, "ALTER TABLE weather DROP COLUMN city", CamusDBErrorCodes.DependentObjectsExist);

        // Table-level, composite, with actions.
        await Ddl(executor, dbname,
            "CREATE TABLE regions (country string(2) NOT NULL, code int64 NOT NULL, name string(80), PRIMARY KEY (country, code))");
        await Ddl(executor, dbname,
            "CREATE TABLE stores (id oid NOT NULL DEFAULT (gen_id()), country string(2), region int64, PRIMARY KEY (id), " +
            "CONSTRAINT stores_region_fk FOREIGN KEY (country, region) REFERENCES regions (country, code) " +
            "ON DELETE RESTRICT ON UPDATE NO ACTION)");

        ForeignKeySchema stores = database.Schema.Tables["stores"].ForeignKeys!.Single();
        Assert.AreEqual(ForeignKeyAction.Restrict, stores.OnDelete);
        Assert.AreEqual(ForeignKeyAction.NoAction, stores.OnUpdate);

        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO regions (country, code, name) VALUES ('us', 6, 'California')");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO stores (country, region) VALUES ('us', 6)");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO stores (country, region) VALUES ('us', NULL)");
        await Expect(executor, dbname, "INSERT INTO stores (country, region) VALUES ('us', 7)", CamusDBErrorCodes.ForeignKeyViolation);

        // A self-reference to the primary key, with no column list.
        await Ddl(executor, dbname,
            "CREATE TABLE employees (id int64 NOT NULL, manager_id int64 REFERENCES employees, PRIMARY KEY (id))");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO employees (id, manager_id) VALUES (2, 1), (1, NULL)");
        await Expect(executor, dbname, "DELETE FROM employees WHERE id = 1", CamusDBErrorCodes.ForeignKeyRestrictDelete);
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "DELETE FROM employees WHERE id > 0");

        // DROP and ADD by ALTER TABLE; ADD validates the rows that exist.
        await Ddl(executor, dbname, "ALTER TABLE weather DROP CONSTRAINT weather_city_fkey");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO weather (city, temp_lo, day) VALUES ('Berkeley', 45, '1994-11-28')");

        // The orphan query of the page's known-limitation section finds exactly that row.
        List<QueryResultRow> orphans = await ForeignKeyAlterScenarios.Query(executor, dbname,
            "SELECT id FROM weather WHERE city IS NOT NULL AND city NOT IN (SELECT name FROM cities)");
        Assert.AreEqual(1, orphans.Count, "The orphan query finds the one row without a parent");

        await ExpectDdl(executor, dbname,
            "ALTER TABLE weather ADD CONSTRAINT weather_city_fk FOREIGN KEY (city) REFERENCES cities (name)",
            CamusDBErrorCodes.ForeignKeyViolation);
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "DELETE FROM weather WHERE city = 'Berkeley'");
        await Ddl(executor, dbname, "ALTER TABLE weather ADD CONSTRAINT weather_city_fk FOREIGN KEY (city) REFERENCES cities (name)");
        Assert.AreEqual(SchemaElementState.Public, database.Schema.Tables["weather"].ForeignKeys!.Single().State);

        // An action that v1 does not run is refused by name.
        CamusDBException cascade = await ExpectDdl(executor, dbname,
            "CREATE TABLE trips (id int64 PRIMARY KEY NOT NULL, city string(80) REFERENCES cities (name) ON DELETE CASCADE)",
            CamusDBErrorCodes.FeatureNotSupported);
        Assert.That(cascade.Message, Does.Contain("CASCADE").IgnoreCase);
    }

    private static Task Ddl(CommandExecutor executor, string dbname, string sql) => ForeignKeyAlterScenarios.Ddl(executor, dbname, sql);

    private static Task<CamusDBException> ExpectDdl(CommandExecutor executor, string dbname, string sql, string code)
    {
        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, sql))!;
        Assert.AreEqual(code, exception.Code, $"{sql}: {exception.Message}");
        return Task.FromResult(exception);
    }

    private static async Task Expect(CommandExecutor executor, string dbname, string sql, string code)
    {
        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ForeignKeyAlterScenarios.Dml(executor, dbname, sql))!;
        Assert.AreEqual(code, exception.Code, $"{sql}: {exception.Message}");
    }

    /// <summary>Every stored field of every constraint, keyed by constraint id.</summary>
    private static Dictionary<string, string> DescribeConstraints(Schema schema, string[] tables)
    {
        Dictionary<string, string> described = new(StringComparer.Ordinal);

        foreach (string table in tables)
        {
            foreach (ForeignKeySchema fk in schema.Tables[table].ForeignKeys ?? [])
            {
                described[fk.Id] = string.Join("|",
                    table, fk.Name, string.Join(",", fk.ColumnIds), fk.ReferencedTableId, string.Join(",", fk.ReferencedColumnIds),
                    fk.ReferencedIndexId, fk.BackingIndexId, fk.OnDelete, fk.OnUpdate, fk.Match, fk.State);
            }
        }

        return described;
    }

    /// <summary>The published plans of each table, child side and parent side, in a stable text form.</summary>
    private static string DescribeGraph(Schema schema, string[] tables)
    {
        StringBuilder sb = new();
        sb.Append("count=").Append(schema.ForeignKeys.Count).Append('\n');

        foreach (string table in tables)
        {
            string id = schema.Tables[table].Id!;

            foreach ((string side, ForeignKeyPlan[] plans) in new[] { ("child", schema.ForeignKeys.ChildPlansOf(id)), ("parent", schema.ForeignKeys.ParentPlansOf(id)) })
            {
                foreach (ForeignKeyPlan plan in plans.OrderBy(p => p.Constraint.Id, StringComparer.Ordinal))
                {
                    sb.Append(table).Append(' ').Append(side).Append(' ')
                      .Append(plan.Constraint.Id).Append(' ')
                      .Append(plan.ChildTableId).Append("->").Append(plan.ParentTableId).Append(' ')
                      .Append(string.Join(",", plan.ChildColumnNames)).Append("->").Append(string.Join(",", plan.ParentColumnNames)).Append(' ')
                      .Append("parentOrder=").Append(string.Join(",", plan.ParentKeyOrder)).Append(' ')
                      .Append("parentDirections=").Append(plan.ParentKeyDirections is null ? "-" : string.Join(",", plan.ParentKeyDirections)).Append(' ')
                      .Append("backingOrder=").Append(string.Join(",", plan.BackingKeyOrder)).Append(' ')
                      .Append("backingDirections=").Append(plan.BackingKeyDirections is null ? "-" : string.Join(",", plan.BackingKeyDirections)).Append(' ')
                      .Append("enforced=").Append(plan.IsEnforced).Append('\n');
                }
            }
        }

        return sb.ToString();
    }
}

/// <summary>The cross-cutting acceptance checks on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyAcceptance : BaseTest
{
    [Test] public async Task EveryConstraintAndTheGraphSurviveAReopen() => await Run(ForeignKeyAcceptanceScenarios.EveryConstraintAndTheGraphSurviveAReopen);
    [Test] public async Task AddingAConstraintToATtlParentIsRefused() => await Run(ForeignKeyAcceptanceScenarios.AddingAConstraintToATtlParentIsRefused);
    [Test] public async Task ParentDeleteIsCheckedWhenTheChildTableIsNotOpen() => await Run(ForeignKeyAcceptanceScenarios.ParentDeleteIsCheckedWhenTheChildTableIsNotOpen);
    [Test] public async Task TheDocumentedExamplesBehaveAsDocumented() => await Run(ForeignKeyAcceptanceScenarios.TheDocumentedExamplesBehaveAsDocumented);

    private async Task Run(Func<CommandExecutor, string, Task> scenario)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, dbname);
    }
}

/// <summary>The cross-cutting acceptance checks on a cluster-mode engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyAcceptanceCluster : SharedNodeBaseTest
{
    [Test] public async Task EveryConstraintAndTheGraphSurviveAReopen() => await Run(ForeignKeyAcceptanceScenarios.EveryConstraintAndTheGraphSurviveAReopen);
    [Test] public async Task AddingAConstraintToATtlParentIsRefused() => await Run(ForeignKeyAcceptanceScenarios.AddingAConstraintToATtlParentIsRefused);
    [Test] public async Task ParentDeleteIsCheckedWhenTheChildTableIsNotOpen() => await Run(ForeignKeyAcceptanceScenarios.ParentDeleteIsCheckedWhenTheChildTableIsNotOpen);
    [Test] public async Task TheDocumentedExamplesBehaveAsDocumented() => await Run(ForeignKeyAcceptanceScenarios.TheDocumentedExamplesBehaveAsDocumented);

    private async Task Run(Func<CommandExecutor, string, Task> scenario)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, dbname);
    }
}
