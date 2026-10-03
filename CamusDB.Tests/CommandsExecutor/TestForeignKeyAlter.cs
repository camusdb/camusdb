/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
/// <c>ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY</c> and <c>DROP CONSTRAINT</c> on a table that
/// already has rows, through the real SQL entry point. Every scenario runs on a standalone engine and
/// on a cluster-mode engine: the two build the constraint's index with different backfill loops.
/// </summary>
internal static class ForeignKeyAlterScenarios
{
    private const string CreateCities =
        "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))";

    private const string CreateWeather = "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string)";

    internal const string AddConstraint =
        "ALTER TABLE weather ADD CONSTRAINT weather_city_fk FOREIGN KEY (city) REFERENCES cities (name)";

    private const string OwnedIndex = "~fk_weather_city_fk";

    /// <summary>
    /// The table has rows and no index on the referencing column, so the statement builds one and
    /// backfills it. The parent-side check then finds the existing children through that index, which
    /// proves the backfill. Everything survives a reopen.
    /// </summary>
    public static async Task AddOverRowsBuildsAnIndexAndSurvivesReopen(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Seed(executor, dbname, withUserIndex: false);

        await Ddl(executor, dbname, AddConstraint);

        TableSchema weather = database.Schema.Tables["weather"];
        ForeignKeySchema constraint = weather.ForeignKeys!.Single();
        TableIndexSchema owned = weather.Indexes!.Single(i => i.Name == OwnedIndex);

        Assert.AreEqual("weather_city_fk", constraint.Name);
        Assert.AreEqual(SchemaElementState.Public, constraint.State, "ALTER must return with the constraint public");
        Assert.AreEqual(owned.KvId, constraint.BackingIndexId);
        Assert.AreEqual(constraint.Id, owned.OwnerConstraintId);
        Assert.AreEqual(SchemaElementState.Public, owned.State);
        Assert.IsTrue(database.Schema.ForeignKeys.ChildPlansOf(weather.Id!).Single().IsEnforced);
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database), "No rollout job may be left behind");

        await AssertEnforcedOnBothSides(executor, dbname);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        TableSchema reloaded = reopened.Schema.Tables["weather"];
        ForeignKeySchema loaded = reloaded.ForeignKeys!.Single();
        Assert.AreEqual(constraint.Id, loaded.Id);
        Assert.AreEqual(SchemaElementState.Public, loaded.State);
        Assert.AreEqual(loaded.Id, reloaded.Indexes!.Single(i => i.Name == OwnedIndex).OwnerConstraintId);

        await AssertEnforcedOnBothSides(executor, dbname);
    }

    /// <summary>DROP removes the constraint and the index the engine built for it, and stays dropped after a reopen.</summary>
    public static async Task DropRemovesTheConstraintAndItsOwnedIndex(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Seed(executor, dbname, withUserIndex: false);
        await Ddl(executor, dbname, AddConstraint);

        await Ddl(executor, dbname, "ALTER TABLE weather DROP CONSTRAINT weather_city_fk");

        TableSchema weather = database.Schema.Tables["weather"];
        Assert.IsNull(weather.ForeignKeys);
        Assert.IsFalse(weather.Indexes!.Any(i => i.Name == OwnedIndex), "The index built for the constraint must go with it");
        Assert.IsTrue(database.Schema.ForeignKeys.IsEmpty);

        // Neither side is checked any more.
        await Dml(executor, dbname, "INSERT INTO weather (id, city) VALUES (100, 'atlantis')");
        await Dml(executor, dbname, "DELETE FROM cities WHERE id = 1");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.IsNull(reopened.Schema.Tables["weather"].ForeignKeys);
        Assert.IsFalse(reopened.Schema.Tables["weather"].Indexes!.Any(i => i.Name == OwnedIndex));
    }

    /// <summary>A user index that leads with the column is reused; DROP keeps it.</summary>
    public static async Task AddReusesAUserIndexAndDropKeepsIt(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Seed(executor, dbname, withUserIndex: true);
        int indexes = database.Schema.Tables["weather"].Indexes!.Count;

        await Ddl(executor, dbname, AddConstraint);

        TableSchema weather = database.Schema.Tables["weather"];
        Assert.AreEqual(indexes, weather.Indexes!.Count, "A matching user index must be reused, not duplicated");
        Assert.AreEqual(weather.Indexes.Single(i => i.Name == "weather_city").KvId, weather.ForeignKeys!.Single().BackingIndexId);
        Assert.IsTrue(weather.Indexes.All(i => i.OwnerConstraintId is null));

        await AssertEnforcedOnBothSides(executor, dbname);

        await Ddl(executor, dbname, "ALTER TABLE weather DROP CONSTRAINT weather_city_fk");

        Assert.IsNull(database.Schema.Tables["weather"].ForeignKeys);
        Assert.IsNotNull(database.Schema.Tables["weather"].Indexes!.SingleOrDefault(i => i.Name == "weather_city"), "A user index must stay");
    }

    /// <summary>
    /// A row without a parent fails the ALTER with the key in the message. Nothing is left: no
    /// constraint, no index built for it, no job.
    /// </summary>
    public static async Task AddOverAnOrphanIsRefusedAndLeavesNothing(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Seed(executor, dbname, withUserIndex: false);
        await Dml(executor, dbname, "INSERT INTO weather (id, city) VALUES (50, 'atlantis')");
        int indexes = database.Schema.Tables["weather"].Indexes!.Count;

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, AddConstraint))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code, exception.Message);
        Assert.That(exception.Message, Does.Contain("atlantis"));
        Assert.That(exception.Message, Does.Contain("weather_city_fk"));

        TableSchema weather = database.Schema.Tables["weather"];
        Assert.IsNull(weather.ForeignKeys);
        Assert.AreEqual(indexes, weather.Indexes!.Count, "The index built for the refused constraint must be dropped");
        Assert.IsTrue(database.Schema.ForeignKeys.IsEmpty);
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));

        // The same statement succeeds once the orphan is gone.
        await Dml(executor, dbname, "DELETE FROM weather WHERE id = 50");
        await Ddl(executor, dbname, AddConstraint);
        Assert.AreEqual(SchemaElementState.Public, database.Schema.Tables["weather"].ForeignKeys!.Single().State);
    }

    /// <summary>With a reused user index, a refused ALTER keeps that index.</summary>
    public static async Task AddOverAnOrphanKeepsAReusedUserIndex(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Seed(executor, dbname, withUserIndex: true);
        await Dml(executor, dbname, "INSERT INTO weather (id, city) VALUES (50, 'atlantis')");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, AddConstraint))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code, exception.Message);
        Assert.IsNull(database.Schema.Tables["weather"].ForeignKeys);
        Assert.IsNotNull(database.Schema.Tables["weather"].Indexes!.SingleOrDefault(i => i.Name == "weather_city"));
    }

    /// <summary>
    /// A refused statement changes nothing: no index is built for a constraint whose definition is wrong.
    /// </summary>
    public static async Task InvalidDefinitionsAreRefusedBeforeAnyChange(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Seed(executor, dbname, withUserIndex: false);
        await Ddl(executor, dbname, "ALTER TABLE weather ADD CONSTRAINT weather_rule CHECK (id > 0)");
        long version = database.Schema.SchemaVersion;

        (string Sql, string Code)[] cases =
        [
            ("ALTER TABLE weather ADD CONSTRAINT w_fk FOREIGN KEY (city) REFERENCES nowhere (name)", CamusDBErrorCodes.TableDoesntExist),
            ("ALTER TABLE weather ADD CONSTRAINT w_fk FOREIGN KEY (id) REFERENCES cities (name)", CamusDBErrorCodes.InvalidForeignKeyDefinition),
            ("ALTER TABLE weather ADD CONSTRAINT w_fk FOREIGN KEY (nope) REFERENCES cities (name)", CamusDBErrorCodes.UnknownColumn),
            ("ALTER TABLE weather ADD CONSTRAINT w_fk FOREIGN KEY (city) REFERENCES cities (id, name)", CamusDBErrorCodes.InvalidForeignKeyDefinition),
            ("ALTER TABLE weather ADD CONSTRAINT weather_rule FOREIGN KEY (city) REFERENCES cities (name)", CamusDBErrorCodes.InvalidInput),
            ("ALTER TABLE weather ADD CONSTRAINT w_fk FOREIGN KEY (city) REFERENCES cities (name) ON DELETE CASCADE", CamusDBErrorCodes.FeatureNotSupported),
        ];

        foreach ((string sql, string code) in cases)
        {
            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(executor, dbname, sql))!;
            Assert.AreEqual(code, exception.Code, $"{sql}: {exception.Message}");
        }

        Assert.AreEqual(version, database.Schema.SchemaVersion, "A refused ALTER must not advance the schema");
        Assert.IsNull(database.Schema.Tables["weather"].ForeignKeys);
        Assert.IsFalse(database.Schema.Tables["weather"].Indexes!.Any(i => i.Name.StartsWith("~fk_", StringComparison.Ordinal)));
    }

    /// <summary>DROP CONSTRAINT of a name that matches nothing keeps the existing error.</summary>
    public static async Task DropOfAnUnknownNameIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Seed(executor, dbname, withUserIndex: false);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Ddl(executor, dbname, "ALTER TABLE weather DROP CONSTRAINT nothing_here"))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        Assert.That(exception.Message, Does.Contain("does not exist"));
    }

    /// <summary>
    /// Two tables that would reference each other are refused with the cycle code, and no index is built
    /// for the refused constraint. A table that references itself is accepted and enforced.
    /// </summary>
    public static async Task CyclesAreRefusedAndASelfReferenceIsAccepted(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, dbname, "CREATE TABLE teams (id int64 PRIMARY KEY NOT NULL, captain_id int64)");
        await Ddl(executor, dbname, "CREATE TABLE players (id int64 PRIMARY KEY NOT NULL, team_id int64)");
        await Ddl(executor, dbname, "ALTER TABLE players ADD CONSTRAINT players_team_fk FOREIGN KEY (team_id) REFERENCES teams (id)");
        int teamIndexes = database.Schema.Tables["teams"].Indexes!.Count;

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Ddl(executor, dbname, "ALTER TABLE teams ADD CONSTRAINT teams_captain_fk FOREIGN KEY (captain_id) REFERENCES players (id)"))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyCycle, exception.Code, exception.Message);
        Assert.IsNull(database.Schema.Tables["teams"].ForeignKeys);
        Assert.AreEqual(teamIndexes, database.Schema.Tables["teams"].Indexes!.Count, "No index may be built for a refused cycle");

        await Ddl(executor, dbname, "CREATE TABLE employees (id int64 PRIMARY KEY NOT NULL, manager_id int64)");
        await Dml(executor, dbname, "INSERT INTO employees (id, manager_id) VALUES (1, NULL), (2, 1)");
        await Ddl(executor, dbname, "ALTER TABLE employees ADD CONSTRAINT employees_manager_fk FOREIGN KEY (manager_id) REFERENCES employees (id)");

        Assert.AreEqual(SchemaElementState.Public, database.Schema.Tables["employees"].ForeignKeys!.Single().State);

        CamusDBException orphan = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, dbname, "INSERT INTO employees (id, manager_id) VALUES (3, 99)"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, orphan.Code);
    }

    /// <summary>
    /// The workload runs through the whole statement: it inserts children of live parents and deletes
    /// parents that have no children. Its writes land before the constraint exists, while it is
    /// WriteOnly and after it is Public. A burst runs at the one point the rollout cannot see on its own:
    /// after every node enforces the constraint and before the validation pass. At the end a full check
    /// finds no orphan.
    /// </summary>
    public static async Task AddSucceedsUnderAConcurrentWorkload(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Seed(executor, dbname, withUserIndex: false);
        await Dml(executor, dbname, "INSERT INTO cities (id, name) VALUES " +
            string.Join(", ", Enumerable.Range(10, 40).Select(i => $"({i}, 'c{i}')")));

        int nextId = 1000;
        int refusedDeletes = 0;

        // Only legal writes: cities 10..29 are never deleted and get the children, cities 30..49 never
        // get children and are deleted. A delete of a parent with children before the constraint
        // exists would make a real orphan, and the ALTER would then fail for a good reason.
        async Task WorkloadStep(int round)
        {
            int child = Interlocked.Increment(ref nextId);
            await Dml(executor, dbname, $"INSERT INTO weather (id, city) VALUES ({child}, 'c{10 + round % 20}')");
            await Dml(executor, dbname, $"DELETE FROM cities WHERE id = {30 + round % 20}");
        }

        // Runs only while the constraint is WriteOnly: city 10 has children, so its delete must be refused.
        async Task BurstStep(int round)
        {
            await WorkloadStep(round);

            try
            {
                await Dml(executor, dbname, "DELETE FROM cities WHERE id = 10");
            }
            catch (CamusDBException ex) when (ex.Code is CamusDBErrorCodes.ForeignKeyRestrictDelete or CamusDBErrorCodes.ForeignKeyViolation)
            {
                Interlocked.Increment(ref refusedDeletes);
            }
        }

        await WorkloadStep(0);

        using CancellationTokenSource stop = new();
        Task background = Task.Run(async () =>
        {
            int round = 1;
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    await WorkloadStep(round++);
                }
                catch (CamusDBException)
                {
                    // A write that conflicts with the DDL is refused, not lost; the next round goes on.
                }
            }
        });

        int burstRefusals = -1;
        executor.TestInterceptBeforeForeignKeyValidation = async () =>
        {
            int before = Volatile.Read(ref refusedDeletes);
            for (int i = 0; i < 5; i++)
                await BurstStep(100 + i);
            burstRefusals = Volatile.Read(ref refusedDeletes) - before;
        };

        try
        {
            await Ddl(executor, dbname, AddConstraint);
        }
        finally
        {
            executor.TestInterceptBeforeForeignKeyValidation = null;
            stop.Cancel();
            await background;
        }

        Assert.AreEqual(SchemaElementState.Public, database.Schema.Tables["weather"].ForeignKeys!.Single().State);
        Assert.AreEqual(5, burstRefusals, "While WriteOnly, a parent DELETE that would orphan a child must be refused");
        Assert.IsTrue(database.Schema.Tables["weather"].Indexes!.Any(i => i.Name == OwnedIndex));

        Assert.IsEmpty(await Orphans(executor, dbname), "A full check after the statement must find no orphan");
        Assert.DoesNotThrowAsync(async () => await executor.ValidateForeignKeyRowsAsync(database, "weather", "weather_city_fk"));
    }

    /// <summary>
    /// ADD CONSTRAINT and DROP TABLE of the parent, at the same time. Exactly one succeeds, and the
    /// survivor is consistent after a reopen.
    /// </summary>
    public static async Task AddRacingADropOfTheParentLetsExactlyOneWin(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Seed(executor, dbname, withUserIndex: false);

        Task add = Task.Run(() => Ddl(executor, dbname, AddConstraint));
        Task drop = Task.Run(() => Ddl(executor, dbname, "DROP TABLE cities"));

        Exception? addError = await Capture(add);
        Exception? dropError = await Capture(drop);

        Assert.IsTrue((addError is null) ^ (dropError is null),
            $"Exactly one statement must succeed. ADD: {addError?.Message ?? "ok"}; DROP: {dropError?.Message ?? "ok"}");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        TableSchema weather = reopened.Schema.Tables["weather"];

        if (addError is null)
        {
            Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, ((CamusDBException)dropError!).Code, dropError.Message);
            Assert.IsTrue(reopened.Schema.Tables.ContainsKey("cities"));
            Assert.AreEqual(SchemaElementState.Public, weather.ForeignKeys!.Single().State);
            Assert.IsTrue(reopened.Schema.ForeignKeys.ChildPlansOf(weather.Id!).Single().IsEnforced);
            await AssertEnforcedOnBothSides(executor, dbname);
        }
        else
        {
            Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, ((CamusDBException)addError).Code, addError.Message);
            Assert.IsFalse(reopened.Schema.Tables.ContainsKey("cities"));
            Assert.IsNull(weather.ForeignKeys);
            Assert.IsFalse(weather.Indexes!.Any(i => i.Name == OwnedIndex));
            Assert.IsTrue(reopened.Schema.ForeignKeys.IsEmpty);
        }

        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(reopened));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>cities 1..3 and weather rows that reference them, plus one NULL key.</summary>
    internal static async Task Seed(CommandExecutor executor, string dbname, bool withUserIndex)
    {
        await Ddl(executor, dbname, CreateCities);
        await Ddl(executor, dbname, withUserIndex
            ? "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string, KEY weather_city (city))"
            : CreateWeather);

        await Dml(executor, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima'), (2, 'quito'), (3, 'bogota')");
        await Dml(executor, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima'), (2, 'quito'), (3, 'lima'), (4, NULL)");
    }

    /// <summary>A child without a parent is refused, and so is the DELETE of a parent that has children.</summary>
    private static async Task AssertEnforcedOnBothSides(CommandExecutor executor, string dbname)
    {
        CamusDBException child = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, dbname, "INSERT INTO weather (id, city) VALUES (900, 'atlantis')"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, child.Code, child.Message);

        CamusDBException parent = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, dbname, "DELETE FROM cities WHERE id = 1"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, parent.Code, parent.Message);
    }

    /// <summary>The weather rows whose city names no row of cities, read with plain queries.</summary>
    internal static async Task<List<long>> Orphans(CommandExecutor executor, string dbname)
    {
        HashSet<string> names = [];
        foreach (QueryResultRow row in await Query(executor, dbname, "SELECT name FROM cities"))
            names.Add(row.Row["name"].StrValue!);

        List<long> orphans = [];
        foreach (QueryResultRow row in await Query(executor, dbname, "SELECT id, city FROM weather"))
        {
            ColumnValue city = row.Row["city"];
            if (city.Type != ColumnType.Null && !names.Contains(city.StrValue!))
                orphans.Add(row.Row["id"].LongValue);
        }

        return orphans;
    }

    internal static async Task<List<QueryResultRow>> Query(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> rows) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            List<QueryResultRow> result = [];
            await foreach (QueryResultRow row in rows)
                result.Add(row);
            return result;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<Exception?> Capture(Task task)
    {
        try
        {
            await task;
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    internal static async Task Ddl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
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

    internal static async Task Dml(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
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
}

/// <summary>ALTER TABLE ADD and DROP CONSTRAINT of a foreign key on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyAlter : BaseTest
{
    [Test] public async Task AddOverRowsBuildsAnIndexAndSurvivesReopen() => await Run(ForeignKeyAlterScenarios.AddOverRowsBuildsAnIndexAndSurvivesReopen);
    [Test] public async Task DropRemovesTheConstraintAndItsOwnedIndex() => await Run(ForeignKeyAlterScenarios.DropRemovesTheConstraintAndItsOwnedIndex);
    [Test] public async Task AddReusesAUserIndexAndDropKeepsIt() => await Run(ForeignKeyAlterScenarios.AddReusesAUserIndexAndDropKeepsIt);
    [Test] public async Task AddOverAnOrphanIsRefusedAndLeavesNothing() => await Run(ForeignKeyAlterScenarios.AddOverAnOrphanIsRefusedAndLeavesNothing);
    [Test] public async Task AddOverAnOrphanKeepsAReusedUserIndex() => await Run(ForeignKeyAlterScenarios.AddOverAnOrphanKeepsAReusedUserIndex);
    [Test] public async Task InvalidDefinitionsAreRefusedBeforeAnyChange() => await Run(ForeignKeyAlterScenarios.InvalidDefinitionsAreRefusedBeforeAnyChange);
    [Test] public async Task DropOfAnUnknownNameIsRefused() => await Run(ForeignKeyAlterScenarios.DropOfAnUnknownNameIsRefused);
    [Test] public async Task CyclesAreRefusedAndASelfReferenceIsAccepted() => await Run(ForeignKeyAlterScenarios.CyclesAreRefusedAndASelfReferenceIsAccepted);
    [Test] public async Task AddSucceedsUnderAConcurrentWorkload() => await Run(ForeignKeyAlterScenarios.AddSucceedsUnderAConcurrentWorkload);
    [Test] public async Task AddRacingADropOfTheParentLetsExactlyOneWin() => await Run(ForeignKeyAlterScenarios.AddRacingADropOfTheParentLetsExactlyOneWin);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}

/// <summary>
/// ALTER TABLE ADD and DROP CONSTRAINT of a foreign key on a cluster-mode engine, where the index is built
/// by the staged coordinator and the constraint passes the ack gate before its validation.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyAlterCluster : SharedNodeBaseTest
{
    [Test] public async Task AddOverRowsBuildsAnIndexAndSurvivesReopen() => await Run(ForeignKeyAlterScenarios.AddOverRowsBuildsAnIndexAndSurvivesReopen);
    [Test] public async Task DropRemovesTheConstraintAndItsOwnedIndex() => await Run(ForeignKeyAlterScenarios.DropRemovesTheConstraintAndItsOwnedIndex);
    [Test] public async Task AddReusesAUserIndexAndDropKeepsIt() => await Run(ForeignKeyAlterScenarios.AddReusesAUserIndexAndDropKeepsIt);
    [Test] public async Task AddOverAnOrphanIsRefusedAndLeavesNothing() => await Run(ForeignKeyAlterScenarios.AddOverAnOrphanIsRefusedAndLeavesNothing);
    [Test] public async Task AddOverAnOrphanKeepsAReusedUserIndex() => await Run(ForeignKeyAlterScenarios.AddOverAnOrphanKeepsAReusedUserIndex);
    [Test] public async Task InvalidDefinitionsAreRefusedBeforeAnyChange() => await Run(ForeignKeyAlterScenarios.InvalidDefinitionsAreRefusedBeforeAnyChange);
    [Test] public async Task DropOfAnUnknownNameIsRefused() => await Run(ForeignKeyAlterScenarios.DropOfAnUnknownNameIsRefused);
    [Test] public async Task CyclesAreRefusedAndASelfReferenceIsAccepted() => await Run(ForeignKeyAlterScenarios.CyclesAreRefusedAndASelfReferenceIsAccepted);
    [Test] public async Task AddSucceedsUnderAConcurrentWorkload() => await Run(ForeignKeyAlterScenarios.AddSucceedsUnderAConcurrentWorkload);
    [Test] public async Task AddRacingADropOfTheParentLetsExactlyOneWin() => await Run(ForeignKeyAlterScenarios.AddRacingADropOfTheParentLetsExactlyOneWin);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}
