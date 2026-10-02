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

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The child side of a foreign key through every INSERT entry point: a referenced parent must exist when
/// the statement ends. Each scenario runs at Read Committed and Serializable with pessimistic locking,
/// and at Read Committed with optimistic locking, on a standalone and on a cluster-mode engine: the
/// rendezvous lock is taken in every one of those modes.
/// </summary>
internal static class ForeignKeyInsertScenarios
{
    private const string CreateCities =
        "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))";

    private const string CreateWeather =
        "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name))";

    public static async Task ExistingParentIsAccepted(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");

        await Dml(executor, database, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima'), (2, 'lima')");

        Assert.AreEqual(2, await Count(executor, database, dbname, "weather"));
    }

    public static async Task MissingParentIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, database, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima'), (2, 'quito')"))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code);
        Assert.That(exception.Message, Does.Contain("weather_city_fkey"));
        Assert.That(exception.Message, Does.Contain("key (city)=(quito)"));
        Assert.That(exception.Message, Does.Contain("'weather'"));
        Assert.That(exception.Message, Does.Contain("'cities'"));
        Assert.AreEqual(0, await Count(executor, database, dbname, "weather"), "The refused statement must leave no row");
    }

    public static async Task NullReferenceIsAccepted(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);

        await Dml(executor, database, dbname, "INSERT INTO weather (id, city) VALUES (1, NULL)");
        await Dml(executor, database, dbname, "INSERT INTO weather (id) VALUES (2)");

        Assert.AreEqual(2, await Count(executor, database, dbname, "weather"));
    }

    /// <summary>MATCH SIMPLE: any NULL column satisfies the constraint; two non-NULL columns must match.</summary>
    public static async Task CompositeKeyFollowsMatchSimple(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, database, dbname,
            "CREATE TABLE regions (id int64 PRIMARY KEY NOT NULL, country string NOT NULL, code int64 NOT NULL, UNIQUE KEY regions_key (country, code))");
        await Ddl(executor, database, dbname,
            "CREATE TABLE sites (id int64 PRIMARY KEY NOT NULL, country string, code int64, FOREIGN KEY (code, country) REFERENCES regions (code, country))");
        await Dml(executor, database, dbname, "INSERT INTO regions (id, country, code) VALUES (1, 'pe', 10)");

        await Dml(executor, database, dbname, "INSERT INTO sites (id, country, code) VALUES (1, 'pe', 10)");
        await Dml(executor, database, dbname, "INSERT INTO sites (id, country, code) VALUES (2, 'xx', NULL), (3, NULL, 99)");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, database, dbname, "INSERT INTO sites (id, country, code) VALUES (4, 'pe', 20)"))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code);
        Assert.That(exception.Message, Does.Contain("key (code, country)=(20, pe)"));
        Assert.AreEqual(3, await Count(executor, database, dbname, "sites"));
    }

    /// <summary>
    /// The check runs after the statement's writes, so a row can reference itself, and a parent can come
    /// after its child in the same statement.
    /// </summary>
    public static async Task SelfReferenceInsideOneStatementIsAccepted(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, database, dbname,
            "CREATE TABLE employees (id int64 PRIMARY KEY NOT NULL, manager_id int64 REFERENCES employees (id))");

        await Dml(executor, database, dbname, "INSERT INTO employees (id, manager_id) VALUES (1, 1)");
        await Dml(executor, database, dbname, "INSERT INTO employees (id, manager_id) VALUES (2, 3), (3, NULL)");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, database, dbname, "INSERT INTO employees (id, manager_id) VALUES (4, 40)"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code);

        Assert.AreEqual(3, await Count(executor, database, dbname, "employees"));
    }

    public static async Task ParentThenChildInOneTransactionIsAccepted(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await Run(executor, tx, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");
        await Run(executor, tx, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima')");
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, await Count(executor, database, dbname, "weather"));
    }

    /// <summary>The check reads the transaction's own writes, so a parent this transaction deleted is gone.</summary>
    public static async Task ChildOfAParentDeletedInTheSameTransactionIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await Run(executor, tx, dbname, "DELETE FROM cities WHERE id = 1");

            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
                await Run(executor, tx, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima')"))!;
            Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.AreEqual(1, await Count(executor, database, dbname, "cities"), "The rollback must restore the parent");
    }

    /// <summary>
    /// Records what an explicit transaction looks like after a violation. The engine does not roll back
    /// a failed statement: its rows stay staged in the transaction, as after a duplicate key in the middle
    /// of a statement. The transports roll the whole transaction back when a statement in it fails, so a
    /// client never commits them; a caller of the engine API must do the same.
    /// </summary>
    public static async Task ExplicitTransactionAfterAViolationStillHoldsTheRows(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            Assert.ThrowsAsync<CamusDBException>(async () =>
                await Run(executor, tx, dbname, "INSERT INTO weather (id, city) VALUES (1, 'nowhere')"));

            Assert.AreEqual(1, await CountIn(executor, tx, dbname, "weather"),
                "The failed statement's row is still staged in the transaction");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.AreEqual(0, await Count(executor, database, dbname, "weather"));
    }

    /// <summary>
    /// INSERT … SELECT writes in pages. The checker spans every page and runs once at the end, so an
    /// orphan in the last page is still found, and a parent in a later page still serves a child in an
    /// earlier one. Needs an engine that writes two rows per page.
    /// </summary>
    public static async Task InsertSelectIsCheckedOnceAcrossPages(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, database, dbname,
            "CREATE TABLE employees (id int64 PRIMARY KEY NOT NULL, manager_id int64 REFERENCES employees (id))");
        await Ddl(executor, database, dbname, "CREATE TABLE staging (id int64 PRIMARY KEY NOT NULL, manager_id int64)");

        // Row 1 references row 5, which the last page writes.
        await Dml(executor, database, dbname, "INSERT INTO staging (id, manager_id) VALUES (1, 5), (2, 1), (3, 2), (4, 3), (5, NULL)");
        await Dml(executor, database, dbname, "INSERT INTO employees (id, manager_id) SELECT id, manager_id FROM staging");
        Assert.AreEqual(5, await Count(executor, database, dbname, "employees"));

        await Ddl(executor, database, dbname, "CREATE TABLE staging2 (id int64 PRIMARY KEY NOT NULL, manager_id int64)");
        await Dml(executor, database, dbname, "INSERT INTO staging2 (id, manager_id) VALUES (11, 1), (12, 2), (13, 3), (14, 4), (15, 99)");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, database, dbname, "INSERT INTO employees (id, manager_id) SELECT id, manager_id FROM staging2"))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code);
        Assert.That(exception.Message, Does.Contain("key (manager_id)=(99)"));
        Assert.AreEqual(5, await Count(executor, database, dbname, "employees"));
    }

    /// <summary>
    /// The parent writes first: a transaction has deleted the parent and not yet committed. A child
    /// insert in another transaction, in the cell's mode, must not commit. Its rendezvous lock meets the
    /// pending delete, so it either waits and then finds the parent gone, or is told to retry; it never
    /// sees the stale parent.
    ///
    /// <para>In the optimistic cell the DELETE is optimistic too. It takes no lock when it writes, so it
    /// is the parent-side check that fences the key before the DELETE commits.</para>
    /// </summary>
    public static async Task ChildInsertNeverCommitsAgainstAPendingParentDelete(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");

        KvTransaction parentTx = await database.Transactions.BeginAsync();
        try
        {
            await Run(executor, parentTx, dbname, "DELETE FROM cities WHERE id = 1");

            Task child = Dml(executor, database, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima')");

            await Task.Delay(100);
            await database.Transactions.CommitAsync(parentTx);

            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await child)!;
            Assert.That(exception.Code, Is.AnyOf(
                CamusDBErrorCodes.ForeignKeyViolation,
                CamusDBErrorCodes.TransactionMustRetry,
                CamusDBErrorCodes.TransactionConflict), exception.Message);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(parentTx);
        }

        Assert.AreEqual(0, await Count(executor, database, dbname, "weather"), "No child may reference the deleted parent");
    }

    /// <summary>The row API (and the gRPC Rows service behind it) reaches the same check.</summary>
    public static async Task RowApiInsertIsChecked(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Insert(new InsertTicket(
                txnState: tx,
                databaseName: dbname,
                tableName: "weather",
                values:
                [
                    new Dictionary<string, ColumnValue>
                    {
                        ["id"] = new(ColumnType.Integer64, 1),
                        ["city"] = new(ColumnType.String, "nowhere"),
                    }
                ])))!;

            Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// 1000 child rows that reference 3 parents take 3 rendezvous locks and one batched read. A second
    /// statement in the same transaction finds the keys already locked.
    /// </summary>
    public static async Task DistinctKeysAreLockedAndReadOnce(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima'), (2, 'quito'), (3, 'bogota')");

        string[] names = ["lima", "quito", "bogota"];
        StringBuilder sql = new("INSERT INTO weather (id, city) VALUES ");
        for (int i = 1; i <= 1000; i++)
        {
            if (i > 1)
                sql.Append(", ");
            sql.Append('(').Append(i).Append(", '").Append(names[i % 3]).Append("')");
        }

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            using (ForeignKeyOperationCounter counter = ForeignKeyOperationCounter.Start())
            {
                await Run(executor, tx, dbname, sql.ToString());

                Assert.AreEqual(3, counter.Count("lock_acquired"), "One rendezvous lock per distinct parent key");
                Assert.AreEqual(1, counter.Count("child_probe_batch"), "One batched read per constraint");
                Assert.AreEqual(3, counter.Count("child_probe_key"));
            }

            using (ForeignKeyOperationCounter counter = ForeignKeyOperationCounter.Start())
            {
                await Run(executor, tx, dbname, "INSERT INTO weather (id, city) VALUES (1001, 'lima'), (1002, 'quito')");

                Assert.AreEqual(0, counter.Count("lock_acquired"), "The keys are already locked by this transaction");
                Assert.AreEqual(2, counter.Count("lock_covered"));
            }

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.AreEqual(1002, await Count(executor, database, dbname, "weather"));
    }

    /// <summary>A table that owns no constraint does no foreign-key work, even in a database that has some.</summary>
    public static async Task TableWithoutConstraintsDoesNoForeignKeyWork(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, database, dbname, "CREATE TABLE plain (id int64 PRIMARY KEY NOT NULL, v string)");

        using (ForeignKeyOperationCounter counter = ForeignKeyOperationCounter.Start())
        {
            await Dml(executor, database, dbname, "INSERT INTO plain (id, v) VALUES (1, 'a'), (2, 'b')");
            Assert.AreEqual(0, counter.Total(), "A database with no constraint");
        }

        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");

        using (ForeignKeyOperationCounter counter = ForeignKeyOperationCounter.Start())
        {
            await Dml(executor, database, dbname, "INSERT INTO plain (id, v) VALUES (3, 'c')");
            await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (2, 'quito')");
            Assert.AreEqual(0, counter.Total(), "A table that owns no constraint, and a parent written alone");
        }
    }

    /// <summary>CREATE TABLE AS SELECT copies rows, not constraints, so its table has nothing to check.</summary>
    public static async Task CreateTableAsSelectCopiesNoConstraint(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");
        await Dml(executor, database, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima')");

        await Dml(executor, database, dbname, "CREATE TABLE weather_copy AS SELECT id, city FROM weather");

        Assert.IsNull(database.Schema.Tables["weather_copy"].ForeignKeys);
        Assert.IsEmpty(database.Schema.ForeignKeys.ChildPlansOf(database.Schema.Tables["weather_copy"].Id!));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static async Task CreateCitiesAndWeather(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, database, dbname, CreateCities);
        await Ddl(executor, database, dbname, CreateWeather);
    }

    private static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>One statement in its own transaction, rolled back when it fails, as a transport does.</summary>
    private static async Task Dml(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await Run(executor, tx, dbname, sql);
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Run(CommandExecutor executor, KvTransaction tx, string dbname, string sql) =>
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

    private static async Task<int> Count(CommandExecutor executor, DatabaseDescriptor database, string dbname, string table)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            return await CountIn(executor, tx, dbname, table);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> CountIn(CommandExecutor executor, KvTransaction tx, string dbname, string table)
    {
        (_, IAsyncEnumerable<QueryResultRow> rows) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, $"SELECT id FROM {table}", null));

        int count = 0;
        await foreach (QueryResultRow _ in rows)
            count++;

        return count;
    }
}

/// <summary>The fixture cells: isolation level and locking mode.</summary>
internal static class ForeignKeyInsertCells
{
    public static CamusDBOptions Apply(CamusDBOptions defaults, CamusIsolationLevel isolation, KeyValueTransactionLocking locking) =>
        defaults with { DefaultIsolationLevel = isolation, DefaultTransactionLocking = locking, ForceSpillThresholdRows = 2 };
}

/// <summary>INSERT enforcement on a standalone engine.</summary>
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
public sealed class TestForeignKeyInsert : BaseTest
{
    private readonly CamusIsolationLevel isolation;
    private readonly KeyValueTransactionLocking locking;

    public TestForeignKeyInsert(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        this.isolation = isolation;
        this.locking = locking;
    }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => ForeignKeyInsertCells.Apply(defaults, isolation, locking);

    [Test] public async Task ExistingParentIsAccepted() => await Run(ForeignKeyInsertScenarios.ExistingParentIsAccepted);
    [Test] public async Task MissingParentIsRefused() => await Run(ForeignKeyInsertScenarios.MissingParentIsRefused);
    [Test] public async Task NullReferenceIsAccepted() => await Run(ForeignKeyInsertScenarios.NullReferenceIsAccepted);
    [Test] public async Task CompositeKeyFollowsMatchSimple() => await Run(ForeignKeyInsertScenarios.CompositeKeyFollowsMatchSimple);
    [Test] public async Task SelfReferenceInsideOneStatementIsAccepted() => await Run(ForeignKeyInsertScenarios.SelfReferenceInsideOneStatementIsAccepted);
    [Test] public async Task ParentThenChildInOneTransactionIsAccepted() => await Run(ForeignKeyInsertScenarios.ParentThenChildInOneTransactionIsAccepted);
    [Test] public async Task ChildOfAParentDeletedInTheSameTransactionIsRefused() => await Run(ForeignKeyInsertScenarios.ChildOfAParentDeletedInTheSameTransactionIsRefused);
    [Test] public async Task ExplicitTransactionAfterAViolationStillHoldsTheRows() => await Run(ForeignKeyInsertScenarios.ExplicitTransactionAfterAViolationStillHoldsTheRows);
    [Test] public async Task InsertSelectIsCheckedOnceAcrossPages() => await Run(ForeignKeyInsertScenarios.InsertSelectIsCheckedOnceAcrossPages);
    [Test] public async Task ChildInsertNeverCommitsAgainstAPendingParentDelete() => await Run(ForeignKeyInsertScenarios.ChildInsertNeverCommitsAgainstAPendingParentDelete);
    [Test] public async Task RowApiInsertIsChecked() => await Run(ForeignKeyInsertScenarios.RowApiInsertIsChecked);
    [Test] public async Task DistinctKeysAreLockedAndReadOnce() => await Run(ForeignKeyInsertScenarios.DistinctKeysAreLockedAndReadOnce);
    [Test] public async Task TableWithoutConstraintsDoesNoForeignKeyWork() => await Run(ForeignKeyInsertScenarios.TableWithoutConstraintsDoesNoForeignKeyWork);
    [Test] public async Task CreateTableAsSelectCopiesNoConstraint() => await Run(ForeignKeyInsertScenarios.CreateTableAsSelectCopiesNoConstraint);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}

/// <summary>INSERT enforcement on a cluster-mode engine.</summary>
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
public sealed class TestForeignKeyInsertCluster : SharedNodeBaseTest
{
    private readonly CamusIsolationLevel isolation;
    private readonly KeyValueTransactionLocking locking;

    public TestForeignKeyInsertCluster(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        this.isolation = isolation;
        this.locking = locking;
    }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => ForeignKeyInsertCells.Apply(defaults, isolation, locking);

    [Test] public async Task ExistingParentIsAccepted() => await Run(ForeignKeyInsertScenarios.ExistingParentIsAccepted);
    [Test] public async Task MissingParentIsRefused() => await Run(ForeignKeyInsertScenarios.MissingParentIsRefused);
    [Test] public async Task NullReferenceIsAccepted() => await Run(ForeignKeyInsertScenarios.NullReferenceIsAccepted);
    [Test] public async Task CompositeKeyFollowsMatchSimple() => await Run(ForeignKeyInsertScenarios.CompositeKeyFollowsMatchSimple);
    [Test] public async Task SelfReferenceInsideOneStatementIsAccepted() => await Run(ForeignKeyInsertScenarios.SelfReferenceInsideOneStatementIsAccepted);
    [Test] public async Task ParentThenChildInOneTransactionIsAccepted() => await Run(ForeignKeyInsertScenarios.ParentThenChildInOneTransactionIsAccepted);
    [Test] public async Task ChildOfAParentDeletedInTheSameTransactionIsRefused() => await Run(ForeignKeyInsertScenarios.ChildOfAParentDeletedInTheSameTransactionIsRefused);
    [Test] public async Task ExplicitTransactionAfterAViolationStillHoldsTheRows() => await Run(ForeignKeyInsertScenarios.ExplicitTransactionAfterAViolationStillHoldsTheRows);
    [Test] public async Task InsertSelectIsCheckedOnceAcrossPages() => await Run(ForeignKeyInsertScenarios.InsertSelectIsCheckedOnceAcrossPages);
    [Test] public async Task ChildInsertNeverCommitsAgainstAPendingParentDelete() => await Run(ForeignKeyInsertScenarios.ChildInsertNeverCommitsAgainstAPendingParentDelete);
    [Test] public async Task RowApiInsertIsChecked() => await Run(ForeignKeyInsertScenarios.RowApiInsertIsChecked);
    [Test] public async Task DistinctKeysAreLockedAndReadOnce() => await Run(ForeignKeyInsertScenarios.DistinctKeysAreLockedAndReadOnce);
    [Test] public async Task TableWithoutConstraintsDoesNoForeignKeyWork() => await Run(ForeignKeyInsertScenarios.TableWithoutConstraintsDoesNoForeignKeyWork);
    [Test] public async Task CreateTableAsSelectCopiesNoConstraint() => await Run(ForeignKeyInsertScenarios.CreateTableAsSelectCopiesNoConstraint);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}
