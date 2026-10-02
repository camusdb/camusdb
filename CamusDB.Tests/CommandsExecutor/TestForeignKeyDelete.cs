/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
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
/// The parent side of a foreign key: a DELETE must not remove a row that another row still references.
/// Each scenario runs at Read Committed and Serializable with pessimistic locking, and at Read Committed
/// with optimistic locking, on a standalone and on a cluster-mode engine. The optimistic cells matter
/// most: an optimistic DELETE takes no lock of its own, so the parent side must fence the key itself.
/// </summary>
internal static class ForeignKeyDeleteScenarios
{
    private const string CreateCities =
        "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))";

    private const string CreateWeather =
        "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name))";

    public static async Task DeleteOfAReferencedParentIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima'), (2, 'quito')");
        await Dml(executor, database, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima')");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, database, dbname, "DELETE FROM cities WHERE id >= 1"))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, exception.Code);
        Assert.That(exception.Message, Does.Contain("weather_city_fkey"));
        Assert.That(exception.Message, Does.Contain("key (name)=(lima)"));
        Assert.That(exception.Message, Does.Contain("'weather'"));
        Assert.AreEqual(2, await Count(executor, database, dbname, "cities"), "The refused statement must delete nothing");
    }

    public static async Task DeleteSucceedsOnceTheChildrenAreGone(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima'), (2, 'quito')");
        await Dml(executor, database, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima'), (2, NULL)");

        await Dml(executor, database, dbname, "DELETE FROM cities WHERE id = 2");
        await Dml(executor, database, dbname, "DELETE FROM weather WHERE id = 1");
        await Dml(executor, database, dbname, "DELETE FROM cities WHERE id = 1");

        Assert.AreEqual(0, await Count(executor, database, dbname, "cities"));
        Assert.AreEqual(1, await Count(executor, database, dbname, "weather"), "A NULL reference does not hold a parent");
    }

    /// <summary>The probe reads the transaction's own writes: children deleted first in the same transaction do not count.</summary>
    public static async Task ChildrenThenParentInOneTransactionIsAccepted(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");
        await Dml(executor, database, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima'), (2, 'lima')");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await Run(executor, tx, dbname, "DELETE FROM weather WHERE city = 'lima'");
            await Run(executor, tx, dbname, "DELETE FROM cities WHERE id = 1");
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.AreEqual(0, await Count(executor, database, dbname, "cities"));
    }

    /// <summary>
    /// Self-reference: deleting a whole tree in one statement succeeds, because rows the statement
    /// deleted do not count. Deleting a manager alone is refused.
    /// </summary>
    public static async Task WholeSelfReferencingTreeDeletesInOneStatement(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, database, dbname,
            "CREATE TABLE employees (id int64 PRIMARY KEY NOT NULL, manager_id int64 REFERENCES employees (id))");
        await Dml(executor, database, dbname, "INSERT INTO employees (id, manager_id) VALUES (1, NULL), (2, 1), (3, 2), (4, 2)");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, database, dbname, "DELETE FROM employees WHERE id = 2"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, exception.Code);

        await Dml(executor, database, dbname, "DELETE FROM employees WHERE id >= 1");

        Assert.AreEqual(0, await Count(executor, database, dbname, "employees"));
    }

    /// <summary>The row API (and the gRPC Rows service behind it) reaches the same check.</summary>
    public static async Task RowApiDeleteIsChecked(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");
        await Dml(executor, database, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima')");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await executor.Delete(new DeleteTicket(
                txnState: tx,
                databaseName: dbname,
                tableName: "cities",
                where: null,
                filters: null)))!;

            Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, exception.Code);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// A DELETE from a table that nothing references does no foreign-key work, even when the table is
    /// itself a child.
    /// </summary>
    public static async Task UnreferencedTableDoesNoForeignKeyWork(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");
        await Dml(executor, database, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima'), (2, 'lima')");

        using ForeignKeyOperationCounter counter = ForeignKeyOperationCounter.Start();
        await Dml(executor, database, dbname, "DELETE FROM weather WHERE id >= 1");
        Assert.AreEqual(0, counter.Total());
    }

    /// <summary>One probe per distinct removed key, and the optimistic fence only in an optimistic cell.</summary>
    public static async Task OneProbePerDistinctRemovedKey(CommandExecutor executor, DatabaseDescriptor database, string dbname, bool optimistic)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima'), (2, 'quito'), (3, 'bogota')");

        using ForeignKeyOperationCounter counter = ForeignKeyOperationCounter.Start();
        await Dml(executor, database, dbname, "DELETE FROM cities WHERE id >= 1");

        Assert.AreEqual(3, counter.Count("parent_probe"));
        Assert.AreEqual(optimistic ? 3 : 0, counter.Count("parent_lock"));
    }

    /// <summary>
    /// DROP TABLE removes a self-referencing table with all its rows: the removal must not refuse
    /// itself.
    /// </summary>
    public static async Task DropOfASelfReferencingTableSucceeds(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, database, dbname,
            "CREATE TABLE employees (id int64 PRIMARY KEY NOT NULL, manager_id int64 REFERENCES employees (id))");
        await Dml(executor, database, dbname, "INSERT INTO employees (id, manager_id) VALUES (1, NULL), (2, 1)");

        await Ddl(executor, database, dbname, "DROP TABLE employees");

        Assert.IsFalse(database.Schema.Tables.ContainsKey("employees"));
    }

    // ── Concurrency ───────────────────────────────────────────────────────────

    /// <summary>
    /// The child locks first. A child transaction inserts a child and stays open; an older parent
    /// transaction deletes the parent and waits. The child commits within the deadline, and the DELETE
    /// then sees it.
    /// </summary>
    public static async Task DeleteWaitsForAnOpenChildAndThenRefuses(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");

        // The parent transaction begins first, so it is the older one and waits rather than dies.
        KvTransaction parentTx = await database.Transactions.BeginAsync();
        KvTransaction childTx = await database.Transactions.BeginAsync();
        try
        {
            await Run(executor, childTx, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima')");

            Task delete = Run(executor, parentTx, dbname, "DELETE FROM cities WHERE id = 1");

            await Task.Delay(100);
            await database.Transactions.CommitAsync(childTx);

            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await delete)!;
            Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, exception.Code, exception.Message);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(childTx);
            await database.Transactions.RollbackIfNotCompletedAsync(parentTx);
        }

        Assert.AreEqual(1, await Count(executor, database, dbname, "cities"));
        Assert.AreEqual(1, await Count(executor, database, dbname, "weather"));
    }

    /// <summary>
    /// The child stays open past the lock-wait deadline. The DELETE is told to retry. Once the child
    /// rolls back, the same DELETE succeeds.
    /// </summary>
    public static async Task DeleteGivesUpOnAChildThatStaysOpen(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");

        KvTransaction parentTx = await database.Transactions.BeginAsync();
        KvTransaction childTx = await database.Transactions.BeginAsync();
        try
        {
            await Run(executor, childTx, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima')");

            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
                await Run(executor, parentTx, dbname, "DELETE FROM cities WHERE id = 1"))!;
            Assert.That(exception.Code, Is.AnyOf(CamusDBErrorCodes.TransactionMustRetry, CamusDBErrorCodes.TransactionConflict), exception.Message);

            await database.Transactions.RollbackAsync(childTx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(childTx);
            await database.Transactions.RollbackIfNotCompletedAsync(parentTx);
        }

        await Dml(executor, database, dbname, "DELETE FROM cities WHERE id = 1");
        Assert.AreEqual(0, await Count(executor, database, dbname, "cities"));
    }

    /// <summary>
    /// The parent writes first and stays open, in the cell's own locking mode. A child insert in another
    /// transaction must not commit; after the DELETE commits, the parent is gone.
    /// </summary>
    public static async Task ChildCannotCommitAgainstAnOpenDelete(CommandExecutor executor, DatabaseDescriptor database, string dbname)
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

    /// <summary>
    /// A transaction that began before a child committed still sees that child when it deletes the
    /// parent: the probe reads the newest committed state, never an old snapshot.
    /// </summary>
    public static async Task DeleteInAnOlderTransactionSeesAChildCommittedSinceItBegan(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await CreateCitiesAndWeather(executor, database, dbname);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima')");

        KvTransaction parentTx = await database.Transactions.BeginAsync();
        try
        {
            await Dml(executor, database, dbname, "INSERT INTO weather (id, city) VALUES (1, 'lima')");

            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
                await Run(executor, parentTx, dbname, "DELETE FROM cities WHERE id = 1"))!;
            Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, exception.Code, exception.Message);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(parentTx);
        }
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
            (_, IAsyncEnumerable<QueryResultRow> rows) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, $"SELECT id FROM {table}", null));

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

/// <summary>DELETE enforcement on a standalone engine.</summary>
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
public sealed class TestForeignKeyDelete : BaseTest
{
    private readonly CamusIsolationLevel isolation;
    private readonly KeyValueTransactionLocking locking;

    public TestForeignKeyDelete(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        this.isolation = isolation;
        this.locking = locking;
    }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => ForeignKeyInsertCells.Apply(defaults, isolation, locking);

    [Test] public async Task DeleteOfAReferencedParentIsRefused() => await Run(ForeignKeyDeleteScenarios.DeleteOfAReferencedParentIsRefused);
    [Test] public async Task DeleteSucceedsOnceTheChildrenAreGone() => await Run(ForeignKeyDeleteScenarios.DeleteSucceedsOnceTheChildrenAreGone);
    [Test] public async Task ChildrenThenParentInOneTransactionIsAccepted() => await Run(ForeignKeyDeleteScenarios.ChildrenThenParentInOneTransactionIsAccepted);
    [Test] public async Task WholeSelfReferencingTreeDeletesInOneStatement() => await Run(ForeignKeyDeleteScenarios.WholeSelfReferencingTreeDeletesInOneStatement);
    [Test] public async Task RowApiDeleteIsChecked() => await Run(ForeignKeyDeleteScenarios.RowApiDeleteIsChecked);
    [Test] public async Task UnreferencedTableDoesNoForeignKeyWork() => await Run(ForeignKeyDeleteScenarios.UnreferencedTableDoesNoForeignKeyWork);
    [Test] public async Task OneProbePerDistinctRemovedKey() => await Run((e, d, n) => ForeignKeyDeleteScenarios.OneProbePerDistinctRemovedKey(e, d, n, locking == KeyValueTransactionLocking.Optimistic));
    [Test] public async Task DropOfASelfReferencingTableSucceeds() => await Run(ForeignKeyDeleteScenarios.DropOfASelfReferencingTableSucceeds);
    [Test] public async Task DeleteWaitsForAnOpenChildAndThenRefuses() => await Run(ForeignKeyDeleteScenarios.DeleteWaitsForAnOpenChildAndThenRefuses);
    [Test] public async Task DeleteGivesUpOnAChildThatStaysOpen() => await Run(ForeignKeyDeleteScenarios.DeleteGivesUpOnAChildThatStaysOpen);
    [Test] public async Task ChildCannotCommitAgainstAnOpenDelete() => await Run(ForeignKeyDeleteScenarios.ChildCannotCommitAgainstAnOpenDelete);
    [Test] public async Task DeleteInAnOlderTransactionSeesAChildCommittedSinceItBegan() => await Run(ForeignKeyDeleteScenarios.DeleteInAnOlderTransactionSeesAChildCommittedSinceItBegan);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}

/// <summary>DELETE enforcement on a cluster-mode engine.</summary>
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
public sealed class TestForeignKeyDeleteCluster : SharedNodeBaseTest
{
    private readonly CamusIsolationLevel isolation;
    private readonly KeyValueTransactionLocking locking;

    public TestForeignKeyDeleteCluster(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        this.isolation = isolation;
        this.locking = locking;
    }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => ForeignKeyInsertCells.Apply(defaults, isolation, locking);

    [Test] public async Task DeleteOfAReferencedParentIsRefused() => await Run(ForeignKeyDeleteScenarios.DeleteOfAReferencedParentIsRefused);
    [Test] public async Task DeleteSucceedsOnceTheChildrenAreGone() => await Run(ForeignKeyDeleteScenarios.DeleteSucceedsOnceTheChildrenAreGone);
    [Test] public async Task ChildrenThenParentInOneTransactionIsAccepted() => await Run(ForeignKeyDeleteScenarios.ChildrenThenParentInOneTransactionIsAccepted);
    [Test] public async Task WholeSelfReferencingTreeDeletesInOneStatement() => await Run(ForeignKeyDeleteScenarios.WholeSelfReferencingTreeDeletesInOneStatement);
    [Test] public async Task RowApiDeleteIsChecked() => await Run(ForeignKeyDeleteScenarios.RowApiDeleteIsChecked);
    [Test] public async Task UnreferencedTableDoesNoForeignKeyWork() => await Run(ForeignKeyDeleteScenarios.UnreferencedTableDoesNoForeignKeyWork);
    [Test] public async Task OneProbePerDistinctRemovedKey() => await Run((e, d, n) => ForeignKeyDeleteScenarios.OneProbePerDistinctRemovedKey(e, d, n, locking == KeyValueTransactionLocking.Optimistic));
    [Test] public async Task DropOfASelfReferencingTableSucceeds() => await Run(ForeignKeyDeleteScenarios.DropOfASelfReferencingTableSucceeds);
    [Test] public async Task DeleteWaitsForAnOpenChildAndThenRefuses() => await Run(ForeignKeyDeleteScenarios.DeleteWaitsForAnOpenChildAndThenRefuses);
    [Test] public async Task DeleteGivesUpOnAChildThatStaysOpen() => await Run(ForeignKeyDeleteScenarios.DeleteGivesUpOnAChildThatStaysOpen);
    [Test] public async Task ChildCannotCommitAgainstAnOpenDelete() => await Run(ForeignKeyDeleteScenarios.ChildCannotCommitAgainstAnOpenDelete);
    [Test] public async Task DeleteInAnOlderTransactionSeesAChildCommittedSinceItBegan() => await Run(ForeignKeyDeleteScenarios.DeleteInAnOlderTransactionSeesAChildCommittedSinceItBegan);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}
