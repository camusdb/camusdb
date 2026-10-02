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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// UPDATE against a foreign key, from both sides: a child column moved to a value with no parent, and a
/// referenced parent key moved while children still use it. An UPDATE that assigns no constraint column
/// must do no foreign-key work and must never wait for a child. Each scenario runs at Read Committed and
/// Serializable with pessimistic locking, and at Read Committed with optimistic locking, on a standalone
/// and on a cluster-mode engine.
/// </summary>
internal static class ForeignKeyUpdateScenarios
{
    private const string CreateCities =
        "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, population int64, UNIQUE KEY cities_name (name))";

    private const string CreateWeather =
        "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name), temp int64)";

    public static async Task ChildUpdateToAMissingParentIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);

        await Dml(executor, database, dbname, "UPDATE weather SET city = 'quito' WHERE id = 1");
        await Dml(executor, database, dbname, "UPDATE weather SET city = NULL WHERE id = 1");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, database, dbname, "UPDATE weather SET city = 'atlantis' WHERE id = 1"))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code);
        Assert.That(exception.Message, Does.Contain("key (city)=(atlantis)"));
    }

    public static async Task ParentKeyUpdateWithChildrenIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, database, dbname, "UPDATE cities SET name = 'lima2' WHERE id = 1"))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictUpdate, exception.Code);
        Assert.That(exception.Message, Does.Contain("key (name)=(lima)"));
        Assert.That(exception.Message, Does.Contain("'weather'"));

        // The parent without children can change its key.
        await Dml(executor, database, dbname, "UPDATE cities SET name = 'quito2' WHERE id = 2");
    }

    /// <summary>Setting a key to the value it already has changes nothing, so nothing is checked.</summary>
    public static async Task ParentKeyUpdateToTheSameValueDoesNoCheck(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);

        using ForeignKeyOperationCounter counter = ForeignKeyOperationCounter.Start();
        await Dml(executor, database, dbname, "UPDATE cities SET name = 'lima' WHERE id = 1");
        await Dml(executor, database, dbname, "UPDATE weather SET city = 'lima' WHERE id = 1");

        Assert.AreEqual(0, counter.Total());
    }

    /// <summary>An UPDATE of other columns does no foreign-key work, on either side.</summary>
    public static async Task UpdateOfOtherColumnsDoesNoForeignKeyWork(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);

        using ForeignKeyOperationCounter counter = ForeignKeyOperationCounter.Start();
        await Dml(executor, database, dbname, "UPDATE cities SET population = 10 WHERE id >= 1");
        await Dml(executor, database, dbname, "UPDATE weather SET temp = temp + 1 WHERE id >= 1");

        Assert.AreEqual(0, counter.Total());
    }

    /// <summary>
    /// A child transaction holds the rendezvous lock on a parent key. An UPDATE of a non-key column of
    /// that parent does not touch the key's index entry, so it must not wait for the child.
    /// </summary>
    public static async Task NonKeyParentUpdateDoesNotWaitForAnOpenChild(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);

        KvTransaction childTx = await database.Transactions.BeginAsync();
        try
        {
            await Run(executor, childTx, dbname, "INSERT INTO weather (id, city) VALUES (10, 'lima')");

            System.Diagnostics.Stopwatch watch = System.Diagnostics.Stopwatch.StartNew();
            await Dml(executor, database, dbname, "UPDATE cities SET population = 99 WHERE id = 1");
            watch.Stop();

            Assert.Less(watch.ElapsedMilliseconds, 400, "The update must not wait out the lock deadline");

            await database.Transactions.CommitAsync(childTx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(childTx);
        }
    }

    /// <summary>
    /// A parent key UPDATE that a child holds the rendezvous lock on must not commit while the child is
    /// open, and after the child commits it sees the child.
    /// </summary>
    public static async Task ParentKeyUpdateWaitsForAnOpenChildAndThenRefuses(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);

        KvTransaction parentTx = await database.Transactions.BeginAsync();
        KvTransaction childTx = await database.Transactions.BeginAsync();
        try
        {
            await Run(executor, childTx, dbname, "INSERT INTO weather (id, city) VALUES (10, 'quito')");

            Task update = Run(executor, parentTx, dbname, "UPDATE cities SET name = 'quito2' WHERE id = 2");

            await Task.Delay(100);
            await database.Transactions.CommitAsync(childTx);

            CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () => await update)!;
            Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictUpdate, exception.Code, exception.Message);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(childTx);
            await database.Transactions.RollbackIfNotCompletedAsync(parentTx);
        }
    }

    /// <summary>
    /// Two rows trade the parents they reference in one statement. Both new references exist, so the
    /// statement succeeds.
    /// </summary>
    public static async Task SelfReferenceSwapInOneStatementSucceeds(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, database, dbname,
            "CREATE TABLE employees (id int64 PRIMARY KEY NOT NULL, manager_id int64 REFERENCES employees (id))");
        await Dml(executor, database, dbname, "INSERT INTO employees (id, manager_id) VALUES (1, NULL), (2, 1), (3, 2)");

        await Dml(executor, database, dbname,
            "UPDATE employees SET manager_id = CASE WHEN id = 2 THEN 3 ELSE 1 END WHERE id >= 2");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, database, dbname, "UPDATE employees SET manager_id = 9 WHERE id = 3"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, exception.Code);
    }

    /// <summary>
    /// Under NO ACTION, an UPDATE that hands a referenced key to another row in the same statement keeps
    /// the constraint: the key still exists when the statement ends, as in PostgreSQL.
    /// </summary>
    public static async Task KeyHandedToAnotherRowUnderNoActionSucceeds(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Setup(executor, database, dbname);

        await Dml(executor, database, dbname,
            "UPDATE cities SET name = CASE WHEN id = 1 THEN 'quito' ELSE 'lima' END WHERE id >= 1");
    }

    /// <summary>The same hand-over under RESTRICT is refused: RESTRICT does not look for the key elsewhere.</summary>
    public static async Task KeyHandedToAnotherRowUnderRestrictIsRefused(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, database, dbname, CreateCities);
        await Ddl(executor, database, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string, temp int64, FOREIGN KEY (city) REFERENCES cities (name) ON UPDATE RESTRICT)");
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name) VALUES (1, 'lima'), (2, 'quito')");
        await Dml(executor, database, dbname, "INSERT INTO weather (id, city, temp) VALUES (1, 'lima', 20)");

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Dml(executor, database, dbname,
                "UPDATE cities SET name = CASE WHEN id = 1 THEN 'quito' ELSE 'lima' END WHERE id >= 1"))!;

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictUpdate, exception.Code);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// <summary>cities lima (1) and quito (2); one weather row on lima.</summary>
    private static async Task Setup(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await Ddl(executor, database, dbname, CreateCities);
        await Ddl(executor, database, dbname, CreateWeather);
        await Dml(executor, database, dbname, "INSERT INTO cities (id, name, population) VALUES (1, 'lima', 1), (2, 'quito', 2)");
        await Dml(executor, database, dbname, "INSERT INTO weather (id, city, temp) VALUES (1, 'lima', 20)");
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
}

/// <summary>UPDATE enforcement on a standalone engine.</summary>
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
public sealed class TestForeignKeyUpdate : BaseTest
{
    private readonly CamusIsolationLevel isolation;
    private readonly KeyValueTransactionLocking locking;

    public TestForeignKeyUpdate(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        this.isolation = isolation;
        this.locking = locking;
    }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => ForeignKeyInsertCells.Apply(defaults, isolation, locking);

    [Test] public async Task ChildUpdateToAMissingParentIsRefused() => await Run(ForeignKeyUpdateScenarios.ChildUpdateToAMissingParentIsRefused);
    [Test] public async Task ParentKeyUpdateWithChildrenIsRefused() => await Run(ForeignKeyUpdateScenarios.ParentKeyUpdateWithChildrenIsRefused);
    [Test] public async Task ParentKeyUpdateToTheSameValueDoesNoCheck() => await Run(ForeignKeyUpdateScenarios.ParentKeyUpdateToTheSameValueDoesNoCheck);
    [Test] public async Task UpdateOfOtherColumnsDoesNoForeignKeyWork() => await Run(ForeignKeyUpdateScenarios.UpdateOfOtherColumnsDoesNoForeignKeyWork);
    [Test] public async Task NonKeyParentUpdateDoesNotWaitForAnOpenChild() => await Run(ForeignKeyUpdateScenarios.NonKeyParentUpdateDoesNotWaitForAnOpenChild);
    [Test] public async Task ParentKeyUpdateWaitsForAnOpenChildAndThenRefuses() => await Run(ForeignKeyUpdateScenarios.ParentKeyUpdateWaitsForAnOpenChildAndThenRefuses);
    [Test] public async Task SelfReferenceSwapInOneStatementSucceeds() => await Run(ForeignKeyUpdateScenarios.SelfReferenceSwapInOneStatementSucceeds);
    [Test] public async Task KeyHandedToAnotherRowUnderNoActionSucceeds() => await Run(ForeignKeyUpdateScenarios.KeyHandedToAnotherRowUnderNoActionSucceeds);
    [Test] public async Task KeyHandedToAnotherRowUnderRestrictIsRefused() => await Run(ForeignKeyUpdateScenarios.KeyHandedToAnotherRowUnderRestrictIsRefused);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}

/// <summary>UPDATE enforcement on a cluster-mode engine.</summary>
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.Serializable, KeyValueTransactionLocking.Pessimistic)]
[TestFixture(CamusIsolationLevel.ReadCommitted, KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
public sealed class TestForeignKeyUpdateCluster : SharedNodeBaseTest
{
    private readonly CamusIsolationLevel isolation;
    private readonly KeyValueTransactionLocking locking;

    public TestForeignKeyUpdateCluster(CamusIsolationLevel isolation, KeyValueTransactionLocking locking)
    {
        this.isolation = isolation;
        this.locking = locking;
    }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => ForeignKeyInsertCells.Apply(defaults, isolation, locking);

    [Test] public async Task ChildUpdateToAMissingParentIsRefused() => await Run(ForeignKeyUpdateScenarios.ChildUpdateToAMissingParentIsRefused);
    [Test] public async Task ParentKeyUpdateWithChildrenIsRefused() => await Run(ForeignKeyUpdateScenarios.ParentKeyUpdateWithChildrenIsRefused);
    [Test] public async Task ParentKeyUpdateToTheSameValueDoesNoCheck() => await Run(ForeignKeyUpdateScenarios.ParentKeyUpdateToTheSameValueDoesNoCheck);
    [Test] public async Task UpdateOfOtherColumnsDoesNoForeignKeyWork() => await Run(ForeignKeyUpdateScenarios.UpdateOfOtherColumnsDoesNoForeignKeyWork);
    [Test] public async Task NonKeyParentUpdateDoesNotWaitForAnOpenChild() => await Run(ForeignKeyUpdateScenarios.NonKeyParentUpdateDoesNotWaitForAnOpenChild);
    [Test] public async Task ParentKeyUpdateWaitsForAnOpenChildAndThenRefuses() => await Run(ForeignKeyUpdateScenarios.ParentKeyUpdateWaitsForAnOpenChildAndThenRefuses);
    [Test] public async Task SelfReferenceSwapInOneStatementSucceeds() => await Run(ForeignKeyUpdateScenarios.SelfReferenceSwapInOneStatementSucceeds);
    [Test] public async Task KeyHandedToAnotherRowUnderNoActionSucceeds() => await Run(ForeignKeyUpdateScenarios.KeyHandedToAnotherRowUnderNoActionSucceeds);
    [Test] public async Task KeyHandedToAnotherRowUnderRestrictIsRefused() => await Run(ForeignKeyUpdateScenarios.KeyHandedToAnotherRowUnderRestrictIsRefused);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}
