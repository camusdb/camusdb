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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Self-repair of a node whose in-memory schema silently fell behind the durable checkpoint.
///
/// <para>Committed schema deltas are delivered exactly once, to the subscribers registered at that
/// instant, and never redelivered. A delta that commits while a database is unopened — or inside the
/// open-time load-to-register gap — therefore never reaches that node's catalog, and the node has no
/// local signal of the loss: the fence only advances on delivery. Observed in production as one
/// cluster node answering every request against a freshly created table with TableDoesntExist for an
/// entire run. These tests drive the repair paths: the miss-triggered probe on the table-open
/// chokepoint and the periodic freshness sweep, both of which reload the schema from the durable
/// checkpoint when its version is ahead of memory.</para>
///
/// <para>The tests simulate the dropped delivery directly — they regress the in-memory schema below
/// the persisted version — because the real race (a Raft commit landing in another node's
/// microsecond-wide registration gap) cannot be scheduled deterministically from a test.</para>
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test.
[NonParallelizable]
public sealed class TestSchemaFreshnessReconcile : BaseTest
{
    private const string TableName = "robots";

    private static async Task CreateRobotsTableAsync(CommandExecutor executor, string dbname)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: TableName,
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);
    }

    private static async Task InsertRobotAsync(CommandExecutor executor, string dbname, string name)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: txn,
            databaseName: dbname,
            tableName: TableName,
            values: new()
            {
                new()
                {
                    { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, name) },
                }
            }));
        await database.Transactions.CommitAsync(txn);
    }

    private static async Task<int> CountRobotsAsync(CommandExecutor executor, string dbname)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);

        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txn, dbname, $"SELECT * FROM {TableName}", null));

        int rows = 0;
        await foreach (QueryResultRow _ in cursor)
            rows++;

        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    /// <summary>
    /// Regresses the in-memory catalog to the state a node is left in after a dropped delivery of
    /// the table's CREATE: the durable checkpoint carries the table, memory does not, and the fence
    /// sees nothing because head is regressed with the applied version.
    /// </summary>
    private static async Task SimulateDroppedCreateDeliveryAsync(DatabaseDescriptor database)
    {
        await database.Schema.AcquireLockAsync();
        try
        {
            Assert.IsTrue(database.Schema.Tables.Remove(TableName), "Precondition: the table is in memory");
            database.Schema.SchemaVersion -= 1;
            database.Schema.RebuildRelationNameIndex();
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        database.TableDescriptors.Clear();
    }

    /// <summary>
    /// The reported production failure: a request against a table this node never learned about.
    /// The miss on the table-open chokepoint must probe the durable checkpoint, find memory behind,
    /// reload, and answer the request — instead of failing forever.
    /// </summary>
    [Test]
    public async Task AMissAgainstAStaleCatalogHealsFromTheCheckpoint()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);
        await InsertRobotAsync(executor, dbname, "TX-3000");

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        long committedVersion = database.Schema.SchemaVersion;

        await SimulateDroppedCreateDeliveryAsync(database);

        // The miss-triggered probe is cooldown-limited per database, and the open-time probe of
        // CreateDatabase already stamped the gate; wait out the cooldown so the miss can probe.
        await Task.Delay(1200);

        Assert.AreEqual(1, await CountRobotsAsync(executor, dbname),
            "A query that misses because the catalog is stale must heal from the checkpoint and answer");

        Assert.AreEqual(committedVersion, database.Schema.SchemaVersion,
            "The reload must restore the checkpoint's schema version");
        Assert.IsTrue(database.Schema.Tables.ContainsKey(TableName),
            "The reload must restore the missed table");
    }

    /// <summary>
    /// The silent variant with no user-visible miss: nothing queries the missed table, so only the
    /// periodic sweep can notice the staleness. One sweep tick must repair it.
    /// </summary>
    [Test]
    public async Task ThePeriodicSweepRepairsAStaleCatalog()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        long committedVersion = database.Schema.SchemaVersion;

        await SimulateDroppedCreateDeliveryAsync(database);

        int repaired = await executor.SweepSchemaFreshnessForTests();

        Assert.AreEqual(1, repaired, "The sweep must report exactly the one stale database as repaired");
        Assert.AreEqual(committedVersion, database.Schema.SchemaVersion,
            "The sweep must restore the checkpoint's schema version");
        Assert.IsTrue(database.Schema.Tables.ContainsKey(TableName),
            "The sweep must restore the missed table");
    }

    /// <summary>
    /// A catalog that matches the checkpoint must be left completely alone: the sweep reports
    /// nothing repaired and the live TableSchema instances are not replaced, so open descriptors
    /// and in-flight statements keep their references.
    /// </summary>
    [Test]
    public async Task ACurrentCatalogIsNotTouched()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        TableSchema before = database.Schema.Tables[TableName];

        int repaired = await executor.SweepSchemaFreshnessForTests();

        Assert.AreEqual(0, repaired, "Nothing is stale, so nothing may be repaired");
        Assert.AreSame(before, database.Schema.Tables[TableName],
            "A no-op probe must not replace live TableSchema instances");
    }

    /// <summary>
    /// A genuinely nonexistent table still fails with the same error as before: the probe finds
    /// memory current, changes nothing, and the miss surfaces unchanged.
    /// </summary>
    [Test]
    public async Task AGenuinelyMissingTableStillFails()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);

        KvTransaction txn = await database.Transactions.BeginAsync();
        try
        {
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            {
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                    new ExecuteSQLTicket(txn, dbname, "SELECT * FROM no_such_table", null));
                await foreach (QueryResultRow _ in cursor) { }
            });

            Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, ex!.Code,
                "A real miss must keep its error code after the freshness probe declines to act");
        }
        finally
        {
            await database.Transactions.RollbackAsync(txn);
        }
    }
}
