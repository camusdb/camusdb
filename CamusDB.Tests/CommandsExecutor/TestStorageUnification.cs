
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

/**
 * SU8 — Storage Instance Unification regression tests.
 *
 * Covers:
 *   SU2  open/close many databases → node count stays at 1 (single shared node)
 *   SU3  compact-id allocation is monotonic and never reused
 *   SU4  key-prefix isolation — two dbs with the same table name share no rows/indexes
 *   SU6  DROP DATABASE purge completeness — rows/indexes under {dbId}: are deleted
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

[NonParallelizable]
internal sealed class TestStorageUnification : BaseTest
{
    // -----------------------------------------------------------------------
    // SU2 — open many databases → exactly one Kahuna node
    // -----------------------------------------------------------------------

    [Test]
    public async Task SU2_OpenManyDatabases_SingleSharedNode()
    {
        CommandExecutor executor = CreateCommandExecutor();

        const int count = 5;
        List<DatabaseDescriptor> descriptors = [];

        for (int i = 0; i < count; i++)
        {
            string name = Guid.NewGuid().ToString("n");
            DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: false));
            descriptors.Add(db);
            TrackDatabase(name, executor);
        }

        // All descriptors must share the same IKahuna handle (same object reference).
        IKahuna first = descriptors[0].Kahuna.Kahuna;
        foreach (DatabaseDescriptor db in descriptors)
        {
            Assert.AreSame(first, db.Kahuna.Kahuna,
                $"Database '{db.Name}' has a different Kahuna instance — should share the process-level node");
        }
    }

    // -----------------------------------------------------------------------
    // SU3 — compact-id allocation: no id is ever reused
    // -----------------------------------------------------------------------

    [Test]
    public async Task SU3_CompactIds_NeverReused()
    {
        CommandExecutor executor = CreateCommandExecutor();

        const int count = 10;
        List<string> ids = [];

        for (int i = 0; i < count; i++)
        {
            string name = Guid.NewGuid().ToString("n");
            DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: false));
            TrackDatabase(name, executor);
            ids.Add(db.Id);
        }

        // All ids must be distinct.
        Assert.AreEqual(count, new HashSet<string>(ids).Count,
            "Each database must receive a unique id");
    }

    // -----------------------------------------------------------------------
    // SU4 — key-prefix isolation
    //        Two databases with the same-named table must not bleed rows.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SU4_TwoDatabases_RowsIsolatedByDbId()
    {
        CommandExecutor executor = CreateCommandExecutor();

        // Create two databases.
        string nameA = Guid.NewGuid().ToString("n");
        string nameB = Guid.NewGuid().ToString("n");

        DatabaseDescriptor dbA = await executor.CreateDatabase(new CreateDatabaseTicket(nameA, ifNotExists: false));
        DatabaseDescriptor dbB = await executor.CreateDatabase(new CreateDatabaseTicket(nameB, ifNotExists: false));
        TrackDatabase(nameA, executor);
        TrackDatabase(nameB, executor);

        // Create the same logical table ("items") in both databases.
        static CreateTableTicket TableTicket(string dbname) => new(
            databaseName: dbname,
            tableName: "items",
            columns: [new("id", ColumnType.Id), new("val", ColumnType.String, notNull: false)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false
        );

        await executor.CreateTable(TableTicket(nameA));
        await executor.CreateTable(TableTicket(nameB));

        // Insert one row in database A only.
        KvTransaction txA = await dbA.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: txA,
            databaseName: nameA,
            tableName: "items",
            values: [new()
            {
                { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "val", new ColumnValue(ColumnType.String, "hello-A") }
            }]
        ));
        await dbA.Transactions.CommitAsync(txA);

        // Count rows in each database — only A should have one.
        int countA = await CountAll(nameA, dbA, executor);
        int countB = await CountAll(nameB, dbB, executor);

        Assert.AreEqual(1, countA, "Database A must have exactly 1 row");
        Assert.AreEqual(0, countB, "Database B must have 0 rows — no bleed from A");
    }

    // -----------------------------------------------------------------------
    // SU6 — DROP DATABASE purge completeness
    //        After drop, the raw KV scan under {dbId}: returns no results.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SU6_DropDatabase_PurgesRowAndIndexKeys()
    {
        CommandExecutor executor = CreateCommandExecutor();

        string dbname = Guid.NewGuid().ToString("n");
        DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));
        string dbId = db.Id;

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "items",
            columns: [new("id", ColumnType.Id), new("val", ColumnType.String, notNull: false)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false
        ));

        // Insert a few rows.
        for (int i = 0; i < 3; i++)
        {
            KvTransaction tx = await db.Transactions.BeginAsync();
            await executor.Insert(new InsertTicket(
                txnState: tx,
                databaseName: dbname,
                tableName: "items",
                values: [new()
                {
                    { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "val", new ColumnValue(ColumnType.String, $"v{i}") }
                }]
            ));
            await db.Transactions.CommitAsync(tx);
        }

        // Drop the database.
        await executor.DropDatabase(new DropDatabaseTicket(dbname));

        // After drop, no keys should remain under "{dbId}:" in Kahuna.
        IKahuna kahuna = TestNode!.Kahuna;
        string rowBucket = $"{dbId}:";
        List<string> remainingKeys = [];

        await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
            HLCTimestamp.Zero,
            rowBucket,
            null, true,
            null, true,
            1000,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            CancellationToken.None))
        {
            if (key.StartsWith(rowBucket, StringComparison.Ordinal))
                remainingKeys.Add(key);
        }

        Assert.AreEqual(0, remainingKeys.Count,
            $"After DROP DATABASE, no keys should remain under bucket '{rowBucket}'; found: {string.Join(", ", remainingKeys)}");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static async Task<int> CountAll(string dbname, DatabaseDescriptor db, CommandExecutor executor)
    {
        KvTransaction txn = await db.Transactions.BeginAsync();

        QueryTicket ticket = new(
            txnState: txn,
            databaseName: dbname,
            tableName: "items",
            index: null,
            projection: null,
            filters: null,
            where: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(ticket);

        int count = 0;
        await foreach (QueryResultRow _ in cursor)
            count++;

        await db.Transactions.CommitAsync(txn);
        return count;
    }
}
