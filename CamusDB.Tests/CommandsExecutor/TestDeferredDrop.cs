
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Deferred (recoverable) drop: a non-FORCE DROP of a root database/table retains all data as an
/// orphan (recoverable later via RELINK / reclaimed by the GC), while DROP ... FORCE keeps the old
/// immediate-physical-purge behavior. These tests assert both the logical detach (name/table gone
/// from the live catalog) and the physical retention/removal of the underlying KV keys.
/// </summary>
[NonParallelizable]
internal sealed class TestDeferredDrop : SharedNodeBaseTest
{
    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private async Task<int> CountKeysAsync(string bucket, string keyPrefix)
    {
        int count = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry _) in SharedKahuna.LocateAndScanRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(keyPrefix, StringComparison.Ordinal))
                count++;
        }
        return count;
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs, string tableId)> SetupTableWithRows(int rows)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        CatalogsManager catalogs = new(logger);

        KvTransaction ddlTx = await database.Transactions.BeginAsync();
        CreateTableTicket createTicket = new(
            databaseName: dbname,
            tableName: "robots",
            new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );
        await executor.CreateTable(createTicket);
        await database.Transactions.CommitAsync(ddlTx);

        string tableId = database.Schema.Tables["robots"].Id!;

        KvTransaction insertTx = await database.Transactions.BeginAsync();
        for (int i = 0; i < rows; i++)
        {
            InsertTicket ticket = new(
                txnState: insertTx,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "robot " + i) },
                        { "year", new(ColumnType.Integer64, 2000 + i) },
                    }
                }
            );
            await executor.Insert(ticket);
        }
        await database.Transactions.CommitAsync(insertTx);

        return (dbname, database, executor, catalogs, tableId);
    }

    // -----------------------------------------------------------------------
    // DROP TABLE — deferred (orphan) vs FORCE (immediate)
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task DropTable_Deferred_DetachesButRetainsDataAndWritesOrphan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs, string tableId) =
            await SetupTableWithRows(25);

        string rowBucket = $"{database.Id}:{tableId}:r";
        string rowPrefix = $"{database.Id}:{tableId}:r/";

        Assert.AreEqual(25, await CountKeysAsync(rowBucket, rowPrefix), "rows must exist before drop");

        Assert.True(await executor.DropTable(new DropTableTicket(dbname, "robots", ifExists: false)));

        // Logical detach: table is gone from the live catalog.
        Assert.False(catalogs.TableExists(database, "robots"), "table must be detached from live schema");

        // Physical retention: every row key is still on disk.
        Assert.AreEqual(25, await CountKeysAsync(rowBucket, rowPrefix), "deferred drop must NOT delete row data");

        // Orphan record captures the identity + full schema for later relink.
        List<OrphanTableRecord> orphans = await catalogs.LoadTableOrphansAsync(database);
        Assert.AreEqual(1, orphans.Count, "one orphan table record expected");
        Assert.AreEqual(tableId, orphans[0].TableId);
        Assert.AreEqual("robots", orphans[0].FormerName);
        Assert.AreEqual(3, orphans[0].Schema.Columns!.Count, "orphan record must carry the full column layout");
    }

    [Test]
    [NonParallelizable]
    public async Task DropTable_Force_PurgesDataAndWritesNoOrphan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs, string tableId) =
            await SetupTableWithRows(25);

        string rowBucket = $"{database.Id}:{tableId}:r";
        string rowPrefix = $"{database.Id}:{tableId}:r/";

        Assert.True(await executor.DropTable(new DropTableTicket(dbname, "robots", ifExists: false, force: true)));

        Assert.False(catalogs.TableExists(database, "robots"));
        Assert.AreEqual(0, await CountKeysAsync(rowBucket, rowPrefix), "FORCE drop must physically delete row data");
        Assert.IsEmpty(await catalogs.LoadTableOrphansAsync(database), "FORCE drop must NOT write an orphan record");
    }

    [Test]
    [NonParallelizable]
    public async Task DropTable_ForceViaSql_Parses()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, CatalogsManager catalogs, string tableId) =
            await SetupTableWithRows(3);

        string rowBucket = $"{database.Id}:{tableId}:r";
        string rowPrefix = $"{database.Id}:{tableId}:r/";

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, "DROP TABLE robots FORCE", null));

        Assert.False(catalogs.TableExists(database, "robots"));
        Assert.AreEqual(0, await CountKeysAsync(rowBucket, rowPrefix), "FORCE via SQL must physically delete row data");
        Assert.IsEmpty(await catalogs.LoadTableOrphansAsync(database));
    }

    // -----------------------------------------------------------------------
    // DROP DATABASE — deferred (orphan) vs FORCE (immediate)
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task DropDatabase_Deferred_DetachesButRetainsKeyspaceAndWritesOrphan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _, string tableId) =
            await SetupTableWithRows(10);
        string dbId = database.Id;

        string rowBucket = $"{dbId}:{tableId}:r";
        string rowPrefix = $"{dbId}:{tableId}:r/";
        string metaBucket = $"{dbId}/meta";
        string metaPrefix = $"{dbId}/meta";

        await executor.DropDatabase(new DropDatabaseTicket(dbname));

        // Logical detach: name no longer resolvable.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () => await executor.OpenDatabase(dbname));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);

        // Physical retention: rows and meta keys are still on disk.
        Assert.AreEqual(10, await CountKeysAsync(rowBucket, rowPrefix), "deferred drop must retain row data");
        Assert.Greater(await CountKeysAsync(metaBucket, metaPrefix), 0, "deferred drop must retain meta keys");

        // Orphan record present, keyed by the preserved id and carrying the former name.
        OrphanDatabaseRecord? orphan = await sharedRegistry!.TryGetDatabaseOrphanAsync(dbId);
        Assert.NotNull(orphan);
        Assert.AreEqual(dbId, orphan!.Id);
        Assert.AreEqual(dbname, orphan.FormerName);
    }

    [Test]
    [NonParallelizable]
    public async Task DropDatabase_Force_PurgesKeyspaceAndWritesNoOrphan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _, string tableId) =
            await SetupTableWithRows(10);
        string dbId = database.Id;

        string rowBucket = $"{dbId}:{tableId}:r";
        string rowPrefix = $"{dbId}:{tableId}:r/";
        string metaBucket = $"{dbId}/meta";
        string metaPrefix = $"{dbId}/meta";

        await executor.DropDatabase(new DropDatabaseTicket(dbname, ifExists: false, force: true));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () => await executor.OpenDatabase(dbname));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);

        Assert.AreEqual(0, await CountKeysAsync(rowBucket, rowPrefix), "FORCE drop must physically delete row data");
        Assert.AreEqual(0, await CountKeysAsync(metaBucket, metaPrefix), "FORCE drop must physically delete meta keys");
        Assert.IsNull(await sharedRegistry!.TryGetDatabaseOrphanAsync(dbId), "FORCE drop must NOT write an orphan record");
    }

    [Test]
    [NonParallelizable]
    public async Task DropDatabase_Deferred_FreesNameForFreshCreateWithNewId()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, _, _) = await SetupTableWithRows(1);
        string firstId = database.Id;

        await executor.DropDatabase(new DropDatabaseTicket(dbname));

        // The name is free immediately; a fresh (non-relink) create gets a NEW id and does not
        // disturb the orphan (which keeps the old id).
        DatabaseDescriptor recreated = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));
        Assert.AreNotEqual(firstId, recreated.Id, "recreated database must get a fresh id");

        OrphanDatabaseRecord? orphan = await sharedRegistry!.TryGetDatabaseOrphanAsync(firstId);
        Assert.NotNull(orphan, "orphan for the old id must survive a same-name fresh create");
        Assert.AreEqual(firstId, orphan!.Id);
    }
}
