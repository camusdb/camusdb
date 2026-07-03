
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Covers the persisted keyspace catalog and the DROP DATABASE purge it drives.
///
/// The catalog (<c>{dbId}/meta/keyspace:{tableId}</c>) grows monotonically: it records every
/// index id ever allocated for a table and survives DROP TABLE / DROP INDEX. Its purpose is to
/// let DROP DATABASE purge overlay entries belonging to tables/indexes that are no longer in the
/// live schema — entries the previous live-schema-only purge could not reach. DDL currently
/// still eagerly purges on DROP TABLE/INDEX, so these tests inject an orphan entry under a dropped
/// object's keyspace to exercise the catalog-driven purge path directly.
/// </summary>
[NonParallelizable]
internal sealed class TestKeyspaceCatalogPurge : BaseTest
{
    private const string TableName = "robots";

    private static CreateTableTicket TableTicket(string dbname) => new(
        databaseName: dbname,
        tableName: TableName,
        columns: [new("id", ColumnType.Id), new("name", ColumnType.String, notNull: true)],
        constraints: [new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })],
        ifNotExists: false
    );

    private static async Task AddNameIndex(CommandExecutor executor, string dbname) =>
        Assert.IsTrue(await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: TableName,
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        )));

    private static async Task<byte[]?> AutocommitGet(IKahuna kahuna, string key)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None);
        return type == KeyValueResponseType.Get ? entry?.Value : null;
    }

    private static async Task AutocommitSet(IKahuna kahuna, string key, byte[] value)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, value, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Set, type, $"Failed to write key {key}");
    }

    /// <summary>Scans the exact routing bucket (string before the last '/') and counts keys under <paramref name="prefix"/>.</summary>
    private static async Task<int> CountKeysUnder(IKahuna kahuna, string bucket, string prefix)
    {
        int count = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(prefix, System.StringComparison.Ordinal))
                count++;
        }
        return count;
    }

    /// <summary>
    /// The catalog must list every index id (including the primary key) and must remain readable
    /// after DROP TABLE — that survival is what lets DROP DATABASE later reach a dropped table's
    /// overlay. PersistDroppedTableAsync deletes the table schema key but not the catalog key.
    /// </summary>
    [Test]
    public async Task KeyspaceCatalog_RecordsIndexIds_AndSurvivesDropTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await executor.CreateTable(TableTicket(dbname));
        await AddNameIndex(executor, dbname);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, TableName));
        string tableId = table.Id;
        string pkId = table.Indexes["~pk"].KvId;
        string nameIdxId = table.Indexes["name_idx"].KvId;

        IKahuna kahuna = database.Kahuna.Kahuna;
        string catalogKey = $"{database.Id}/meta/keyspace:{tableId}";

        byte[]? before = await AutocommitGet(kahuna, catalogKey);
        Assert.IsNotNull(before, "Catalog entry must be written when the table schema is persisted");
        string beforeJson = Encoding.UTF8.GetString(before!);
        Assert.IsTrue(beforeJson.Contains(pkId), "Catalog must record the primary-key index id");
        Assert.IsTrue(beforeJson.Contains(nameIdxId), "Catalog must record the secondary index id");

        await executor.DropTable(new DropTableTicket(databaseName: dbname, tableName: TableName, ifExists: false));

        byte[]? after = await AutocommitGet(kahuna, catalogKey);
        Assert.IsNotNull(after, "Catalog entry must survive DROP TABLE so DROP DATABASE can still purge the dropped table's overlay");
        string afterJson = Encoding.UTF8.GetString(after!);
        Assert.IsTrue(afterJson.Contains(nameIdxId), "Dropped table's index ids must remain in the catalog");
    }

    /// <summary>
    /// DROP DATABASE must purge row-bucket entries for a table that was dropped earlier, even
    /// though that table is no longer in the live schema. The catalog (kept past DROP TABLE) is
    /// the only thing that names the dropped table's id for the purge.
    /// </summary>
    [Test]
    public async Task DropDatabase_PurgesOrphanRowsUnderDroppedTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await executor.CreateTable(TableTicket(dbname));

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, TableName));
        string dbId = database.Id;
        string tableId = table.Id;
        IKahuna kahuna = database.Kahuna.Kahuna;

        await executor.DropTable(new DropTableTicket(databaseName: dbname, tableName: TableName, ifExists: false));

        // Inject an orphan row under the dropped table's keyspace, simulating an overlay entry the
        // live-schema purge cannot reach (the table id is gone from the in-memory schema).
        string rowBucket = $"{dbId}:{tableId}:r";
        string orphanKey = $"{rowBucket}/{ObjectIdGenerator.Generate()}";
        await AutocommitSet(kahuna, orphanKey, BranchKvCodec.EncodeValue(Encoding.UTF8.GetBytes("orphan")));

        Assert.AreEqual(1, await CountKeysUnder(kahuna, rowBucket, rowBucket + "/"),
            "Sanity: the injected orphan row must be present before DROP DATABASE");

        await executor.DropDatabase(new DropDatabaseTicket(dbname));

        Assert.AreEqual(0, await CountKeysUnder(kahuna, rowBucket, rowBucket + "/"),
            "DROP DATABASE must purge orphan rows under a previously dropped table via the keyspace catalog");
    }

    /// <summary>
    /// DROP DATABASE must purge index-bucket entries for an index that was dropped earlier. The
    /// catalog retains the dropped index id (it is grow-only), so the purge reaches its keyspace
    /// even though the live schema no longer references it.
    /// </summary>
    [Test]
    public async Task DropDatabase_PurgesOrphanEntriesUnderDroppedIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await executor.CreateTable(TableTicket(dbname));
        await AddNameIndex(executor, dbname);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, TableName));
        string dbId = database.Id;
        string tableId = table.Id;
        string nameIdxId = table.Indexes["name_idx"].KvId;
        IKahuna kahuna = database.Kahuna.Kahuna;

        // Drop the index — its live entries are purged and its id is retained in the catalog.
        Assert.IsTrue(await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: TableName,
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.DropIndex
        )));

        // Inject an orphan under the dropped index's keyspace.
        string indexBucket = $"{dbId}:{tableId}:i:{nameIdxId}";
        string orphanKey = $"{indexBucket}/orphankey";
        await AutocommitSet(kahuna, orphanKey, BranchKvCodec.EncodeValue(Encoding.UTF8.GetBytes(ObjectIdGenerator.Generate().ToString())));

        Assert.AreEqual(1, await CountKeysUnder(kahuna, indexBucket, indexBucket + "/"),
            "Sanity: the injected orphan index entry must be present before DROP DATABASE");

        await executor.DropDatabase(new DropDatabaseTicket(dbname));

        Assert.AreEqual(0, await CountKeysUnder(kahuna, indexBucket, indexBucket + "/"),
            "DROP DATABASE must purge orphan entries under a previously dropped index via the keyspace catalog");
    }
}
