
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Catalogs.Models;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// DROP DATABASE tests in cluster mode (shared Kahuna node, OwnsKahuna=false).
/// Verifies PurgeClusterKeyspaceAsync actually removes keys from the shared KV store.
/// </summary>
[TestFixture]
internal sealed class TestDatabaseDropperCluster : SharedNodeBaseTest
{
    /// <summary>
    /// Scans the shared Kahuna node for all keys whose stored key starts with
    /// <paramref name="prefix"/> and returns them.
    ///
    /// Uses <see cref="HLCTimestamp.Zero"/> as the transaction id (autocommit read) so
    /// the scan sees every committed key directly — no MVCC snapshot required.  This
    /// mirrors the pattern used in Kahuna's own integration tests for LocateAndScanRange
    /// (see TestScanRangeFullTableSingleNode) and matches the autocommit path used by
    /// <see cref="DatabaseDropper.PurgeClusterKeyspaceAsync"/>.
    ///
    /// The scan bucket (prefix argument to LocateAndScanRange) must be the "bucket prefix"
    /// — the part before the last '/' of actual stored keys — e.g. "{tableId}:r" not
    /// "{tableId}:r/", consistent with how KvTableStore.ScanRows uses rowBucketPrefix.
    /// </summary>
    private async Task<List<string>> ScanKeysAsync(IKahuna kahuna, string bucketPrefix, string keyPrefix)
    {
        List<string> found = [];
        await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
            HLCTimestamp.Zero,
            bucketPrefix, null, true, null, true,
            512,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            CancellationToken.None))
        {
            if (key.StartsWith(keyPrefix, StringComparison.Ordinal))
                found.Add(key);
        }
        return found;
    }

    /// <summary>
    /// After DROP DATABASE in cluster mode:
    ///   - {id}/ meta keys must be gone
    ///   - {id}: stats keys must be gone
    ///   - {tableId}:r/ row keys must be gone
    ///   - {tableId}:i:{indexId}/ index keys must be gone
    ///   - the registry entry must be removed so the name can be reused with a fresh id
    /// </summary>
    [Test]
    public async Task ClusterDrop_AllKeysRemovedAndNameReusable()
    {
        (string dbname, DatabaseDescriptor first, CommandExecutor executor) = await CreateDatabase();
        string firstId = first.Id;

        // Create a table so the shared node gets real meta, row, and index keys.
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "things",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String, notNull: true)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(10));

        // Insert a row so row-data keys actually exist in the shared node.
        KvTransaction txn = await first.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: txn,
            databaseName: dbname,
            tableName: "things",
            values: new() { new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "name", new(ColumnType.String, "alice") } } }
        )).WaitAsync(TimeSpan.FromSeconds(10));
        await first.Transactions.CommitAsync(txn);

        // Capture the table id and index id before dropping (schema is torn down during drop).
        string? tableId = null;
        string? indexId = null;
        foreach (TableSchema t in first.Schema.Tables.Values)
        {
            tableId = t.Id;
            indexId = t.Indexes?.Count > 0 ? t.Indexes[0].Id : null;
        }
        Assert.IsNotNull(tableId, "Expected at least one table in schema");

        IKahuna sharedKahuna = first.Kahuna.Kahuna;

        // Verify keys exist before drop so the test isn't vacuously true.
        // Bucket prefix = the string before the last '/' of the stored keys (matches
        // CatalogsManager, KvTableStore, and DatabaseDropper conventions).
        List<string> metaBefore = await ScanKeysAsync(sharedKahuna, $"{firstId}/meta", $"{firstId}/");
        Assert.IsNotEmpty(metaBefore, $"Expected {firstId}/ meta keys before drop");

        List<string> rowsBefore = await ScanKeysAsync(sharedKahuna, $"{tableId}:r", $"{tableId}:r/");
        Assert.IsNotEmpty(rowsBefore, $"Expected {tableId}:r/ row keys before drop");

        await executor.DropDatabase(new DropDatabaseTicket(dbname));

        // Post-drop: scan using the same kahuna reference (the shared node is not disposed).

        // ---- assert meta keys gone ----
        List<string> metaAfter = await ScanKeysAsync(sharedKahuna, $"{firstId}/meta", $"{firstId}/");
        Assert.IsEmpty(metaAfter, $"Expected all {firstId}/ meta keys to be purged after drop; found: {string.Join(", ", metaAfter)}");

        // ---- assert stats keys gone ----
        // Stats keys are {id}:stats:{tableId} — bucket = "{id}:" (everything sharing that prefix).
        List<string> statsAfter = await ScanKeysAsync(sharedKahuna, $"{firstId}:", $"{firstId}:");
        Assert.IsEmpty(statsAfter, $"Expected all {firstId}: stats keys to be purged after drop; found: {string.Join(", ", statsAfter)}");

        // ---- assert row keys gone ----
        List<string> rowsAfter = await ScanKeysAsync(sharedKahuna, $"{tableId}:r", $"{tableId}:r/");
        Assert.IsEmpty(rowsAfter, $"Expected all {tableId}:r/ row keys to be purged after drop; found: {string.Join(", ", rowsAfter)}");

        // ---- assert index keys gone (if an index id was found) ----
        if (indexId is not null)
        {
            List<string> indexAfter = await ScanKeysAsync(sharedKahuna, $"{tableId}:i:{indexId}", $"{tableId}:i:{indexId}/");
            Assert.IsEmpty(indexAfter, $"Expected all {tableId}:i:{indexId}/ index keys to be purged after drop; found: {string.Join(", ", indexAfter)}");
        }

        // ---- no on-disk directory in cluster mode ----
        Assert.IsFalse(Directory.Exists(Path.Combine(CamusConfig.DataDirectory, firstId)),
            "No per-database directory must exist in cluster mode");

        // ---- registry entry must be gone ----
        CommandExecutor executor2 = CreateCommandExecutor();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor2.OpenDatabase(dbname));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);

        // ---- name reusable with a fresh id ----
        DatabaseDescriptor second = await executor2.CreateDatabase(
            new CreateDatabaseTicket(dbname, ifNotExists: false));
        Assert.AreNotEqual(firstId, second.Id, "Recreated database must have a fresh id");
        Assert.AreEqual(dbname, second.Name);
    }

    /// <summary>
    /// DROP DATABASE IF EXISTS on an unknown name is a no-op in cluster mode.
    /// </summary>
    [Test]
    public async Task ClusterDrop_IfExistsOnUnknown_IsNoOp()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string ghost = Guid.NewGuid().ToString("n");
        await executor.DropDatabase(new DropDatabaseTicket(ghost, ifExists: true));
    }
}
