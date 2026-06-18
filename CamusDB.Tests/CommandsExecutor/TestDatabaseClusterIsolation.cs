
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// DBX.1 — Cluster mode isolation: two databases sharing a single in-memory Kahuna node
/// must have fully independent key spaces (rows, schema, stats), independent schemas,
/// and RENAME on one must leave the other unaffected.
/// </summary>
[TestFixture]
internal sealed class TestDatabaseClusterIsolation : SharedNodeBaseTest
{
    // -----------------------------------------------------------------------
    // Helpers (mirrors TestDatabaseDropperCluster.ScanKeysAsync)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Scans the shared Kahuna node for all keys whose stored key starts with
    /// <paramref name="keyPrefix"/>, using <paramref name="bucketPrefix"/> as the
    /// LocateAndScanRange bucket argument (the substring before the last '/' of stored keys).
    /// </summary>
    private static async Task<List<string>> ScanKeysAsync(IKahuna kahuna, string bucketPrefix, string keyPrefix)
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

    private static CreateTableTicket ItemsTableTicket(string dbName) => new(
        databaseName: dbName,
        tableName: "items",
        columns:
        [
            new ColumnInfo("id",  ColumnType.Id),
            new ColumnInfo("val", ColumnType.String, notNull: true)
        ],
        constraints:
        [
            new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                [new ColumnIndexInfo("id", OrderType.Ascending)])
        ],
        ifNotExists: false
    );

    private static async Task<string> InsertRow(
        CommandExecutor executor, DatabaseDescriptor db, string dbName, string val)
    {
        string rowId = ObjectIdGenerator.Generate().ToString();
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: tx,
            databaseName: dbName,
            tableName: "items",
            values: new() { new() {
                { "id",  new(ColumnType.Id,     rowId) },
                { "val", new(ColumnType.String, val)   }
            } }
        )).WaitAsync(TimeSpan.FromSeconds(10));
        await db.Transactions.CommitAsync(tx);
        return rowId;
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// <summary>
    /// Row keys written to database A must live under A's table id prefix and must not
    /// appear under database B's table id prefix, even though both databases share the same
    /// Kahuna node (confirmed at the raw KV level — id-based namespacing is load-bearing).
    /// </summary>
    [Test]
    public async Task ClusterTwoDatabases_RowKeysAreNamespaced()
    {
        (string nameA, DatabaseDescriptor dbA, CommandExecutor executor) = await CreateDatabase();
        (string nameB, DatabaseDescriptor dbB, _) = await CreateDatabase();

        await executor.CreateTable(ItemsTableTicket(nameA)).WaitAsync(TimeSpan.FromSeconds(10));
        await executor.CreateTable(ItemsTableTicket(nameB)).WaitAsync(TimeSpan.FromSeconds(10));

        string tableIdA = dbA.Schema.Tables["items"].Id!;
        string tableIdB = dbB.Schema.Tables["items"].Id!;

        Assert.AreNotEqual(tableIdA, tableIdB,
            "Two databases must have independent table ids for a same-named table");

        await InsertRow(executor, dbA, nameA, "from-A");
        await InsertRow(executor, dbB, nameB, "from-B");

        IKahuna kahuna = SharedKahuna;

        // Each database's bucket must contain exactly the one row inserted above.
        // A count of 2 would mean a duplicate bleed (one insert wrote into both prefixes);
        // a count of 0 would mean the insert never landed.
        List<string> rowsA = await ScanKeysAsync(kahuna, $"{tableIdA}:r", $"{tableIdA}:r/");
        Assert.AreEqual(1, rowsA.Count,
            $"db A's bucket must contain exactly 1 row key under {tableIdA}:r/; got {rowsA.Count}");

        List<string> rowsB = await ScanKeysAsync(kahuna, $"{tableIdB}:r", $"{tableIdB}:r/");
        Assert.AreEqual(1, rowsB.Count,
            $"db B's bucket must contain exactly 1 row key under {tableIdB}:r/; got {rowsB.Count}");
    }

    /// <summary>
    /// Schema for database A (table "items") must be invisible to database B's descriptor, and
    /// schema for database B (table "widgets") must be invisible to database A's descriptor.
    /// This confirms the schema-log keys are id-prefixed and replication does not bleed across
    /// the two databases sharing the same node.
    /// </summary>
    [Test]
    public async Task ClusterTwoDatabases_SchemaIsIsolated()
    {
        (string nameA, DatabaseDescriptor dbA, CommandExecutor executor) = await CreateDatabase();
        (string nameB, DatabaseDescriptor dbB, _) = await CreateDatabase();

        // Each database gets a differently named table.
        await executor.CreateTable(new CreateTableTicket(
            databaseName: nameA,
            tableName: "items",
            columns:   [new ColumnInfo("id", ColumnType.Id), new ColumnInfo("val", ColumnType.String)],
            constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(10));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: nameB,
            tableName: "widgets",
            columns:   [new ColumnInfo("id", ColumnType.Id), new ColumnInfo("name", ColumnType.String)],
            constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
            ifNotExists: false
        )).WaitAsync(TimeSpan.FromSeconds(10));

        // A knows about "items" but must not know about "widgets".
        Assert.IsTrue(dbA.Schema.Tables.ContainsKey("items"),
            "db A must have the 'items' table in its schema");
        Assert.IsFalse(dbA.Schema.Tables.ContainsKey("widgets"),
            "db A must NOT have db B's 'widgets' table");

        // B knows about "widgets" but must not know about "items".
        Assert.IsTrue(dbB.Schema.Tables.ContainsKey("widgets"),
            "db B must have the 'widgets' table in its schema");
        Assert.IsFalse(dbB.Schema.Tables.ContainsKey("items"),
            "db B must NOT have db A's 'items' table");
    }

    /// <summary>
    /// Renaming database A to a new name must not disturb database B:
    ///   - B's id, schema, and row keys are unchanged.
    ///   - B remains accessible through its original name.
    ///   - A is accessible through the new name (returns the same cached descriptor).
    ///   - Opening A by its old name throws DatabaseDoesntExist.
    ///
    /// Both databases are created through a single CommandExecutor so the descriptor cache
    /// is shared — AreSame assertions check the cache is hit, not a fresh reload.
    /// </summary>
    [Test]
    public async Task ClusterTwoDatabases_RenameOneDoesNotDisturbOther()
    {
        // Use one executor for both databases so both descriptors live in the same cache.
        CommandExecutor executor = CreateCommandExecutor();

        string nameA = "db_" + Guid.NewGuid().ToString("n");
        string nameB = "db_" + Guid.NewGuid().ToString("n");
        TrackDatabase(nameA, executor);
        TrackDatabase(nameB, executor);

        DatabaseDescriptor dbA = await executor.CreateDatabase(new CreateDatabaseTicket(nameA, ifNotExists: false));
        DatabaseDescriptor dbB = await executor.CreateDatabase(new CreateDatabaseTicket(nameB, ifNotExists: false));

        await executor.CreateTable(ItemsTableTicket(nameA)).WaitAsync(TimeSpan.FromSeconds(10));
        await executor.CreateTable(ItemsTableTicket(nameB)).WaitAsync(TimeSpan.FromSeconds(10));

        string tableIdB = dbB.Schema.Tables["items"].Id!;

        await InsertRow(executor, dbA, nameA, "a-row");
        await InsertRow(executor, dbB, nameB, "b-row");

        // Verify B's rows exist before rename.
        List<string> bRowsBefore = await ScanKeysAsync(SharedKahuna, $"{tableIdB}:r", $"{tableIdB}:r/");
        Assert.IsNotEmpty(bRowsBefore, "B must have row keys before rename of A");

        string newNameA = "db_" + Guid.NewGuid().ToString("n");
        TrackDatabase(newNameA, executor);

        await executor.RenameDatabase(new RenameDatabaseTicket(nameA, newNameA));

        // Old name A must throw.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.OpenDatabase(nameA));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code,
            "Opening A by old name after rename must throw DatabaseDoesntExist");

        // New name A must return the same cached descriptor (cache keyed by id, id unchanged).
        DatabaseDescriptor reopenedA = await executor.OpenDatabase(newNameA);
        Assert.AreSame(dbA, reopenedA,
            "Open(newNameA) must return the same cached descriptor — no eviction/reload");
        Assert.AreEqual(dbA.Id, reopenedA.Id, "A's id must be unchanged after rename");

        // B must still return its cached descriptor from the same executor.
        DatabaseDescriptor reopenedB = await executor.OpenDatabase(nameB);
        Assert.AreSame(dbB, reopenedB,
            "B must still return its cached descriptor after A was renamed");
        Assert.AreEqual(dbB.Id, reopenedB.Id, "B's id must be unchanged after A's rename");

        // B's row keys must be intact in the KV store.
        List<string> bRowsAfter = await ScanKeysAsync(SharedKahuna, $"{tableIdB}:r", $"{tableIdB}:r/");
        Assert.AreEqual(bRowsBefore.Count, bRowsAfter.Count,
            "B's row key count must be unchanged after A was renamed");

        // B's schema must be intact.
        Assert.IsTrue(reopenedB.Schema.Tables.ContainsKey("items"),
            "B's 'items' table must still be in its schema after A was renamed");
    }

    /// <summary>
    /// The registry's <c>_system/</c> key prefix and per-database <c>{id}/</c> meta key
    /// prefixes must be disjoint.  Both scans are asserted non-empty so neither direction
    /// of the check is vacuous: the registry scan proves registry keys exist in the shared
    /// node; the meta scan (after table creation) proves per-database meta keys exist.
    /// </summary>
    [Test]
    public async Task ClusterRegistry_SystemPrefixDoesNotCollideWithDatabaseIds()
    {
        CommandExecutor executor = CreateCommandExecutor();

        string nameA = "db_" + Guid.NewGuid().ToString("n");
        string nameB = "db_" + Guid.NewGuid().ToString("n");
        TrackDatabase(nameA, executor);
        TrackDatabase(nameB, executor);

        DatabaseDescriptor dbA = await executor.CreateDatabase(new CreateDatabaseTicket(nameA, ifNotExists: false));
        DatabaseDescriptor dbB = await executor.CreateDatabase(new CreateDatabaseTicket(nameB, ifNotExists: false));

        // Create a table in each database so per-database meta keys are actually written.
        await executor.CreateTable(ItemsTableTicket(nameA)).WaitAsync(TimeSpan.FromSeconds(10));
        await executor.CreateTable(ItemsTableTicket(nameB)).WaitAsync(TimeSpan.FromSeconds(10));

        // ── registry direction ───────────────────────────────────────────────────
        // Registry keys are stored as _system/dbregistry/db:{name}.
        // The Kahuna scan bucket is the string before the last segment separator:
        // bucket = "_system/dbregistry", key-filter prefix = "_system/dbregistry/".
        List<string> systemKeys = await ScanKeysAsync(SharedKahuna, "_system/dbregistry", "_system/dbregistry/");
        Assert.IsNotEmpty(systemKeys,
            "The shared node must have _system/dbregistry/ registry keys; if empty, the registry prefix has changed");

        foreach (string key in systemKeys)
        {
            Assert.IsFalse(key.StartsWith(dbA.Id, StringComparison.Ordinal),
                $"Registry key '{key}' must not start with db A's id '{dbA.Id}'");
            Assert.IsFalse(key.StartsWith(dbB.Id, StringComparison.Ordinal),
                $"Registry key '{key}' must not start with db B's id '{dbB.Id}'");
        }

        // ── per-database meta direction ──────────────────────────────────────────
        List<string> aMetaKeys = await ScanKeysAsync(SharedKahuna, $"{dbA.Id}/meta", $"{dbA.Id}/");
        Assert.IsNotEmpty(aMetaKeys,
            $"db A must have meta keys under {dbA.Id}/; if empty, no table was persisted");

        foreach (string key in aMetaKeys)
            Assert.IsFalse(key.StartsWith("_system/", StringComparison.Ordinal),
                $"db A meta key '{key}' must not start with _system/");

        List<string> bMetaKeys = await ScanKeysAsync(SharedKahuna, $"{dbB.Id}/meta", $"{dbB.Id}/");
        Assert.IsNotEmpty(bMetaKeys,
            $"db B must have meta keys under {dbB.Id}/; if empty, no table was persisted");

        foreach (string key in bMetaKeys)
            Assert.IsFalse(key.StartsWith("_system/", StringComparison.Ordinal),
                $"db B meta key '{key}' must not start with _system/");
    }
}
