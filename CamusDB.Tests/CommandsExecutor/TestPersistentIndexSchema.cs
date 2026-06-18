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
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Proves the persist → reopen → deserialize round-trip for TableSchema.Indexes.
///
/// Extends <see cref="BaseTest"/> directly (NOT SharedNodeBaseTest) so each test gets
/// a per-database SQLite-backed Kahuna node. CloseDatabase flushes + disposes it;
/// the subsequent OpenDatabase recreates the node from the same on-disk files, exercising
/// the full LoadMetaAsync → TableSchema.Indexes deserialization path.
/// </summary>
internal sealed class TestPersistentIndexSchema : BaseTest
{
    private const string TableName = "robots";

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTableWithIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();

        CreateTableTicket createTicket = new(
            databaseName: dbname,
            tableName: TableName,
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        );

        await executor.CreateTable(createTicket);
        await database.Transactions.CommitAsync(tx);

        // Insert a handful of rows so the index has entries to scan.
        for (int i = 1; i <= 5; i++)
        {
            KvTransaction txIns = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: txIns,
                database: dbname,
                sql: $"INSERT INTO {TableName} (id, name, year) VALUES (gen_id(), 'robot {i}', {2000 + i})",
                parameters: null
            ));
            await database.Transactions.CommitAsync(txIns);
        }

        // Add a secondary multi-valued index on 'name'.
        AlterIndexTicket alterTicket = new(
            databaseName: dbname,
            tableName: TableName,
            indexName: "name_idx",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex
        );
        Assert.IsTrue(await executor.AlterIndex(alterTicket));

        return (dbname, database, executor);
    }

    /// <summary>
    /// After a close+reopen cycle the table's persisted KV entry must deserialize
    /// TableSchema.Indexes correctly, and FORCE_INDEX must return rows.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task IndexSchemaRoundTrip_PersistsAndDeserializesAfterReopen()
    {
        (string dbname, _, CommandExecutor executor) = await SetupTableWithIndex();

        // Flush + evict the descriptor. The SQLite files remain on disk.
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

        // Fresh open: new Kahuna node, LoadMetaAsync reads TableSchema.Indexes from KV.
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        // TableSchema.Indexes must be populated with at least the PK and the secondary index.
        Assert.IsTrue(db2.Schema.Tables.TryGetValue(TableName, out TableSchema? schema),
            "Table must be present after reopen");

        Assert.IsNotNull(schema!.Indexes, "TableSchema.Indexes must not be null after reopen");
        Assert.IsTrue(schema.Indexes!.Count >= 2,
            $"Expected at least 2 indexes (pk + name_idx), got {schema.Indexes.Count}");

        TableIndexSchema? nameIdx = schema.Indexes.FirstOrDefault(ix => ix.Name == "name_idx");
        Assert.IsNotNull(nameIdx, "name_idx must be present in TableSchema.Indexes after reopen");
        Assert.AreEqual(SchemaElementState.Public, nameIdx!.State);
        Assert.IsNotNull(nameIdx.ColumnIds, "ColumnIds must be persisted for name_idx");
        Assert.AreEqual(1, nameIdx.ColumnIds!.Length);

        // FORCE_INDEX scan must work end-to-end — proves TableOpener resolved the index.
        KvTransaction tx = await db2.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: tx,
                database: dbname,
                sql: $"SELECT id FROM {TableName}@{{FORCE_INDEX=name_idx}}",
                parameters: null
            ));

        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db2.Transactions.CommitAsync(tx);

        Assert.AreEqual(5, rows.Count,
            "FORCE_INDEX scan after reopen must return all 5 rows");
    }

    /// <summary>
    /// Even with SystemSchema.Indexes cleared in memory, reopening the table descriptor
    /// must load the index from TableSchema.Indexes alone — the new B1 path.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task IndexSchemaRoundTrip_NewPathStandsAloneWithSystemSchemaCleared()
    {
        (string dbname, _, CommandExecutor executor) = await SetupTableWithIndex();

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        // Simulate the post-migration state: SystemSchema.Indexes is empty.
        // This mirrors a node that has fully migrated and no longer writes to SystemSchema.
        db2.SystemSchema.Indexes.Clear();

        // Evict the cached TableDescriptor so the next access rebuilds it from scratch
        // via TableOpener.LoadTable — which must use tableSchema.Indexes, not SystemSchema.
        db2.TableDescriptors.TryRemove(TableName, out _);

        // FORCE_INDEX forces TableOpener to resolve the index. It must succeed without
        // SystemSchema.Indexes.
        KvTransaction tx = await db2.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: tx,
                database: dbname,
                sql: $"SELECT id FROM {TableName}@{{FORCE_INDEX=name_idx}}",
                parameters: null
            ));

        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db2.Transactions.CommitAsync(tx);

        Assert.AreEqual(5, rows.Count,
            "FORCE_INDEX scan must work when SystemSchema.Indexes is empty (B1 new path)");
    }

    /// <summary>
    /// Dropping an index must delete its KV entries. Verified via a unique index:
    /// if the old entries were not purged, the re-add backfill would fail with
    /// DuplicateUniqueKeyValue when it encounters the stale SetIfNotExists entries.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task DropIndex_PurgesKvEntries_AllowsCleanReAdd()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithIndex();

        // Add a UNIQUE index so any stale KV entries would cause a duplicate error on re-add.
        AlterIndexTicket addUnique = new(
            databaseName: dbname,
            tableName: TableName,
            indexName: "name_unique",
            columns: new ColumnIndexInfo[] { new("name", OrderType.Ascending) },
            operation: AlterIndexOperation.AddUniqueIndex
        );
        Assert.IsTrue(await executor.AlterIndex(addUnique));

        // Drop it — DropIndexEntries must purge the 5 KV entries.
        AlterIndexTicket drop = new(
            databaseName: dbname,
            tableName: TableName,
            indexName: "name_unique",
            columns: [],
            operation: AlterIndexOperation.DropIndex
        );
        Assert.IsTrue(await executor.AlterIndex(drop));

        // Re-add the same unique index. The backfill uses SetIfNotExists; stale entries
        // would cause it to throw DuplicateUniqueKeyValue here.
        Assert.DoesNotThrowAsync(async () => await executor.AlterIndex(addUnique),
            "Re-adding a unique index after drop must not find stale KV entries");

        // Verify the re-added index is queryable and returns the right count.
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: tx,
                database: dbname,
                sql: $"SELECT id FROM {TableName}@{{FORCE_INDEX=name_unique}}",
                parameters: null
            ));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(5, rows.Count,
            "FORCE_INDEX on re-added unique index must return exactly 5 rows");
    }

    /// <summary>
    /// <summary>
    /// Proves that BackfillIndexEntriesAsync fires its onCheckpoint callback exactly once
    /// when the row count crosses the BackfillBatchSize boundary (600 rows → 2 batches:
    /// batch 1 = 500 rows + checkpoint, batch 2 = 100 rows + no checkpoint because the
    /// scan returns fewer than BackfillBatchSize entries, signalling completion).
    ///
    /// Calls BackfillIndexEntriesAsync directly (bypassing the coordinator) so the
    /// checkpoint batching logic is tested independent of the cluster DDL path.
    /// The unique-index type exercises the backfill-mode idempotency read-back (NotSet
    /// on SetIfNotExists → read back to confirm same rowId, not a genuine duplicate).
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BackfillCheckpointFiresOnceForTwoBatches()
    {
        const int RowCount = 600;

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: TableName,
            columns:
            [
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64)
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            ],
            ifNotExists: false
        ));

        for (int i = 0; i < RowCount; i++)
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: tx,
                database: dbname,
                sql: $"INSERT INTO {TableName} (id, name, year) VALUES (gen_id(), 'robot_{i:D5}', {2000 + i})",
                parameters: null
            ));
            await database.Transactions.CommitAsync(tx);
        }

        // Resolve the 'name' column ID from the persisted schema so IndexBuildInfo is valid.
        database.Schema.Tables.TryGetValue(TableName, out TableSchema? tableSchema);
        string nameColId = tableSchema!.Columns!.First(c => c.Name == "name").Id!;

        // Use a unique index to exercise the SetIfNotExists + read-back idempotency path.
        IndexBuildInfo indexInfo = new(
            IndexId: "000000000000000000000001",
            IndexName: "name_backfill_test",
            ColumnIds: [nameColId],
            ColumnNames: ["name"],
            IndexType: IndexType.Unique
        );

        // Track intermediate checkpoint fires and record a captured onCheckpoint offset
        // to confirm the checkpoint write callback is also invoked.
        int checkpointFires = 0;
        string? capturedCheckpointOffset = null;
        executor.TestInterceptAfterBackfillCheckpoint = () =>
        {
            Interlocked.Increment(ref checkpointFires);
            return Task.CompletedTask;
        };

        try
        {
            // Call BackfillIndexEntriesAsync directly so we can supply our own onCheckpoint.
            // This tests the batching + checkpoint logic without the full coordinator path.
            await executor.BackfillIndexEntriesAsync(
                database,
                TableName,
                indexInfo,
                startOffset: null,
                onCheckpoint: offset =>
                {
                    capturedCheckpointOffset = offset;
                    return Task.CompletedTask;
                }
            );
        }
        finally
        {
            executor.TestInterceptAfterBackfillCheckpoint = null;
        }

        // Exactly one intermediate checkpoint: after batch 1 (500 rows). Batch 2 has
        // 100 rows < BackfillBatchSize, so the loop exits with no further checkpoint.
        Assert.AreEqual(1, checkpointFires,
            "Expected exactly one intermediate checkpoint for 600 rows with batch size 500");

        // The onCheckpoint callback must also have been called with a non-empty offset
        // (this is the value that would be persisted to Kahuna for leader-change resume).
        Assert.IsNotNull(capturedCheckpointOffset,
            "onCheckpoint callback must be invoked with the last rowId of batch 1");
        Assert.IsNotEmpty(capturedCheckpointOffset,
            "Checkpoint offset must be a non-empty rowId string");
    }

    /// <summary>
    /// Re-running the backfill over rows it already indexed must be a no-op, not a duplicate-key
    /// error. This is the leader-change-resume safety net: the checkpoint advances only AFTER a
    /// batch commits, so a crash in that window leaves a committed batch whose offset was never
    /// recorded — on resume the backfill re-processes those rows. For a unique index each re-write
    /// returns <c>NotSet</c>, and <c>backfillMode</c> reads the existing entry back: same rowId ⇒
    /// idempotent skip (no throw). Running the backfill twice from the start reproduces exactly
    /// that overlap and exercises the read-back skip arm for every row.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task UniqueIndexBackfillRerunIsIdempotent()
    {
        const int RowCount = 50;

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: TableName,
            columns:
            [
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true)
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            ],
            ifNotExists: false
        ));

        for (int i = 0; i < RowCount; i++)
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: tx,
                database: dbname,
                sql: $"INSERT INTO {TableName} (id, name) VALUES (gen_id(), 'robot_{i:D5}')",
                parameters: null
            ));
            await database.Transactions.CommitAsync(tx);
        }

        database.Schema.Tables.TryGetValue(TableName, out TableSchema? tableSchema);
        string nameColId = tableSchema!.Columns!.First(c => c.Name == "name").Id!;

        IndexBuildInfo indexInfo = new(
            IndexId: "000000000000000000000001",
            IndexName: "name_idempotent",
            ColumnIds: [nameColId],
            ColumnNames: ["name"],
            IndexType: IndexType.Unique
        );

        // First pass: indexes all rows (every PutIndexEntry returns Set).
        await executor.BackfillIndexEntriesAsync(database, TableName, indexInfo, startOffset: null);

        // Second pass over the SAME rows: every key now exists, so each PutIndexEntry returns
        // NotSet and backfillMode reads it back. Same rowId ⇒ skip. Must not throw.
        Assert.DoesNotThrowAsync(
            async () => await executor.BackfillIndexEntriesAsync(database, TableName, indexInfo, startOffset: null),
            "Re-running the unique-index backfill over already-indexed rows must be idempotent (no DuplicateUniqueKeyValue)");

        // The index must remain intact — exactly RowCount entries, no duplicates introduced.
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, TableName));
        KvTransaction scanTx = await database.Transactions.BeginAsync();
        int entries = 0;
        await foreach (var _ in table.Store.ScanIndex(
            scanTx, indexInfo.IndexName, [ColumnType.String], from: null, to: null, unique: true))
        {
            entries++;
        }
        await database.Transactions.CommitAsync(scanTx);

        Assert.AreEqual(RowCount, entries,
            "The unique index must hold exactly one entry per row after a re-run (no duplicate entries)");
    }

    /// <summary>
    /// The complementary arm of the <c>backfillMode</c> read-back: a *genuine* duplicate (two
    /// distinct rows sharing the same unique-key value) must still be rejected. The read-back sees
    /// a different rowId for the existing entry and throws <c>DuplicateUniqueKeyValue</c> — backfill
    /// mode relaxes idempotent re-writes, not uniqueness itself.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task UniqueIndexBackfillRejectsGenuineDuplicate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: TableName,
            columns:
            [
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true)
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            ],
            ifNotExists: false
        ));

        // Two distinct rows (distinct rowIds) with the SAME name — a real uniqueness violation.
        foreach (int _ in new[] { 1, 2 })
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: tx,
                database: dbname,
                sql: $"INSERT INTO {TableName} (id, name) VALUES (gen_id(), 'duplicate')",
                parameters: null
            ));
            await database.Transactions.CommitAsync(tx);
        }

        database.Schema.Tables.TryGetValue(TableName, out TableSchema? tableSchema);
        string nameColId = tableSchema!.Columns!.First(c => c.Name == "name").Id!;

        IndexBuildInfo indexInfo = new(
            IndexId: "000000000000000000000002",
            IndexName: "name_dup_reject",
            ColumnIds: [nameColId],
            ColumnNames: ["name"],
            IndexType: IndexType.Unique
        );

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.BackfillIndexEntriesAsync(database, TableName, indexInfo, startOffset: null),
            "A unique-index backfill over genuinely duplicate values must throw");
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex!.Code,
            "The rejection must be a DuplicateUniqueKeyValue (different rowId for the same key)");
    }

    /// <summary>
    /// An index dropped before close must remain absent after reopen.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task DroppedIndexDoesNotSurviveReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableWithIndex();

        // Drop the index before closing.
        KvTransaction txDrop = await database.Transactions.BeginAsync();
        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: TableName,
            indexName: "name_idx",
            columns: [],
            operation: AlterIndexOperation.DropIndex
        ));
        await database.Transactions.CommitAsync(txDrop);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        Assert.IsTrue(db2.Schema.Tables.TryGetValue(TableName, out TableSchema? schema));

        // name_idx must be absent from the persisted Indexes list.
        bool hasNameIdx = schema!.Indexes?.Any(ix => ix.Name == "name_idx") ?? false;
        Assert.IsFalse(hasNameIdx, "Dropped index must not appear in TableSchema.Indexes after reopen");

        // FORCE_INDEX on the dropped index must throw (not silently succeed).
        KvTransaction tx = await db2.Transactions.BeginAsync();
        Assert.ThrowsAsync<CamusDB.Core.CamusDBException>(async () =>
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                    txnState: tx,
                    database: dbname,
                    sql: $"SELECT id FROM {TableName}@{{FORCE_INDEX=name_idx}}",
                    parameters: null
                ));
            await cursor.ToListAsync();
        });
        await db2.Transactions.RollbackAsync(tx);
    }
}
