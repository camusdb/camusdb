
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies the batched UPDATE path (UpdateRowsBatch) introduced by EP1: chunked row reads,
/// single-round-trip lock acquisition, and combined delete-old/set-new index mutations.
/// Tests cover correctness at multiple row counts, unique-index semantics (DuplicateUniqueKeyValue,
/// NULL-to-value, value-to-NULL), and that the SpillEffectiveThreshold chunking produces correct
/// results even when rows span multiple chunks.
/// </summary>
[NonParallelizable]
public sealed class TestRowUpdaterBatch : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupDatabase()
        => await CreateDatabase();

    // -----------------------------------------------------------------------
    // Table helpers
    // -----------------------------------------------------------------------

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids)>
        SetupSimpleTable(int rowCount = 10)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "items",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("label", ColumnType.String, notNull: true),
                new("score", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        List<string> ids = new(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            string oid = ObjectIdGenerator.Generate().ToString();
            ids.Add(oid);

            await executor.Insert(new InsertTicket(
                txnState: tx,
                databaseName: dbname,
                tableName: "items",
                values: new() { new()
                {
                    { "id",    new(ColumnType.Id,        oid) },
                    { "label", new(ColumnType.String,    "label-" + i) },
                    { "score", new(ColumnType.Integer64, i) },
                } }));
        }

        await database.Transactions.CommitAsync(tx);

        return (dbname, database, executor, ids);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids)>
        SetupTableWithUniqueIndex(int rowCount = 10)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "items",
            columns: new ColumnInfo[]
            {
                new("id",  ColumnType.Id),
                new("code", ColumnType.String, notNull: false),
                new("val",  ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",  new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexUnique, "u_code", new ColumnIndexInfo[] { new("code", OrderType.Ascending) }),
            },
            ifNotExists: false));

        List<string> ids = new(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            string oid = ObjectIdGenerator.Generate().ToString();
            ids.Add(oid);

            await executor.Insert(new InsertTicket(
                txnState: tx,
                databaseName: dbname,
                tableName: "items",
                values: new() { new()
                {
                    { "id",   new(ColumnType.Id,        oid) },
                    { "code", new(ColumnType.String,    "CODE-" + i) },
                    { "val",  new(ColumnType.Integer64, i) },
                } }));
        }

        await database.Transactions.CommitAsync(tx);

        return (dbname, database, executor, ids);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids)>
        SetupTableWithNullableUniqueIndex(int rowCount = 5)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupDatabase();

        KvTransaction tx = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "items",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("code", ColumnType.String, notNull: false),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",    new ColumnIndexInfo[] { new("id",   OrderType.Ascending) }),
                new(ConstraintType.IndexUnique, "u_code", new ColumnIndexInfo[] { new("code", OrderType.Ascending) }),
            },
            ifNotExists: false));

        List<string> ids = new(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            string oid = ObjectIdGenerator.Generate().ToString();
            ids.Add(oid);

            await executor.Insert(new InsertTicket(
                txnState: tx,
                databaseName: dbname,
                tableName: "items",
                values: new() { new()
                {
                    { "id",   new(ColumnType.Id, oid) },
                    // first row starts with a NULL code; rest have values
                    { "code", i == 0 ? new(ColumnType.Null, null!) : new(ColumnType.String, "CODE-" + i) },
                } }));
        }

        await database.Transactions.CommitAsync(tx);

        return (dbname, database, executor, ids);
    }

    // -----------------------------------------------------------------------
    // Query helper
    // -----------------------------------------------------------------------

    private static async Task<List<QueryResultRow>> QueryAllAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string table)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        QueryTicket qt = new(
            txnState: tx, databaseName: dbname, tableName: table,
            index: null, projection: null, filters: null, where: null,
            orderBy: null, limit: null, offset: null, parameters: null);
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(qt);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task BatchUpdate_ManyRows_AllUpdated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) =
            await SetupSimpleTable(rowCount: 100);

        KvTransaction tx = await database.Transactions.BeginAsync();

        UpdateTicket ticket = new(
            txnState: tx,
            databaseName: dbname,
            tableName: "items",
            plainValues: new() { { "label", new(ColumnType.String, "updated") } },
            exprValues: null,
            where: null,
            filters: null,
            parameters: null);

        UpdateResult result = await executor.Update(ticket);
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(100, result.UpdatedRows);

        List<QueryResultRow> rows = await QueryAllAsync(executor, database, dbname, "items");
        Assert.AreEqual(100, rows.Count);
        Assert.IsTrue(rows.All(r => r.Row["label"].StrValue == "updated"),
            "Every row should carry the new label value");
    }

    [Test]
    [NonParallelizable]
    public async Task BatchUpdate_ChunkedBySpillThreshold_AllUpdated()
    {
        // Drive the chunk boundary by forcing a small spill threshold so 20 rows span
        // multiple chunks. Verifies that chunking produces correct results with no
        // missed or double-applied mutations.
        int savedThreshold = CamusDBConfig.ForceSpillThresholdRows ?? 0;
        CamusDBConfig.ForceSpillThresholdRows = 5;
        try
        {
            (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) =
                await SetupSimpleTable(rowCount: 20);

            KvTransaction tx = await database.Transactions.BeginAsync();

            UpdateTicket ticket = new(
                txnState: tx,
                databaseName: dbname,
                tableName: "items",
                plainValues: new() { { "score", new(ColumnType.Integer64, 999) } },
                exprValues: null,
                where: null,
                filters: null,
                parameters: null);

            UpdateResult result = await executor.Update(ticket);
            await database.Transactions.CommitAsync(tx);

            Assert.AreEqual(20, result.UpdatedRows);

            List<QueryResultRow> rows = await QueryAllAsync(executor, database, dbname, "items");
            Assert.AreEqual(20, rows.Count);
            Assert.IsTrue(rows.All(r => r.Row["score"].LongValue == 999L),
                "Every row should have score=999 after chunked update");
        }
        finally
        {
            CamusDBConfig.ForceSpillThresholdRows = savedThreshold == 0 ? null : savedThreshold;
        }
    }

    [Test]
    [NonParallelizable]
    public async Task BatchUpdate_UniqueIndex_UpdatedValueQueryable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) =
            await SetupTableWithUniqueIndex(rowCount: 5);

        // Update code from "CODE-0" → "NEW-0" for the first row only.
        KvTransaction tx = await database.Transactions.BeginAsync();

        UpdateTicket ticket = new(
            txnState: tx,
            databaseName: dbname,
            tableName: "items",
            plainValues: new() { { "code", new(ColumnType.String, "NEW-0") } },
            exprValues: null,
            where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, ids[0])) },
            parameters: null);

        UpdateResult result = await executor.Update(ticket);
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, result.UpdatedRows);

        // Query by the new code — must find the row.
        KvTransaction tx2 = await database.Transactions.BeginAsync();
        QueryTicket qt = new(
            txnState: tx2, databaseName: dbname, tableName: "items",
            index: null, projection: null,
            filters: new() { new("code", "=", new(ColumnType.String, "NEW-0")) },
            where: null, orderBy: null, limit: null, offset: null, parameters: null);
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(qt);
        List<QueryResultRow> found = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx2);

        Assert.AreEqual(1, found.Count, "The new unique code should be found via index");
        Assert.AreEqual(ids[0], found[0].Row["id"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task BatchUpdate_UniqueIndex_DuplicateValueThrows()
    {
        // Insert two rows with unique codes. Update the second row's code to match the first.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) =
            await SetupTableWithUniqueIndex(rowCount: 3);

        KvTransaction tx = await database.Transactions.BeginAsync();

        UpdateTicket ticket = new(
            txnState: tx,
            databaseName: dbname,
            tableName: "items",
            plainValues: new() { { "code", new(ColumnType.String, "CODE-0") } },
            exprValues: null,
            where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, ids[1])) },
            parameters: null);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.Update(ticket));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue),
            "Updating to an existing unique code must throw DuplicateUniqueKeyValue");
    }

    [Test]
    [NonParallelizable]
    public async Task BatchUpdate_NullToValueUniqueIndex_AddsIndexEntry()
    {
        // Row 0 starts with a NULL code — no unique index entry. Update it to a real value
        // so that the new unique entry is created and the row is findable by code.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) =
            await SetupTableWithNullableUniqueIndex(rowCount: 3);

        KvTransaction tx = await database.Transactions.BeginAsync();

        UpdateTicket ticket = new(
            txnState: tx,
            databaseName: dbname,
            tableName: "items",
            plainValues: new() { { "code", new(ColumnType.String, "CODE-NEW") } },
            exprValues: null,
            where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, ids[0])) },
            parameters: null);

        UpdateResult result3 = await executor.Update(ticket);
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, result3.UpdatedRows);

        KvTransaction tx2 = await database.Transactions.BeginAsync();
        QueryTicket qt = new(
            txnState: tx2, databaseName: dbname, tableName: "items",
            index: null, projection: null,
            filters: new() { new("code", "=", new(ColumnType.String, "CODE-NEW")) },
            where: null, orderBy: null, limit: null, offset: null, parameters: null);
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(qt);
        List<QueryResultRow> found = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx2);

        Assert.AreEqual(1, found.Count, "Row with formerly-NULL code should be findable after update");
        Assert.AreEqual(ids[0], found[0].Row["id"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task BatchUpdate_ValueToNullUniqueIndex_RemovesIndexEntry()
    {
        // Row 1 starts with code "CODE-1". Update it to NULL. The unique index entry must be
        // removed; a subsequent query by the old code should find nothing.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) =
            await SetupTableWithNullableUniqueIndex(rowCount: 3);

        KvTransaction tx = await database.Transactions.BeginAsync();

        UpdateTicket ticket = new(
            txnState: tx,
            databaseName: dbname,
            tableName: "items",
            plainValues: new() { { "code", new(ColumnType.Null, null!) } },
            exprValues: null,
            where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, ids[1])) },
            parameters: null);

        UpdateResult result4 = await executor.Update(ticket);
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, result4.UpdatedRows);

        KvTransaction tx2 = await database.Transactions.BeginAsync();
        QueryTicket qt = new(
            txnState: tx2, databaseName: dbname, tableName: "items",
            index: null, projection: null,
            filters: new() { new("code", "=", new(ColumnType.String, "CODE-1")) },
            where: null, orderBy: null, limit: null, offset: null, parameters: null);
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(qt);
        List<QueryResultRow> found = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx2);

        Assert.AreEqual(0, found.Count, "Old unique index entry must be gone after setting to NULL");
    }

    [Test]
    [NonParallelizable]
    public async Task BatchUpdate_SequentialSingleRowUpdates_UniqueIndexStaysConsistent()
    {
        // A sequence of 12 independent single-row UPDATE statements (each its own transaction),
        // each retargeting one row's unique code. This does NOT exercise multiple rows within one
        // chunk — every statement matches exactly one row — but guards that repeated single-row
        // unique-index retargeting leaves a consistent index. Genuine multi-row-per-chunk unique
        // coverage lives in BatchUpdate_MultiRowChunk_IntUniqueIndex_DistinctExprValues below.
        int savedThreshold = CamusDBConfig.ForceSpillThresholdRows ?? 0;
        CamusDBConfig.ForceSpillThresholdRows = 3;
        try
        {
            (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<string> ids) =
                await SetupTableWithUniqueIndex(rowCount: 12);

            // Update each row to a new unique code prefix "NEW-{i}" — all distinct, no collisions.
            for (int i = 0; i < ids.Count; i++)
            {
                KvTransaction tx = await database.Transactions.BeginAsync();

                UpdateTicket ticket = new(
                    txnState: tx,
                    databaseName: dbname,
                    tableName: "items",
                    plainValues: new() { { "code", new(ColumnType.String, "NEW-" + i) } },
                    exprValues: null,
                    where: null,
                    filters: new() { new("id", "=", new(ColumnType.Id, ids[i])) },
                    parameters: null);

                await executor.Update(ticket);
                await database.Transactions.CommitAsync(tx);
            }

            // Verify all original codes are gone and all new codes are present.
            List<QueryResultRow> rows = await QueryAllAsync(executor, database, dbname, "items");
            Assert.AreEqual(12, rows.Count);

            HashSet<string?> codes = rows.Select(r => r.Row["code"].StrValue).ToHashSet();
            for (int i = 0; i < 12; i++)
                Assert.IsTrue(codes.Contains("NEW-" + i), $"Expected 'NEW-{i}' to be present");
        }
        finally
        {
            CamusDBConfig.ForceSpillThresholdRows = savedThreshold == 0 ? null : savedThreshold;
        }
    }

    // -----------------------------------------------------------------------
    // Genuine multi-row-per-chunk unique-index coverage. These drive a SINGLE UPDATE statement
    // that matches many rows and assigns each a DISTINCT new unique value via an expression
    // (val = val + N) — impossible with a plain-value UPDATE, which would set every matched row
    // to the same value and violate the unique index. This is what actually exercises
    // UpdateRowsBatch's per-chunk lock-key dedup, within-batch unique guard, and
    // delete-old-before-set-new ordering across multiple rows.
    // -----------------------------------------------------------------------

    private static async Task<List<QueryResultRow>> SelectByValAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, long val)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, $"SELECT * FROM items WHERE val = {val}", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task CreateIntUniqueTableAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, int rowCount)
    {
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, dbname,
            "CREATE TABLE items (id OBJECT_ID PRIMARY KEY, val INT64)", null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, dbname,
            "CREATE UNIQUE INDEX u_val ON items (val)", null));

        KvTransaction tx = await database.Transactions.BeginAsync();
        for (int i = 0; i < rowCount; i++)
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
                $"INSERT INTO items (id, val) VALUES (gen_id(), {i})", null));
        await database.Transactions.CommitAsync(tx);
    }

    [Test]
    [NonParallelizable]
    public async Task BatchUpdate_MultiRowChunk_IntUniqueIndex_DistinctExprValues()
    {
        // 12 rows through a unique index in ONE statement, forced into 4 chunks of 3. Each row gets
        // a distinct new value (val + 1000) that does not overlap any existing value, so every chunk
        // performs multiple unique delete-old/set-new mutations concurrently.
        int savedThreshold = CamusDBConfig.ForceSpillThresholdRows ?? 0;
        CamusDBConfig.ForceSpillThresholdRows = 3;
        try
        {
            (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
            await CreateIntUniqueTableAsync(executor, database, dbname, rowCount: 12);

            KvTransaction upTx = await database.Transactions.BeginAsync();
            ExecuteNonSQLResult res = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                upTx, dbname, "UPDATE items SET val = val + 1000 WHERE val >= 0", null));
            await database.Transactions.CommitAsync(upTx);

            Assert.AreEqual(12, res.ModifiedRows);

            for (int i = 0; i < 12; i++)
            {
                Assert.AreEqual(1, (await SelectByValAsync(executor, database, dbname, 1000 + i)).Count,
                    $"new unique value {1000 + i} must be findable via the index exactly once");
                Assert.AreEqual(0, (await SelectByValAsync(executor, database, dbname, i)).Count,
                    $"old unique value {i} must be gone from the index");
            }
        }
        finally
        {
            CamusDBConfig.ForceSpillThresholdRows = savedThreshold == 0 ? null : savedThreshold;
        }
    }

    [Test]
    [NonParallelizable]
    public async Task BatchUpdate_SingleChunk_IntUniqueIndex_OverlappingShift()
    {
        // val = val + 1 shifts every row's unique value onto the next row's OLD value (0..4 → 1..5).
        // This succeeds only because the batch deletes ALL old unique entries before setting ANY new
        // one within the chunk; a naive per-row delete+set would hit a transient duplicate on the
        // second row. No forced threshold, so the whole shift is a single delete-then-set batch.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateIntUniqueTableAsync(executor, database, dbname, rowCount: 5);

        KvTransaction upTx = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult res = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            upTx, dbname, "UPDATE items SET val = val + 1 WHERE val >= 0", null));
        await database.Transactions.CommitAsync(upTx);

        Assert.AreEqual(5, res.ModifiedRows);

        Assert.AreEqual(0, (await SelectByValAsync(executor, database, dbname, 0)).Count,
            "value 0 must be gone after the shift");
        for (long v = 1; v <= 5; v++)
            Assert.AreEqual(1, (await SelectByValAsync(executor, database, dbname, v)).Count,
                $"shifted value {v} must be present exactly once");
    }

    [Test]
    [NonParallelizable]
    public async Task BatchUpdate_ExceedsMutationLimit_ThrowsAndRollsBack()
    {
        // A batched UPDATE that reserves more mutations than MaxMutationsPerTransaction must fail with
        // TransactionMutationLimitExceeded and leave the data untouched after rollback. The table has
        // no secondary index, so each row is exactly one mutation — 25 rows against a limit of 10 must
        // trip the guard. ReserveMutations is checked before the writes in the chunk, so the limit is
        // enforced rather than silently exceeded.
        int savedLimit = CamusDBConfig.MaxMutationsPerTransaction;
        try
        {
            (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, dbname,
                "CREATE TABLE items (id OBJECT_ID PRIMARY KEY, val INT64)", null));

            // Seed under the default (generous) limit — one row per insert transaction so the seed
            // itself never trips the limit we are about to lower.
            for (int i = 0; i < 25; i++)
            {
                KvTransaction insTx = await database.Transactions.BeginAsync();
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insTx, dbname,
                    $"INSERT INTO items (id, val) VALUES (gen_id(), {i})", null));
                await database.Transactions.CommitAsync(insTx);
            }

            // Now lower the limit; the update's transaction reads it at BeginAsync.
            CamusDBConfig.MaxMutationsPerTransaction = 10;
            KvTransaction upTx = await database.Transactions.BeginAsync();
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    upTx, dbname, "UPDATE items SET val = val + 1000 WHERE val >= 0", null)));
            await database.Transactions.RollbackIfNotCompletedAsync(upTx);

            Assert.AreEqual(CamusDBErrorCodes.TransactionMutationLimitExceeded, ex?.Code,
                "Exceeding MaxMutationsPerTransaction must throw TransactionMutationLimitExceeded");

            // After rollback the original values must be intact — the failed update left no partial state.
            for (int i = 0; i < 25; i++)
                Assert.AreEqual(1, (await SelectByValAsync(executor, database, dbname, i)).Count,
                    $"original value {i} must survive the rolled-back over-limit update");
            for (int i = 0; i < 25; i++)
                Assert.AreEqual(0, (await SelectByValAsync(executor, database, dbname, 1000 + i)).Count,
                    $"no row should carry the would-be updated value {1000 + i}");
        }
        finally
        {
            CamusDBConfig.MaxMutationsPerTransaction = savedLimit;
        }
    }
}
