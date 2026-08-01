/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Covers secondary indexes with stored/payload (INCLUDE) columns. This file targets the schema and
/// DDL surface (metadata only): parsing, validation, persistence of <c>IncludeColumnIds</c>, and
/// re-resolution to <c>IncludeColumns</c> after a close/reopen.
/// </summary>
[NonParallelizable]
internal sealed class TestIndexIncludeColumns : BaseTest
{
    private const string TableName = "orders";

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateOrdersTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            $"CREATE TABLE {TableName} (id oid primary key, customer_id int64 not null, status string(32) not null, total float64 not null)");
        return (dbname, database, executor);
    }

    private static async Task ExecDDL(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecSelect(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>
    /// rows_read from the first index scan node in EXPLAIN ANALYZE, or null if the plan chose a table
    /// scan (no index node). rows_read == 0 proves the covering branch ran (no primary-row fetch).
    /// </summary>
    private static async Task<long?> ScanRowsRead(CommandExecutor executor, string dbname, string sql)
    {
        List<QueryResultRow> rows = await ExecSelect(executor, dbname, "EXPLAIN (ANALYZE) " + sql);
        foreach (QueryResultRow r in rows)
        {
            if (r.Row.TryGetValue("node", out ColumnValue? node)
                && node.StrValue is "index-lookup" or "index-range-scan"
                && r.Row.TryGetValue("rows_read", out ColumnValue? rr)
                && rr.Type == ColumnType.Integer64)
                return rr.LongValue;
        }
        return null;
    }

    private static async Task SeedRows(CommandExecutor executor, string dbname, int count)
    {
        for (int i = 1; i <= count; i++)
        {
            string total = (i * 1.5).ToString(System.Globalization.CultureInfo.InvariantCulture);
            await ExecNonQuery(executor, dbname,
                $"INSERT INTO {TableName} (id, customer_id, status, total) VALUES (gen_id(), {i}, 'open', {total})");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task CreateIndexWithInclude_PopulatesIncludeColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateOrdersTable();

        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status, total)");

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, TableName));
        Assert.IsTrue(table.Indexes.TryGetValue("idx_customer", out TableIndexSchema? index));
        Assert.AreEqual(new[] { "customer_id" }, index!.Columns);
        Assert.AreEqual(new[] { "status", "total" }, index.IncludeColumns);
        Assert.IsTrue(index.HasIncludeColumns);
    }

    [Test]
    [NonParallelizable]
    public async Task IncludeColumns_SurviveCloseReopen()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();

        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status, total)");

        // Force the persisted-form round-trip: the reopen reads TableSchema.Indexes (IncludeColumnIds)
        // from KV and re-resolves IncludeColumns at table open.
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        TableSchema schema = db2.Schema.Tables[TableName];
        TableIndexSchema persisted = schema.Indexes!.First(ix => ix.Name == "idx_customer");
        Assert.IsNotNull(persisted.IncludeColumnIds);
        Assert.AreEqual(2, persisted.IncludeColumnIds!.Length);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, TableName));
        TableIndexSchema resolved = table.Indexes["idx_customer"];
        Assert.AreEqual(new[] { "status", "total" }, resolved.IncludeColumns);
    }

    [Test]
    [NonParallelizable]
    public async Task UniqueIndexWithInclude_PopulatesIncludeColumns()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();

        await ExecDDL(executor, dbname,
            $"CREATE UNIQUE INDEX idx_customer_u ON {TableName} (customer_id) INCLUDE (status)");

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, TableName));
        TableIndexSchema index = table.Indexes["idx_customer_u"];
        Assert.AreEqual(IndexType.Unique, index.Type);
        Assert.AreEqual(new[] { "status" }, index.IncludeColumns);
    }

    // ── Covered reads (rows_read == 0) ───────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task CoveredRangeScan_ReturnsIncludeValues_WithZeroRowFetches()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status, total)");
        await SeedRows(executor, dbname, 5);

        const string sql = $"SELECT customer_id, status, total FROM {TableName} WHERE customer_id = 3";

        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.IsNotNull(rowsRead, "cost model must choose an index scan, not a table scan");
        Assert.AreEqual(0L, rowsRead!.Value, "covering path must read zero primary rows");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(3, rows[0].Row["customer_id"].LongValue);
        Assert.AreEqual("open", rows[0].Row["status"].StrValue);
        Assert.AreEqual(4.5, rows[0].Row["total"].FloatValue);
    }

    [Test]
    [NonParallelizable]
    public async Task CoveredUniqueLookup_ReturnsIncludeValues_WithZeroRowFetches()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        await ExecDDL(executor, dbname,
            $"CREATE UNIQUE INDEX idx_customer_u ON {TableName} (customer_id) INCLUDE (status, total)");
        await SeedRows(executor, dbname, 5);

        const string sql = $"SELECT customer_id, status, total FROM {TableName} WHERE customer_id = 2";

        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.IsNotNull(rowsRead, "cost model must choose an index lookup, not a table scan");
        Assert.AreEqual(0L, rowsRead!.Value, "covering unique lookup must read zero primary rows");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("open", rows[0].Row["status"].StrValue);
        Assert.AreEqual(3.0, rows[0].Row["total"].FloatValue);
    }

    [Test]
    [NonParallelizable]
    public async Task NotCovered_FetchesRow_WhenProjectingNonIncludedColumn()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status)");
        await SeedRows(executor, dbname, 5);

        // 'total' is not an include column → must fetch the primary row.
        const string sql = $"SELECT customer_id, status, total FROM {TableName} WHERE customer_id = 3";

        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.IsNotNull(rowsRead, "cost model must choose an index scan");
        Assert.Greater(rowsRead!.Value, 0L, "non-covered projection must fetch the primary row");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(4.5, rows[0].Row["total"].FloatValue);
    }

    [Test]
    [NonParallelizable]
    public async Task UpdateTouchingIncludeColumn_RefreshesCoveredRead()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status, total)");
        await SeedRows(executor, dbname, 5);

        // Update only an INCLUDE column (key unchanged) — the covered scan must return the new value.
        await ExecNonQuery(executor, dbname,
            $"UPDATE {TableName} SET status = 'shipped' WHERE customer_id = 3");

        const string sql = $"SELECT customer_id, status FROM {TableName} WHERE customer_id = 3";
        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.AreEqual(0L, rowsRead!.Value, "still covered after include-only update");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("shipped", rows[0].Row["status"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task UpdateTouchingIncludeColumn_UniqueIndex_RefreshesCoveredRead()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        await ExecDDL(executor, dbname,
            $"CREATE UNIQUE INDEX idx_customer_u ON {TableName} (customer_id) INCLUDE (status)");
        await SeedRows(executor, dbname, 5);

        // Guards the SetIfNotExists-vs-Set trap: an include-only rewrite of a UNIQUE entry must
        // overwrite the existing key in place, not silently no-op.
        await ExecNonQuery(executor, dbname,
            $"UPDATE {TableName} SET status = 'shipped' WHERE customer_id = 2");

        const string sql = $"SELECT customer_id, status FROM {TableName} WHERE customer_id = 2";
        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.AreEqual(0L, rowsRead!.Value, "still covered after include-only unique update");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("shipped", rows[0].Row["status"].StrValue);
    }

    // ── Inline CREATE TABLE + SHOW rendering ─────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task InlineCreateTableKey_WithInclude_CreatesCoveringIndex()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            $"CREATE TABLE {TableName} (id oid primary key, customer_id int64 not null, status string(32) not null, total float64 not null, KEY `idx_customer` (customer_id) INCLUDE (status, total))");

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, TableName));
        TableIndexSchema index = table.Indexes["idx_customer"];
        Assert.AreEqual(new[] { "customer_id" }, index.Columns);
        Assert.AreEqual(new[] { "status", "total" }, index.IncludeColumns);
    }

    [Test]
    [NonParallelizable]
    public async Task InlineCreateTableKey_Include_RejectsKeyOverlap()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname,
                $"CREATE TABLE {TableName} (id oid primary key, customer_id int64 not null, status string(32) not null, total float64 not null, KEY `idx_bad` (customer_id) INCLUDE (customer_id))"));
        Assert.That(ex!.Message, Does.Contain("already indexed as a key column"));
    }

    [Test]
    [NonParallelizable]
    public async Task ShowCreateTable_RendersInclude_AndReparses()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status, total)");

        List<QueryResultRow> shown = await ExecSelect(executor, dbname, $"SHOW CREATE TABLE {TableName}");
        Assert.AreEqual(1, shown.Count);
        string ddl = shown[0].Row["Create Table"].StrValue!;
        Assert.That(ddl, Does.Contain("INCLUDE"));
        Assert.That(ddl, Does.Contain("`status`"));

        // Round-trip: the rendered DDL must re-parse and recreate the covering index on a new table.
        string ddl2 = ddl.Replace(TableName, "orders_copy");
        await ExecDDL(executor, dbname, ddl2);

        TableDescriptor copy = await executor.OpenTable(new OpenTableTicket(dbname, "orders_copy"));
        TableIndexSchema index = copy.Indexes["idx_customer"];
        Assert.AreEqual(new[] { "status", "total" }, index.IncludeColumns);
    }

    [Test]
    [NonParallelizable]
    public async Task ShowIndexes_RendersIncludeColumn()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status, total)");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, $"SHOW INDEXES FROM {TableName}");
        QueryResultRow row = rows.Find(r => r.Row["Key_name"].StrValue == "idx_customer")!;
        Assert.AreEqual("status,total", row.Row["Include"].StrValue);
    }

    // ── Validation ──────────────────────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Include_RejectsColumnAlsoInKey()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname,
                $"CREATE INDEX idx_bad ON {TableName} (customer_id) INCLUDE (customer_id)"));
        Assert.That(ex!.Message, Does.Contain("already indexed as a key column"));
    }

    [Test]
    [NonParallelizable]
    public async Task Include_RejectsUnknownColumn()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname,
                $"CREATE INDEX idx_bad ON {TableName} (customer_id) INCLUDE (nope)"));
        Assert.That(ex!.Message, Does.Contain("does not exist"));
    }

    [Test]
    [NonParallelizable]
    public async Task Include_RejectsDuplicateColumn()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname,
                $"CREATE INDEX idx_bad ON {TableName} (customer_id) INCLUDE (status, status)"));
        Assert.That(ex!.Message, Does.Contain("Duplicate INCLUDE"));
    }

    [Test]
    [NonParallelizable]
    public async Task Include_RejectsDirection()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname,
                $"CREATE INDEX idx_bad ON {TableName} (customer_id) INCLUDE (status ASC)"));
        Assert.That(ex!.Message, Does.Contain("INCLUDE columns cannot specify ASC/DESC"));
    }

    // ── Backfill / restart / drop-column ─────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Backfill_OverExistingRows_CoveredReadReturnsIncludeValues()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        await SeedRows(executor, dbname, 5);

        // Create the covering index AFTER the rows exist → the backfill path must write include tuples.
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status, total)");

        const string sql = $"SELECT customer_id, status, total FROM {TableName} WHERE customer_id = 4";
        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.AreEqual(0L, rowsRead!.Value, "backfilled entries must cover the read");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("open", rows[0].Row["status"].StrValue);
        Assert.AreEqual(6.0, rows[0].Row["total"].FloatValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Backfill_WithNullIncludeValue_CoveredReadReturnsNull()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            $"CREATE TABLE {TableName} (id oid primary key, customer_id int64 not null, note string(64))");
        // note is nullable and left unset → NULL payload after backfill.
        await ExecNonQuery(executor, dbname,
            $"INSERT INTO {TableName} (id, customer_id) VALUES (gen_id(), 7)");

        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (note)");

        const string sql = $"SELECT customer_id, note FROM {TableName} WHERE customer_id = 7";
        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.AreEqual(0L, rowsRead!.Value, "covered even with a NULL included value");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Null, rows[0].Row["note"].Type);
    }

    [Test]
    [NonParallelizable]
    public async Task Restart_CoveredReadStillWorks()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status, total)");
        await SeedRows(executor, dbname, 5);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        await executor.OpenDatabase(dbname);

        const string sql = $"SELECT customer_id, status, total FROM {TableName} WHERE customer_id = 3";
        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.AreEqual(0L, rowsRead!.Value, "covered read must survive close/reopen");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual("open", rows[0].Row["status"].StrValue);
        Assert.AreEqual(4.5, rows[0].Row["total"].FloatValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DropColumn_UsedByInclude_IsRejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status)");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname, $"ALTER TABLE {TableName} DROP COLUMN status"));
        Assert.That(ex!.Message, Does.Contain("INCLUDE column"));
    }

    /// <summary>
    /// Regression: renaming a covering index must preserve its INCLUDE metadata. Before the fix,
    /// <c>ApplyRenameIndex</c> rebuilt the schema without the include-column ids, so after rename the
    /// index stopped being recognized as covering and DML stopped maintaining the payload — surfacing
    /// as a covered read that suddenly fetches primary rows (or returns stale/missing payload) after a
    /// reopen. This exercises the full path: rename, then DML, then close/reopen, then covered reads.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task RenameCoveringIndex_PreservesInclude_AcrossDmlAndReopen()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status, total)");
        await SeedRows(executor, dbname, 5);

        // Include-only update BEFORE the rename (entry payload refreshed under the old name).
        await ExecNonQuery(executor, dbname,
            $"UPDATE {TableName} SET status = 'shipped' WHERE customer_id = 2");

        await ExecDDL(executor, dbname,
            $"ALTER TABLE {TableName} RENAME INDEX idx_customer TO idx_cust2");

        // Persisted include metadata must survive the rename.
        TableDescriptor afterRename = await executor.OpenTable(new OpenTableTicket(dbname, TableName));
        TableIndexSchema renamed = afterRename.Indexes["idx_cust2"];
        Assert.AreEqual(new[] { "status", "total" }, renamed.IncludeColumns);

        // DML AFTER the rename must still maintain the payload (guards HasIncludeColumns going false).
        await ExecNonQuery(executor, dbname,
            $"INSERT INTO {TableName} (id, customer_id, status, total) VALUES (gen_id(), 6, 'open', 9.0)");
        await ExecNonQuery(executor, dbname,
            $"UPDATE {TableName} SET status = 'done' WHERE customer_id = 3");

        // Close/reopen forces the persisted-form round-trip and descriptor re-resolution.
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        await executor.OpenDatabase(dbname);

        // Every covered read must still plan index-only (rows_read == 0) and return fresh payload.
        await AssertCovered(executor, dbname, customerId: 2, expectedStatus: "shipped");  // pre-rename update
        await AssertCovered(executor, dbname, customerId: 6, expectedStatus: "open");     // post-rename insert
        await AssertCovered(executor, dbname, customerId: 3, expectedStatus: "done");     // post-rename update
    }

    // ── Selective include decode (E4) ────────────────────────────────────────────────────────

    private async Task<(string dbname, CommandExecutor executor)> CreateWideCoveringTable()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            $"CREATE TABLE {TableName} (id oid primary key, customer_id int64 not null, status string(32) not null, total float64 not null, note string(64) not null)");
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_wide ON {TableName} (customer_id) INCLUDE (status, total, note)");
        for (int i = 1; i <= 4; i++)
        {
            string total = (i * 1.5).ToString(System.Globalization.CultureInfo.InvariantCulture);
            await ExecNonQuery(executor, dbname,
                $"INSERT INTO {TableName} (id, customer_id, status, total, note) VALUES (gen_id(), {i}, 'open', {total}, 'note-{i}')");
        }
        return (dbname, executor);
    }

    [Test]
    [NonParallelizable]
    public async Task Covered_ProjectingOnlyTrailingInclude_SkipsEarlierIncludes()
    {
        (string dbname, CommandExecutor executor) = await CreateWideCoveringTable();

        // Projects only 'note' — the codec must skip the earlier 'status' (string) and 'total' (float)
        // include columns and still decode 'note' correctly (guards SkipColumnValue pointer advance).
        const string sql = $"SELECT customer_id, note FROM {TableName} WHERE customer_id = 3";
        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.AreEqual(0L, rowsRead!.Value, "covered while projecting only a trailing include");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("note-3", rows[0].Row["note"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Covered_ProjectingMiddleInclude_SkipsAroundIt()
    {
        (string dbname, CommandExecutor executor) = await CreateWideCoveringTable();

        // Projects only 'total' — skip 'status' before it, and never touch 'note' after it.
        const string sql = $"SELECT customer_id, total FROM {TableName} WHERE customer_id = 4";
        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.AreEqual(0L, rowsRead!.Value);

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(6.0, rows[0].Row["total"].FloatValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Covered_ProjectingOnlyKeyColumn_DoesNotDecodeAnyInclude()
    {
        (string dbname, CommandExecutor executor) = await CreateWideCoveringTable();

        // Projects only the key column though the index has INCLUDE columns: still covered, and the
        // include tuple is never decoded at all (HasIncludeSlots == false).
        const string sql = $"SELECT customer_id FROM {TableName} WHERE customer_id = 2";
        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.AreEqual(0L, rowsRead!.Value, "key-only projection over a covering index stays covered");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(2, rows[0].Row["customer_id"].LongValue);
    }

    // ── Limits (E3): column-count and payload-byte ceilings ──────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task ColumnCountLimit_RejectsIndex_ExceedingKeyPlusIncludeCeiling()
    {
        // The validator fixes its configuration when the engine is built, so the ceiling is lowered
        // before the table (and the executor behind it) are created.
        int saved = CamusDBConfig.MaxIndexColumns;
        CamusDBConfig.MaxIndexColumns = 2; // key(1) + include(2) = 3 > 2

        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        try
        {
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecDDL(executor, dbname,
                    $"CREATE INDEX idx_wide ON {TableName} (customer_id) INCLUDE (status, total)"));
            Assert.That(ex!.Message, Does.Contain("exceeding the maximum"));
            Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, ex.Code);
        }
        finally
        {
            CamusDBConfig.MaxIndexColumns = saved;
        }
    }

    [Test]
    [NonParallelizable]
    public async Task ColumnCountLimit_RejectsInlineCreateTableIndex()
    {
        // Lowered before the engine that must enforce it is created.
        int saved = CamusDBConfig.MaxIndexColumns;
        CamusDBConfig.MaxIndexColumns = 2;

        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        try
        {
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecDDL(executor, dbname,
                    $"CREATE TABLE {TableName} (id oid primary key, customer_id int64 not null, status string(32) not null, total float64 not null, KEY `idx_wide` (customer_id) INCLUDE (status, total))"));
            Assert.That(ex!.Message, Does.Contain("exceeding the maximum"));
            Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, ex.Code);
        }
        finally
        {
            CamusDBConfig.MaxIndexColumns = saved;
        }
    }

    [Test]
    [NonParallelizable]
    public async Task PayloadByteLimit_RejectsInsert_WithOversizedIncludeTuple()
    {
        (string dbname, _, CommandExecutor executor) = await CreateOrdersTable();
        // Index created on the empty table, so backfill writes nothing — this isolates the INSERT path.
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status)");

        int saved = CamusDBConfig.MaxIndexIncludeTupleBytes;
        CamusDBConfig.MaxIndexIncludeTupleBytes = 8; // a short string encodes to > 8 bytes
        try
        {
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(executor, dbname,
                    $"INSERT INTO {TableName} (id, customer_id, status, total) VALUES (gen_id(), 1, 'shipped', 1.0)"));
            Assert.That(ex!.Message, Does.Contain("INCLUDE payload"));
            Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, ex.Code);
        }
        finally
        {
            CamusDBConfig.MaxIndexIncludeTupleBytes = saved;
        }

        // The rejected insert must not have persisted a row (transaction rolled back).
        List<QueryResultRow> rows = await ExecSelect(executor, dbname,
            $"SELECT customer_id FROM {TableName} WHERE customer_id = 1");
        Assert.AreEqual(0, rows.Count, "the oversized-payload insert must not persist");
    }

    // ── Lazy DML serialization (E5) ──────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Insert_NullUniqueKey_DoesNotSerializeOrCheckIncludePayload()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            $"CREATE TABLE {TableName} (id oid primary key, code int64, big string(200))");
        // Unique index on a nullable key column with an INCLUDE payload.
        await ExecDDL(executor, dbname,
            $"CREATE UNIQUE INDEX ux_code ON {TableName} (code) INCLUDE (big)");

        int saved = CamusDBConfig.MaxIndexIncludeTupleBytes;
        CamusDBConfig.MaxIndexIncludeTupleBytes = 8; // any real 'big' string encodes larger than this
        try
        {
            // NULL unique key → NULLs are distinct → no index entry is written. With lazy
            // serialization the oversized payload is never encoded or byte-checked, so the row inserts.
            await ExecNonQuery(executor, dbname,
                $"INSERT INTO {TableName} (id, big) VALUES (gen_id(), 'a-very-long-payload-value-well-over-eight-bytes')");

            // A row WITH a non-null key does emit an entry, so the byte gate fires for it.
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ExecNonQuery(executor, dbname,
                    $"INSERT INTO {TableName} (id, code, big) VALUES (gen_id(), 5, 'a-very-long-payload-value-well-over-eight-bytes')"));
            Assert.AreEqual(CamusDBErrorCodes.SchemaLimitExceeded, ex!.Code);
        }
        finally
        {
            CamusDBConfig.MaxIndexIncludeTupleBytes = saved;
        }

        // The NULL-key row persisted; the oversized-key row did not.
        List<QueryResultRow> rows = await ExecSelect(executor, dbname, $"SELECT id FROM {TableName}");
        Assert.AreEqual(1, rows.Count, "only the NULL-key insert should have persisted");
    }

    [Test]
    [NonParallelizable]
    public async Task Update_UnrelatedColumn_LeavesCoveredReadCorrect()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            $"CREATE TABLE {TableName} (id oid primary key, customer_id int64 not null, status string(32) not null, note string(64) not null)");
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status)");
        await ExecNonQuery(executor, dbname,
            $"INSERT INTO {TableName} (id, customer_id, status, note) VALUES (gen_id(), 3, 'open', 'n1')");

        // 'note' is neither key nor include → the covering index entry is skipped (no rewrite).
        await ExecNonQuery(executor, dbname,
            $"UPDATE {TableName} SET note = 'n2' WHERE customer_id = 3");

        const string sql = $"SELECT customer_id, status FROM {TableName} WHERE customer_id = 3";
        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.AreEqual(0L, rowsRead!.Value, "still covered after an unrelated-column update");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual("open", rows[0].Row["status"].StrValue);
    }

    private static async Task AssertCovered(CommandExecutor executor, string dbname, int customerId, string expectedStatus)
    {
        string sql = $"SELECT customer_id, status, total FROM {TableName} WHERE customer_id = {customerId}";
        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.IsNotNull(rowsRead, $"customer_id={customerId}: plan must still use the covering index after rename");
        Assert.AreEqual(0L, rowsRead!.Value, $"customer_id={customerId}: must remain a covered read (zero primary fetches)");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(expectedStatus, rows[0].Row["status"].StrValue);
    }
}
