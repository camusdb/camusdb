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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Hardening coverage for covering (INCLUDE) indexes on the failure/concurrency windows the review
/// flagged as unverified: DML that runs while the index is still <c>WriteOnly</c> (before publish),
/// and include-only rewrites on a copy-on-write branch database (where the branch code path — Set,
/// not tombstone+SetIfNotExists — is exercised).
/// </summary>
[NonParallelizable]
internal sealed class TestIndexIncludeColumnsHardening : BaseTest
{
    private const string TableName = "orders";

    private static string NewName() => "db_" + Guid.NewGuid().ToString("n");

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

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateOrdersTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            $"CREATE TABLE {TableName} (id oid primary key, customer_id int64 not null, status string(32) not null, total float64 not null)");
        return (dbname, database, executor);
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

    // ── Concurrent DML while the covering index is WriteOnly (pre-publish) ────────────────────

    [Test]
    [NonParallelizable]
    public async Task ConcurrentInsertDuringBackfill_WritesIncludeTuple()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateOrdersTable();
        await SeedRows(executor, dbname, 5);

        // Fires once after backfill has scanned existing rows and before publish: the index is
        // WriteOnly, so this INSERT's index maintenance must write the entry — with its INCLUDE tuple.
        TableIndexAdder.TestHookBeforePublish = async () =>
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
                $"INSERT INTO {TableName} (id, customer_id, status, total) VALUES (gen_id(), 99, 'hooked', 42.0)", null));
            await database.Transactions.CommitAsync(tx);
        };

        try
        {
            await ExecDDL(executor, dbname,
                $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status, total)");
        }
        finally
        {
            TableIndexAdder.TestHookBeforePublish = null;
        }

        // The row written during the WriteOnly window must be covered with its payload intact.
        const string sql = $"SELECT customer_id, status, total FROM {TableName} WHERE customer_id = 99";
        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.AreEqual(0L, rowsRead!.Value, "WriteOnly-window insert must be covered after publish");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("hooked", rows[0].Row["status"].StrValue);
        Assert.AreEqual(42.0, rows[0].Row["total"].FloatValue);

        // A backfilled row is also covered — the two maintenance paths agree.
        List<QueryResultRow> backfilled = await ExecSelect(executor, dbname,
            $"SELECT customer_id, status FROM {TableName} WHERE customer_id = 3");
        Assert.AreEqual("open", backfilled[0].Row["status"].StrValue);
    }

    // ── Branch database (copy-on-write) include-only rewrites ─────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Branch_IncludeOnlyUpdate_NonUnique_RewritesEntry_AndIsolated()
    {
        (string rootName, _, CommandExecutor executor) = await CreateOrdersTable();
        await ExecDDL(executor, rootName,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status)");
        await SeedRows(executor, rootName, 3);

        // Branch from the root (ancestorStores non-empty → the branch write path runs).
        string branchName = NewName();
        DatabaseDescriptor branchDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Include-only update on the branch: key unchanged, so this is an in-place value overwrite
        // (branch Set, not tombstone+SetIfNotExists).
        await ExecNonQuery(executor, branchName,
            $"UPDATE {TableName} SET status = 'branch-shipped' WHERE customer_id = 2");

        // Covered read on the branch returns the new payload...
        const string sql = $"SELECT customer_id, status FROM {TableName} WHERE customer_id = 2";
        long? rowsRead = await ScanRowsRead(executor, branchName, sql);
        Assert.AreEqual(0L, rowsRead!.Value, "branch covered read after include-only update");
        List<QueryResultRow> branchRows = await ExecSelect(executor, branchName, sql);
        Assert.AreEqual("branch-shipped", branchRows[0].Row["status"].StrValue);

        // ...and the root is unchanged (branch isolation).
        List<QueryResultRow> rootRows = await ExecSelect(executor, rootName, sql);
        Assert.AreEqual("open", rootRows[0].Row["status"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Branch_IncludeOnlyUpdate_Unique_RewritesEntry_AndIsolated()
    {
        (string rootName, _, CommandExecutor executor) = await CreateOrdersTable();
        await ExecDDL(executor, rootName,
            $"CREATE UNIQUE INDEX ux_customer ON {TableName} (customer_id) INCLUDE (status)");
        await SeedRows(executor, rootName, 3);

        string branchName = NewName();
        DatabaseDescriptor branchDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Include-only update on a UNIQUE covering index on the branch: the same key already exists
        // (inherited), so the rewrite must Set in place — SetIfNotExists would no-op and leave stale
        // payload, and a tombstone+reinsert would wrongly trip the branch duplicate guard.
        await ExecNonQuery(executor, branchName,
            $"UPDATE {TableName} SET status = 'branch-done' WHERE customer_id = 2");

        const string sql = $"SELECT customer_id, status FROM {TableName} WHERE customer_id = 2";
        long? rowsRead = await ScanRowsRead(executor, branchName, sql);
        Assert.AreEqual(0L, rowsRead!.Value, "branch unique covered read after include-only update");
        List<QueryResultRow> branchRows = await ExecSelect(executor, branchName, sql);
        Assert.AreEqual("branch-done", branchRows[0].Row["status"].StrValue);

        List<QueryResultRow> rootRows = await ExecSelect(executor, rootName, sql);
        Assert.AreEqual("open", rootRows[0].Row["status"].StrValue);
    }
}
