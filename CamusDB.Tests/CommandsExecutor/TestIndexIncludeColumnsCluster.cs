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
/// Covering-index (INCLUDE) behavior on the cluster DDL path. <see cref="SharedNodeBaseTest"/> runs
/// with <c>isClusterMode: true</c>, so index creation flows through the replicated schema log and the
/// staged coordinator/backfill path — the same path a real follower observes over shared KV. Verifies
/// the include metadata replicates and that covered reads work through the cluster executor.
/// </summary>
[NonParallelizable]
internal sealed class TestIndexIncludeColumnsCluster : SharedNodeBaseTest
{
    private const string TableName = "orders";

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

    [Test]
    [NonParallelizable]
    public async Task ClusterCreateIndexWithInclude_ReplicatesMetadata_AndCoversReads()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            $"CREATE TABLE {TableName} (id oid primary key, customer_id int64 not null, status string(32) not null, total float64 not null)");

        for (int i = 1; i <= 5; i++)
        {
            string total = (i * 1.5).ToString(System.Globalization.CultureInfo.InvariantCulture);
            await ExecNonQuery(executor, dbname,
                $"INSERT INTO {TableName} (id, customer_id, status, total) VALUES (gen_id(), {i}, 'open', {total})");
        }

        // Cluster-mode index creation → replicated schema log + staged coordinator backfill.
        await ExecDDL(executor, dbname,
            $"CREATE INDEX idx_customer ON {TableName} (customer_id) INCLUDE (status, total)");

        // Include metadata replicated into the per-table schema (immutable ids), resolved at open.
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, TableName));
        TableIndexSchema index = table.Indexes["idx_customer"];
        Assert.AreEqual(new[] { "status", "total" }, index.IncludeColumns);
        TableIndexSchema persisted = table.Schema.Indexes!.First(ix => ix.Name == "idx_customer");
        Assert.IsNotNull(persisted.IncludeColumnIds);
        Assert.AreEqual(2, persisted.IncludeColumnIds!.Length);

        // Covered read through the cluster executor: zero primary-row fetches, correct values.
        const string sql = $"SELECT customer_id, status, total FROM {TableName} WHERE customer_id = 4";
        long? rowsRead = await ScanRowsRead(executor, dbname, sql);
        Assert.AreEqual(0L, rowsRead!.Value, "covered read must fetch zero primary rows on the cluster path");

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("open", rows[0].Row["status"].StrValue);
        Assert.AreEqual(6.0, rows[0].Row["total"].FloatValue);
    }
}
