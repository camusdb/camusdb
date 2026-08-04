/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The standalone index backfill processes rows in bounded batches: the first batch runs in the DDL
/// transaction and every further batch commits on its own. These tests drive a table larger than one
/// batch so the multi-batch path actually runs — a single-batch table would exercise none of it — and
/// assert that the resulting index is complete (rows from every batch are found through it) and that a
/// build which fails in a later batch leaves no index behind and can be retried.
///
/// <para>
/// Extends <see cref="BaseTest"/>, not <see cref="SharedNodeBaseTest"/>: the latter builds its executor
/// with <c>isClusterMode: true</c>, which routes ADD INDEX through the coordinator's own batched
/// backfill and would exercise none of the standalone flux path under test here.
/// </para>
/// </summary>
internal sealed class TestIndexBackfillBatching : BaseTest
{
    // Comfortably more than one backfill batch (500 rows), so the build spans three batches.
    private const int RowCount = 1_200;

    private static async Task ExecuteDdl(string dbname, DatabaseDescriptor database, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
        Assert.IsTrue(result.Success);
    }

    /// <summary>
    /// Seeds <see cref="RowCount"/> rows where <c>val</c> equals the row number, except that
    /// <paramref name="duplicateOf"/> (when given) makes the last row repeat an earlier value so a
    /// unique build fails after at least one batch has already committed.
    /// </summary>
    private static async Task SeedRows(string dbname, DatabaseDescriptor database, CommandExecutor executor, int? duplicateOf = null)
    {
        const int rowsPerStatement = 200;

        for (int start = 0; start < RowCount; start += rowsPerStatement)
        {
            StringBuilder sql = new("INSERT INTO items (id, val) VALUES ");

            for (int i = start; i < start + rowsPerStatement; i++)
            {
                int value = duplicateOf is not null && i == RowCount - 1 ? duplicateOf.Value : i;

                if (i > start)
                    sql.Append(", ");

                sql.Append('(').Append(i).Append(", ").Append(value).Append(')');
            }

            KvTransaction tx = await database.Transactions.BeginAsync();
            ExecuteNonSQLResult inserted = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql.ToString(), null));
            await database.Transactions.CommitAsync(tx);
            Assert.AreEqual(rowsPerStatement, inserted.ModifiedRows);
        }
    }

    /// <summary>
    /// Reports whether the planner serves the query from an index node. A completeness assertion needs
    /// this: a row count taken from a full table scan would pass even if the backfill wrote nothing.
    /// </summary>
    private static async Task<bool> UsedIndexScan(string dbname, DatabaseDescriptor database, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, "EXPLAIN " + sql, null));
        List<QueryResultRow> plan = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);

        return plan.Any(r => r.Row.TryGetValue("node", out ColumnValue? node)
            && node.StrValue is "index-lookup" or "index-range-scan");
    }

    private static async Task<int> CountWhere(string dbname, DatabaseDescriptor database, CommandExecutor executor, string where)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, $"SELECT id FROM items WHERE {where}", null));
        int count = (await cursor.ToListAsync()).Count;
        await database.Transactions.CommitAsync(tx);
        return count;
    }

    /// <summary>
    /// Every row must be reachable through an index built over a table spanning several batches —
    /// including rows in the second and third batch, which are the ones a build that silently stopped
    /// after the first batch would lose.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BackfillAcrossBatches_IndexesEveryRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(dbname, database, executor, "CREATE TABLE items (id INT64 NOT NULL PRIMARY KEY, val INT64 NOT NULL)");
        await SeedRows(dbname, database, executor);
        await ExecuteDdl(dbname, database, executor, "CREATE INDEX val_idx ON items (val)");

        // The assertions below are only meaningful if the planner actually reads the new index.
        Assert.IsTrue(await UsedIndexScan(dbname, database, executor, "SELECT id FROM items WHERE val = 700"),
            "the probes must be served by the new index, not by a full table scan");

        // One row from each batch, plus the boundaries where a batch hands over to the next.
        foreach (int probe in new[] { 0, 499, 500, 999, 1000, RowCount - 1 })
            Assert.AreEqual(1, await CountWhere(dbname, database, executor, $"val = {probe}"),
                $"row with val={probe} must be reachable through the backfilled index");

        Assert.AreEqual(0, await CountWhere(dbname, database, executor, $"val = {RowCount}"));

        // Ranges straddling each batch handover: every entry on both sides must be present. A range
        // narrow enough for the planner to prefer the index, so a gap at the seam cannot hide behind a
        // full table scan.
        foreach (int seam in new[] { 500, 1000 })
        {
            string range = $"val >= {seam - 20} AND val < {seam + 20}";

            Assert.IsTrue(await UsedIndexScan(dbname, database, executor, $"SELECT id FROM items WHERE {range}"),
                $"the range across the batch handover at {seam} must be served by the new index");
            Assert.AreEqual(40, await CountWhere(dbname, database, executor, range),
                $"every entry around the batch handover at {seam} must be in the index");
        }

        // Whole-table sanity check (planner will use a full scan here — the index checks are above).
        Assert.AreEqual(RowCount, await CountWhere(dbname, database, executor, "val >= 0"));
    }

    /// <summary>
    /// A unique index whose duplicate lies beyond the first batch must still be rejected, and the
    /// aborted build must leave no trace: the index is absent from the schema, and once the duplicate
    /// is removed the same index builds cleanly and indexes every row.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task UniqueBackfillAcrossBatches_RejectsDuplicateAndLeavesNoIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(dbname, database, executor, "CREATE TABLE items (id INT64 NOT NULL PRIMARY KEY, val INT64 NOT NULL)");
        await SeedRows(dbname, database, executor, duplicateOf: 42);

        KvTransaction failingTx = await database.Transactions.BeginAsync();
        CamusDBException? rejected = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(failingTx, dbname,
                "CREATE UNIQUE INDEX val_uniq ON items (val)", null)));
        await database.Transactions.RollbackIfNotCompletedAsync(failingTx);

        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, rejected!.Code);

        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        Assert.IsFalse(reopened.Schema.Tables["items"].Indexes!.Exists(ix => ix.Name == "val_uniq"),
            "an aborted unique build must not leave the index in the schema");

        // Drop the duplicate and rebuild: the retry must succeed and index the whole table.
        KvTransaction deleteTx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(deleteTx, dbname, $"DELETE FROM items WHERE id = {RowCount - 1}", null));
        await database.Transactions.CommitAsync(deleteTx);

        await ExecuteDdl(dbname, database, executor, "CREATE UNIQUE INDEX val_uniq ON items (val)");

        Assert.AreEqual(1, await CountWhere(dbname, database, executor, "val = 42"));
        Assert.AreEqual(1, await CountWhere(dbname, database, executor, "val = 1000"));
        Assert.AreEqual(RowCount - 1, await CountWhere(dbname, database, executor, "val >= 0"));
    }
}
