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

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Repro scaffold: a row deleted after a snapshot must remain visible to
/// SELECT ... AS OF SYSTEM TIME at that snapshot, for the whole retention window.
/// Probes repeatedly at increasing delays after the DELETE to observe when/if the
/// historical row disappears.
/// </summary>
[NonParallelizable]
public sealed class TestAsOfDeletedRowRepro : SharedNodeBaseTest
{
    private long NowMillis() =>
        SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(SharedNode.Raft.GetLocalNodeId()).L;

    private static async Task<int> RunNonQuery(
        string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        await db.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    private static async Task<List<QueryResultRow>> RunSelect(string dbName, CommandExecutor executor, string sql)
    {
        KvTransaction tx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        return await cursor.ToListAsync();
    }

    [Test]
    public async Task DeletedRowStaysVisibleAtPreDeleteSnapshot()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE orders (id OBJECT_ID PRIMARY KEY, customer STRING, total INT64)",
            parameters: null));

        await RunNonQuery(dbName, db, executor,
            "INSERT INTO orders (id, customer, total) VALUES " +
            "(gen_id(), \"acme\", 10), (gen_id(), \"acme\", 20), (gen_id(), \"globex\", 30)");

        await Task.Delay(60);
        long snapshotMs = NowMillis();
        await Task.Delay(60);

        await RunNonQuery(dbName, db, executor, "DELETE FROM orders WHERE customer = \"globex\"");

        // Probe the same snapshot at increasing delays after the delete.
        int[] delaysMs = [0, 50, 200, 500, 1000, 2000, 4000];
        List<string> observations = [];
        foreach (int delay in delaysMs)
        {
            if (delay > 0)
                await Task.Delay(delay);

            List<QueryResultRow> rows = await RunSelect(dbName, executor,
                $"SELECT customer, total FROM orders AS OF SYSTEM TIME {snapshotMs}");

            bool deletedVisible = rows.Any(r => r.Row["customer"].StrValue == "globex");
            observations.Add($"+{delay}ms: rows={rows.Count} deletedVisible={deletedVisible}");
        }

        TestContext.Progress.WriteLine("AS-OF probe observations:\n  " + string.Join("\n  ", observations));

        Assert.IsTrue(observations.All(o => o.Contains("rows=3 deletedVisible=True")),
            "deleted row must stay visible at the pre-delete snapshot; observed:\n" + string.Join("\n", observations));
    }
}
