
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
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
/// H6 §3.5 — cross-subsystem MustRetry / ordering contract.
///
/// Validates that a transient <c>SchemaCatchingUp</c> (CADB0503) fence error
/// is automatically retried by <c>ExecuteNonSQLQuery</c> and resolves to success
/// once the node's applied schema version catches up to the committed head, rather
/// than surfacing as a permanent abort.
///
/// Also validates (via <c>TestKvTransactionsManager</c>) that the CADB0504 /
/// CADB0501 error-code distinction is maintained for the commit path.
/// </summary>
[TestFixture]
public sealed class TestH6MustRetryContract : SharedNodeBaseTest
{
    private static CreateTableTicket BasicTable(string dbname, string tableName) => new(
        databaseName: dbname,
        tableName: tableName,
        columns:
        [
            new ColumnInfo("id", ColumnType.Id),
            new ColumnInfo("val", ColumnType.Integer64)
        ],
        constraints:
        [
            new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                [new ColumnIndexInfo("id", OrderType.Ascending)])
        ],
        ifNotExists: false
    );

    // H6 §3.5 — fence auto-retry: ExecuteNonSQLQuery retries on SchemaCatchingUp.
    //
    // Scenario:
    //   1. Create table (schema version advances to 1 on all nodes).
    //   2. Artificially advance HeadSchemaVersion to applied+2, activating the fence.
    //   3. Start an INSERT in a background task — it will hit the fence (CADB0503)
    //      and retry with exponential backoff.
    //   4. In the background, run a real CreateTable DDL so schema applied advances
    //      from 1 to 2; now head=3, applied=2, gap=1 ≤ 1 → fence clears.
    //   5. Assert the INSERT task eventually succeeds (1 row affected), not an error.
    //
    // This proves that the SchemaCatchingUp path is retried, not surfaced as Aborted.
    [Test]
    [NonParallelizable]
    public async Task ExecuteNonSqlQuery_RetriesInsert_WhenSchemaCatchingUpFenceClears()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Set up the table.
        await executor.CreateTable(BasicTable(dbname, "fence_tbl"));

        long appliedAfterDdl = database.Schema.SchemaVersion; // e.g., 1

        // Artificially advance HeadSchemaVersion by 2 to activate the fence.
        // The fence fires when head − applied > 1.
        database.ObserveSchemaEntryHead(appliedAfterDdl + 2);

        Assert.Greater(database.HeadSchemaVersion - database.Schema.SchemaVersion, 1,
            "Precondition: fence must be active before starting the INSERT");

        // Begin a transaction. The fence fires before any writes, so if the INSERT
        // retries we can reuse the same tx (no modifications were made on the fenced attempt).
        KvTransaction tx = await database.Transactions.BeginAsync();

        // Start the INSERT in the background. It will immediately hit the fence on the
        // first attempt, wait 100ms, then retry. We must clear the fence within that window.
        Task<ExecuteNonSQLResult> insertTask = executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx,
            database: dbname,
            sql: "INSERT INTO fence_tbl (id, val) VALUES (gen_id(), 42)",
            parameters: null
        ));

        // Run the DDL that advances applied while the INSERT is sleeping between fence retries.
        // Timing: INSERT attempt 0 fires immediately → fence → sleep 100ms.
        //         INSERT attempt 1 fires at ~100ms → fence (if applied still 1) → sleep 200ms.
        //         INSERT attempt 2 fires at ~300ms → fence should clear.
        // We delay 150ms so CreateTable starts after the INSERT's first retry sleep begins,
        // then await CreateTable (which advances applied from 1→2 under its own schema lock).
        // At attempt 1 or 2 the INSERT sees head=3, applied=2, gap=1 ≤ 1 → fence clears.
        await Task.Delay(150).ConfigureAwait(false);
        await executor.CreateTable(BasicTable(dbname, "fence_tbl2"));

        // The INSERT should now complete successfully within the retry window.
        ExecuteNonSQLResult result = await insertTask.WaitAsync(TimeSpan.FromSeconds(10));
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, result.ModifiedRows,
            "INSERT must succeed after schema catches up (fence auto-retry path)");
    }

    // H6 §3.5 — fence exhaustion: after MaxFenceRetries (3) the fence exception propagates
    // as CADB0503 (SchemaCatchingUp), NOT as CADB0501 (TransactionAlreadyCompleted).
    // This verifies the error code is correctly typed even at retry exhaustion.
    [Test]
    [NonParallelizable]
    public async Task ExecuteNonSqlQuery_ThrowsSchemaCatchingUp_WhenFenceNeverClears()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(BasicTable(dbname, "fence_tbl3"));

        long appliedAfterDdl = database.Schema.SchemaVersion;

        // Activate the fence with a very large gap so it never self-heals during the test.
        database.ObserveSchemaEntryHead(appliedAfterDdl + 100);

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: tx,
                    database: dbname,
                    sql: "INSERT INTO fence_tbl3 (id, val) VALUES (gen_id(), 99)",
                    parameters: null
                )));

            Assert.IsNotNull(ex);
            Assert.AreEqual(CamusDBErrorCodes.SchemaCatchingUp, ex!.Code,
                "Fence retry exhaustion must surface as CADB0503, not CADB0501 or any other code");
            Assert.AreNotEqual(CamusDBErrorCodes.TransactionAlreadyCompleted, ex.Code,
                "A fence error must never appear as the permanent-abort CADB0501");
            Assert.AreNotEqual(CamusDBErrorCodes.TransactionMustRetry, ex.Code,
                "A fence error is CADB0503, not the commit-routing CADB0504");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }
}
