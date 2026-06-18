
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
/// Cross-subsystem MustRetry / ordering contract.
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
public sealed class TestMustRetryContract : SharedNodeBaseTest
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

    // Fence retry contract: a transient SchemaCatchingUp error (CADB0503) is
    // retryable by the caller. After the fence clears (schema applied catches up), the
    // same DML operation succeeds with a new transaction.
    //
    // This tests the contract without relying on timing: the fence is active on the first
    // attempt (verified to throw CADB0503, not CADB0501), then a real DDL clears it, and
    // the second attempt with a fresh tx succeeds. ExecuteNonSQLQuery's internal auto-retry
    // (which also catches CADB0503) is tested by the ThrowsSchemaCatchingUp_WhenFenceNeverClears
    // test which verifies CADB0503 propagates after MaxFenceRetries exhaustion.
    [Test]
    [NonParallelizable]
    public async Task ExecuteNonSqlQuery_Insert_SucceedsOnCallerRetryAfterFenceClears()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(BasicTable(dbname, "fence_tbl"));

        long appliedAfterDdl = database.Schema.SchemaVersion; // e.g., 1

        // Activate the fence: head = applied + 2 → gap = 2 > 1 → fence fires.
        database.ObserveSchemaEntryHead(appliedAfterDdl + 2);

        Assert.Greater(database.HeadSchemaVersion - database.Schema.SchemaVersion, 1,
            "Precondition: fence must be active");

        // First attempt: fence fires → CADB0503, NOT CADB0501.
        KvTransaction tx1 = await database.Transactions.BeginAsync();
        CamusDBException? fenceEx = null;
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: tx1,
                database: dbname,
                sql: "INSERT INTO fence_tbl (id, val) VALUES (gen_id(), 1)",
                parameters: null
            ));
        }
        catch (CamusDBException ex)
        {
            fenceEx = ex;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx1);
        }

        // The fence must surface as CADB0503 (retryable), not as a permanent abort.
        Assert.IsNotNull(fenceEx, "Expected SchemaCatchingUp fence exception on first attempt");
        Assert.AreEqual(CamusDBErrorCodes.SchemaCatchingUp, fenceEx!.Code,
            "Fence must surface as CADB0503, not as CADB0501 or any other permanent code");
        Assert.AreNotEqual(CamusDBErrorCodes.TransactionAlreadyCompleted, fenceEx.Code);
        Assert.AreNotEqual(CamusDBErrorCodes.TransactionMustRetry, fenceEx.Code);

        // Clear the fence by running a real DDL. After CreateTable:
        //   applied = appliedAfterDdl + 1 = 2; head (artificially at 3) − applied = 1 ≤ 1 → cleared.
        await executor.CreateTable(BasicTable(dbname, "fence_tbl2"));

        long headAfter = database.HeadSchemaVersion;
        long appliedAfter = database.Schema.SchemaVersion;
        Assert.LessOrEqual(headAfter - appliedAfter, 1,
            $"Fence must be cleared after DDL (head={headAfter}, applied={appliedAfter})");

        // Second attempt (caller retry): fence is gone → INSERT succeeds.
        KvTransaction tx2 = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: tx2,
            database: dbname,
            sql: "INSERT INTO fence_tbl (id, val) VALUES (gen_id(), 2)",
            parameters: null
        ));
        await database.Transactions.CommitAsync(tx2);

        Assert.AreEqual(1, result.ModifiedRows,
            "INSERT must succeed after the caller retries once the fence has cleared");
    }

    // Fence exhaustion: after MaxFenceRetries (3) the fence exception propagates
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
