
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// PostgreSQL's rule for a failed statement inside an explicit transaction: the failure aborts the
/// transaction. Every later statement on it, reads included, is refused with the rollback-only code,
/// the commit rolls it back and reports the same code, and nothing the failed statement did is stored.
/// The rule holds whether the failed statement staged a write (a foreign-key check runs after the row
/// writes) or none (a transactional Kahuna batch with a duplicate key stages nothing), because a client
/// cannot tell the two apart. The storage-level proof that a refused batch stages nothing lives in the
/// KvTableStore retry fixture; this fixture pins the engine contract.
/// </summary>
[NonParallelizable]
internal sealed class TestFailedStatementAbortsTransaction : BaseTest
{
    private static readonly JsonSerializerOptions WireJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    // ─── Statement helpers ────────────────────────────────────────────────────

    private static async Task Ddl(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> Autocommit(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            await database.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    /// <summary>The rows of <paramref name="sql"/> read on <paramref name="tx"/>, fully enumerated.</summary>
    private static async Task<List<QueryResultRow>> RowsIn(CommandExecutor executor, KvTransaction tx, string db, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        return await cursor.ToListAsync();
    }

    private static async Task<List<QueryResultRow>> Rows(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            return await RowsIn(executor, tx, db, sql);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>The committed (first column, second column) pairs of <paramref name="table"/>, in key order.</summary>
    private static async Task<List<(long, long)>> Pairs(CommandExecutor executor, DatabaseDescriptor database, string db, string table, string first, string second)
    {
        List<QueryResultRow> rows = await Rows(executor, database, db, $"SELECT {first}, {second} FROM {table}");
        return rows.Select(r => (r.Row[first].LongValue, r.Row[second].LongValue)).OrderBy(p => p.Item1).ThenBy(p => p.Item2).ToList();
    }

    /// <summary>A database with <c>x (id INT64 PRIMARY KEY, v INT64 NOT NULL)</c> that holds the row (1, 1).</summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> TableWithOneRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE x (id INT64 PRIMARY KEY, v INT64 NOT NULL)");
        await Autocommit(executor, database, db, "INSERT INTO x (id, v) VALUES (1, 1)");
        return (db, database, executor);
    }

    /// <summary>A database with <c>t (id INT64 PRIMARY KEY, u INT64)</c>, <c>u</c> unique, holding (1, 10), (2, 20), (3, 30).</summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> TableWithUniqueColumn()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE t (id INT64 PRIMARY KEY, u INT64, UNIQUE KEY us (u))");
        await Autocommit(executor, database, db, "INSERT INTO t (id, u) VALUES (1, 10), (2, 20), (3, 30)");
        return (db, database, executor);
    }

    /// <summary>A database with <c>parent (id, n)</c> holding (1, 1), (2, 2) and <c>child</c> referencing parent 2.</summary>
    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> ParentWithOneReferencedRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, database, db, "CREATE TABLE parent (id INT64 PRIMARY KEY, n INT64)");
        await Ddl(executor, database, db, "CREATE TABLE child (id INT64 PRIMARY KEY, parent_id INT64 REFERENCES parent (id))");
        await Autocommit(executor, database, db, "INSERT INTO parent (id, n) VALUES (1, 1), (2, 2)");
        await Autocommit(executor, database, db, "INSERT INTO child (id, parent_id) VALUES (10, 2)");
        return (db, database, executor);
    }

    private static readonly List<(long, long)> OneRow = [(1, 1)];
    private static readonly List<(long, long)> UniqueSeed = [(1, 10), (2, 20), (3, 30)];
    private static readonly List<(long, long)> Parents = [(1, 1), (2, 2)];

    /// <summary>
    /// Runs <paramref name="statement"/> in a new explicit transaction and expects it to fail, with one
    /// of <paramref name="expectedCode"/> when any is given. Then the transaction must be aborted: a SQL
    /// read, a typed read, a write and a schema statement on it are refused with
    /// <see cref="CamusDBErrorCodes.TransactionRollbackOnly"/> before they run, the commit rolls it back
    /// and reports the same code, and every refusal names the failure that aborted the transaction.
    /// </summary>
    private static async Task<CamusDBException> FailThenTransactionIsAborted(
        CommandExecutor executor, DatabaseDescriptor database, string db, string table,
        Func<KvTransaction, Task> statement, params string[] expectedCode)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        CamusDBException failure = Assert.ThrowsAsync<CamusDBException>(() => statement(tx))!;
        if (expectedCode.Length > 0)
            Assert.That(expectedCode, Does.Contain(failure.Code), failure.Message);

        Assert.IsNotNull(tx.RollbackOnlyReason, "the failed statement aborted the transaction");
        StringAssert.Contains(failure.Code, tx.RollbackOnlyReason!);

        CamusDBException read = Assert.ThrowsAsync<CamusDBException>(() => RowsIn(executor, tx, db, $"SELECT COUNT(*) FROM {table}"))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionRollbackOnly, read.Code, read.Message);
        StringAssert.Contains("is aborted", read.Message);
        StringAssert.Contains(failure.Code, read.Message);

        CamusDBException typedRead = Assert.ThrowsAsync<CamusDBException>(() =>
            executor.Query(new QueryTicket(tx, db, table, null, null, null, null, null, null, null, null)))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionRollbackOnly, typedRead.Code, typedRead.Message);

        CamusDBException write = Assert.ThrowsAsync<CamusDBException>(() =>
            executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, $"DELETE FROM {table} WHERE 1 = 0", null)))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionRollbackOnly, write.Code, write.Message);

        CamusDBException ddl = Assert.ThrowsAsync<CamusDBException>(() =>
            executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, "CREATE TABLE never_made (id INT64 PRIMARY KEY)", null)))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionRollbackOnly, ddl.Code, ddl.Message);

        CamusDBException commit = Assert.ThrowsAsync<CamusDBException>(() => database.Transactions.CommitAsync(tx))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionRollbackOnly, commit.Code, commit.Message);
        StringAssert.Contains("has been rolled back", commit.Message);
        StringAssert.Contains(failure.Code, commit.Message);
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status, "the refused commit ended the transaction with a rollback");

        return failure;
    }

    // ─── Writes a key conflict refuses: nothing staged, transaction aborted all the same ──

    [Test]
    public async Task Insert_PrimaryKeyConflictOnALaterRow_AbortsTheTransaction_AndNothingIsStored()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithOneRow();

        // The batch carries the row records and primary-key entries of both rows; Kahuna refuses the
        // entry of key 1 and stages none of the four keys, so no row 21 and no second record for key 1
        // can ever be stored, with or without the abort.
        CamusDBException failure = await FailThenTransactionIsAborted(executor, database, db, "x",
            tx => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO x (id, v) VALUES (21, 21), (1, 1)", null)),
            CamusDBErrorCodes.DuplicateUniqueKeyValue);
        StringAssert.Contains("Duplicate entry", failure.Message);

        CollectionAssert.AreEqual(OneRow, await Pairs(executor, database, db, "x", "id", "v"));
        await TableIntegrityChecker.AssertConsistentAsync(executor, db, "x", "id");
    }

    [Test]
    public async Task Insert_AfterTheRollback_TheRefusedKeysCanBeWrittenAgain()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithOneRow();

        await FailThenTransactionIsAborted(executor, database, db, "x",
            tx => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO x (id, v) VALUES (21, 21), (1, 1)", null)),
            CamusDBErrorCodes.DuplicateUniqueKeyValue);

        // The rollback released every lock of the failed statement, and nothing of it was staged.
        Assert.AreEqual(1, await Autocommit(executor, database, db, "INSERT INTO x (id, v) VALUES (21, 21)"));

        CollectionAssert.AreEqual(new List<(long, long)> { (1, 1), (21, 21) }, await Pairs(executor, database, db, "x", "id", "v"));
        await TableIntegrityChecker.AssertConsistentAsync(executor, db, "x", "id");
    }

    [Test]
    public async Task Insert_UniqueConflictOnALaterRow_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithUniqueColumn();

        await FailThenTransactionIsAborted(executor, database, db, "t",
            tx => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO t (id, u) VALUES (4, 40), (5, 10)", null)),
            CamusDBErrorCodes.DuplicateUniqueKeyValue);

        CollectionAssert.AreEqual(UniqueSeed, await Pairs(executor, database, db, "t", "id", "u"));
        await TableIntegrityChecker.AssertConsistentAsync(executor, db, "t", "id");
    }

    [Test]
    public async Task InsertSelect_PrimaryKeyConflict_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithOneRow();
        await Ddl(executor, database, db, "CREATE TABLE src (id INT64 PRIMARY KEY, v INT64 NOT NULL)");
        await Autocommit(executor, database, db, "INSERT INTO src (id, v) VALUES (7, 7), (1, 9)");

        await FailThenTransactionIsAborted(executor, database, db, "x",
            tx => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO x (id, v) SELECT id, v FROM src", null)),
            CamusDBErrorCodes.DuplicateUniqueKeyValue);

        CollectionAssert.AreEqual(OneRow, await Pairs(executor, database, db, "x", "id", "v"));
        await TableIntegrityChecker.AssertConsistentAsync(executor, db, "x", "id");
    }

    [Test]
    public async Task InsertReturning_ThroughTheQueryEntryPoint_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithOneRow();

        // A RETURNING statement sent to the query entry point runs the write through the same
        // dispatcher, so the rule holds there too.
        await FailThenTransactionIsAborted(executor, database, db, "x",
            tx => RowsIn(executor, tx, db, "INSERT INTO x (id, v) VALUES (21, 21), (1, 1) RETURNING id"),
            CamusDBErrorCodes.DuplicateUniqueKeyValue);

        CollectionAssert.AreEqual(OneRow, await Pairs(executor, database, db, "x", "id", "v"));
        await TableIntegrityChecker.AssertConsistentAsync(executor, db, "x", "id");
    }

    [Test]
    public async Task TypedInsertTicket_PrimaryKeyConflict_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithOneRow();

        List<Dictionary<string, ColumnValue>> rows =
        [
            new() { ["id"] = new ColumnValue(ColumnType.Integer64, 21L), ["v"] = new ColumnValue(ColumnType.Integer64, 21L) },
            new() { ["id"] = new ColumnValue(ColumnType.Integer64, 1L), ["v"] = new ColumnValue(ColumnType.Integer64, 1L) },
        ];

        await FailThenTransactionIsAborted(executor, database, db, "x",
            tx => executor.Insert(new InsertTicket(tx, db, "x", rows)),
            CamusDBErrorCodes.DuplicateUniqueKeyValue);

        CollectionAssert.AreEqual(OneRow, await Pairs(executor, database, db, "x", "id", "v"));
        await TableIntegrityChecker.AssertConsistentAsync(executor, db, "x", "id");
    }

    [Test]
    public async Task Update_UniqueConflictOnALaterRow_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithUniqueColumn();

        // Row 1 moves to 20, which row 2 gives up in the same statement; row 2 moves to 30, which row 3
        // still holds. Both rows ride one batch, which Kahuna refuses as a whole.
        await FailThenTransactionIsAborted(executor, database, db, "t",
            tx => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "UPDATE t SET u = u + 10 WHERE id <= 2", null)),
            CamusDBErrorCodes.DuplicateUniqueKeyValue);

        CollectionAssert.AreEqual(UniqueSeed, await Pairs(executor, database, db, "t", "id", "u"));
        await TableIntegrityChecker.AssertConsistentAsync(executor, db, "t", "id");
    }

    [Test]
    public async Task Upsert_WhoseUpdateConflicts_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithUniqueColumn();

        // Row 1 exists, and its update to u = 20 collides with row 2; row 4 is new and is never written.
        await FailThenTransactionIsAborted(executor, database, db, "t",
            tx => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db,
                "INSERT INTO t (id, u) VALUES (4, 40), (1, 20) ON CONFLICT (id) DO UPDATE SET u = excluded.u", null)),
            CamusDBErrorCodes.DuplicateUniqueKeyValue);

        CollectionAssert.AreEqual(UniqueSeed, await Pairs(executor, database, db, "t", "id", "u"));
        await TableIntegrityChecker.AssertConsistentAsync(executor, db, "t", "id");
    }

    // ─── A failure after a staged write ──────────────────────────────────────

    [Test]
    public async Task Delete_RefusedByAForeignKey_AfterItStagedADelete_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await ParentWithOneReferencedRow();

        // Parent 1 has no child and its delete is staged; parent 2 is referenced, and the foreign-key
        // check, which runs after the row writes, refuses the statement. The rollback discards the
        // staged delete.
        await FailThenTransactionIsAborted(executor, database, db, "parent",
            tx => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "DELETE FROM parent WHERE id >= 1", null)),
            CamusDBErrorCodes.ForeignKeyRestrictDelete, CamusDBErrorCodes.ForeignKeyViolation);

        CollectionAssert.AreEqual(Parents, await Pairs(executor, database, db, "parent", "id", "n"));
        await TableIntegrityChecker.AssertConsistentAsync(executor, db, "parent", "id");
    }

    // ─── Failures that write nothing abort the transaction too ────────────────

    [Test]
    public async Task AStatementRefusedBeforeItWrites_AbortsTheTransactionToo()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithOneRow();

        // Validation refuses the row before any write; the transaction is aborted all the same, because
        // the rule is one rule for every failed statement.
        await FailThenTransactionIsAborted(executor, database, db, "x",
            tx => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO x (id, v) VALUES (5, NULL)", null)),
            CamusDBErrorCodes.NotNullViolation);

        CollectionAssert.AreEqual(OneRow, await Pairs(executor, database, db, "x", "id", "v"));
    }

    [Test]
    public async Task ARead_ThatFailsWhileItPlans_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithOneRow();

        await FailThenTransactionIsAborted(executor, database, db, "x",
            tx => RowsIn(executor, tx, db, "SELECT no_such_column FROM x"));

        CollectionAssert.AreEqual(OneRow, await Pairs(executor, database, db, "x", "id", "v"));
    }

    [Test]
    public async Task ARead_ThatFailsWhileItsRowsStream_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithOneRow();

        // The statement plans fine; the division by zero surfaces when the row (1, 1) is evaluated,
        // inside the lazy cursor. The cursor guard turns that into the same abort.
        await FailThenTransactionIsAborted(executor, database, db, "x",
            tx => RowsIn(executor, tx, db, "SELECT mod(v, v - 1) FROM x"));

        CollectionAssert.AreEqual(OneRow, await Pairs(executor, database, db, "x", "id", "v"));
    }

    [Test]
    public async Task ATypedRead_ThatFails_AbortsTheTransaction()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithOneRow();

        await FailThenTransactionIsAborted(executor, database, db, "x",
            tx => executor.Query(new QueryTicket(tx, db, "no_such_table", null, null, null, null, null, null, null, null)));

        CollectionAssert.AreEqual(OneRow, await Pairs(executor, database, db, "x", "id", "v"));
    }

    [Test]
    public async Task AStatementThatSucceeds_LeavesTheTransactionUsable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithOneRow();

        // The control case: no failure, no abort, and every statement of the transaction commits.
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO x (id, v) VALUES (6, 6)", null));
        Assert.AreEqual(2, (await RowsIn(executor, tx, db, "SELECT id FROM x")).Count);
        Assert.IsNull(tx.RollbackOnlyReason);
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO x (id, v) VALUES (7, 7)", null));
        await database.Transactions.CommitAsync(tx);

        CollectionAssert.AreEqual(new List<(long, long)> { (1, 1), (6, 6), (7, 7) }, await Pairs(executor, database, db, "x", "id", "v"));
        await TableIntegrityChecker.AssertConsistentAsync(executor, db, "x", "id");
    }

    // ─── HTTP: an explicit transaction whose statement fails ─────────────────

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body, WireJson));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }

    [Test]
    public async Task Http_ExplicitTransaction_AFailedInsertIsNeverCommitted()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await TableWithOneRow();
        HttpTransactionCoordinator coordinator = new(executor);
        PreparedStatementRegistry registry = new(Options);

        KvTransaction tx = await coordinator.StartAsync(db, null, null, sessionOwned: true);

        ExecuteSQLController controller = new(executor, coordinator, registry, logger, Options)
        {
            ControllerContext = Context(new
            {
                txnIdPT = tx.ClientId.L,
                txnIdCounter = tx.ClientId.C,
                databaseName = db,
                sql = "INSERT INTO x (id, v) VALUES (21, 21), (1, 1)",
            }),
        };

        JsonResult result = await controller.ExecuteNonSQLQuery();
        ExecuteNonSQLQueryResponse response = (ExecuteNonSQLQueryResponse)result.Value!;
        Assert.AreEqual("failed", response.Status);
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, response.Code, response.Message);

        // The transport rolled the transaction back on the failure, and the engine had aborted it
        // first: neither a commit by handle nor one by the transaction itself can store the rows.
        Assert.IsNotNull(tx.RollbackOnlyReason);
        Assert.Throws<CamusDBException>(() => coordinator.GetState(tx.ClientId.L, tx.ClientId.C));
        Assert.ThrowsAsync<CamusDBException>(() => coordinator.CommitTrackedAsync(tx));
        CamusDBException commit = Assert.ThrowsAsync<CamusDBException>(() => database.Transactions.CommitAsync(tx))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionRollbackOnly, commit.Code, commit.Message);

        CollectionAssert.AreEqual(OneRow, await Pairs(executor, database, db, "x", "id", "v"));
        await TableIntegrityChecker.AssertConsistentAsync(executor, db, "x", "id");
    }
}
