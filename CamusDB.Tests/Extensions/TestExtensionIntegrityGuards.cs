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
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// The integrity guards around extension code: foreign keys and NOT NULL when a BEFORE UPDATE trigger
/// changes columns the statement does not assign, an extension that catches an error from its own SQL,
/// nested SQL that changes rows the outer statement still holds, byte arrays that extension code
/// mutates, cursors pulled after another engine was entered, the reserved aggregate names, the AFTER
/// queue's byte budget, the per-row large-value carry, and the same-transaction retry boundary.
/// </summary>
[NonParallelizable]
public sealed class TestExtensionIntegrityGuards : ExtensionTestBase
{
    private const string Fixture = "CamusDB.Fixtures.Good.";

    private static async Task ParentAndChild(CommandExecutor executor, string db)
    {
        await ExecDdl(executor, db, "CREATE TABLE parent (id INT64 PRIMARY KEY, name STRING NOT NULL, n INT64, UNIQUE KEY names (name))");
        await ExecDdl(executor, db, "CREATE TABLE child (id INT64 PRIMARY KEY, name STRING REFERENCES parent (name), n INT64)");
        await ExecNonQuery(executor, db, "INSERT INTO parent (id, name, n) VALUES (1, 'a', 0)");
        await ExecNonQuery(executor, db, "INSERT INTO child (id, name, n) VALUES (1, 'a', 0)");
    }

    // ── foreign keys and NOT NULL under BEFORE UPDATE ─────────────────────────

    [Test]
    public async Task BeforeUpdate_ChangingTheChildKey_IsCheckedAgainstTheParent()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ParentAndChild(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER up ON child BEFORE UPDATE EXTERNAL NAME '{Fixture}UppercaseTrigger'");

        // SET assigns only n; the trigger changes name to 'A', which no parent has.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "UPDATE child SET n = 1 WHERE id = 1"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, ex.Code, ex.Message);
        Assert.AreEqual("a", (await Scalar(executor, db, "SELECT name FROM child WHERE id = 1")).StrValue);
    }

    [Test]
    public async Task BeforeUpdate_ChangingAReferencedKey_CannotOrphanChildren()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ParentAndChild(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER up ON parent BEFORE UPDATE EXTERNAL NAME '{Fixture}UppercaseTrigger'");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "UPDATE parent SET n = 1 WHERE id = 1"))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.ForeignKeyRestrictUpdate).Or.EqualTo(CamusDBErrorCodes.ForeignKeyViolation), ex.Message);
        Assert.AreEqual("a", (await Scalar(executor, db, "SELECT name FROM parent WHERE id = 1")).StrValue);
    }

    [Test]
    public async Task BeforeUpdate_CanRepairANullBeforeNotNullValidation()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ExecDdl(executor, db, "CREATE TABLE required (id INT64 PRIMARY KEY, name STRING NOT NULL)");
        await ExecNonQuery(executor, db, "INSERT INTO required (id, name) VALUES (1, 'old')");
        await ExecDdl(executor, db, $"CREATE TRIGGER repair ON required BEFORE UPDATE EXTERNAL NAME '{Fixture}RepairNullTrigger'");

        await ExecNonQuery(executor, db, "UPDATE required SET name = NULL WHERE id = 1");
        Assert.AreEqual("repaired", (await Scalar(executor, db, "SELECT name FROM required")).StrValue);

        // Without a repairing trigger the final validation still refuses the NULL.
        await ExecDdl(executor, db, "DROP TRIGGER repair ON required");
        await ExecDdl(executor, db, $"CREATE TRIGGER noop ON required BEFORE UPDATE EXTERNAL NAME '{Fixture}GuardTrigger'");
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "UPDATE required SET name = NULL WHERE id = 1"))!;
        Assert.AreEqual(CamusDBErrorCodes.NotNullViolation, ex.Code);
    }

    // ── caught errors from the extension's own SQL ────────────────────────────

    [Test]
    public async Task Procedure_ThatCatchesItsSqlError_StillFails_AndCommitsNothing()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ParentAndChild(executor, db);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => ExecNonQuery(executor, db, "CALL fx_swallow()"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionCaughtSqlError, ex.Code, ex.Message);
        Assert.IsEmpty(await ExecQuery(executor, db, "SELECT id FROM child WHERE id = 2"));
    }

    [Test]
    public async Task Trigger_ThatCatchesItsSqlError_StillFails_AndCommitIsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ParentAndChild(executor, db);
        await ExecDdl(executor, db, "CREATE TABLE events (id INT64 PRIMARY KEY, n INT64)");
        await ExecDdl(executor, db, $"CREATE TRIGGER swallow ON events AFTER INSERT EXTERNAL NAME '{Fixture}SwallowErrorTrigger'");

        // A caller that ignores the statement error and commits anyway is refused too.
        KvTransaction tx = await database.Transactions.BeginAsync();
        CamusDBException statement = Assert.ThrowsAsync<CamusDBException>(() =>
            executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO events (id, n) VALUES (1, 1)", null)))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionCaughtSqlError, statement.Code);

        CamusDBException commit = Assert.ThrowsAsync<CamusDBException>(() => database.Transactions.CommitAsync(tx))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionRollbackOnly, commit.Code);
        await database.Transactions.RollbackAsync(tx);

        Assert.IsEmpty(await ExecQuery(executor, db, "SELECT id FROM events"));
        Assert.IsEmpty(await ExecQuery(executor, db, "SELECT id FROM child WHERE id = 99"));
    }

    // ── reentrant writes to rows the outer statement holds ────────────────────

    private static async Task Reentrant(CommandExecutor executor, string db)
    {
        await ExecDdl(executor, db, "CREATE TABLE reentrant (id INT64 PRIMARY KEY, name STRING, n INT64, UNIQUE KEY names (name))");
        await ExecNonQuery(executor, db, "INSERT INTO reentrant (id, name, n) VALUES (1, 'a', 0), (2, 'b', 0)");
    }

    [Test]
    public async Task BeforeUpdate_NestedUpdateOfARowInTheSameChunk_IsRefused()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await Reentrant(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER nested ON reentrant BEFORE UPDATE EXTERNAL NAME '{Fixture}UpdateNextRowTrigger'");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "UPDATE reentrant SET n = 1 WHERE id >= 1"))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerMutationConflict, ex.Code, ex.Message);

        // Nothing changed, and the unique index still holds 'b'.
        Assert.AreEqual("b", (await Scalar(executor, db, "SELECT name FROM reentrant WHERE id = 2")).StrValue);
        CamusDBException duplicate = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO reentrant (id, name, n) VALUES (3, 'b', 0)"))!;
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, duplicate.Code);
    }

    [Test]
    public async Task BeforeDelete_NestedDeleteOfARowInTheSameChunk_IsRefused()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await Reentrant(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER nested ON reentrant BEFORE DELETE EXTERNAL NAME '{Fixture}DeleteNextRowTrigger'");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "DELETE FROM reentrant WHERE id >= 1"))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerMutationConflict, ex.Code, ex.Message);
        Assert.AreEqual(2, (await ExecQuery(executor, db, "SELECT id FROM reentrant")).Count);
    }

    [Test]
    public async Task BeforeUpdate_NestedUpdateOfARowInAnEarlierChunk_IsAllowed()
    {
        // One-row chunks: when the trigger on row 1 runs, row 2 is not held by any chunk yet, so the
        // outer statement later reads row 2 fresh and nothing is stale.
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGood(
            WithExtensions("Fixture.Good") with { SpillEnabled = true, ForceSpillThresholdRows = 1 });
        await Reentrant(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER nested ON reentrant BEFORE UPDATE EXTERNAL NAME '{Fixture}UpdateNextRowTrigger'");

        await ExecNonQuery(executor, db, "UPDATE reentrant SET n = 1 WHERE id >= 1");

        Assert.AreEqual("nested", (await Scalar(executor, db, "SELECT name FROM reentrant WHERE id = 2")).StrValue);
        await ExecNonQuery(executor, db, "INSERT INTO reentrant (id, name, n) VALUES (3, 'b', 0)");
        CamusDBException duplicate = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO reentrant (id, name, n) VALUES (4, 'nested', 0)"))!;
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, duplicate.Code, "the index follows the nested write");
    }

    // ── byte arrays ───────────────────────────────────────────────────────────

    [Test]
    public async Task MutatingReturnedBytes_ChangesNothingStored()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ExecDdl(executor, db, "CREATE TABLE blobs (id INT64 PRIMARY KEY, payload BYTES, n INT64, UNIQUE KEY payloads (payload))");
        await ExecNonQuery(executor, db, "INSERT INTO blobs (id, payload, n) VALUES (1, X'01', 0)");
        await ExecDdl(executor, db, $"CREATE TRIGGER mutate ON blobs BEFORE UPDATE EXTERNAL NAME '{Fixture}MutateBytesTrigger'");

        await ExecNonQuery(executor, db, "UPDATE blobs SET n = 1 WHERE id = 1");

        CollectionAssert.AreEqual(new byte[] { 1 }, (await Scalar(executor, db, "SELECT payload FROM blobs WHERE id = 1")).BytesValue);
        CamusDBException duplicate = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO blobs (id, payload, n) VALUES (2, X'01', 0)"))!;
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, duplicate.Code, "the index still holds X'01'");
    }

    [Test]
    public void BytesMapping_CopiesInBothDirections()
    {
        byte[] stored = [1, 2, 3];
        ColumnValue value = new(stored);

        byte[] handedOut = CamusDB.Core.Extensions.ExtensionValueMapping.ToBytes(value, "f", 0);
        handedOut[0] = 9;
        Assert.AreEqual(1, stored[0], "an argument is a copy");

        byte[] returned = [4, 5];
        ColumnValue kept = CamusDB.Core.Extensions.ExtensionValueMapping.FromBytes(returned);
        returned[0] = 9;
        Assert.AreEqual(4, kept.BytesValue![0], "a returned array is copied");

        byte[] boxed = (byte[])CamusDB.Core.Extensions.ExtensionValueMapping.ToObject(value)!;
        boxed[1] = 9;
        Assert.AreEqual(2, stored[1], "a boxed value is a copy");
    }

    // ── cursors and the function scope ────────────────────────────────────────

    [Test]
    public async Task Cursor_KeepsItsEngineScope_AfterAnotherEngineIsEntered()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        (string otherDb, DatabaseDescriptor otherDatabase, CommandExecutor other) = await CreateDatabase(Options);
        await ExecDdl(executor, db, "CREATE TABLE source (id INT64 PRIMARY KEY, name STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO source (id, name) VALUES (1, 'A B')");

        KvTransaction tx = await database.Transactions.BeginAsync();
        KvTransaction otherTx = await otherDatabase.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db, "SELECT fx_slugify(name) AS slug FROM source", null));
            await other.ExecuteSQLQuery(new ExecuteSQLTicket(otherTx, otherDb, "SELECT 1", null));

            List<QueryResultRow> rows = await cursor.ToListAsync();
            Assert.AreEqual("a-b", rows.Single().Row["slug"].StrValue);
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
            await otherDatabase.Transactions.RollbackAsync(otherTx);
        }
    }

    private static async Task<IAsyncEnumerable<QueryResultRow>> OpenCursorAsync(CommandExecutor executor, ExecuteSQLTicket ticket)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        return cursor;
    }

    [Test]
    public async Task Cursor_KeepsItsEngineScope_WhenAnAsyncHelperOpenedIt()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ExecDdl(executor, db, "CREATE TABLE source (id INT64 PRIMARY KEY, name STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO source (id, name) VALUES (1, 'A B')");

        // Clear this flow's scope, as a caller that never entered the engine would have it.
        CamusDB.Core.Extensions.ExtensionFunctionScope.Enter(null);

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            IAsyncEnumerable<QueryResultRow> cursor = await OpenCursorAsync(
                executor, new ExecuteSQLTicket(tx, db, "SELECT fx_slugify(name) AS slug FROM source", null));
            CamusDB.Core.Extensions.ExtensionFunctionScope.Enter(null);

            Assert.AreEqual("a-b", (await cursor.ToListAsync()).Single().Row["slug"].StrValue);
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    // ── startup and resource limits ───────────────────────────────────────────

    [Test]
    public void AggregateName_IsRefusedAtStartup()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => CreateCommandExecutor(WithExtensions("Fixture.Aggregate")))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionDuplicateMember, ex.Code);
        StringAssert.Contains("'sum'", ex.Message);
    }

    [Test]
    public async Task AfterQueue_IsBoundedByTheByteBudget()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGood(
            WithExtensions("Fixture.Good") with { SpillThresholdBytes = 128 * 1024 });
        await ExecDdl(executor, db, "CREATE TABLE t (id OID PRIMARY KEY DEFAULT (gen_id()), name STRING NOT NULL, wide STRING(200000))");
        await ExecDdl(executor, db, $"CREATE TRIGGER count ON t AFTER INSERT EXTERNAL NAME '{Fixture}CountingTrigger'");

        string wide = new('w', 20_000);
        Dictionary<string, ColumnValue> parameters = new() { ["@w"] = new ColumnValue(ColumnType.String, wide) };

        // Strings are charged as UTF-16: a row is about 40 KB, so two rows fit in 128 KiB and ten do not.
        await ExecNonQuery(executor, db, "INSERT INTO t (name, wide) VALUES ('a', @w), ('b', @w)", parameters);

        string tooMany = string.Join(", ", Enumerable.Range(0, 10).Select(i => $"('r{i}', @w)"));
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "INSERT INTO t (name, wide) VALUES " + tooMany, parameters))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerQueueTooLarge, ex.Code);
        Assert.AreEqual(2, (await ExecQuery(executor, db, "SELECT id FROM t")).Count);
    }

    // ── large-value carry under BEFORE UPDATE ─────────────────────────────────

    [Test]
    public async Task BeforeUpdate_ThatSetsACarriedColumn_StoresTheNewValue()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ExecDdl(executor, db, "CREATE TABLE docs (id INT64 PRIMARY KEY, name STRING, wide STRING(200000))");
        await ExecNonQuery(executor, db, "INSERT INTO docs (id, name, wide) VALUES (1, 'a', @w), (2, 'b', @w)",
            new Dictionary<string, ColumnValue> { ["@w"] = new ColumnValue(ColumnType.String, new string('w', 100_000)) });
        await ExecDdl(executor, db, $"CREATE TRIGGER widen ON docs BEFORE UPDATE EXTERNAL NAME '{Fixture}SetWideTrigger'");

        // The statement assigns only name; wide is carried unless the trigger changes it.
        await ExecNonQuery(executor, db, "UPDATE docs SET name = 'x' WHERE id = 1");

        Dictionary<string, ColumnValue> changed = (await ExecQuery(executor, db, "SELECT name, wide FROM docs WHERE id = 1")).Single();
        Assert.AreEqual("x", changed["name"].StrValue);
        Assert.AreEqual("changed by trigger", changed["wide"].StrValue);
        Assert.AreEqual(100_000, (await Scalar(executor, db, "SELECT wide FROM docs WHERE id = 2")).StrValue!.Length);
    }

    // ── the same-transaction retry boundary ───────────────────────────────────

    [Test]
    public async Task WriteCount_CountsEveryWriteBatch_EvenWithTheMutationLimitOff()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(
            Options with { MaxMutationsPerTransaction = 0 });
        await ExecDdl(executor, db, "CREATE TABLE w (id INT64 PRIMARY KEY)");

        KvTransaction tx = await database.Transactions.BeginAsync();
        long before = tx.WriteCount;
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO w (id) VALUES (1)", null));
        Assert.Greater(tx.WriteCount, before);
        await database.Transactions.CommitAsync(tx);
    }

    [Test]
    public void FenceRetry_InTheSameTransaction_OnlyWhenTheAttemptWroteNothing()
    {
        KvTransaction tx = new(new Kommander.Time.HLCTimestamp(1, 1, 1), "probe");
        CamusDBException fence = new(CamusDBErrorCodes.SchemaCatchingUp, "catching up");
        long before = tx.WriteCount;

        Assert.IsTrue(NonQueryStatementDispatcher.RetryFenceInSameTransaction(fence, 0, tx, before));
        Assert.IsFalse(NonQueryStatementDispatcher.FenceAfterWrite(fence, tx, before));

        tx.ReserveMutations(1);

        Assert.IsFalse(NonQueryStatementDispatcher.RetryFenceInSameTransaction(fence, 0, tx, before), "a write happened");
        Assert.IsTrue(NonQueryStatementDispatcher.FenceAfterWrite(fence, tx, before), "it becomes a retry from a new transaction");
    }
}
