
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Transactional;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Schema DDL inside an explicit transaction with <c>transactional_ddl_enabled</c> on, on a single node:
/// the change is visible to later statements of the transaction only, <c>COMMIT</c> publishes it together
/// with the data, and <c>ROLLBACK</c> undoes both.
/// </summary>
[TestFixture]
internal sealed class TestTransactionalDdl : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { TransactionalDdlEnabled = true };

    private static async Task<KvTransaction> BeginSessionAsync(DatabaseDescriptor database)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        tx.MarkSessionOwned();
        return tx;
    }

    private static Task Ddl(CommandExecutor executor, KvTransaction tx, string dbname, string sql) =>
        executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));

    private static Task Write(CommandExecutor executor, KvTransaction tx, string dbname, string sql) =>
        executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

    private static async Task<List<Dictionary<string, ColumnValue>>> QueryAsync(
        CommandExecutor executor, KvTransaction tx, string dbname, string sql)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

        List<Dictionary<string, ColumnValue>> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(new Dictionary<string, ColumnValue>(row.Row));

        return rows;
    }

    /// <summary>Runs <paramref name="sql"/> in an autocommit-style transaction of its own.</summary>
    private static async Task<List<Dictionary<string, ColumnValue>>> QueryOutsideAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            return await QueryAsync(executor, tx, dbname, sql);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task ExecuteOutsideAsync(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql, bool ddl)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            if (ddl)
                await Ddl(executor, tx, dbname, sql);
            else
                await Write(executor, tx, dbname, sql);

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<DatabaseDescriptor> ReopenAsync(CommandExecutor executor, string dbname)
    {
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        return await executor.OpenDatabase(dbname);
    }

    [Test]
    [NonParallelizable]
    public async Task CreateTable_ThenInsert_ThenRollback_LeavesNoTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE t1 (id int64 PRIMARY KEY, name string)");
        await Write(executor, tx, dbname, "INSERT INTO t1 (id, name) VALUES (1, 'one')");

        Assert.AreEqual(1, (await QueryAsync(executor, tx, dbname, "SELECT * FROM t1")).Count, "the transaction sees its own table");
        Assert.IsFalse(database.Schema.Tables.ContainsKey("t1"), "no other flow sees the table before the commit");

        CamusDBException? outside = Assert.ThrowsAsync<CamusDBException>(async () =>
            await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM t1"));
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, outside!.Code);

        await database.Transactions.RollbackAsync(tx);

        Assert.IsFalse(database.Schema.Tables.ContainsKey("t1"));
        Assert.IsNull(database.Schema.PendingChangeSet);

        DatabaseDescriptor reopened = await ReopenAsync(executor, dbname);
        Assert.IsFalse(reopened.Schema.Tables.ContainsKey("t1"), "a reopen finds no trace of the rolled-back table");
    }

    [Test]
    [NonParallelizable]
    public async Task CreateTable_ThenInsert_ThenCommit_PublishesTableAndRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        long versionBefore = database.Schema.SchemaVersion;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE t1 (id int64 PRIMARY KEY, name string)");
        await Ddl(executor, tx, dbname, "CREATE INDEX t1_name ON t1 (name)");
        await Write(executor, tx, dbname, "INSERT INTO t1 (id, name) VALUES (1, 'one')");
        await Write(executor, tx, dbname, "INSERT INTO t1 (id, name) VALUES (2, 'two')");
        await database.Transactions.CommitAsync(tx);

        Assert.IsTrue(database.Schema.Tables.ContainsKey("t1"));
        Assert.IsNull(database.Schema.PendingChangeSet, "the publish ends the change set");
        Assert.AreEqual(versionBefore + 2, database.Schema.SchemaVersion, "one pending entry and one publish entry");

        List<Dictionary<string, ColumnValue>> rows = await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM t1 WHERE name = 'two'");
        Assert.AreEqual(1, rows.Count, "the row and the index built inside the transaction are both visible");

        DatabaseDescriptor reopened = await ReopenAsync(executor, dbname);
        Assert.IsTrue(reopened.Schema.Tables.ContainsKey("t1"), "the checkpoint of the publish holds the table");
        Assert.AreEqual(2, (await QueryOutsideAsync(executor, reopened, dbname, "SELECT * FROM t1")).Count);
    }

    [Test]
    [NonParallelizable]
    public async Task Insert_ThenAddColumn_ThenRollback_UndoesBoth()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE t2 (id int64 PRIMARY KEY, name string)", ddl: true);
        await ExecuteOutsideAsync(executor, database, dbname, "INSERT INTO t2 (id, name) VALUES (1, 'one')", ddl: false);

        KvTransaction tx = await BeginSessionAsync(database);
        await Write(executor, tx, dbname, "INSERT INTO t2 (id, name) VALUES (2, 'two')");
        await Ddl(executor, tx, dbname, "ALTER TABLE t2 ADD COLUMN w int64");
        await Write(executor, tx, dbname, "UPDATE t2 SET w = 7 WHERE id = 2");

        List<Dictionary<string, ColumnValue>> inside = await QueryAsync(executor, tx, dbname, "SELECT id, w FROM t2 WHERE id = 2");
        Assert.AreEqual(7, inside[0]["w"].LongValue, "the transaction reads and writes its new column");

        await database.Transactions.RollbackAsync(tx);

        Assert.IsFalse(database.Schema.Tables["t2"].Columns!.Exists(c => c.Name == "w"), "the column is gone");
        Assert.AreEqual(1, (await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM t2")).Count, "the row is gone");
    }

    [Test]
    [NonParallelizable]
    public async Task Insert_ThenAddColumn_ThenCommit_CommitsBoth()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE t4 (id int64 PRIMARY KEY, name string)", ddl: true);
        await ExecuteOutsideAsync(executor, database, dbname, "INSERT INTO t4 (id, name) VALUES (1, 'one')", ddl: false);

        KvTransaction tx = await BeginSessionAsync(database);
        await Write(executor, tx, dbname, "INSERT INTO t4 (id, name) VALUES (2, 'two')");
        await Ddl(executor, tx, dbname, "ALTER TABLE t4 ADD COLUMN w int64");
        await Write(executor, tx, dbname, "INSERT INTO t4 (id, name, w) VALUES (3, 'three', 9)");

        // Before this feature the commit failed with CADB0502: the transaction's own pin on t4 was stale.
        await database.Transactions.CommitAsync(tx);

        List<Dictionary<string, ColumnValue>> rows = await QueryOutsideAsync(executor, database, dbname, "SELECT id, w FROM t4 ORDER BY id");
        Assert.AreEqual(3, rows.Count);
        Assert.AreEqual(ColumnType.Null, rows[0]["w"].Type, "a row written before the column reads NULL");
        Assert.AreEqual(ColumnType.Null, rows[1]["w"].Type, "a row the transaction wrote before its ALTER reads NULL");
        Assert.AreEqual(9, rows[2]["w"].LongValue);

        DatabaseDescriptor reopened = await ReopenAsync(executor, dbname);
        rows = await QueryOutsideAsync(executor, reopened, dbname, "SELECT id, w FROM t4 ORDER BY id");
        Assert.AreEqual(9, rows[2]["w"].LongValue, "the layout the transaction wrote with survives a reopen");
    }

    [Test]
    [NonParallelizable]
    public async Task DropTable_ThenRollback_KeepsTableAndRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE t3 (id int64 PRIMARY KEY)", ddl: true);
        await ExecuteOutsideAsync(executor, database, dbname, "INSERT INTO t3 (id) VALUES (1)", ddl: false);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "DROP TABLE t3");

        CamusDBException? inside = Assert.ThrowsAsync<CamusDBException>(async () =>
            await QueryAsync(executor, tx, dbname, "SELECT * FROM t3"));
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, inside!.Code, "the transaction no longer sees the table");

        Assert.AreEqual(1, (await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM t3")).Count, "others still do");

        await database.Transactions.RollbackAsync(tx);

        Assert.AreEqual(1, (await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM t3")).Count);
    }

    [Test]
    [NonParallelizable]
    public async Task DropTable_ThenCommit_DropsTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE t3 (id int64 PRIMARY KEY)", ddl: true);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "DROP TABLE t3");
        await database.Transactions.CommitAsync(tx);

        Assert.IsFalse(database.Schema.Tables.ContainsKey("t3"));

        DatabaseDescriptor reopened = await ReopenAsync(executor, dbname);
        Assert.IsFalse(reopened.Schema.Tables.ContainsKey("t3"), "the checkpoint deleted the table's meta key");
    }

    [Test]
    [NonParallelizable]
    public async Task RenameAndComment_ThenCommit_ArePublished()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE a (id int64 PRIMARY KEY, x int64)", ddl: true);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "ALTER TABLE a RENAME TO b");
        await Ddl(executor, tx, dbname, "ALTER TABLE b RENAME COLUMN x TO y");
        await Ddl(executor, tx, dbname, "COMMENT ON TABLE b IS 'renamed'");
        await Ddl(executor, tx, dbname, "CREATE VIEW bv AS SELECT id, y FROM b");
        await Write(executor, tx, dbname, "INSERT INTO b (id, y) VALUES (1, 5)");

        Assert.IsTrue(database.Schema.Tables.ContainsKey("a"), "others keep the old name until the commit");

        await database.Transactions.CommitAsync(tx);

        Assert.IsFalse(database.Schema.Tables.ContainsKey("a"));
        Assert.AreEqual("renamed", database.Schema.Tables["b"].Comment);
        Assert.AreEqual(5, (await QueryOutsideAsync(executor, database, dbname, "SELECT y FROM bv"))[0]["y"].LongValue);

        DatabaseDescriptor reopened = await ReopenAsync(executor, dbname);
        Assert.IsTrue(reopened.Schema.Tables.ContainsKey("b"));
        Assert.IsTrue(reopened.Schema.Views.ContainsKey("bv"));
        Assert.AreEqual("renamed", reopened.Schema.Tables["b"].Comment);
    }

    [Test]
    [NonParallelizable]
    public async Task ConcurrentSchemaChange_RefusesTheCommit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE mine (id int64 PRIMARY KEY)");

        // Moves the shared schema under the transaction. The DDL lock does not cover the ticket API, and
        // the commit must catch the moved base anyway.
        await executor.CreateTable(new CreateTableTicket(
            dbname, "theirs",
            [new ColumnInfo("id", ColumnType.Integer64, notNull: true)],
            [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
            ifNotExists: false));

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await database.Transactions.CommitAsync(tx));
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, refused!.Code);

        Assert.IsFalse(database.Schema.Tables.ContainsKey("mine"));
        Assert.IsTrue(database.Schema.Tables.ContainsKey("theirs"));
        Assert.IsNull(database.Schema.PendingChangeSet);
    }

    [Test]
    [NonParallelizable]
    public async Task StatementThatCannotRollBack_IsRefusedInsideTheTransaction()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await BeginSessionAsync(database);

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Ddl(executor, tx, dbname, "CREATE SEQUENCE s"));
        Assert.AreEqual(CamusDBErrorCodes.StatementNotAllowedInTransaction, refused!.Code);

        await database.Transactions.RollbackAsync(tx);
        Assert.IsFalse(database.Schema.Sequences.ContainsKey("s"));
    }

    [Test]
    [NonParallelizable]
    public async Task CreateIndex_OnATableWithRows_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE r (id int64 PRIMARY KEY, v int64)", ddl: true);

        KvTransaction tx = await BeginSessionAsync(database);

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Ddl(executor, tx, dbname, "CREATE INDEX r_v ON r (v)"));
        Assert.AreEqual(CamusDBErrorCodes.StatementNotAllowedInTransaction, refused!.Code);

        await database.Transactions.RollbackAsync(tx);
        Assert.IsFalse(database.Schema.Tables["r"].Indexes!.Exists(i => i.Name == "r_v"));
    }

    [Test]
    [NonParallelizable]
    public async Task SettingOff_DdlInsideATransactionCommitsAtOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with { TransactionalDdlEnabled = false });

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE t1 (id int64 PRIMARY KEY)");

        Assert.IsTrue(database.Schema.Tables.ContainsKey("t1"), "with the setting off the table exists at once");
        Assert.IsNull(tx.SchemaChanges);

        await database.Transactions.RollbackAsync(tx);
        Assert.IsTrue(database.Schema.Tables.ContainsKey("t1"), "and a rollback does not undo it");
    }

    [Test]
    [NonParallelizable]
    public async Task SecondDdlTransaction_WaitsForTheFirst()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction first = await BeginSessionAsync(database);
        await Ddl(executor, first, dbname, "CREATE TABLE one (id int64 PRIMARY KEY)");

        KvTransaction second = await BeginSessionAsync(database);
        Task secondDdl = Ddl(executor, second, dbname, "CREATE TABLE two (id int64 PRIMARY KEY)");

        await Task.Delay(300);
        Assert.IsFalse(secondDdl.IsCompleted, "the second DDL transaction waits for the DDL lock");

        await database.Transactions.CommitAsync(first);
        await secondDdl.WaitAsync(TimeSpan.FromSeconds(30));
        await database.Transactions.CommitAsync(second);

        Assert.IsTrue(database.Schema.Tables.ContainsKey("one"));
        Assert.IsTrue(database.Schema.Tables.ContainsKey("two"));
    }

    [Test]
    [NonParallelizable]
    public async Task Resolver_AbortsAPendingChangeSetWhoseTransactionDidNotCommit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE lost (id int64 PRIMARY KEY)");

        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, CancellationToken.None);
        Assert.IsNotNull(database.Schema.PendingChangeSet, "the change set is announced");

        // The driver loses track of its commit: the KV transaction rolls back and nobody decides.
        overlay.OutcomeUnknown = true;
        await database.Transactions.RollbackAsync(tx);
        Assert.IsNotNull(database.Schema.PendingChangeSet, "an unknown outcome leaves the decision to the resolver");

        CamusDBException? blocked = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE other (id int64 PRIMARY KEY)", ddl: true));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, blocked!.Code, "a pending change set refuses other schema changes");

        bool? decision = await executor.ResolvePendingChangeSetForTests(database);

        Assert.AreEqual(false, decision);
        Assert.IsNull(database.Schema.PendingChangeSet);
        Assert.IsFalse(database.Schema.Tables.ContainsKey("lost"));

        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE other (id int64 PRIMARY KEY)", ddl: true);
        Assert.IsTrue(database.Schema.Tables.ContainsKey("other"));
    }

    [Test]
    [NonParallelizable]
    public async Task Resolver_PublishesAPendingChangeSetWhoseTransactionCommitted()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE kept (id int64 PRIMARY KEY)");
        await Write(executor, tx, dbname, "INSERT INTO kept (id) VALUES (42)");

        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, CancellationToken.None);

        // The KV commit lands, and the driver dies before it publishes.
        await database.Kahuna.Kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);

        // What CommitAsync records when it cannot learn the KV outcome; a later rollback of the handle
        // then leaves the decision to the resolver instead of aborting.
        overlay.OutcomeUnknown = true;
        Assert.IsFalse(database.Schema.Tables.ContainsKey("kept"), "nothing is visible before the publish");

        bool? decision = await executor.ResolvePendingChangeSetForTests(database);

        Assert.AreEqual(true, decision);
        Assert.IsNull(database.Schema.PendingChangeSet);
        Assert.AreEqual(42, (await QueryOutsideAsync(executor, database, dbname, "SELECT id FROM kept"))[0]["id"].LongValue);

        await overlay.CloseAsync();
    }

    [Test]
    [NonParallelizable]
    public async Task PendingChangeSet_SurvivesAReopen_AndTheResolverAbortsIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE lost (id int64 PRIMARY KEY)");

        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, CancellationToken.None);
        string changeSetId = database.Schema.PendingChangeSet!.ChangeSetId;

        overlay.OutcomeUnknown = true;
        await database.Transactions.RollbackAsync(tx);

        DatabaseDescriptor reopened = await ReopenAsync(executor, dbname);
        Assert.AreEqual(changeSetId, reopened.Schema.PendingChangeSet?.ChangeSetId, "the reopen restores the pending change set");

        Assert.AreEqual(false, await executor.ResolvePendingChangeSetForTests(reopened));
        Assert.IsNull(reopened.Schema.PendingChangeSet);
        Assert.IsFalse(reopened.Schema.Tables.ContainsKey("lost"));

        DatabaseDescriptor again = await ReopenAsync(executor, dbname);
        Assert.IsNull(again.Schema.PendingChangeSet, "the abort checkpoint deleted the pending record");
    }

    [Test]
    [NonParallelizable]
    public async Task PendingChangeSet_SurvivesAReopen_AndTheResolverPublishesIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE kept (id int64 PRIMARY KEY)");
        await Write(executor, tx, dbname, "INSERT INTO kept (id) VALUES (7)");

        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, CancellationToken.None);
        await database.Kahuna.Kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);

        // What CommitAsync records when it cannot learn the KV outcome; a later rollback of the handle
        // then leaves the decision to the resolver instead of aborting.
        overlay.OutcomeUnknown = true;
        await overlay.CloseAsync();

        DatabaseDescriptor reopened = await ReopenAsync(executor, dbname);
        Assert.IsNotNull(reopened.Schema.PendingChangeSet, "the reopen restores the pending change set");
        Assert.IsFalse(reopened.Schema.Tables.ContainsKey("kept"));

        Assert.AreEqual(true, await executor.ResolvePendingChangeSetForTests(reopened));
        Assert.AreEqual(7, (await QueryOutsideAsync(executor, reopened, dbname, "SELECT id FROM kept"))[0]["id"].LongValue);

        DatabaseDescriptor again = await ReopenAsync(executor, dbname);
        Assert.IsNull(again.Schema.PendingChangeSet);
        Assert.IsTrue(again.Schema.Tables.ContainsKey("kept"), "the publish checkpoint holds the table");
    }

    [Test]
    [NonParallelizable]
    public async Task ParentDelete_DuringThePendingWindow_WaitsAndThenMeetsTheNewForeignKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE parents (id int64 PRIMARY KEY)", ddl: true);
        await ExecuteOutsideAsync(executor, database, dbname, "INSERT INTO parents (id) VALUES (1)", ddl: false);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE children (id int64 PRIMARY KEY, parent_id int64 REFERENCES parents (id))");
        await Write(executor, tx, dbname, "INSERT INTO children (id, parent_id) VALUES (10, 1)");

        // The rows commit in KV and the publish has not run yet: the window the pending change set fences.
        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, CancellationToken.None);
        await database.Kahuna.Kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);

        // What CommitAsync records when it cannot learn the KV outcome; a later rollback of the handle
        // then leaves the decision to the resolver instead of aborting.
        overlay.OutcomeUnknown = true;
        await overlay.CloseAsync();

        Assert.IsTrue(database.Schema.PendingChangeSet!.Affects(database.Schema.Tables["parents"].Id), "the parent of the new constraint is fenced");

        Task deleteParent = ExecuteOutsideAsync(executor, database, dbname, "DELETE FROM parents WHERE id = 1", ddl: false);

        await Task.Delay(300);
        Assert.IsFalse(deleteParent.IsCompleted, "a statement on a fenced table waits for the publish");

        Assert.AreEqual(true, await executor.ResolvePendingChangeSetForTests(database));

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () => await deleteParent.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, refused!.Code, "after the publish the delete checks the new child table");

        Assert.AreEqual(1, (await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM parents")).Count);
    }

    [Test]
    [NonParallelizable]
    public async Task UnconfirmedPublish_FailsTheCommitWithARetry_AndARetriedCommitCompletesIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE late (id int64 PRIMARY KEY)");
        await Write(executor, tx, dbname, "INSERT INTO late (id) VALUES (5)");

        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        overlay.Committer.TestPublishFailure = new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "the schema leader is gone");

        try
        {
            CamusDBException? unresolved = Assert.ThrowsAsync<CamusDBException>(async () => await database.Transactions.CommitAsync(tx));
            Assert.AreEqual(CamusDBErrorCodes.TransactionFinalizeUnresolved, unresolved!.Code, "a commit whose publish is not confirmed never reports success");
            Assert.AreEqual(KvTransactionStatus.Committed, tx.Status, "the rows are committed");
            Assert.IsTrue(overlay.PublicationOutstanding);
            Assert.IsFalse(database.Schema.Tables.ContainsKey("late"));
        }
        finally
        {
            overlay.Committer.TestPublishFailure = null;
        }

        await database.Transactions.CommitAsync(tx);

        Assert.IsFalse(overlay.PublicationOutstanding);
        Assert.AreEqual(5, (await QueryOutsideAsync(executor, database, dbname, "SELECT id FROM late"))[0]["id"].LongValue);
    }

    [Test]
    [NonParallelizable]
    public async Task RenameChain_PublishesOnlyTheFinalName_AndReplacesOnlyTheChangedTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE a (id int64 PRIMARY KEY)", ddl: true);
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE untouched (id int64 PRIMARY KEY)", ddl: true);

        TableSchema untouchedBefore = database.Schema.Tables["untouched"];
        TableSchema aBefore = database.Schema.Tables["a"];

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "ALTER TABLE a RENAME TO b");
        await Ddl(executor, tx, dbname, "ALTER TABLE b RENAME TO c");
        await database.Transactions.CommitAsync(tx);

        Assert.IsFalse(database.Schema.Tables.ContainsKey("a"));
        Assert.IsFalse(database.Schema.Tables.ContainsKey("b"), "the middle name never reaches the shared catalog");
        Assert.AreEqual(aBefore.Id, database.Schema.Tables["c"].Id);
        Assert.AreNotSame(aBefore, database.Schema.Tables["c"], "a changed table is a new object, installed in one step");
        Assert.AreSame(untouchedBefore, database.Schema.Tables["untouched"], "an unchanged table keeps its object");
    }

    [Test]
    [NonParallelizable]
    public async Task NoOpDdl_LeavesNoPrivateSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE t (id int64 PRIMARY KEY)", ddl: true);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE IF NOT EXISTS t (id int64 PRIMARY KEY)");

        Assert.IsNull(tx.SchemaChanges, "a statement that changed nothing leaves the transaction on the shared schema");

        await Write(executor, tx, dbname, "INSERT INTO t (id) VALUES (1)");
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, (await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM t")).Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TypedRowRequests_ReadTheTransactionsPrivateSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE typed (id int64 PRIMARY KEY, name string)");

        await executor.Insert(new InsertTicket(
            txnState: tx,
            databaseName: dbname,
            tableName: "typed",
            values: [new Dictionary<string, ColumnValue> { ["id"] = new(ColumnType.Integer64, 1L), ["name"] = new(ColumnType.String, "one") }]));

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(new QueryTicket(
            txnState: tx, databaseName: dbname, tableName: "typed", index: null, projection: null, filters: null,
            where: null, orderBy: null, limit: null, offset: null, parameters: null));

        int seen = 0;
        await foreach (QueryResultRow _ in cursor)
            seen++;

        Assert.AreEqual(1, seen, "a typed read in the transaction sees the table and the row it wrote");

        await database.Transactions.CommitAsync(tx);
        Assert.AreEqual(1, (await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM typed")).Count);
    }

    [Test]
    [NonParallelizable]
    public async Task TableCreatedInThisProcess_CanLoadALayoutItHasNotApplied()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE fresh (id int64 PRIMARY KEY)", ddl: true);

        await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM fresh");

        Assert.IsNotNull(database.Schema.Tables["fresh"].SchemaHistoryLoader, "every opened table can load a layout by version");
    }

    [Test]
    [NonParallelizable]
    public async Task CreateInsertDrop_InOneTransaction_LeavesARelinkableOrphan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE brief (id int64 PRIMARY KEY)");
        await Ddl(executor, tx, dbname, "ALTER TABLE brief ADD COLUMN note string");
        await Write(executor, tx, dbname, "INSERT INTO brief (id, note) VALUES (3, 'kept')");

        string tableId = ((TransactionSchemaOverlay)tx.SchemaChanges!).Schema.Tables["brief"].Id!;

        await Ddl(executor, tx, dbname, "DROP TABLE brief");
        await database.Transactions.CommitAsync(tx);

        Assert.IsFalse(database.Schema.Tables.ContainsKey("brief"));

        DatabaseDescriptor reopened = await ReopenAsync(executor, dbname);
        await ExecuteOutsideAsync(executor, reopened, dbname, $"CREATE TABLE back RELINK TO \"{tableId}\"", ddl: true);

        List<Dictionary<string, ColumnValue>> rows = await QueryOutsideAsync(executor, reopened, dbname, "SELECT id, note FROM back");
        Assert.AreEqual(1, rows.Count, "the orphan record holds the rows the transaction committed");
        Assert.AreEqual("kept", rows[0]["note"].StrValue, "and the layout the transaction wrote them with");
    }

    [Test]
    [NonParallelizable]
    public async Task DropIdentityColumn_RemovesItsSequenceWithThePublish()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE owning (id int64 PRIMARY KEY, n serial)", ddl: true);
        Assert.IsTrue(database.Schema.Sequences.ContainsKey("owning_n_seq"));

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "ALTER TABLE owning DROP COLUMN n");
        Assert.IsTrue(database.Schema.Sequences.ContainsKey("owning_n_seq"), "others keep the sequence until the commit");

        await database.Transactions.CommitAsync(tx);

        Assert.IsFalse(database.Schema.Sequences.ContainsKey("owning_n_seq"), "the publish removes the owned sequence with its column");

        DatabaseDescriptor reopened = await ReopenAsync(executor, dbname);
        Assert.IsFalse(reopened.Schema.Sequences.ContainsKey("owning_n_seq"));
    }

    [Test]
    [NonParallelizable]
    public async Task PendingChangeSet_IsResolvedWithTheCoordinatorSweepOff()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(
            Options with { CoordinatorJobSweepIntervalMs = 0, TransactionalDdlResolveAfterMs = 0 });

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "CREATE TABLE orphaned (id int64 PRIMARY KEY)");

        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, CancellationToken.None);

        overlay.OutcomeUnknown = true;
        await database.Transactions.RollbackAsync(tx);

        long deadline = Environment.TickCount64 + 20_000;
        while (database.Schema.PendingChangeSet is not null && Environment.TickCount64 < deadline)
            await Task.Delay(50);

        Assert.IsNull(database.Schema.PendingChangeSet, "the watcher of the pending change set ends it with no sweep running");
        Assert.IsFalse(database.Schema.Tables.ContainsKey("orphaned"));
    }

    [Test]
    public void EverySchemaStatementHasAClass()
    {
        List<NodeType> missing = [];

        foreach (NodeType nodeType in Enum.GetValues<NodeType>())
        {
            bool handledAsSchemaDdl = StatementScope.IsSchemaDdl(nodeType)
                || nodeType is NodeType.CommentOnTable or NodeType.CommentOnColumn or NodeType.CommentOnIndex
                    or NodeType.CommentOnView or NodeType.CommentOnMaterializedView or NodeType.CommentOnSequence
                    or NodeType.CreateTableAsSelect or NodeType.CreateTableAsSelectIfNotExists
                    or NodeType.RefreshMaterializedView;

            if (handledAsSchemaDdl && DdlStatementClasses.ClassOf(nodeType) is null)
                missing.Add(nodeType);
        }

        Assert.IsEmpty(missing, "every schema statement needs a transactional-DDL class: " + string.Join(", ", missing));
    }
}
