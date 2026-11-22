
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Storage.Kv;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
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
        CommandExecutor executor, KvTransaction tx, string dbname, string sql, CacheMetadataHolder? meta = null)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null), meta);

        List<Dictionary<string, ColumnValue>> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(new Dictionary<string, ColumnValue>(row.Row));

        return rows;
    }

    /// <summary>An autocommit snapshot read, the only kind of read the result cache serves.</summary>
    private static async Task<(List<Dictionary<string, ColumnValue>> Rows, CacheMetadataHolder Meta)> CachedReadAsync(
        CommandExecutor executor, string dbname, string sql)
    {
        CacheMetadataHolder meta = new();
        List<Dictionary<string, ColumnValue>> rows = await QueryAsync(executor, KvTransaction.CreateReadOnly(), dbname, sql, meta);
        return (rows, meta);
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

    /// <summary>
    /// One SQL text that runs inside a DDL transaction and outside it binds to two schemas. The parse
    /// cache gives both runs the same AST, and the bound-query cache, the plan cache and the descriptor
    /// cache key on what the two runs share. Neither run may get the binding of the other, and a result
    /// cached before the transaction must not survive its publish.
    /// </summary>
    [TestCase(true)]
    [TestCase(false)]
    [NonParallelizable]
    public async Task SameStatementInsideAndOutside_SharesNoCachedBindingOrResult(bool commit)
    {
        CamusDBOptions options = Options;
        QueryResultCache cache = new(options, sweepIntervalMs: -1);
        CommandExecutor cachingExecutor = CreateCommandExecutor(
            options, new CamusDB.Core.Config.CamusDBOptionsHolder(options), cache: cache);
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabaseWith(cachingExecutor);

        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE base (id int64 PRIMARY KEY, name string)", ddl: true);
        await ExecuteOutsideAsync(executor, database, dbname, "INSERT INTO base (id, name) VALUES (1, 'one')", ddl: false);

        const string BoundSql = "SELECT * FROM base WHERE id = 1";
        const string CachedSql = "SELECT * FROM base {cache=ddl}";

        // Warm every cache with the shared schema, and check that the result cache serves.
        for (int i = 0; i < 2; i++)
            Assert.IsFalse((await QueryOutsideAsync(executor, database, dbname, BoundSql))[0].ContainsKey("w"));

        await CachedReadAsync(executor, dbname, CachedSql);
        (_, CacheMetadataHolder warmMeta) = await CachedReadAsync(executor, dbname, CachedSql);
        Assert.AreEqual(QueryCacheStatus.Hit, warmMeta.Status,
            $"the result cache must serve before the transaction (was {warmMeta.Status}/{warmMeta.BypassReason})");

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "ALTER TABLE base ADD COLUMN w int64");
        await Write(executor, tx, dbname, "INSERT INTO base (id, name, w) VALUES (2, 'two', 20)");

        for (int i = 0; i < 2; i++)
            Assert.IsTrue((await QueryAsync(executor, tx, dbname, BoundSql))[0].ContainsKey("w"),
                "the transaction binds the warm statement to its own schema");

        CacheMetadataHolder insideMeta = new();
        List<Dictionary<string, ColumnValue>> inside = await QueryAsync(executor, tx, dbname, CachedSql, insideMeta);
        Assert.AreEqual(2, inside.Count, "the transaction reads its own row, not the cached result");
        Assert.AreNotEqual(QueryCacheStatus.Hit, insideMeta.Status);

        for (int i = 0; i < 2; i++)
            Assert.IsFalse((await QueryOutsideAsync(executor, database, dbname, BoundSql))[0].ContainsKey("w"),
                "the binding of the transaction must not reach another transaction");

        (List<Dictionary<string, ColumnValue>> during, _) = await CachedReadAsync(executor, dbname, CachedSql);
        Assert.AreEqual(1, during.Count);
        Assert.IsFalse(during[0].ContainsKey("w"), "the cached read of another transaction keeps the shared schema");

        if (commit)
            await database.Transactions.CommitAsync(tx);
        else
            await database.Transactions.RollbackAsync(tx);

        Assert.AreEqual(commit, (await QueryOutsideAsync(executor, database, dbname, BoundSql))[0].ContainsKey("w"),
            "after the transaction ends, the bound statement follows the shared schema");

        (List<Dictionary<string, ColumnValue>> after, CacheMetadataHolder afterMeta) = await CachedReadAsync(executor, dbname, CachedSql);
        Assert.AreEqual(commit ? 2 : 1, after.Count, $"the cached read after the transaction ended (status {afterMeta.Status})");
        Assert.AreEqual(commit, after[0].ContainsKey("w"));
    }

    /// <summary>Counts the stored keys under <paramref name="prefix"/> in one key space, with a plain read of the latest committed keys.</summary>
    private static async Task<int> CountKeysAsync(DatabaseDescriptor database, string keySpace, string prefix)
    {
        int count = 0;

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in database.Kahuna.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, keySpace, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal) && entry.Value is not null)
                count++;
        }

        return count;
    }

    private static Task<int> CountIndexEntriesAsync(DatabaseDescriptor database, string indexSpace) =>
        CountKeysAsync(database, indexSpace, indexSpace + "/");

    private static Task<int> CountPurgeRecordsAsync(DatabaseDescriptor database) =>
        CountKeysAsync(database, MetaKeys.MetaBucketPrefix(database.Id), MetaKeys.PurgeKeyPrefix(database.Id));

    /// <summary>
    /// Creates <c>items</c> with the index <c>val_idx</c> and three rows, and returns the key space of the
    /// index entries.
    /// </summary>
    private static async Task<string> CreateIndexedTableAsync(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE items (id int64 PRIMARY KEY, val int64)", ddl: true);
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE INDEX val_idx ON items (val)", ddl: true);
        await ExecuteOutsideAsync(executor, database, dbname, "INSERT INTO items (id, val) VALUES (1, 10), (2, 20), (3, 30)", ddl: false);

        TableSchema table = database.Schema.Tables["items"];
        TableIndexSchema index = table.Indexes!.First(i => i.Name == "val_idx");
        string indexSpace = KvKeyBuilder.IndexSpaceOf(database.Id, table.EffectiveStorageId, index.KvId);

        Assert.AreEqual(3, await CountIndexEntriesAsync(database, indexSpace), "one entry per row before the drop");
        return indexSpace;
    }

    [Test]
    [NonParallelizable]
    public async Task DropIndex_ThenRollback_KeepsTheIndexAndItsEntries()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with { CoordinatorJobSweepIntervalMs = 0 });
        string indexSpace = await CreateIndexedTableAsync(executor, database, dbname);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "DROP INDEX val_idx ON items");
        await database.Transactions.RollbackAsync(tx);

        Assert.IsTrue(database.Schema.Tables["items"].Indexes!.Exists(i => i.Name == "val_idx"));
        Assert.AreEqual(3, await CountIndexEntriesAsync(database, indexSpace), "a rollback leaves every entry");
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database), "a rollback writes no purge record");
        Assert.AreEqual(1, (await QueryOutsideAsync(executor, database, dbname, "SELECT id FROM items WHERE val = 20")).Count);
    }

    [Test]
    [NonParallelizable]
    public async Task DropIndex_ThenCommit_RemovesItsEntriesAfterThePublish()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with { CoordinatorJobSweepIntervalMs = 0 });
        string indexSpace = await CreateIndexedTableAsync(executor, database, dbname);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "DROP INDEX val_idx ON items");
        Assert.AreEqual(3, await CountIndexEntriesAsync(database, indexSpace), "the entries stay while the transaction is open");

        await database.Transactions.CommitAsync(tx);

        Assert.IsFalse(database.Schema.Tables["items"].Indexes!.Exists(i => i.Name == "val_idx"));
        Assert.AreEqual(0, await CountIndexEntriesAsync(database, indexSpace), "the committing node removes the entries after its publish");
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database), "and then deletes the record");
        Assert.AreEqual(3, (await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM items")).Count, "the rows stay");
    }

    [Test]
    [NonParallelizable]
    public async Task DropIndex_RemovalStoppedByACrash_IsFinishedAfterAReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with { CoordinatorJobSweepIntervalMs = 0 });
        string indexSpace = await CreateIndexedTableAsync(executor, database, dbname);

        // The committing node stops right after its publish, before it removes the entries.
        executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = true;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "DROP INDEX val_idx ON items");
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(3, await CountIndexEntriesAsync(database, indexSpace));
        Assert.AreEqual(1, await CountPurgeRecordsAsync(database), "the record committed with the transaction");

        executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = false;
        DatabaseDescriptor reopened = await ReopenAsync(executor, dbname);

        Assert.AreEqual(1, await executor.DeferredPurgesForTests.RunDueAsync(reopened), "the first run after the reopen finds the record");
        Assert.AreEqual(0, await CountIndexEntriesAsync(reopened, indexSpace));
        Assert.AreEqual(0, await CountPurgeRecordsAsync(reopened));

        reopened.MarkDeferredPurgesDue();
        Assert.AreEqual(0, await executor.DeferredPurgesForTests.RunDueAsync(reopened), "a second run finds nothing to do");
    }

    [Test]
    [NonParallelizable]
    public async Task DropIndex_RecordOfAPendingChangeSet_WaitsForThePublish()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with { CoordinatorJobSweepIntervalMs = 0 });
        string indexSpace = await CreateIndexedTableAsync(executor, database, dbname);

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "DROP INDEX val_idx ON items");

        // The rows and the record commit in KV; the driver stops before the publish.
        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, CancellationToken.None);
        await database.Kahuna.Kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);
        overlay.OutcomeUnknown = true;
        await overlay.CloseAsync();

        Assert.IsNotNull(database.Schema.PendingChangeSet);
        Assert.AreEqual(1, await CountPurgeRecordsAsync(database));

        database.MarkDeferredPurgesDue();
        Assert.AreEqual(0, await executor.DeferredPurgesForTests.RunDueAsync(database), "no run removes data while the change set is pending");
        Assert.AreEqual(3, await CountIndexEntriesAsync(database, indexSpace), "the shared schema still uses the index");

        Assert.AreEqual(true, await executor.ResolvePendingChangeSetForTests(database));

        Assert.AreEqual(1, await executor.DeferredPurgesForTests.RunDueAsync(database), "the publish makes the record due");
        Assert.AreEqual(0, await CountIndexEntriesAsync(database, indexSpace));
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database));
    }

    private static Task<int> CountStoredRowsAsync(DatabaseDescriptor database, string storageId)
    {
        string rowSpace = KvKeyBuilder.RowSpaceOf(database.Id, storageId);
        return CountKeysAsync(database, rowSpace, rowSpace + "/");
    }

    private static Task<int> CountOrphanRecordsAsync(DatabaseDescriptor database) =>
        CountKeysAsync(database, MetaKeys.MetaBucketPrefix(database.Id), MetaKeys.OrphanKeyPrefix(database.Id));

    /// <summary>The schema version each stored row of a table was written under, read from the raw row keys.</summary>
    private static async Task<List<int>> StoredRowVersionsAsync(DatabaseDescriptor database, string storageId)
    {
        string rowSpace = KvKeyBuilder.RowSpaceOf(database.Id, storageId);
        List<int> versions = [];

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in database.Kahuna.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, rowSpace, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            // The stored value starts with a one-byte envelope, then the version word of the row payload.
            if (key.StartsWith(rowSpace + "/", StringComparison.Ordinal) && entry.Value is { Length: > 5 } value)
                versions.Add(RowStorageForms.StoredVersion(System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(value.AsSpan(1))));
        }

        return versions;
    }

    [Test]
    [NonParallelizable]
    public async Task DropTableForce_ThenRollback_KeepsTheTableAndItsData()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with { CoordinatorJobSweepIntervalMs = 0 });
        string indexSpace = await CreateIndexedTableAsync(executor, database, dbname);
        string storageId = database.Schema.Tables["items"].EffectiveStorageId;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "DROP TABLE items FORCE");
        await database.Transactions.RollbackAsync(tx);

        Assert.IsTrue(database.Schema.Tables.ContainsKey("items"));
        Assert.AreEqual(3, await CountStoredRowsAsync(database, storageId), "a rollback leaves every row");
        Assert.AreEqual(3, await CountIndexEntriesAsync(database, indexSpace), "and every index entry");
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database));
        Assert.AreEqual(3, (await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM items")).Count);
    }

    [Test]
    [NonParallelizable]
    public async Task DropTableForce_ThenCommit_RemovesTheDataAfterThePublish()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with { CoordinatorJobSweepIntervalMs = 0 });
        string indexSpace = await CreateIndexedTableAsync(executor, database, dbname);
        string storageId = database.Schema.Tables["items"].EffectiveStorageId;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "DROP TABLE items FORCE");
        Assert.AreEqual(3, await CountStoredRowsAsync(database, storageId), "the rows stay while the transaction is open");

        await database.Transactions.CommitAsync(tx);

        Assert.IsFalse(database.Schema.Tables.ContainsKey("items"));
        Assert.AreEqual(0, await CountStoredRowsAsync(database, storageId), "the committing node removes the rows after its publish");
        Assert.AreEqual(0, await CountIndexEntriesAsync(database, indexSpace), "and every index entry");
        Assert.AreEqual(0, await CountPurgeRecordsAsync(database), "and then deletes the record");
        Assert.AreEqual(0, await CountOrphanRecordsAsync(database), "a drop without retention leaves nothing to relink");
    }

    [Test]
    [NonParallelizable]
    public async Task DropTableInABranch_ThenCommit_RemovesOnlyTheRowsOfTheBranch()
    {
        // BRANCH FROM takes an identifier, and a generated name can start with a digit.
        CommandExecutor executor = CreateCommandExecutor(Options with { CoordinatorJobSweepIntervalMs = 0 });
        string rootName = "rt" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor root = await executor.CreateDatabase(new CreateDatabaseTicket(name: rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        await ExecuteOutsideAsync(executor, root, rootName, "CREATE TABLE items (id int64 PRIMARY KEY, val int64)", ddl: true);
        await ExecuteOutsideAsync(executor, root, rootName, "INSERT INTO items (id, val) VALUES (1, 10), (2, 20)", ddl: false);

        string branchName = "br" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor branch = (await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName, sql: $"CREATE DATABASE {branchName} BRANCH FROM {rootName}", parameters: null))).Database!;
        TrackDatabase(branchName, executor);

        await ExecuteOutsideAsync(executor, branch, branchName, "INSERT INTO items (id, val) VALUES (3, 30)", ddl: false);

        string storageId = branch.Schema.Tables["items"].EffectiveStorageId;
        Assert.AreEqual(1, await CountStoredRowsAsync(branch, storageId), "the branch stores only its own row");

        KvTransaction tx = await BeginSessionAsync(branch);
        await Ddl(executor, tx, branchName, "DROP TABLE items");
        Assert.AreEqual(1, await CountStoredRowsAsync(branch, storageId), "the row stays while the transaction is open");

        await branch.Transactions.CommitAsync(tx);

        Assert.IsFalse(branch.Schema.Tables.ContainsKey("items"));
        Assert.AreEqual(0, await CountStoredRowsAsync(branch, storageId), "the publish removes the rows of the branch");
        Assert.AreEqual(0, await CountPurgeRecordsAsync(branch));
        Assert.AreEqual(2, (await QueryOutsideAsync(executor, root, rootName, "SELECT * FROM items")).Count, "the root keeps its rows");
    }

    [Test]
    [NonParallelizable]
    public async Task DropColumn_InATransaction_RewritesNoRow()
    {
        // A rewrite of every row would take this transaction past its write limit.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with { MaxMutationsPerTransaction = 10 });
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE wide (id int64 PRIMARY KEY, a int64, gone string)", ddl: true);

        for (int i = 0; i < 8; i++)
            await ExecuteOutsideAsync(executor, database, dbname,
                $"INSERT INTO wide (id, a, gone) VALUES ({(i * 2) + 1}, 1, 'old'), ({(i * 2) + 2}, 2, 'old')", ddl: false);

        string storageId = database.Schema.Tables["wide"].EffectiveStorageId;
        int versionBefore = database.Schema.Tables["wide"].Version;

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "ALTER TABLE wide DROP COLUMN gone");
        await database.Transactions.CommitAsync(tx);

        Assert.Greater(database.Schema.Tables["wide"].Version, versionBefore);
        List<int> versions = await StoredRowVersionsAsync(database, storageId);
        Assert.AreEqual(16, versions.Count);
        Assert.That(versions, Is.All.EqualTo(versionBefore), "every row keeps the layout it was written under");

        List<Dictionary<string, ColumnValue>> rows = await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM wide");
        Assert.AreEqual(16, rows.Count);
        Assert.IsFalse(rows.Exists(row => row.ContainsKey("gone")), "no reader sees the dropped column");

        // A new column with the same name has a new id: the old rows read NULL for it, never the dropped value.
        KvTransaction add = await BeginSessionAsync(database);
        await Ddl(executor, add, dbname, "ALTER TABLE wide ADD COLUMN gone string");
        Assert.IsTrue((await QueryAsync(executor, add, dbname, "SELECT id, gone FROM wide")).TrueForAll(row => row["gone"].Type == ColumnType.Null));
        await database.Transactions.CommitAsync(add);

        Assert.IsTrue((await QueryOutsideAsync(executor, database, dbname, "SELECT id, gone FROM wide")).TrueForAll(row => row["gone"].Type == ColumnType.Null));

        DatabaseDescriptor reopened = await ReopenAsync(executor, dbname);
        Assert.IsTrue((await QueryOutsideAsync(executor, reopened, dbname, "SELECT id, gone FROM wide")).TrueForAll(row => row["gone"].Type == ColumnType.Null),
            "the same after a reopen");
    }

    [Test]
    [NonParallelizable]
    public async Task DropColumn_InATransaction_KeepsAnOutOfLineValueUntilItsRowChanges()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(
            Options with { LargeValueThresholdBytes = 64, LargeValueCompressionEnabled = false });
        await ExecuteOutsideAsync(executor, database, dbname, "CREATE TABLE docs (id int64 PRIMARY KEY, title string, body string)", ddl: true);

        string body = new('b', 500);
        await ExecuteOutsideAsync(executor, database, dbname,
            $"INSERT INTO docs (id, title, body) VALUES (1, 't1', '{body}'), (2, 't2', '{body}')", ddl: false);

        string valueSpace = KvKeyBuilder.LargeValueSpaceOf(database.Id, database.Schema.Tables["docs"].EffectiveStorageId);
        Assert.AreEqual(2, await CountKeysAsync(database, valueSpace, valueSpace + "/"), "each body is stored out of line");

        KvTransaction tx = await BeginSessionAsync(database);
        await Ddl(executor, tx, dbname, "ALTER TABLE docs DROP COLUMN body");
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(2, await CountKeysAsync(database, valueSpace, valueSpace + "/"), "the dropped values stay until their rows change");

        await ExecuteOutsideAsync(executor, database, dbname, "UPDATE docs SET title = 'changed' WHERE id = 1", ddl: false);
        Assert.AreEqual(1, await CountKeysAsync(database, valueSpace, valueSpace + "/"), "an update removes the value of the dropped column");

        await ExecuteOutsideAsync(executor, database, dbname, "DELETE FROM docs WHERE id = 2", ddl: false);
        Assert.AreEqual(0, await CountKeysAsync(database, valueSpace, valueSpace + "/"), "a delete removes it too");

        List<Dictionary<string, ColumnValue>> rows = await QueryOutsideAsync(executor, database, dbname, "SELECT * FROM docs");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("changed", rows[0]["title"].StrValue);
        Assert.IsFalse(rows[0].ContainsKey("body"));
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
