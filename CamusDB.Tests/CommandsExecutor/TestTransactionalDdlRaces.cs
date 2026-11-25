
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using NUnit.Framework;

using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Transactional;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.Transactions;
using CamusDB.Tests.Storage;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Transactional DDL against the work that runs beside it: a sweep that is slow between its scan and its
/// work, a branch created in the middle of a commit, a finalize that is cut off, and data removal that is
/// slower than the commit that asked for it.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestTransactionalDdlRaces : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { TransactionalDdlEnabled = true, CoordinatorJobSweepIntervalMs = 0 };

    private static async Task<KvTransaction> BeginSessionAsync(KvTransactionsManager transactions)
    {
        KvTransaction tx = await transactions.BeginAsync();
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

    private static async Task<List<Dictionary<string, ColumnValue>>> QueryOutsideAsync(
        CommandExecutor executor, DatabaseDescriptor database, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            return await QueryAsync(executor, tx, database.Name, sql);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task ExecuteOutsideAsync(CommandExecutor executor, DatabaseDescriptor database, string sql, bool ddl)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            if (ddl)
                await Ddl(executor, tx, database.Name, sql);
            else
                await Write(executor, tx, database.Name, sql);

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> CountKeysAsync(DatabaseDescriptor database, string keySpace, string prefix)
    {
        int count = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in database.Kahuna.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, keySpace, null, true, null, true, 256, HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (entry.Value is not null && key.StartsWith(prefix, StringComparison.Ordinal))
                count++;
        }

        return count;
    }

    private static Task<int> CountMetaKeysAsync(DatabaseDescriptor database, string prefix) =>
        CountKeysAsync(database, MetaKeys.MetaBucketPrefix(database.Id), prefix);

    private static string DecisionKeyPrefix(DatabaseDescriptor database) => MetaKeys.ChangeSetDecisionKey(database.Id, "");

    /// <summary>A database whose name is an identifier: a generated one can start with a digit, which BRANCH FROM refuses.</summary>
    private async Task<(string Name, DatabaseDescriptor Database, CommandExecutor Executor)> CreateNamedDatabaseAsync(CamusDBOptions? options = null)
    {
        CommandExecutor executor = CreateCommandExecutor(options ?? Options);
        string name = "rt" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(name: name, ifNotExists: false));
        TrackDatabase(name, executor);
        return (name, database, executor);
    }

    private static async Task CreateParentAndChildAsync(CommandExecutor executor, DatabaseDescriptor database)
    {
        await ExecuteOutsideAsync(executor, database, "CREATE TABLE parents (id int64 PRIMARY KEY)", ddl: true);
        await ExecuteOutsideAsync(executor, database, "INSERT INTO parents (id) VALUES (1), (2)", ddl: false);
        await ExecuteOutsideAsync(executor, database, "CREATE TABLE children (id int64 PRIMARY KEY, parent_id int64, v int64 NULL)", ddl: true);
        await ExecuteOutsideAsync(executor, database, "INSERT INTO children (id, parent_id, v) VALUES (10, 1, 5)", ddl: false);
    }

    private static string StageSql(SchemaElementKind kind) => kind switch
    {
        SchemaElementKind.Check => "ALTER TABLE children ADD CONSTRAINT v_positive CHECK (v > 0)",
        SchemaElementKind.NotNull => "ALTER TABLE children ALTER COLUMN v SET NOT NULL",
        SchemaElementKind.Index => "CREATE INDEX children_v ON children (v)",
        SchemaElementKind.Column => "ALTER TABLE children ADD COLUMN w int64 DEFAULT (7)",
        _ => "ALTER TABLE children ADD CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id)",
    };

    private static bool ElementExists(DatabaseDescriptor database, SchemaElementKind kind)
    {
        TableSchema table = database.Schema.Tables["children"];

        return kind switch
        {
            SchemaElementKind.Check => table.CheckConstraints?.Exists(c => c.Name == "v_positive") == true,
            SchemaElementKind.NotNull => table.Columns!.Single(c => c.Name == "v").NotNull,
            SchemaElementKind.Index => table.Indexes?.Exists(i => i.Name == "children_v") == true,
            SchemaElementKind.Column => table.Columns!.Exists(c => c.Name == "w"),
            _ => table.ForeignKeys?.Exists(f => f.Name == "fk_children_parent") == true,
        };
    }

    /// <summary>
    /// A sweep reads the owned job, the owner commits, and then the sweep works on the job it read. The
    /// commit deleted the record and the decision key is gone, so the sweep must find the record gone and
    /// leave the committed element alone.
    /// </summary>
    [TestCase(SchemaElementKind.Check)]
    [TestCase(SchemaElementKind.NotNull)]
    [TestCase(SchemaElementKind.Index)]
    [TestCase(SchemaElementKind.Column)]
    [TestCase(SchemaElementKind.ForeignKey)]
    public async Task ResumeOfAJobReadBeforeItsOwnerCommitted_KeepsTheElement(SchemaElementKind kind)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateParentAndChildAsync(executor, database);

        KvTransaction tx = await BeginSessionAsync(database.Transactions);
        await Ddl(executor, tx, dbname, StageSql(kind));

        List<PersistedCoordinatorJob> read = await executor.Catalogs.LoadCoordinatorJobsAsync(database);
        Assert.IsNotEmpty(read, "the sweep read the job of the open transaction");

        await database.Transactions.CommitAsync(tx);
        Assert.IsTrue(ElementExists(database, kind));

        foreach (PersistedCoordinatorJob job in read)
            await executor.ResumeCoordinatorForTests.ResumeOwnedJobAsync(database, job, job.TableName);

        Assert.IsTrue(ElementExists(database, kind), "the committed element stays");
        Assert.AreEqual(0, await CountMetaKeysAsync(database, DecisionKeyPrefix(database)), "the late work wrote no decision");
        Assert.AreEqual(0, await CountMetaKeysAsync(database, MetaKeys.CoordinatorKeyPrefix(database.Id)));
    }

    /// <summary>
    /// The job a sweep read belongs to a transaction that rolled back, and another transaction then staged
    /// an element of the same name. The late work must neither remove that element nor delete its record.
    /// </summary>
    [TestCase(SchemaElementKind.Check)]
    [TestCase(SchemaElementKind.Index)]
    public async Task ResumeOfAJobReadBeforeItsOwnerRolledBack_LeavesTheElementOfTheNextOwner(SchemaElementKind kind)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateParentAndChildAsync(executor, database);

        KvTransaction first = await BeginSessionAsync(database.Transactions);
        await Ddl(executor, first, dbname, StageSql(kind));
        List<PersistedCoordinatorJob> read = await executor.Catalogs.LoadCoordinatorJobsAsync(database);
        await database.Transactions.RollbackAsync(first);
        Assert.IsFalse(ElementExists(database, kind));

        KvTransaction second = await BeginSessionAsync(database.Transactions);
        await Ddl(executor, second, dbname, StageSql(kind));

        // The second owner lost its DDL lock, so nothing but the record tells its element from the first one.
        await ((TransactionSchemaOverlay)second.SchemaChanges!).ReleaseDdlLockForTestsAsync();

        foreach (PersistedCoordinatorJob job in read)
            await executor.ResumeCoordinatorForTests.ResumeOwnedJobAsync(database, job, job.TableName);

        Assert.IsTrue(ElementExists(database, kind), "the element of the second transaction stays");
        Assert.AreEqual(read.Count, await CountMetaKeysAsync(database, MetaKeys.CoordinatorKeyPrefix(database.Id)), "and so does its record");

        await database.Transactions.CommitAsync(second);
        Assert.IsTrue(ElementExists(database, kind));
        Assert.AreEqual(0, await CountMetaKeysAsync(database, MetaKeys.CoordinatorKeyPrefix(database.Id)));
    }

    /// <summary>
    /// A branch forked while a change set waits for its transaction would copy a transaction that belongs
    /// to the source: its rows can be visible at the fork point while its schema is not. The create waits
    /// for the change set to end and, when it does not end, is refused with the retryable code.
    /// </summary>
    [Test]
    public async Task BranchCreate_WhileAChangeSetStaysPending_IsRefusedRetryably()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateNamedDatabaseAsync();

        KvTransaction tx = await BeginSessionAsync(database.Transactions);
        await Ddl(executor, tx, name, "CREATE TABLE fresh (id int64 PRIMARY KEY)");

        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, CancellationToken.None);
        Assert.IsNotNull(database.Schema.PendingChangeSet);

        string branchName = "br" + Guid.NewGuid().ToString("n");
        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: name, sql: $"CREATE DATABASE {branchName} BRANCH FROM {name}", parameters: null)));

        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, refused!.Code);

        // A node that has not applied the pending entry yet refuses too: the copy reads the record at the
        // fork timestamp.
        PendingSchemaChangeSet? pending = database.Schema.PendingChangeSet;
        database.Schema.PendingChangeSet = null;
        refused = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: name, sql: $"CREATE DATABASE {branchName} BRANCH FROM {name}", parameters: null)));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, refused!.Code);
        database.Schema.PendingChangeSet = pending;

        await database.Transactions.RollbackAsync(tx);
        Assert.IsNull(database.Schema.PendingChangeSet);

        // The refusal left nothing behind: the same name can be created once the change set is gone.
        DatabaseDescriptor branch = (await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: name, sql: $"CREATE DATABASE {branchName} BRANCH FROM {name}", parameters: null))).Database!;
        TrackDatabase(branchName, executor);
        Assert.IsNull(branch.Schema.PendingChangeSet);
    }

    /// <summary>
    /// A branch create that starts between the prepare and the publish of a commit waits for the publish
    /// and then forks the schema and the rows of the transaction together.
    /// </summary>
    [Test]
    public async Task BranchCreate_DuringACommit_WaitsAndForksTheSchemaWithTheRows()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateNamedDatabaseAsync();

        KvTransaction tx = await BeginSessionAsync(database.Transactions);
        await Ddl(executor, tx, name, "CREATE TABLE fresh (id int64 PRIMARY KEY)");
        await Write(executor, tx, name, "INSERT INTO fresh (id) VALUES (1)");

        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, CancellationToken.None);

        string branchName = "br" + Guid.NewGuid().ToString("n");
        Task<ExecuteDDLSQLResult> create = Task.Run(() => executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: name, sql: $"CREATE DATABASE {branchName} BRANCH FROM {name}", parameters: null)));

        await Task.Delay(300);
        Assert.IsFalse(create.IsCompleted, "the create waits while the change set is pending");

        await database.Kahuna.Kahuna.LocateAndCommitTransaction(tx.Handle, CancellationToken.None);
        await overlay.OnCommitOutcomeAsync(tx, SchemaChangesCommitOutcome.Committed);

        DatabaseDescriptor branch = (await create.WaitAsync(TimeSpan.FromSeconds(20))).Database!;
        TrackDatabase(branchName, executor);

        Assert.IsNull(branch.Schema.PendingChangeSet);
        Assert.AreEqual(1, (await QueryOutsideAsync(executor, branch, "SELECT * FROM fresh")).Count);
    }

    /// <summary>
    /// A resolver waits for the decision key while the transaction commits and publishes. The checkpoint of the
    /// publish deletes the key, so the resolver then finds it absent. It must not leave a decision behind for a
    /// change set that has ended, and the change stays published.
    /// </summary>
    [Test]
    public async Task ResolverThatWaitsWhileTheCommitPublishes_LeavesNoDecision()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateNamedDatabaseAsync();

        for (int i = 0; i < 20; i++)
        {
            KvTransaction tx = await BeginSessionAsync(database.Transactions);
            await Ddl(executor, tx, name, $"CREATE TABLE fresh{i} (id int64 PRIMARY KEY)");
            await Write(executor, tx, name, $"INSERT INTO fresh{i} (id) VALUES (1)");

            TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
            Task? resolve = null;

            overlay.Committer.TestAtStep = async step =>
            {
                if (step != ChangeSetStep.Announced)
                    return;

                resolve = Task.Run(async () =>
                {
                    try
                    {
                        await executor.ResolvePendingChangeSetForTests(database);
                    }
                    catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TransactionMustRetry)
                    {
                        // The commit held the key for the whole wait of the resolver.
                    }
                });

                await Task.Delay(10 * (i % 10));
            };

            await database.Transactions.CommitAsync(tx);
            await resolve!.WaitAsync(TimeSpan.FromSeconds(20));

            Assert.IsNull(database.Schema.PendingChangeSet);
            Assert.AreEqual(0, await CountMetaKeysAsync(database, DecisionKeyPrefix(database)), $"round {i}: a decision is left");
            Assert.AreEqual(1, (await QueryOutsideAsync(executor, database, $"SELECT * FROM fresh{i}")).Count);
        }
    }

    /// <summary>
    /// Records that describe work of the source (a decision, data to remove) are not part of the schema a
    /// branch inherits.
    /// </summary>
    [Test]
    public async Task BranchCreate_CopiesNoRecoveryRecordOfTheSource()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateNamedDatabaseAsync();
        await ExecuteOutsideAsync(executor, database, "CREATE TABLE items (id int64 PRIMARY KEY, val int64)", ddl: true);
        await ExecuteOutsideAsync(executor, database, "CREATE INDEX items_val ON items (val)", ddl: true);
        await ExecuteOutsideAsync(executor, database, "INSERT INTO items (id, val) VALUES (1, 10)", ddl: false);

        executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = true;
        try
        {
            KvTransaction tx = await BeginSessionAsync(database.Transactions);
            await Ddl(executor, tx, name, "DROP INDEX items_val ON items");
            await database.Transactions.CommitAsync(tx);

            // The decision of a transaction that vanished stays in the source, where it stops a late commit.
            await PendingChangeSetResolver.ReadOrAbortDecisionAsync(database, "vanished");

            Assert.AreEqual(1, await CountMetaKeysAsync(database, MetaKeys.PurgeKeyPrefix(database.Id)));
            Assert.AreEqual(1, await CountMetaKeysAsync(database, DecisionKeyPrefix(database)));

            string branchName = "br" + Guid.NewGuid().ToString("n");
            DatabaseDescriptor branch = (await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                txnState: null!, database: name, sql: $"CREATE DATABASE {branchName} BRANCH FROM {name}", parameters: null))).Database!;
            TrackDatabase(branchName, executor);

            Assert.AreEqual(0, await CountMetaKeysAsync(branch, MetaKeys.PurgeKeyPrefix(branch.Id)), "the branch has no data to remove");
            Assert.AreEqual(0, await CountMetaKeysAsync(branch, DecisionKeyPrefix(branch)), "and no decision");
            Assert.AreEqual(1, (await QueryOutsideAsync(executor, branch, "SELECT * FROM items")).Count);
        }
        finally
        {
            executor.DeferredPurgesForTests.TestSkipRunsAfterPublish = false;
        }
    }

    private enum FinalizeFault
    {
        /// <summary>The commit request is cancelled, so its outcome is unknown.</summary>
        CommitCancelled,

        /// <summary>The commit request fails with a domain error, so its outcome is unknown.</summary>
        CommitFailed,

        /// <summary>Closing the working set for the result cache is cancelled, before any commit request.</summary>
        CloseCancelled,

        /// <summary>The rollback request is cancelled.</summary>
        RollbackCancelled,

        /// <summary>The rollback stays unresolved.</summary>
        RollbackUnresolved,
    }

    /// <summary>Cuts a finalize off at one point until <see cref="Healthy"/> is set.</summary>
    private sealed class CutOffFinalizeKahuna(IKahuna inner, FinalizeFault fault) : DelegatingKahuna(inner)
    {
        public bool Healthy { get; set; }

        public override Task<TransactionCommitAnswer> LocateAndCommitTransaction(TransactionHandle handle, CancellationToken cancellationToken) =>
            (Healthy, fault) switch
            {
                (false, FinalizeFault.CommitCancelled) => throw new OperationCanceledException("the client went away"),
                (false, FinalizeFault.CommitFailed) => throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "the request failed on the way"),
                _ => base.LocateAndCommitTransaction(handle, cancellationToken),
            };

        public override Task<(KeyValueResponseType, TransactionWorkingSet?)> LocateAndCloseTransaction(
            string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken) =>
            !Healthy && fault == FinalizeFault.CloseCancelled
                ? throw new OperationCanceledException("the client went away")
                : base.LocateAndCloseTransaction(coordinatorKey, transactionId, cancellationToken);

        public override Task<KeyValueResponseType> LocateAndRollbackTransaction(TransactionHandle handle, CancellationToken cancellationToken) =>
            (Healthy, fault) switch
            {
                (false, FinalizeFault.RollbackCancelled) => throw new OperationCanceledException("the client went away"),
                (false, FinalizeFault.RollbackUnresolved) => Task.FromResult(KeyValueResponseType.MustRetry),
                _ => base.LocateAndRollbackTransaction(handle, cancellationToken),
            };
    }

    /// <summary>
    /// A finalize that is cut off frees the DDL lock of the database, so another session can change the
    /// schema once the resolver ended what the cut-off transaction left pending. Without it the renewer of
    /// the lock keeps it for as long as the process lives, because nothing finalizes the transaction again.
    /// </summary>
    [TestCase(nameof(FinalizeFault.CommitCancelled))]
    [TestCase(nameof(FinalizeFault.CommitFailed))]
    [TestCase(nameof(FinalizeFault.CloseCancelled))]
    [TestCase(nameof(FinalizeFault.RollbackCancelled))]
    [TestCase(nameof(FinalizeFault.RollbackUnresolved))]
    public async Task FinalizeThatIsCutOff_FreesTheDdlLock(string faultName)
    {
        FinalizeFault fault = Enum.Parse<FinalizeFault>(faultName);
        bool commits = fault is FinalizeFault.CommitCancelled or FinalizeFault.CommitFailed or FinalizeFault.CloseCancelled;

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CutOffFinalizeKahuna kahuna = new(database.Kahuna.Kahuna, fault);
        using KvTransactionsManager transactions = new(
            kahuna,
            Options with { TransactionFinalizeRetryBudgetMs = 0 },
            cache: new Core.Cache.QueryResultCache(Options));

        KvTransaction tx = await BeginSessionAsync(transactions);
        await Ddl(executor, tx, dbname, "CREATE TABLE fresh (id int64 PRIMARY KEY)");
        await Write(executor, tx, dbname, "INSERT INTO fresh (id) VALUES (1)");

        Assert.CatchAsync(async () =>
        {
            if (commits)
                await transactions.CommitAsync(tx);
            else
                await transactions.RollbackAsync(tx);
        });
        Assert.AreEqual(KvTransactionStatus.Finalizing, tx.Status, "the transaction has no outcome yet");

        await using (TransactionalDdlLock? ddlLock = await TransactionalDdlLock.TryAcquireAsync(database, logger: null))
            Assert.IsNotNull(ddlLock, "nobody holds the DDL lock");

        // The session of the cut-off transaction ends, as it does when its lease lapses.
        kahuna.Healthy = true;
        await transactions.RollbackAsync(tx);

        if (database.Schema.PendingChangeSet is not null)
            Assert.AreEqual(false, await executor.ResolvePendingChangeSetForTests(database), "the resolver aborts what was left pending");

        Assert.IsFalse(database.Schema.Tables.ContainsKey("fresh"));
        await ExecuteOutsideAsync(executor, database, "CREATE TABLE other (id int64 PRIMARY KEY)", ddl: true);
        Assert.IsTrue(database.Schema.Tables.ContainsKey("other"));
    }

    /// <summary>
    /// A commit that was cut off with an unknown outcome is completed by a COMMIT retried on the same
    /// transaction, although the DDL lock is free already.
    /// </summary>
    [TestCase(nameof(FinalizeFault.CommitCancelled))]
    [TestCase(nameof(FinalizeFault.CloseCancelled))]
    public async Task CommitThatIsCutOff_IsCompletedByARetry(string faultName)
    {
        FinalizeFault fault = Enum.Parse<FinalizeFault>(faultName);
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CutOffFinalizeKahuna kahuna = new(database.Kahuna.Kahuna, fault);
        using KvTransactionsManager transactions = new(kahuna, Options, cache: new Core.Cache.QueryResultCache(Options));

        KvTransaction tx = await BeginSessionAsync(transactions);
        await Ddl(executor, tx, dbname, "CREATE TABLE fresh (id int64 PRIMARY KEY)");
        await Write(executor, tx, dbname, "INSERT INTO fresh (id) VALUES (1)");

        Assert.CatchAsync<OperationCanceledException>(async () => await transactions.CommitAsync(tx));

        kahuna.Healthy = true;
        await transactions.CommitAsync(tx);

        Assert.IsNull(database.Schema.PendingChangeSet);
        Assert.AreEqual(1, (await QueryOutsideAsync(executor, database, "SELECT * FROM fresh")).Count);
    }

    /// <summary>
    /// A COMMIT that follows a cut-off ROLLBACK must not commit the rows of a transaction whose schema
    /// changes were never announced.
    /// </summary>
    [Test]
    public async Task CommitAfterACutOffRollback_IsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, "CREATE TABLE kept (id int64 PRIMARY KEY)", ddl: true);

        CutOffFinalizeKahuna kahuna = new(database.Kahuna.Kahuna, FinalizeFault.RollbackCancelled);
        using KvTransactionsManager transactions = new(kahuna, Options);

        KvTransaction tx = await BeginSessionAsync(transactions);
        await Ddl(executor, tx, dbname, "CREATE TABLE fresh (id int64 PRIMARY KEY)");
        await Write(executor, tx, dbname, "INSERT INTO kept (id) VALUES (1)");

        Assert.CatchAsync<OperationCanceledException>(async () => await transactions.RollbackAsync(tx));

        kahuna.Healthy = true;
        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () => await transactions.CommitAsync(tx));
        Assert.AreEqual(CamusDBErrorCodes.TransactionAlreadyCompleted, refused!.Code);

        await transactions.RollbackAsync(tx);
        Assert.AreEqual(0, (await QueryOutsideAsync(executor, database, "SELECT * FROM kept")).Count, "no row of the transaction committed");
        Assert.IsFalse(database.Schema.Tables.ContainsKey("fresh"));
    }

    /// <summary>
    /// The reaper of abandoned transactions frees the DDL lock of a transaction that a client left in the
    /// middle of its finalize, which it cannot roll back.
    /// </summary>
    [Test]
    public async Task ReapOfATransactionLeftFinalizing_FreesTheDdlLock()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        HttpTransactionCoordinator coordinator = new(executor);

        KvTransaction tx = await coordinator.StartAsync(
            dbname, isolationLevel: null, transactionMode: null, sessionOwned: true, cancellationToken: CancellationToken.None);
        await Ddl(executor, tx, dbname, "CREATE TABLE fresh (id int64 PRIMARY KEY)");

        tx.Status = KvTransactionStatus.Finalizing;
        await coordinator.ReapIdleAsync(TimeSpan.Zero, CancellationToken.None);

        await using TransactionalDdlLock? ddlLock = await TransactionalDdlLock.TryAcquireAsync(database, logger: null);
        Assert.IsNotNull(ddlLock, "nobody holds the DDL lock");
    }

    /// <summary>
    /// The transaction that adds an identity column draws from its sequence at once, for the rows it
    /// inserts after the ALTER.
    /// </summary>
    [TestCase(true)]
    [TestCase(false)]
    public async Task OwnerOfANewIdentityColumn_InsertsWithItsDefault(bool commit)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, "CREATE TABLE r (id int64 PRIMARY KEY)", ddl: true);
        await ExecuteOutsideAsync(executor, database, "INSERT INTO r (id) VALUES (1)", ddl: false);

        KvTransaction tx = await BeginSessionAsync(database.Transactions);
        await Ddl(executor, tx, dbname, "ALTER TABLE r ADD COLUMN n INT64 GENERATED ALWAYS AS IDENTITY");
        await Write(executor, tx, dbname, "INSERT INTO r (id) VALUES (2)");

        List<Dictionary<string, ColumnValue>> own = await QueryAsync(executor, tx, dbname, "SELECT id, n FROM r ORDER BY id");
        Assert.AreEqual(2, own.Count);
        Assert.AreEqual(2, own.Select(row => row["n"].LongValue).Distinct().Count(), "each row has its own value");

        if (commit)
        {
            await database.Transactions.CommitAsync(tx);

            List<Dictionary<string, ColumnValue>> rows = await QueryOutsideAsync(executor, database, "SELECT id, n FROM r ORDER BY id");
            Assert.AreEqual(own.Select(r => r["n"].LongValue), rows.Select(r => r["n"].LongValue));

            await ExecuteOutsideAsync(executor, database, "INSERT INTO r (id) VALUES (3)", ddl: false);
            Assert.AreEqual(3, (await QueryOutsideAsync(executor, database, "SELECT n FROM r")).Select(r => r["n"].LongValue).Distinct().Count());
        }
        else
        {
            await database.Transactions.RollbackAsync(tx);

            Assert.IsFalse(database.Schema.Tables["r"].Columns!.Exists(c => c.Name == "n"));
            Assert.IsEmpty(database.Schema.Sequences, "the sequence went with its column");
            Assert.AreEqual(1, (await QueryOutsideAsync(executor, database, "SELECT * FROM r")).Count);
        }
    }

    /// <summary>
    /// ADD FOREIGN KEY that must build its backing index locks each child row, so it is refused at once
    /// when the transaction holds locks on them. With an index that already backs the constraint there is
    /// nothing to build, and the statement runs.
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public async Task AddForeignKey_AfterTheOwnerLockedTheChildRows_IsRefusedOnlyWhenItBuildsAnIndex(bool indexed)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateParentAndChildAsync(executor, database);

        if (indexed)
            await ExecuteOutsideAsync(executor, database, "CREATE INDEX children_parent ON children (parent_id)", ddl: true);

        KvTransaction tx = await BeginSessionAsync(database.Transactions);
        Assert.AreEqual(1, (await QueryAsync(executor, tx, dbname, "SELECT * FROM children FOR UPDATE")).Count);

        Stopwatch watch = Stopwatch.StartNew();

        if (indexed)
        {
            await Ddl(executor, tx, dbname, StageSql(SchemaElementKind.ForeignKey));
            await database.Transactions.CommitAsync(tx);
            Assert.IsTrue(ElementExists(database, SchemaElementKind.ForeignKey));
        }
        else
        {
            CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
                await Ddl(executor, tx, dbname, StageSql(SchemaElementKind.ForeignKey)));
            Assert.AreEqual(CamusDBErrorCodes.StatementNotAllowedInTransaction, refused!.Code);
            Assert.AreEqual(0, await CountMetaKeysAsync(database, MetaKeys.CoordinatorKeyPrefix(database.Id)), "nothing was staged");

            await database.Transactions.RollbackAsync(tx);
            Assert.IsFalse(database.Schema.Tables["children"].Indexes?.Exists(i => i.Name.StartsWith("~fk_", StringComparison.Ordinal)) == true);
        }

        Assert.Less(watch.Elapsed, TimeSpan.FromSeconds(10), "the answer is prompt");
    }

    /// <summary>
    /// COMMIT returns once the schema is published. The removal of the dropped index's entries goes on
    /// behind it, from the durable record.
    /// </summary>
    [TestCase(false)]
    [TestCase(true)]
    public async Task Commit_DoesNotWaitForTheRemovalOfDroppedData(bool dropTable)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteOutsideAsync(executor, database, "CREATE TABLE items (id int64 PRIMARY KEY, val int64)", ddl: true);
        await ExecuteOutsideAsync(executor, database, "CREATE INDEX items_val ON items (val)", ddl: true);
        await ExecuteOutsideAsync(executor, database, "INSERT INTO items (id, val) VALUES (1, 10), (2, 20)", ddl: false);

        TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        executor.DeferredPurgesForTests.TestInterceptBeforePurge = async () =>
        {
            started.TrySetResult();
            await release.Task;
        };

        try
        {
            KvTransaction tx = await BeginSessionAsync(database.Transactions);
            await Ddl(executor, tx, dbname, dropTable ? "DROP TABLE items FORCE" : "DROP INDEX items_val ON items");
            await database.Transactions.CommitAsync(tx).WaitAsync(TimeSpan.FromSeconds(10));
            await started.Task.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.AreEqual(1, await CountMetaKeysAsync(database, MetaKeys.PurgeKeyPrefix(database.Id)), "the record waits for the removal");

            if (dropTable)
                Assert.IsFalse(database.Schema.Tables.ContainsKey("items"), "nobody reaches the table any more");
            else
                Assert.IsFalse(database.Schema.Tables["items"].Indexes!.Exists(i => i.Name == "items_val"), "nobody reaches the index any more");
        }
        finally
        {
            release.TrySetResult();
            executor.DeferredPurgesForTests.TestInterceptBeforePurge = null;
        }

        for (int i = 0; i < 200 && await CountMetaKeysAsync(database, MetaKeys.PurgeKeyPrefix(database.Id)) > 0; i++)
            await Task.Delay(50);

        Assert.AreEqual(0, await CountMetaKeysAsync(database, MetaKeys.PurgeKeyPrefix(database.Id)), "the removal finishes behind the commit");
    }

    /// <summary>
    /// A transaction copies the tables it changes and shares every other one, at its first schema statement
    /// and in the validation of its commit, so the cost of a change does not grow with the catalog.
    /// </summary>
    [Test]
    public async Task ChangeOfOneTable_CopiesNoOtherTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateParentAndChildAsync(executor, database);
        TableSchema sharedParents = database.Schema.Tables["parents"];
        TableSchema sharedChildren = database.Schema.Tables["children"];

        KvTransaction tx = await BeginSessionAsync(database.Transactions);
        await Ddl(executor, tx, dbname, "ALTER TABLE children ADD COLUMN w int64");

        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        Assert.AreSame(sharedParents, overlay.Schema.Tables["parents"], "a table the transaction does not change is shared");

        TableSchema ownChildren = overlay.Schema.Tables["children"];
        Assert.AreNotSame(sharedChildren, ownChildren, "the table it changes is its own");

        await Ddl(executor, tx, dbname, "ALTER TABLE children ADD COLUMN x int64");
        Assert.AreSame(ownChildren, overlay.Schema.Tables["children"], "a later change works on the same copy");

        await database.Transactions.CommitAsync(tx);

        Assert.AreSame(sharedParents, database.Schema.Tables["parents"]);
        Assert.AreNotSame(sharedChildren, database.Schema.Tables["children"], "the publish installs the changed table as a new object");
    }

    /// <summary>
    /// No statement of a transaction, and no validation of its commit, changes a shared table in place: the
    /// private schema holds shared objects, so such a change would be visible to everyone before the commit.
    /// </summary>
    [Test]
    public async Task SchemaStatementsOfATransaction_LeaveEverySharedTableAsItWas()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateParentAndChildAsync(executor, database);
        await ExecuteOutsideAsync(executor, database, "CREATE INDEX children_v ON children (v)", ddl: true);
        await ExecuteOutsideAsync(executor, database, "CREATE TABLE gone (id int64 PRIMARY KEY, a int64)", ddl: true);
        await ExecuteOutsideAsync(executor, database, "CREATE TABLE named (id int64 PRIMARY KEY, a int64, b int64)", ddl: true);
        await ExecuteOutsideAsync(executor, database, "CREATE INDEX named_a ON named (a)", ddl: true);
        await ExecuteOutsideAsync(executor, database, "CREATE VIEW seen AS SELECT id FROM children", ddl: true);

        Dictionary<string, (TableSchema Table, string Json, long Generation)> before = Snapshot(database);

        KvTransaction tx = await BeginSessionAsync(database.Transactions);
        foreach (string sql in new[]
        {
            "ALTER TABLE children ADD COLUMN w int64",
            "ALTER TABLE children RENAME COLUMN w TO w2",
            "COMMENT ON TABLE children IS 'c'",
            "COMMENT ON COLUMN children.parent_id IS 'p'",
            "COMMENT ON INDEX children.children_v IS 'i'",
            "DROP INDEX children_v ON children",
            "ALTER TABLE named RENAME INDEX named_a TO named_a2",
            "ALTER TABLE named DROP COLUMN b",
            "ALTER TABLE named SET (sql_stats_automatic_collection_enabled = false)",
            "ALTER TABLE named RENAME TO renamed",
            "DROP TABLE gone",
            "CREATE TABLE fresh (id int64 PRIMARY KEY, parent_id int64 REFERENCES parents (id))",
            "CREATE INDEX fresh_parent ON fresh (parent_id)",
            "ALTER TABLE fresh ADD CONSTRAINT fresh_positive CHECK (id > 0)",
            "CREATE OR REPLACE VIEW seen AS SELECT id FROM parents",
            "ALTER VIEW seen RENAME TO seen2",
            "DROP VIEW seen2",
        })
        {
            await Ddl(executor, tx, dbname, sql);
            AssertUnchanged(before, database, $"after {sql}");
        }

        // The announcement validates the change set on this node and applies the pending entry.
        TransactionSchemaOverlay overlay = (TransactionSchemaOverlay)tx.SchemaChanges!;
        await overlay.PrepareCommitAsync(tx, CancellationToken.None);
        AssertUnchanged(before, database, "after the announcement");

        await database.Transactions.RollbackAsync(tx);
        AssertUnchanged(before, database, "after the rollback");
        Assert.IsTrue(database.Schema.Views.ContainsKey("seen"));
    }

    private static Dictionary<string, (TableSchema Table, string Json, long Generation)> Snapshot(DatabaseDescriptor database) =>
        database.Schema.Tables.ToDictionary(
            pair => pair.Key,
            pair => (pair.Value, Describe(pair.Value), pair.Value.MetadataGeneration));

    private static string Describe(TableSchema table) =>
        Encoding.UTF8.GetString(MetaJsonSerializer.Serialize(table, MetaJsonContext.Default.TableSchema));

    private static void AssertUnchanged(
        Dictionary<string, (TableSchema Table, string Json, long Generation)> before, DatabaseDescriptor database, string when)
    {
        CollectionAssert.AreEquivalent(before.Keys, database.Schema.Tables.Keys, when);

        foreach ((string name, (TableSchema table, string json, long generation)) in before)
        {
            Assert.AreSame(table, database.Schema.Tables[name], $"{name} {when}");
            Assert.AreEqual(json, Describe(table), $"{name} {when}");
            Assert.AreEqual(generation, table.MetadataGeneration, $"{name} {when}");
        }
    }
}
