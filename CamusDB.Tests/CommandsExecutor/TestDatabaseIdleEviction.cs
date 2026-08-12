/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Releasing idle database descriptors so the open set tracks the working set.
///
/// <para>Eviction is a background optimization, which sets the bar these tests hold it to: it must be
/// invisible. A descriptor that anything could still be using has to be spared — and spared for the
/// right reason, which is why the cases below assert on the refusal reason rather than only on the
/// count. The dangerous failure is not a missed eviction; it is a released descriptor that a running
/// statement still holds, so each "is refused" case is paired with evidence that the work it was
/// protecting then completes normally.</para>
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node and asserts on process-wide open-object counts.
[NonParallelizable]
public sealed class TestDatabaseIdleEviction : BaseTest
{
    private const string TableName = "robots";

    /// <summary>Evict anything idle at all — these tests drive the primitive, not the timer.</summary>
    private const long EvictImmediately = 0;

    /// <summary>
    /// Creates a database whose name is safe to embed in a SQL statement. The leading letter is not
    /// cosmetic: a bare GUID hex string starts with a digit about 60% of the time, and the lexer reads
    /// that as a number rather than an identifier — a test that names databases that way passes or
    /// fails depending on the GUID it happened to draw.
    /// </summary>
    private async Task<string> CreateExtraDatabaseAsync(CommandExecutor executor)
    {
        string dbname = "d" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);
        return dbname;
    }

    private static async Task CreateRobotsTableAsync(CommandExecutor executor, string dbname)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: TableName,
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));
        await database.Transactions.CommitAsync(txn);
    }

    private static async Task InsertRobotsAsync(CommandExecutor executor, string dbname, int count)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);

        KvTransaction txn = await database.Transactions.BeginAsync();
        for (int i = 0; i < count; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: TableName,
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "Robot" + i) },
                        { "year", new(ColumnType.Integer64, (long)(2000 + i)) },
                    }
                }));
        }
        await database.Transactions.CommitAsync(txn);
    }

    private static async Task<int> CountRobotsAsync(CommandExecutor executor, string dbname)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);

        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txn, dbname, $"SELECT * FROM {TableName}", null));

        int rows = 0;
        await foreach (QueryResultRow _ in cursor)
            rows++;

        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    // ── The happy path ───────────────────────────────────────────────────────────────────────

    /// <summary>
    /// An idle database is released, and reopening it is indistinguishable from a first open: the
    /// schema and the rows are all still there. Eviction that lost data — or that produced a
    /// descriptor which merely looked right — would show up here.
    /// </summary>
    [Test]
    public async Task AnIdleDatabaseIsReleasedAndReopensIntact()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);
        await InsertRobotsAsync(executor, dbname, 5);

        Assert.AreEqual(1, executor.OpenDatabaseCount, "Precondition: the database is open");

        int evicted = executor.EvictIdleDatabasesForTests(EvictImmediately);

        Assert.AreEqual(1, evicted, "The idle database must be released");
        Assert.AreEqual(0, executor.OpenDatabaseCount, "No descriptor should remain open");

        // Reopen through the ordinary path and read back through the real query pipeline.
        Assert.AreEqual(5, await CountRobotsAsync(executor, dbname),
            "A reopened database must return exactly what was written before it was released");
        Assert.AreEqual(1, executor.OpenDatabaseCount, "Reading reopens exactly one descriptor");
    }

    /// <summary>
    /// Repeated eviction and reopen must be stable — the second cycle exercises a descriptor built
    /// after a release, which is where a half-torn-down subscription or a disposed shared resource
    /// would surface.
    /// </summary>
    [Test]
    public async Task EvictAndReopenSurvivesRepetition()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);

        for (int cycle = 0; cycle < 3; cycle++)
        {
            await InsertRobotsAsync(executor, dbname, 2);

            Assert.AreEqual(1, executor.EvictIdleDatabasesForTests(EvictImmediately),
                $"Cycle {cycle}: the idle database must be released");

            Assert.AreEqual((cycle + 1) * 2, await CountRobotsAsync(executor, dbname),
                $"Cycle {cycle}: every row written so far must survive the release");

            Assert.AreEqual(1, executor.EvictIdleDatabasesForTests(EvictImmediately),
                $"Cycle {cycle}: the database reopened by the read must be releasable again");
        }
    }

    /// <summary>
    /// DDL still works against a database that was evicted between statements — the schema comes back
    /// from KV and the schema-apply subscription is re-established, rather than the reopened
    /// descriptor inheriting a torn-down one.
    /// </summary>
    [Test]
    public async Task DdlWorksAgainstAReopenedDatabase()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);

        Assert.AreEqual(1, executor.EvictIdleDatabasesForTests(EvictImmediately));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbname, sql: $"ALTER TABLE {TableName} ADD COLUMN nickname STRING", parameters: null));

        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        TableSchema schema = reopened.Schema.Tables[TableName];

        Assert.IsTrue(
            schema.Columns!.Exists(c => c.Name == "nickname"),
            "DDL issued after an eviction must land on the reopened descriptor's schema");

        // And it is durable, not just in memory.
        Assert.AreEqual(1, executor.EvictIdleDatabasesForTests(EvictImmediately));
        DatabaseDescriptor secondReopen = await executor.OpenDatabase(dbname);
        Assert.IsTrue(
            secondReopen.Schema.Tables[TableName].Columns!.Exists(c => c.Name == "nickname"),
            "The added column must survive a second release/reopen cycle");
    }

    // ── Everything that must be spared ───────────────────────────────────────────────────────

    /// <summary>
    /// A descriptor someone holds a use-reference on is spared. This is the reference count that every
    /// operation entry point takes, so it stands for "a statement is running right now".
    /// </summary>
    [Test]
    public async Task ADatabaseInUseIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);

        DatabaseEvictionOutcome outcome;
        using (database.Use())
        {
            outcome = executor.TryEvictDatabaseForTests(database.Id, EvictImmediately);
        }

        Assert.AreEqual(DatabaseEvictionOutcome.InUse, outcome, "A referenced descriptor must be refused");
        Assert.AreEqual(1, executor.OpenDatabaseCount, "The refused descriptor must still be open");

        // Once the reference is gone it becomes evictable — the refusal was about the reference, not a
        // descriptor that had been quietly broken by the attempt.
        Assert.AreEqual(
            DatabaseEvictionOutcome.Evicted, executor.TryEvictDatabaseForTests(database.Id, EvictImmediately),
            "Releasing the reference must make the descriptor evictable again");
    }

    /// <summary>
    /// A database with an open transaction is spared, and — the half that matters — that transaction
    /// then commits normally. An eviction that tore down the transactions manager underneath it would
    /// strand the transaction's locks; this asserts it did not.
    /// </summary>
    [Test]
    public async Task ADatabaseWithAnActiveTransactionIsRefusedAndTheTransactionStillCommits()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: txn,
            databaseName: dbname,
            tableName: TableName,
            values: new()
            {
                new()
                {
                    { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "Survivor") },
                    { "year", new(ColumnType.Integer64, 2026L) },
                }
            }));

        // Nothing holds a use-reference between the statements of an open transaction, so this is
        // exactly the case a reference count alone would get wrong.
        Assert.IsFalse(database.HasLiveUses, "Precondition: no use-reference is held between statements");

        DatabaseEvictionOutcome outcome = executor.TryEvictDatabaseForTests(database.Id, EvictImmediately);

        Assert.AreEqual(DatabaseEvictionOutcome.InUse, outcome,
            "A database with an active transaction must be refused");

        await database.Transactions.CommitAsync(txn);

        Assert.AreEqual(1, await CountRobotsAsync(executor, dbname),
            "The transaction that was protected from eviction must commit its row normally");
    }

    /// <summary>
    /// A transaction whose commit came back unresolved is spared too, and can then be re-committed.
    ///
    /// <para>When the coordinator answers "the outcome is not known yet", the transaction is left
    /// finalizing and tracked on purpose: the client is told to retry the <em>same</em> commit, so the
    /// engine must still be holding it when they come back. Nothing references it in the meantime,
    /// which makes it the longest-lived state in which a database looks idle while genuinely being in
    /// the middle of something. The status is set here directly because reproducing the coordinator's
    /// unresolved answer needs a fault-injected node; what eviction sees is identical either way.</para>
    /// </summary>
    [Test]
    public async Task ADatabaseWithAnUnresolvedFinalizeIsRefusedAndTheCommitCanStillBeRetried()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: txn,
            databaseName: dbname,
            tableName: TableName,
            values: new()
            {
                new()
                {
                    { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "Unresolved") },
                    { "year", new(ColumnType.Integer64, 2026L) },
                }
            }));

        txn.Status = KvTransactionStatus.Finalizing;

        Assert.IsFalse(database.HasLiveUses, "Precondition: a client awaiting a finalize retry holds no reference");

        Assert.AreEqual(
            DatabaseEvictionOutcome.InUse,
            executor.TryEvictDatabaseForTests(database.Id, EvictImmediately),
            "A database whose transaction is mid-finalize must be refused");

        // The half that matters: the retry the client was told to make still works.
        await database.Transactions.CommitAsync(txn);

        Assert.AreEqual(1, await CountRobotsAsync(executor, dbname),
            "The commit that was protected from eviction must resolve on retry");
    }

    /// <summary>
    /// A descriptor used more recently than the window is spared. This is the check that makes the
    /// primitive safe against the resolve-then-reference gap: a caller that has just resolved a
    /// descriptor leaves it looking freshly used, so a window-respecting sweep cannot reach it.
    /// </summary>
    [Test]
    public async Task ARecentlyUsedDatabaseIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);

        // Just resolved, so it is by definition not idle.
        await executor.OpenDatabase(dbname);

        Assert.AreEqual(
            DatabaseEvictionOutcome.NotIdle,
            executor.TryEvictDatabaseForTests(database.Id, idleWindowMs: 60_000),
            "A descriptor resolved moments ago must not be considered idle");
        Assert.AreEqual(1, executor.OpenDatabaseCount, "The refused descriptor must still be open");
    }

    /// <summary>
    /// The ancestor of an open branch is spared. A branch reads its ancestors' keyspaces through its
    /// own fork timestamps, so an ancestor is part of a live read path even though nothing holds a
    /// reference to its descriptor — which a reference count alone cannot see.
    /// </summary>
    [Test]
    public async Task AnAncestorOfAnOpenBranchIsRefused()
    {
        // Both names are embedded in the CREATE DATABASE … BRANCH FROM statement below, so both have
        // to be lexable as identifiers — hence the helper rather than the fixture's default naming.
        (_, _, CommandExecutor executor) = await CreateDatabase(Options);
        string rootName = await CreateExtraDatabaseAsync(executor);

        DatabaseDescriptor root = await executor.OpenDatabase(rootName);
        await CreateRobotsTableAsync(executor, rootName);
        await InsertRobotsAsync(executor, rootName, 3);

        string branchName = "b" + Guid.NewGuid().ToString("n");
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: $"CREATE DATABASE {branchName} BRANCH FROM {rootName}", parameters: null));
        TrackDatabase(branchName, executor);

        // Open the branch so it is part of this node's working set.
        await executor.OpenDatabase(branchName);

        Assert.AreEqual(
            DatabaseEvictionOutcome.BranchAncestor,
            executor.TryEvictDatabaseForTests(root.Id, EvictImmediately),
            "A database an open branch reads through must be refused");
    }

    // ── What eviction actually frees ─────────────────────────────────────────────────────────

    /// <summary>
    /// Eviction releases the per-database state that lives <em>outside</em> the descriptor. The
    /// statistics cache is a process-wide map keyed by table, so a descriptor released without telling
    /// it would move the memory rather than free it — the node would go on paying for every database
    /// it had ever served.
    /// </summary>
    [Test]
    public async Task EvictionReleasesCachedStatisticsForTheDatabase()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);
        await InsertRobotsAsync(executor, dbname, 5);

        Assert.Greater(executor.Statistics.CachedTableCount, 0,
            "Precondition: writing rows tracks statistics for the table");

        Assert.AreEqual(
            DatabaseEvictionOutcome.Evicted, executor.TryEvictDatabaseForTests(database.Id, EvictImmediately));

        Assert.AreEqual(0, executor.Statistics.CachedTableCount,
            "Releasing a database must release its tables' statistics entries too");

        // The statistics are rebuilt from the persisted blob on reopen rather than lost.
        Assert.AreEqual(5, await CountRobotsAsync(executor, dbname));
    }

    // ── Isolation between databases ──────────────────────────────────────────────────────────

    /// <summary>
    /// Evicting one database must not disturb another that is mid-DDL. They share a node and a
    /// statistics cache, so "released B" touching A's schema subscription or cache entries is a real
    /// failure mode rather than a hypothetical one.
    /// </summary>
    [Test]
    public async Task EvictingOneDatabaseDoesNotDisturbDdlOnAnother()
    {
        (string busy, _, CommandExecutor executor) = await CreateDatabase(Options);
        string idle = await CreateExtraDatabaseAsync(executor);

        await CreateRobotsTableAsync(executor, busy);
        await CreateRobotsTableAsync(executor, idle);

        // Run DDL on one database while the other is being released.
        Task ddl = executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: busy, sql: $"ALTER TABLE {TableName} ADD COLUMN extra STRING", parameters: null));

        DatabaseDescriptor idleDescriptor = await executor.OpenDatabase(idle);
        int evicted = executor.EvictIdleDatabasesForTests(EvictImmediately);

        await ddl;

        Assert.GreaterOrEqual(evicted, 0, "Eviction must not throw while another database runs DDL");

        DatabaseDescriptor busyDescriptor = await executor.OpenDatabase(busy);
        Assert.IsTrue(
            busyDescriptor.Schema.Tables[TableName].Columns!.Exists(c => c.Name == "extra"),
            "DDL on the untouched database must have completed normally");

        // And the other database is still usable, whether or not it happened to be released.
        Assert.AreEqual(0, await CountRobotsAsync(executor, idle),
            "The database targeted by eviction must still be readable afterwards");
        Assert.IsNotNull(idleDescriptor);
    }

    // ── The configured sweep ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Polls until <paramref name="condition"/> holds or the budget runs out, so a test can wait for a
    /// background sweep without pinning an exact tick. Returns whether it held.
    /// </summary>
    private static async Task<bool> WaitForAsync(Func<bool> condition, int timeoutMs = 10_000)
    {
        for (int waited = 0; waited < timeoutMs; waited += 50)
        {
            if (condition())
                return true;

            await Task.Delay(50);
        }

        return condition();
    }

    /// <summary>
    /// The background sweep releases an idle database on its own, with no test seam involved. This is
    /// what proves the loop is actually wired and reading its configured window — the primitive being
    /// correct says nothing about anything ever calling it.
    /// </summary>
    [Test]
    public async Task TheBackgroundSweepReleasesAnIdleDatabase()
    {
        CamusDBOptions options = Options with { DatabaseIdleEvictionMs = 300 };

        (string dbname, _, CommandExecutor executor) = await CreateDatabase(options);
        await CreateRobotsTableAsync(executor, dbname);
        await InsertRobotsAsync(executor, dbname, 3);

        Assert.IsTrue(
            await WaitForAsync(() => executor.OpenDatabaseCount == 0),
            "The configured sweep must release a database left idle past its window");

        // Still correct afterwards: the sweep released a descriptor, not the data.
        Assert.AreEqual(3, await CountRobotsAsync(executor, dbname),
            "A database released by the background sweep must reopen with its rows intact");
    }

    /// <summary>
    /// Eviction turned off keeps databases open indefinitely.
    ///
    /// <para>Two engines, deliberately: a component fixes its configuration when it is constructed, so
    /// the on and off arms cannot be the same engine reconfigured between phases — that reads as
    /// coverage while actually comparing a result against itself.</para>
    /// </summary>
    [Test]
    public async Task DisablingEvictionKeepsDatabasesOpenWhileEnablingReleasesThem()
    {
        (string keptName, _, CommandExecutor evictionOff) =
            await CreateDatabase(Options with { DatabaseIdleEvictionMs = 0 });

        (string releasedName, _, CommandExecutor evictionOn) =
            await CreateDatabase(Options with { DatabaseIdleEvictionMs = 300 });

        await CreateRobotsTableAsync(evictionOff, keptName);
        await CreateRobotsTableAsync(evictionOn, releasedName);

        Assert.IsTrue(
            await WaitForAsync(() => evictionOn.OpenDatabaseCount == 0),
            "Precondition: the engine with eviction enabled releases its idle database");

        Assert.AreEqual(
            1, evictionOff.OpenDatabaseCount,
            "An engine with idle eviction disabled must keep its database open");

        // And it is still open after another few windows' worth of time — disabled means disabled, not
        // merely slower.
        await Task.Delay(1_000);

        Assert.AreEqual(
            1, evictionOff.OpenDatabaseCount,
            "A disabled sweep must never release the descriptor, however long it sits idle");
    }

    /// <summary>
    /// Renaming a database and then releasing its descriptor must leave the database reachable under
    /// its new name with its rows intact. Rename deliberately keeps the descriptor alive and rewrites
    /// its display name in place, so it is the one path where an evicted-and-rebuilt descriptor could
    /// disagree with the registry about what the database is called.
    /// </summary>
    [Test]
    public async Task ARenamedDatabaseSurvivesEvictionAndReopensUnderTheNewName()
    {
        (_, _, CommandExecutor executor) = await CreateDatabase(Options);
        string original = await CreateExtraDatabaseAsync(executor);

        await CreateRobotsTableAsync(executor, original);
        await InsertRobotsAsync(executor, original, 4);

        string renamed = "d" + Guid.NewGuid().ToString("n");
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: "", sql: $"ALTER DATABASE {original} RENAME TO {renamed}", parameters: null));
        TrackDatabase(renamed, executor);

        DatabaseDescriptor renamedDescriptor = await executor.OpenDatabase(renamed);

        Assert.AreEqual(
            DatabaseEvictionOutcome.NotIdle,
            executor.TryEvictDatabaseForTests(renamedDescriptor.Id, idleWindowMs: 60_000),
            "A database renamed moments ago must not look idle");

        Assert.AreEqual(
            DatabaseEvictionOutcome.Evicted,
            executor.TryEvictDatabaseForTests(renamedDescriptor.Id, EvictImmediately),
            "Once the window is satisfied the renamed database is an ordinary eviction candidate");

        Assert.AreEqual(4, await CountRobotsAsync(executor, renamed),
            "The renamed database must reopen under its new name with its rows intact");
    }

    /// <summary>
    /// A database being dropped belongs to the drop path, which has its own quiesce-and-drain
    /// protocol. Eviction must stay out of it: two owners disposing one descriptor is how a drop ends
    /// up completing against state that has already been torn down.
    /// </summary>
    [Test]
    public async Task ADroppedDatabaseIsLeftToTheDropPath()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options);
        await CreateRobotsTableAsync(executor, dbname);

        await executor.DropDatabase(new DropDatabaseTicket(dbname, ifExists: false));

        // Whatever the drop left behind, eviction must never report that it released it.
        DatabaseEvictionOutcome outcome = executor.TryEvictDatabaseForTests(database.Id, EvictImmediately);

        Assert.AreNotEqual(DatabaseEvictionOutcome.Evicted, outcome,
            "Eviction must not take ownership of a descriptor the drop path is responsible for");
    }
}
