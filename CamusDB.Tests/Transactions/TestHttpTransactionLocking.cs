
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;

using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Covers the HTTP-facing locking-mode surface: the <c>locking</c> request field flows through
/// <see cref="HttpTransactionCoordinator.StartAsync"/> into the begun transaction, and the string
/// field is parsed and validated consistently with the other transaction-shape fields. These tests
/// exercise the client-facing seam (coordinator + request parsing) rather than the core
/// <c>BeginAsync(locking:)</c> path, which the executor-level optimistic tests already cover.
/// </summary>
[TestFixture]
public sealed class TestHttpTransactionLocking : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        HttpTransactionCoordinator coord = new(executor);
        return (dbname, db, coord, executor);
    }

    // ── Coordinator threading: the locking argument reaches the begun transaction ──────────────

    [Test]
    public async Task StartAsync_Optimistic_ThreadsIntoTransaction()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await coord.StartAsync(
            dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            KeyValueTransactionLocking.Optimistic, cancellationToken: CancellationToken.None);

        Assert.That(tx.Locking, Is.EqualTo(KeyValueTransactionLocking.Optimistic));

        await coord.RollbackAsync(tx, CancellationToken.None);
    }

    [Test]
    public async Task StartAsync_ExplicitPessimistic_ThreadsIntoTransaction()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await coord.StartAsync(
            dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            KeyValueTransactionLocking.Pessimistic, cancellationToken: CancellationToken.None);

        Assert.That(tx.Locking, Is.EqualTo(KeyValueTransactionLocking.Pessimistic));

        await coord.RollbackAsync(tx, CancellationToken.None);
    }

    [Test]
    public async Task StartAsync_NoLocking_UsesServerDefault()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor executor) = await SetupAsync();

        // locking omitted (null) → the resolved mode is the configured server default.
        KvTransaction tx = await coord.StartAsync(
            dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            locking: null, cancellationToken: CancellationToken.None);

        Assert.That(tx.Locking, Is.EqualTo(CamusDBOptions.Default.DefaultTransactionLocking));

        await coord.RollbackAsync(tx, CancellationToken.None);
    }

    // ── End-to-end: an optimistic transaction begun via the coordinator actually functions ─────

    [Test]
    public async Task OptimisticTransaction_ViaCoordinator_CommitsAndIsReadable()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor executor) = await SetupAsync();

        await executor.ExecuteDDLSQL(new(
            await db.Transactions.BeginAsync(), dbname,
            "CREATE TABLE cities_http (id STRING NOT NULL PRIMARY KEY, name STRING NOT NULL)", null));

        KvTransaction tx = await coord.StartAsync(
            dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            KeyValueTransactionLocking.Optimistic, cancellationToken: CancellationToken.None);
        await executor.ExecuteNonSQLQuery(new(tx, dbname,
            "INSERT INTO cities_http (id, name) VALUES (\"lis\", \"Lisbon\")", null));
        await coord.CommitAsync(db, tx, CancellationToken.None);

        KvTransaction txq = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(txq, dbname, "SELECT id, name FROM cities_http", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(txq);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("lis", rows[0].Row["id"].StrValue);
        Assert.AreEqual("Lisbon", rows[0].Row["name"].StrValue);
    }

    [Test]
    public async Task OptimisticConcurrentDuplicate_ViaCoordinator_OnlyOneCommits()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor executor) = await SetupAsync();

        await executor.ExecuteDDLSQL(new(
            await db.Transactions.BeginAsync(), dbname,
            "CREATE TABLE ports_http (id STRING NOT NULL PRIMARY KEY, name STRING NOT NULL)", null));

        const string sql = "INSERT INTO ports_http (id, name) VALUES (\"rot\", \"Rotterdam\")";

        async Task<bool> TryInsert()
        {
            KvTransaction tx = await coord.StartAsync(
                dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
                KeyValueTransactionLocking.Optimistic, cancellationToken: CancellationToken.None);
            try
            {
                await executor.ExecuteNonSQLQuery(new(tx, dbname, sql, null));
                await coord.CommitAsync(db, tx, CancellationToken.None);
                return true;
            }
            catch (CamusDBException)
            {
                await coord.RollbackIfNotCompletedAsync(tx, CancellationToken.None);
                return false;
            }
        }

        bool[] results = await Task.WhenAll(TryInsert(), TryInsert());

        Assert.AreEqual(1, results.Count(ok => ok),
            "exactly one concurrent optimistic insert of the same key must commit");
    }

    // ── Request-field parsing: valid values parse, bad values are rejected consistently ────────

    [Test]
    public void ParseRequestLevelMode_ParsesLockingCaseInsensitively()
    {
        (_, _, KeyValueTransactionLocking? opt, _) = LockingParseProbe.Parse(new ExecuteSQLRequest { Locking = "optimistic" });
        Assert.That(opt, Is.EqualTo(KeyValueTransactionLocking.Optimistic));

        (_, _, KeyValueTransactionLocking? pess, _) = LockingParseProbe.Parse(new ExecuteSQLRequest { Locking = "Pessimistic" });
        Assert.That(pess, Is.EqualTo(KeyValueTransactionLocking.Pessimistic));
    }

    [Test]
    public void ParseRequestLevelMode_NullFields_ResolveToNull()
    {
        (CamusIsolationLevel? level, CamusTransactionMode? mode, KeyValueTransactionLocking? locking, TransactionPriority? priority) =
            LockingParseProbe.Parse(new ExecuteSQLRequest());

        Assert.That(level, Is.Null);
        Assert.That(mode, Is.Null);
        Assert.That(locking, Is.Null);
        Assert.That(priority, Is.Null);
    }

    [Test]
    public void ParseRequestLevelMode_UnknownLocking_Throws()
        => AssertInvalidInput(new ExecuteSQLRequest { Locking = "eventual" });

    // The isolation/mode fields validate identically to locking (a typo is a 400, not a silent
    // fallback to the default) — this is the consistency the shared parse helper guarantees.
    [Test]
    public void ParseRequestLevelMode_UnknownIsolation_Throws()
        => AssertInvalidInput(new ExecuteSQLRequest { IsolationLevel = "snapshot" });

    [Test]
    public void ParseRequestLevelMode_UnknownMode_Throws()
        => AssertInvalidInput(new ExecuteSQLRequest { TransactionMode = "writeonly" });

    // Enum.TryParse also accepts out-of-range numeric strings; the IsDefined guard rejects them.
    [Test]
    public void ParseRequestLevelMode_NumericLocking_Throws()
        => AssertInvalidInput(new ExecuteSQLRequest { Locking = "5" });

    private static void AssertInvalidInput(ExecuteSQLRequest request)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => LockingParseProbe.Parse(request))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
    }

    // ── Deferred coordinator start (the /start-transaction path) ───────────────────────────────

    [Test]
    public async Task DeferredStart_SessionOpensOnFirstWrite_CommitsAndReadable()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor executor) = await SetupAsync();

        await executor.ExecuteDDLSQL(new(
            await db.Transactions.BeginAsync(), dbname,
            "CREATE TABLE deferred_rows (id STRING NOT NULL PRIMARY KEY, v STRING NOT NULL)", null));

        KvTransaction tx = await coord.StartAsync(
            dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            KeyValueTransactionLocking.Optimistic, deferStart: true);

        // Deferred: no Kahuna session yet, but a stable client id is assigned for tracking.
        Assert.That(tx.TransactionId, Is.EqualTo(HLCTimestamp.Zero), "session must not start at begin");
        Assert.That(tx.ClientId, Is.Not.EqualTo(HLCTimestamp.Zero), "a stable client id must be assigned");

        await executor.ExecuteNonSQLQuery(new(tx, dbname,
            "INSERT INTO deferred_rows (id, v) VALUES (\"a\", \"1\")", null));

        Assert.That(tx.TransactionId, Is.Not.EqualTo(HLCTimestamp.Zero), "first write must open the session");
        await coord.CommitAsync(db, tx, CancellationToken.None);

        KvTransaction q = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new(q, dbname, "SELECT v FROM deferred_rows", null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(q);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("1", rows[0].Row["v"].StrValue);
    }

    [Test]
    public async Task SetTransactionLocking_OnDeferredTx_ConfiguresBeforeSessionStart()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor executor) = await SetupAsync();

        // Begin with the default (pessimistic) and no explicit selection.
        KvTransaction tx = await coord.StartAsync(
            dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            locking: null, deferStart: true);
        Assert.That(tx.Locking, Is.EqualTo(CamusDBOptions.Default.DefaultTransactionLocking));

        // SET TRANSACTION LOCKING runs before any data statement and before the session opens.
        (_, IAsyncEnumerable<QueryResultRow> c) =
            await executor.ExecuteSQLQuery(new(tx, dbname, "SET TRANSACTION LOCKING OPTIMISTIC", null));
        await c.ToListAsync();

        Assert.That(tx.Locking, Is.EqualTo(KeyValueTransactionLocking.Optimistic), "SET must reconfigure locking");
        Assert.That(tx.TransactionId, Is.EqualTo(HLCTimestamp.Zero), "SET TRANSACTION LOCKING must not start the session");

        await coord.RollbackAsync(tx, CancellationToken.None);
    }

    [Test]
    public async Task SetTransactionLocking_ViaNonQueryEndpoint_Applies()
    {
        // A client routes a no-rows statement like SET TRANSACTION LOCKING to the non-query endpoint
        // (ExecuteNonSQLQuery), not the row-returning one. That path previously threw
        // "Unknown non-query AST stmt: SetTransactionLocking" because the handler existed only in
        // ExecuteSQLQuery. Both SET variants must apply identically regardless of endpoint.
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await coord.StartAsync(
            dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            locking: null, deferStart: true);
        Assert.That(tx.Locking, Is.EqualTo(CamusDBOptions.Default.DefaultTransactionLocking));

        // SET ISOLATION then SET LOCKING, both through the non-query endpoint, both before any data
        // statement — must not throw and must reconfigure the in-flight transaction.
        await executor.ExecuteNonSQLQuery(new(tx, dbname, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", null));
        await executor.ExecuteNonSQLQuery(new(tx, dbname, "SET TRANSACTION LOCKING OPTIMISTIC", null));

        Assert.That(tx.IsolationLevel, Is.EqualTo(CamusIsolationLevel.Serializable), "SET ISOLATION via non-query applied");
        Assert.That(tx.Locking, Is.EqualTo(KeyValueTransactionLocking.Optimistic), "SET LOCKING via non-query applied");
        Assert.That(tx.TransactionId, Is.EqualTo(HLCTimestamp.Zero), "neither SET starts the session");

        await coord.RollbackAsync(tx, CancellationToken.None);
    }

    [Test]
    public async Task DeferredOptimistic_ReadThenWriteConflict_AbortsAtCommit()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor executor) = await SetupAsync();

        await executor.ExecuteDDLSQL(new(
            await db.Transactions.BeginAsync(), dbname,
            "CREATE TABLE deferred_accounts (id STRING NOT NULL PRIMARY KEY, balance STRING NOT NULL)", null));

        KvTransaction seed = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(seed, dbname,
            "INSERT INTO deferred_accounts (id, balance) VALUES (\"a\", \"100\")", null));
        await db.Transactions.CommitAsync(seed);

        // T1: deferred optimistic. Its first SELECT is a scan, and it must open the session even
        // though a scan no longer folds its reads — the session has to exist before any later
        // statement, and this is the path that silently skipped ensure-start before.
        KvTransaction t1 = await coord.StartAsync(
            dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            KeyValueTransactionLocking.Optimistic, deferStart: true);
        (_, IAsyncEnumerable<QueryResultRow> t1scan) =
            await executor.ExecuteSQLQuery(new(t1, dbname, "SELECT id, balance FROM deferred_accounts", null));
        Assert.AreEqual("100", (await t1scan.ToListAsync()).Single().Row["balance"].StrValue);
        Assert.That(t1.TransactionId, Is.Not.EqualTo(HLCTimestamp.Zero), "a scan must open the deferred session");

        // The folded observation comes from a point read: those still register a read dependency,
        // which is what the commit-time validation below exercises.
        (_, IAsyncEnumerable<QueryResultRow> t1cursor) =
            await executor.ExecuteSQLQuery(new(t1, dbname, "SELECT id, balance FROM deferred_accounts WHERE id = \"a\"", null));
        List<QueryResultRow> t1rows = await t1cursor.ToListAsync();
        Assert.AreEqual("100", t1rows.Single().Row["balance"].StrValue);

        // A concurrent transaction modifies the row T1 read, and commits.
        KvTransaction t2 = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new(t2, dbname,
            "UPDATE deferred_accounts SET balance = \"200\" WHERE id = \"a\"", null));
        await db.Transactions.CommitAsync(t2);

        // T1 writes a disjoint key then commits: with its read folded, commit-time validation sees the
        // stale observation and aborts (read-write / write-skew).
        await executor.ExecuteNonSQLQuery(new(t1, dbname,
            "INSERT INTO deferred_accounts (id, balance) VALUES (\"b\", \"1\")", null));

        Assert.ThrowsAsync<CamusDBException>(
            async () => await coord.CommitAsync(db, t1, CancellationToken.None),
            "a deferred optimistic transaction whose folded read was invalidated must abort at commit");
    }


    [Test]
    public async Task SetIsolationAndLocking_BothApplyBeforeSessionStart()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await coord.StartAsync(
            dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, deferStart: true);

        // Two SET TRANSACTION statements, in either order, both before any data statement.
        (_, IAsyncEnumerable<QueryResultRow> a) =
            await executor.ExecuteSQLQuery(new(tx, dbname, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", null));
        await a.ToListAsync();
        (_, IAsyncEnumerable<QueryResultRow> b) =
            await executor.ExecuteSQLQuery(new(tx, dbname, "SET TRANSACTION LOCKING OPTIMISTIC", null));
        await b.ToListAsync();

        Assert.That(tx.IsolationLevel, Is.EqualTo(CamusIsolationLevel.Serializable), "SET ISOLATION applied");
        Assert.That(tx.Locking, Is.EqualTo(KeyValueTransactionLocking.Optimistic), "SET LOCKING applied");
        Assert.That(tx.TransactionId, Is.EqualTo(HLCTimestamp.Zero), "neither SET statement starts the session");

        await coord.RollbackAsync(tx, CancellationToken.None);
    }

    [Test]
    public async Task DeferredTx_CommittedWithoutStatement_FinalizesCleanly()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor _) = await SetupAsync();

        // A client that opens a transaction and commits without running any statement never opens the
        // Kahuna session (TransactionId stays Zero). It must still be finalized, not left Active in the
        // manager's active-transaction list.
        KvTransaction tx = await coord.StartAsync(
            dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, deferStart: true);
        Assert.That(tx.TransactionId, Is.EqualTo(HLCTimestamp.Zero));
        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Active));

        await coord.CommitAsync(db, tx, CancellationToken.None);

        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.Committed),
            "an empty deferred transaction must finalize on commit, not linger as Active");
    }

    [Test]
    public async Task DeferredTx_RolledBackWithoutStatement_FinalizesCleanly()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor _) = await SetupAsync();

        KvTransaction tx = await coord.StartAsync(
            dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, deferStart: true);

        await coord.RollbackAsync(tx, CancellationToken.None);

        Assert.That(tx.Status, Is.EqualTo(KvTransactionStatus.RolledBack),
            "an empty deferred transaction must finalize on rollback, not linger as Active");
    }

    /// <summary>
    /// Test-only accessor for the <c>protected static</c> <see cref="CommandsController.ParseRequestLevelMode"/>
    /// helper. It is never instantiated — only its static parse pass-through is used.
    /// </summary>
    private sealed class LockingParseProbe : CommandsController
    {
        private LockingParseProbe() : base(null!, null!, null!, CamusDBOptions.Default) { }

        public static (CamusIsolationLevel? level, CamusTransactionMode? mode, KeyValueTransactionLocking? locking, TransactionPriority? priority) Parse(ExecuteSQLRequest request)
            => ParseRequestLevelMode(request);
    }
}
