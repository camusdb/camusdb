
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

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Covers the HTTP-facing locking-mode surface: the <c>locking</c> request field flows through
/// <see cref="HttpTransactionCoordinator.StartAsync"/> into the begun transaction, and the string
/// field is parsed and validated consistently with the other transaction-shape fields. These tests
/// exercise the client-facing seam (coordinator + request parsing) rather than the core
/// <c>BeginAsync(locking:)</c> path, which the executor-level optimistic tests already cover.
/// </summary>
[TestFixture]
[NonParallelizable]
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
            KeyValueTransactionLocking.Optimistic, CancellationToken.None);

        Assert.That(tx.Locking, Is.EqualTo(KeyValueTransactionLocking.Optimistic));

        await coord.RollbackAsync(tx, CancellationToken.None);
    }

    [Test]
    public async Task StartAsync_ExplicitPessimistic_ThreadsIntoTransaction()
    {
        (string dbname, DatabaseDescriptor db, HttpTransactionCoordinator coord, CommandExecutor executor) = await SetupAsync();

        KvTransaction tx = await coord.StartAsync(
            dbname, CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            KeyValueTransactionLocking.Pessimistic, CancellationToken.None);

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

        Assert.That(tx.Locking, Is.EqualTo(CamusDBConfig.DefaultTransactionLocking));

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
            KeyValueTransactionLocking.Optimistic, CancellationToken.None);
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
                KeyValueTransactionLocking.Optimistic, CancellationToken.None);
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
        (_, _, KeyValueTransactionLocking? opt) = LockingParseProbe.Parse(new ExecuteSQLRequest { Locking = "optimistic" });
        Assert.That(opt, Is.EqualTo(KeyValueTransactionLocking.Optimistic));

        (_, _, KeyValueTransactionLocking? pess) = LockingParseProbe.Parse(new ExecuteSQLRequest { Locking = "Pessimistic" });
        Assert.That(pess, Is.EqualTo(KeyValueTransactionLocking.Pessimistic));
    }

    [Test]
    public void ParseRequestLevelMode_NullFields_ResolveToNull()
    {
        (CamusIsolationLevel? level, CamusTransactionMode? mode, KeyValueTransactionLocking? locking) =
            LockingParseProbe.Parse(new ExecuteSQLRequest());

        Assert.That(level, Is.Null);
        Assert.That(mode, Is.Null);
        Assert.That(locking, Is.Null);
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

    /// <summary>
    /// Test-only accessor for the <c>protected static</c> <see cref="CommandsController.ParseRequestLevelMode"/>
    /// helper. It is never instantiated — only its static parse pass-through is used.
    /// </summary>
    private sealed class LockingParseProbe : CommandsController
    {
        private LockingParseProbe() : base(null!, null!, null!) { }

        public static (CamusIsolationLevel? level, CamusTransactionMode? mode, KeyValueTransactionLocking? locking) Parse(ExecuteSQLRequest request)
            => ParseRequestLevelMode(request);
    }
}
