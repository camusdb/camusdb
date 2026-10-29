/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>SELECT … FOR UPDATE</c> takes the row lock an UPDATE takes, so it needs UPDATE on the table in
/// addition to SELECT, as in PostgreSQL. The SELECT privilege rides the ambient statement scope and the
/// statement path checks UPDATE itself, so the test where the two arrive in separate GRANT statements is
/// the one that proves the pair is checked one privilege at a time.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test.
[NonParallelizable]
internal sealed class TestSelectForUpdateAuthorization : BaseTest
{
    private const string LockOne = "SELECT id FROM t WHERE id = 1 FOR UPDATE";

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-key-padded-to-meet-the-32-byte-secret-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    private static async Task<Principal> Login(CommandExecutor ex, string u, string p)
        => await ex.ResolvePrincipalAsync((await ex.LoginAsync(u, p)).Token);

    private static Task ServerDdl(CommandExecutor ex, string sql, Principal? p)
        => ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: p));

    private static async Task TxnDdl(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        await ex.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null, p));
        await d.Transactions.CommitAsync(tx);
    }

    private static Task<KvTransaction> BeginAsync(DatabaseDescriptor d)
        => d.Transactions.BeginAsync(CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite,
            locking: KeyValueTransactionLocking.Pessimistic);

    /// <summary>Runs <paramref name="sql"/> in its own read-write transaction and returns the row count.</summary>
    private static async Task<int> QueryCount(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await BeginAsync(d);
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
            int count = 0;
            await foreach (QueryResultRow _ in cursor)
                count++;
            await d.Transactions.CommitAsync(tx);
            return count;
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static void AssertDenied(Func<Task> act)
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () => await act())!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, e.Code, e.Message);
    }

    /// <summary>A database with table <c>t</c> holding row 1, and a user <c>u</c> with no grant on it.</summary>
    private async Task<(string db, CommandExecutor ex, Principal root)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "authdb" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE t (id int64 PRIMARY KEY NOT NULL, v int64 NULL)", root);

        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        await ex.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO t (id, v) VALUES (1, 10)", null, root));
        await d.Transactions.CommitAsync(tx);

        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        return (db, ex, root);
    }

    [Test]
    public async Task Superuser_MayLock()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();

        Assert.AreEqual(1, await QueryCount(ex, db, LockOne, root));
    }

    [Test]
    public async Task SelectOnly_MayReadButNotLock()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t TO u", root);
        Principal user = await Login(ex, "u", "pw");

        Assert.AreEqual(1, await QueryCount(ex, db, "SELECT id FROM t WHERE id = 1", user));
        AssertDenied(() => QueryCount(ex, db, LockOne, user));
    }

    [Test]
    public async Task UpdateOnly_IsRefused()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT UPDATE ON {db}.t TO u", root);
        Principal user = await Login(ex, "u", "pw");

        AssertDenied(() => QueryCount(ex, db, LockOne, user));
    }

    [Test]
    public async Task SelectAndUpdateInOneGrant_MayLock()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT SELECT, UPDATE ON {db}.t TO u", root);
        Principal user = await Login(ex, "u", "pw");

        Assert.AreEqual(1, await QueryCount(ex, db, LockOne, user));
    }

    [Test]
    public async Task SelectAndUpdateInSeparateGrants_MayLock()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t TO u", root);
        await ServerDdl(ex, $"GRANT UPDATE ON {db}.t TO u", root);
        Principal user = await Login(ex, "u", "pw");

        Assert.AreEqual(1, await QueryCount(ex, db, LockOne, user));
    }

    /// <summary>
    /// As in PostgreSQL, EXPLAIN of a locking read needs what the statement needs, also in its plain
    /// form, which takes no lock. A plain EXPLAIN without the clause still needs SELECT only.
    /// </summary>
    [Test]
    public async Task SelectOnly_MayNotExplainALockingRead()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t TO u", root);
        Principal user = await Login(ex, "u", "pw");

        Assert.That(await QueryCount(ex, db, "EXPLAIN SELECT id FROM t WHERE id = 1", user), Is.GreaterThan(0));
        AssertDenied(() => QueryCount(ex, db, "EXPLAIN " + LockOne, user));
        AssertDenied(() => QueryCount(ex, db, "EXPLAIN (ANALYZE) " + LockOne, user));
    }

    [Test]
    public async Task SelectAndUpdate_MayExplainALockingRead()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT SELECT, UPDATE ON {db}.t TO u", root);
        Principal user = await Login(ex, "u", "pw");

        Assert.That(await QueryCount(ex, db, "EXPLAIN " + LockOne, user), Is.GreaterThan(0));
        Assert.That(await QueryCount(ex, db, "EXPLAIN (ANALYZE) " + LockOne, user), Is.GreaterThan(0));
    }

    [Test]
    public async Task ARefusedLock_LeavesTheRowFree()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, $"GRANT SELECT ON {db}.t TO u", root);
        Principal user = await Login(ex, "u", "pw");

        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction refused = await BeginAsync(d);
        AssertDenied(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await ex.ExecuteSQLQuery(new ExecuteSQLTicket(refused, db, LockOne, null, user));
            await foreach (QueryResultRow _ in cursor) { }
        });

        // The refusal comes before the lock step, so another transaction can lock the row at once
        // while the refused transaction is still open.
        Assert.AreEqual(1, await QueryCount(ex, db, "SELECT id FROM t WHERE id = 1 FOR UPDATE NOWAIT", root));
        await d.Transactions.RollbackIfNotCompletedAsync(refused);
    }
}
