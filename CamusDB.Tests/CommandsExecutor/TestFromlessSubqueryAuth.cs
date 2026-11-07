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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The one-row source is not a relation that a user can hold a grant on, so a SELECT with no FROM as a
/// subquery needs no privilege. A table that the same subquery reads still needs its grant, and a view
/// body with no FROM runs for a caller who holds only the grant on the view.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestFromlessSubqueryAuth : BaseTest
{
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

    private static async Task<int> Query(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p));
            return (await cursor.ToListAsync()).Count;
        }
        finally
        {
            await d.Transactions.RollbackAsync(tx);
        }
    }

    private async Task<(string db, CommandExecutor ex, Principal root, Principal user)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "fromlessauth" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await TxnDdl(ex, db, "CREATE TABLE secret (id int64 PRIMARY KEY NOT NULL, v int64 NULL)", root);
        await TxnDdl(ex, db, "CREATE TABLE open_t (id int64 PRIMARY KEY NOT NULL, v int64 NULL)", root);
        await TxnDdl(ex, db, "CREATE VIEW one_row AS SELECT 5 AS x", root);
        await TxnDdl(ex, db, "CREATE VIEW over_secret AS SELECT (SELECT COUNT(*) FROM secret) AS n", root);

        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.open_t TO u", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.one_row TO u", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.over_secret TO u", root);

        return (db, ex, root, await Login(ex, "u", "pw"));
    }

    [Test]
    public async Task ASubqueryWithNoFrom_NeedsNoPrivilege()
    {
        (string db, CommandExecutor ex, _, Principal user) = await Setup();

        Assert.That(await Query(ex, db, "SELECT (SELECT 1)", user), Is.EqualTo(1));
        Assert.That(await Query(ex, db, "SELECT x FROM (SELECT 5 AS x) d", user), Is.EqualTo(1));
        Assert.That(await Query(ex, db, "SELECT id FROM open_t WHERE id IN (SELECT 1)", user), Is.EqualTo(0));
    }

    [Test]
    public async Task ATableInsideTheSubquery_StillNeedsItsGrant()
    {
        (string db, CommandExecutor ex, _, Principal user) = await Setup();

        foreach (string sql in new[]
        {
            "SELECT (SELECT 1 WHERE EXISTS (SELECT 1 FROM secret))",
            "SELECT x FROM (SELECT (SELECT COUNT(*) FROM secret) AS x) d",
            "SELECT id FROM open_t WHERE id IN (SELECT 1 WHERE EXISTS (SELECT 1 FROM secret))",
        })
        {
            CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () => await Query(ex, db, sql, user))!;
            Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.InsufficientPrivilege), sql);
        }
    }

    [Test]
    public async Task AViewBodyWithNoFrom_RunsWithTheGrantOnTheView()
    {
        (string db, CommandExecutor ex, _, Principal user) = await Setup();

        Assert.That(await Query(ex, db, "SELECT x FROM one_row", user), Is.EqualTo(1));
        Assert.That(await Query(ex, db, "SELECT n FROM over_secret", user), Is.EqualTo(1));
    }
}
