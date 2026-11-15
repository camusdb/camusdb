/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The privileges of <c>UPDATE … FROM</c> and <c>DELETE … USING</c>: UPDATE or DELETE on the target,
/// and SELECT on every relation the statement reads, the target included, as PostgreSQL asks SELECT on
/// the columns a statement reads. Without SELECT on a source table, the statement would be a way to read
/// it: which rows change depends on its rows.
/// </summary>
[NonParallelizable]
internal sealed class TestJoinedDmlPrivileges : SharedNodeBaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => ForeignKeyPrivilegeScenarios.WithAuthentication(defaults);

    private const string UpdateSql = "UPDATE accounts a SET balance = a.balance + d.amount FROM deltas d WHERE d.account_id = a.id";

    private const string DeleteSql = "DELETE FROM accounts a USING deltas d WHERE d.account_id = a.id";

    /// <summary>Creates the database and its tables as root, and a user u with the given grants on it.</summary>
    private async Task<(string db, CommandExecutor ex, Principal user)> Setup(params string[] grants)
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "joinauth" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await Run(ex, db, "CREATE TABLE accounts (id INT64 PRIMARY KEY, balance INT64)", root, ddl: true);
        await Run(ex, db, "CREATE TABLE deltas (did INT64 PRIMARY KEY, account_id INT64, amount INT64)", root, ddl: true);
        await Run(ex, db, "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 200)", root, ddl: false);
        await Run(ex, db, "INSERT INTO deltas (did, account_id, amount) VALUES (10, 1, 5)", root, ddl: false);

        // The shared node keeps its users across the tests of the fixture.
        await ServerDdl(ex, "DROP USER IF EXISTS u", root);
        await ServerDdl(ex, "CREATE USER u IDENTIFIED BY 'pw'", root);
        foreach (string grant in grants)
            await ServerDdl(ex, string.Format(grant, db), root);

        return (db, ex, await Login(ex, "u", "pw"));
    }

    private static async Task<Principal> Login(CommandExecutor ex, string u, string p)
        => await ex.ResolvePrincipalAsync((await ex.LoginAsync(u, p)).Token);

    private static Task ServerDdl(CommandExecutor ex, string sql, Principal? p)
        => ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: p));

    private static async Task<int> Run(CommandExecutor ex, string db, string sql, Principal? p, bool ddl)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();
        try
        {
            int changed = 0;
            if (ddl)
                await ex.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null, p));
            else
                changed = (await ex.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null, p))).ModifiedRows;

            await d.Transactions.CommitAsync(tx);
            return changed;
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static void AssertRefused(Func<Task> action)
    {
        CamusDBException error = Assert.ThrowsAsync<CamusDBException>(async () => await action())!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, error.Code, error.Message);
    }

    [Test]
    public async Task UpdateFrom_WithUpdateOnTheTargetAndSelectOnBothTablesIsAllowed()
    {
        (string db, CommandExecutor ex, Principal user) = await Setup(
            "GRANT UPDATE ON {0}.accounts TO u", "GRANT SELECT ON {0}.accounts TO u", "GRANT SELECT ON {0}.deltas TO u");

        Assert.AreEqual(1, await Run(ex, db, UpdateSql, user, ddl: false));
    }

    [Test]
    public async Task UpdateFrom_WithoutSelectOnTheSourceIsRefused()
    {
        (string db, CommandExecutor ex, Principal user) = await Setup("GRANT UPDATE ON {0}.accounts TO u", "GRANT SELECT ON {0}.accounts TO u");

        AssertRefused(() => Run(ex, db, UpdateSql, user, ddl: false));
    }

    [Test]
    public async Task UpdateFrom_WithoutSelectOnTheTargetIsRefused()
    {
        (string db, CommandExecutor ex, Principal user) = await Setup("GRANT UPDATE ON {0}.accounts TO u", "GRANT SELECT ON {0}.deltas TO u");

        AssertRefused(() => Run(ex, db, UpdateSql, user, ddl: false));
    }

    [Test]
    public async Task UpdateFrom_WithoutUpdateOnTheTargetIsRefused()
    {
        (string db, CommandExecutor ex, Principal user) = await Setup("GRANT SELECT ON {0}.accounts TO u", "GRANT SELECT ON {0}.deltas TO u");

        AssertRefused(() => Run(ex, db, UpdateSql, user, ddl: false));
    }

    [Test]
    public async Task DeleteUsing_WithDeleteOnTheTargetAndSelectOnBothTablesIsAllowed()
    {
        (string db, CommandExecutor ex, Principal user) = await Setup(
            "GRANT DELETE ON {0}.accounts TO u", "GRANT SELECT ON {0}.accounts TO u", "GRANT SELECT ON {0}.deltas TO u");

        Assert.AreEqual(1, await Run(ex, db, DeleteSql, user, ddl: false));
    }

    [Test]
    public async Task DeleteUsing_WithoutSelectOnTheSourceIsRefused()
    {
        (string db, CommandExecutor ex, Principal user) = await Setup("GRANT DELETE ON {0}.accounts TO u", "GRANT SELECT ON {0}.accounts TO u");

        AssertRefused(() => Run(ex, db, DeleteSql, user, ddl: false));
    }
}
