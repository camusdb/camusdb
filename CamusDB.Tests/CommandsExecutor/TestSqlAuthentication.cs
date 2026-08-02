/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Auth;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for SQL authentication DDL (<c>CREATE/ALTER/DROP USER</c>, <c>GRANT</c>,
/// <c>REVOKE</c>, <c>SHOW GRANTS</c>), driven through the real parser → <see cref="CommandExecutor"/>
/// path. Phase 1 is catalog + DDL only: statements execute and persist; there is no privilege
/// enforcement yet.
/// </summary>
[TestFixture]
internal sealed class TestSqlAuthentication : BaseTest
{
    // Database names are referenced by name inside GRANT SQL, so they must be valid bare identifiers
    // (letter/underscore first). BaseTest.CreateDatabase() uses a raw GUID that can start with a digit,
    // so create with a letter-prefixed name instead.
    private async Task<(string dbname, CommandExecutor executor)> CreateAuthDatabase()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string dbname = "authdb" + Guid.NewGuid().ToString("n");
        await executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);
        return (dbname, executor);
    }

    // User/grant DDL is server-level: no database context, no transaction.
    private static async Task ExecuteServerDdl(CommandExecutor executor, string sql)
    {
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null));
    }

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ShowGrants(CommandExecutor executor, string user)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: "", sql: $"SHOW GRANTS FOR {user}", parameters: null));
        return await cursor.ToListAsync();
    }

    [Test]
    public void PasswordHasherRoundTrips()
    {
        Credential credential = PasswordHasher.Hash("app-password", CamusDBConfig.Ambient.PasswordHashIterations);

        Assert.AreEqual(AuthAlgorithm.Pbkdf2Sha256, credential.Algorithm);
        Assert.IsTrue(credential.Salt.Length > 0);
        Assert.IsTrue(credential.Hash.Length > 0);
        Assert.IsTrue(PasswordHasher.Verify("app-password", credential));
        Assert.IsFalse(PasswordHasher.Verify("wrong-password", credential));
        Assert.IsFalse(PasswordHasher.Verify("app-password", null));

        // A second hash of the same password draws a fresh salt, so the stored hash differs.
        Credential second = PasswordHasher.Hash("app-password", CamusDBConfig.Ambient.PasswordHashIterations);
        Assert.AreNotEqual(Convert.ToBase64String(credential.Hash), Convert.ToBase64String(second.Hash));
    }

    [Test]
    public async Task CreateUserGrantAndShow()
    {
        (string dbname, CommandExecutor executor) = await CreateAuthDatabase();

        await ExecuteServerDdl(executor, "CREATE USER myapp IDENTIFIED WITH sha256_password BY 'app-password'");
        await ExecuteServerDdl(executor, $"GRANT SELECT, INSERT, ALTER, CREATE TABLE ON {dbname}.* TO myapp");

        List<QueryResultRow> rows = await ShowGrants(executor, "myapp");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("myapp", rows[0].Row["user"].StrValue);
        Assert.AreEqual($"{dbname}.*", rows[0].Row["object"].StrValue);

        string privileges = rows[0].Row["privileges"].StrValue!;
        Assert.IsTrue(privileges.Contains("SELECT"), privileges);
        Assert.IsTrue(privileges.Contains("INSERT"), privileges);
        Assert.IsTrue(privileges.Contains("ALTER"), privileges);
        Assert.IsTrue(privileges.Contains("CREATE TABLE"), privileges);
    }

    [Test]
    public async Task AlterUserRotatesPassword()
    {
        (_, CommandExecutor executor) = await CreateAuthDatabase();

        await ExecuteServerDdl(executor, "CREATE USER admin IDENTIFIED WITH sha256_password BY 'initial-password'");
        // Rotation succeeds and does not throw.
        await ExecuteServerDdl(executor, "ALTER USER admin IDENTIFIED WITH sha256_password BY 'new-strong-password'");
    }

    [Test]
    public async Task CreateUserDuplicateFails()
    {
        (_, CommandExecutor executor) = await CreateAuthDatabase();

        await ExecuteServerDdl(executor, "CREATE USER dupe IDENTIFIED BY 'x'");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteServerDdl(executor, "CREATE USER dupe IDENTIFIED BY 'y'"))!;
        Assert.AreEqual(CamusDBErrorCodes.UserAlreadyExists, ex.Code);

        // IF NOT EXISTS is a no-op instead.
        await ExecuteServerDdl(executor, "CREATE USER IF NOT EXISTS dupe IDENTIFIED BY 'z'");
    }

    [Test]
    public async Task GrantOnUnknownUserFails()
    {
        (string dbname, CommandExecutor executor) = await CreateAuthDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteServerDdl(executor, $"GRANT SELECT ON {dbname}.* TO ghost"))!;
        Assert.AreEqual(CamusDBErrorCodes.UserDoesNotExist, ex.Code);
    }

    [Test]
    public async Task UnsupportedPluginFails()
    {
        (_, CommandExecutor executor) = await CreateAuthDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteServerDdl(executor, "CREATE USER weird IDENTIFIED WITH mysql_native_password BY 'x'"))!;
        Assert.AreEqual(CamusDBErrorCodes.UnsupportedAuthPlugin, ex.Code);
    }

    [Test]
    public async Task RevokeSubtractsPrivilege()
    {
        (string dbname, CommandExecutor executor) = await CreateAuthDatabase();

        await ExecuteServerDdl(executor, "CREATE USER svc IDENTIFIED BY 'x'");
        await ExecuteServerDdl(executor, $"GRANT SELECT, INSERT ON {dbname}.* TO svc");
        await ExecuteServerDdl(executor, $"REVOKE INSERT ON {dbname}.* FROM svc");

        List<QueryResultRow> rows = await ShowGrants(executor, "svc");
        Assert.AreEqual(1, rows.Count);
        string privileges = rows[0].Row["privileges"].StrValue!;
        Assert.IsTrue(privileges.Contains("SELECT"), privileges);
        Assert.IsFalse(privileges.Contains("INSERT"), privileges);
    }

    [Test]
    public async Task TableScopedGrantResolvesTableId()
    {
        (string dbname, CommandExecutor executor) = await CreateAuthDatabase();
        await ExecuteDdl(executor, dbname, "CREATE TABLE users (id int64 PRIMARY KEY NOT NULL, name string NOT NULL)");

        await ExecuteServerDdl(executor, "CREATE USER reader IDENTIFIED BY 'x'");
        await ExecuteServerDdl(executor, $"GRANT SELECT ON {dbname}.users TO reader");

        List<QueryResultRow> rows = await ShowGrants(executor, "reader");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual($"{dbname}.users", rows[0].Row["object"].StrValue);
    }

    [Test]
    public async Task ReloadSeesPersistedUserAndGrants()
    {
        (string dbname, CommandExecutor executor) = await CreateAuthDatabase();

        await ExecuteServerDdl(executor, "CREATE USER persisted IDENTIFIED BY 'x'");
        await ExecuteServerDdl(executor, $"GRANT SELECT ON {dbname}.* TO persisted");

        // A second executor opens a fresh AuthCatalog that must rebuild its cache from KV — this proves
        // the user and grant were durably persisted, not just held in the first catalog's memory.
        CommandExecutor reopened = CreateCommandExecutor();
        List<QueryResultRow> rows = await ShowGrants(reopened, "persisted");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual($"{dbname}.*", rows[0].Row["object"].StrValue);
        Assert.IsTrue(rows[0].Row["privileges"].StrValue!.Contains("SELECT"));

        await reopened.DisposeAsync();
    }

    [Test]
    public async Task DropUserRemovesUserAndGrants()
    {
        (string dbname, CommandExecutor executor) = await CreateAuthDatabase();

        await ExecuteServerDdl(executor, "CREATE USER temp IDENTIFIED BY 'x'");
        await ExecuteServerDdl(executor, $"GRANT SELECT ON {dbname}.* TO temp");
        await ExecuteServerDdl(executor, "DROP USER temp");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await ShowGrants(executor, "temp"))!;
        Assert.AreEqual(CamusDBErrorCodes.UserDoesNotExist, ex.Code);

        // IF EXISTS makes a repeat drop a no-op.
        await ExecuteServerDdl(executor, "DROP USER IF EXISTS temp");
    }
}
