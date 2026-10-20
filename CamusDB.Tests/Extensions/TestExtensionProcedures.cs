/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// <c>CALL</c> through the engine: a multi-statement procedure that commits as one statement, rolls
/// back as one when it throws, reports the rows it changed, takes parameters, and refuses an unknown
/// name and a wrong argument count.
/// </summary>
[NonParallelizable]
public sealed class TestExtensionProcedures : ExtensionTestBase
{
    private static async Task<(string db, CommandExecutor executor)> SetupAccounts(ExtensionTestBase test, CommandExecutor executor, string db)
    {
        await ExecDdl(executor, db, "CREATE TABLE accounts (id OID PRIMARY KEY DEFAULT (gen_id()), name STRING, balance INT64)");
        await ExecNonQuery(executor, db, "INSERT INTO accounts (name, balance) VALUES ('alice', 100), ('bob', 0)");
        return (db, executor);
    }

    private static async Task<Dictionary<string, long>> Balances(CommandExecutor executor, string db) =>
        (await ExecQuery(executor, db, "SELECT name, balance FROM accounts"))
            .ToDictionary(r => r["name"].StrValue!, r => r["balance"].LongValue);

    [Test]
    public async Task Call_RunsTheProcedureTransactionally()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await SetupAccounts(this, executor, db);

        int changed = await ExecNonQuery(executor, db, "CALL fx_transfer('alice', 'bob', 30)");

        Assert.AreEqual(2, changed, "the CALL reports the rows its SQL changed");
        Dictionary<string, long> balances = await Balances(executor, db);
        Assert.AreEqual(70, balances["alice"]);
        Assert.AreEqual(30, balances["bob"]);
    }

    [Test]
    public async Task Call_ThatThrows_RollsBackEverything()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await SetupAccounts(this, executor, db);

        // The procedure runs both UPDATEs, then reads a negative balance and throws.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => ExecNonQuery(executor, db, "CALL fx_transfer('alice', 'bob', 500)"))!;
        Assert.AreEqual(CamusDBErrorCodes.ProcedureFailed, ex.Code);
        StringAssert.Contains("insufficient funds in alice", ex.Message);

        Dictionary<string, long> balances = await Balances(executor, db);
        Assert.AreEqual(100, balances["alice"], "both UPDATEs roll back with the CALL");
        Assert.AreEqual(0, balances["bob"]);
    }

    [Test]
    public async Task Call_TakesParametersAndExpressions()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await SetupAccounts(this, executor, db);

        await ExecNonQuery(executor, db, "CALL fx_transfer(@from, 'bob', 5 * 2)",
            new Dictionary<string, ColumnValue> { ["@from"] = new ColumnValue(ColumnType.String, "alice") });

        Assert.AreEqual(90, (await Balances(executor, db))["alice"]);
    }

    [Test]
    public async Task Call_Refusals()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();

        CamusDBException unknown = Assert.ThrowsAsync<CamusDBException>(() => ExecNonQuery(executor, db, "CALL fx_nope()"))!;
        Assert.AreEqual(CamusDBErrorCodes.ProcedureNotFound, unknown.Code);

        CamusDBException arity = Assert.ThrowsAsync<CamusDBException>(() => ExecNonQuery(executor, db, "CALL fx_transfer('a')"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, arity.Code);
        StringAssert.Contains("expects 3", arity.Message);

        CamusDBException notCall = Assert.ThrowsAsync<CamusDBException>(() => ExecNonQuery(executor, db, "RUN fx_noop()"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, notCall.Code);

        Assert.AreEqual(0, await ExecNonQuery(executor, db, "CALL fx_noop()"));
    }

    [Test]
    public async Task Call_FiresTriggersOfItsSql()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await SetupAccounts(this, executor, db);
        await ExecDdl(executor, db, "CREATE TABLE audit_log (id OID PRIMARY KEY DEFAULT (gen_id()), op STRING, timing STRING, tbl STRING, detail STRING)");
        await ExecDdl(executor, db, "CREATE TRIGGER audit ON accounts AFTER UPDATE EXTERNAL NAME 'CamusDB.Fixtures.Good.AuditTrigger'");

        await ExecNonQuery(executor, db, "CALL fx_transfer('alice', 'bob', 1)");

        List<Dictionary<string, ColumnValue>> audit = await ExecQuery(executor, db, "SELECT detail FROM audit_log");
        CollectionAssert.AreEquivalent(new[] { "alice", "bob" }, audit.Select(r => r["detail"].StrValue));
    }

    [Test]
    public async Task Session_RefusesDdlAndTransactionControl()
    {
        // The procedure surface is SQL-only; a DDL statement through it is refused before it runs.
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        CamusDB.Core.CommandsExecutor.Models.DatabaseDescriptor database = await executor.OpenDatabase(db);
        CamusDB.Core.Transactions.KvTransaction tx = await database.Transactions.BeginAsync();

        CamusDB.Core.Extensions.ExtensionSqlSession session = new(executor, db, tx, principal: null);

        CamusDBException ddl = Assert.ThrowsAsync<CamusDBException>(() => session.ExecuteAsync("CREATE TABLE x (id INT64 PRIMARY KEY)"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionStatementRefused, ddl.Code);

        CamusDBException commit = Assert.ThrowsAsync<CamusDBException>(() => session.ExecuteAsync("COMMIT"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionStatementRefused, commit.Code);

        CamusDBException select = Assert.ThrowsAsync<CamusDBException>(() => session.ExecuteAsync("SELECT 1"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionStatementRefused, select.Code, "ExecuteAsync is for writes; reads use QueryAsync");

        await database.Transactions.RollbackAsync(tx);
    }
}
