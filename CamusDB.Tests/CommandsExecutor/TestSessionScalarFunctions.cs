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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The session functions <c>current_database()</c>, <c>current_user()</c> and <c>current_role()</c>
/// with authentication off — the default deployment. Exercised through the real SQL entry point in
/// every position an expression can appear (FROM-less projection, projection over a table, WHERE),
/// because the session snapshot they read travels with the ticket and a path that builds its own
/// sub-ticket could drop it.
/// </summary>
public class TestSessionScalarFunctions : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupBasicTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));

        KvTransaction txnState = await database.Transactions.BeginAsync();

        await executor.Insert(new(
            txnState: txnState,
            databaseName: dbname,
            tableName: "robots",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "name", new(ColumnType.String, "robot") },
                }
            }));

        await database.Transactions.CommitAsync(txnState);

        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> ExecuteSelect(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txnState = await database.Transactions.BeginAsync();

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txnState, database: dbname, sql: sql, parameters: null));

        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txnState);
        return rows;
    }

    [Test]
    [NonParallelizable]
    public async Task CurrentDatabase_WithoutFrom_ReturnsDatabaseName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(executor, database, dbname, "SELECT current_database()");

        Assert.AreEqual(1, result.Count);
        ColumnValue value = result[0].Row.Values.First();
        Assert.AreEqual(ColumnType.String, value.Type);
        Assert.AreEqual(dbname, value.StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task CurrentDatabase_InProjectionOverTable_ReturnsDatabaseName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor, database, dbname, "SELECT current_database() FROM robots LIMIT 1");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(dbname, result[0].Row.Values.First().StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task CurrentDatabase_InWhereClause_FiltersOnSessionValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> matching = await ExecuteSelect(
            executor, database, dbname, $"SELECT name FROM robots WHERE current_database() = '{dbname}'");

        Assert.AreEqual(1, matching.Count);

        List<QueryResultRow> nonMatching = await ExecuteSelect(
            executor, database, dbname, "SELECT name FROM robots WHERE current_database() = 'some-other-db'");

        Assert.AreEqual(0, nonMatching.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task CurrentUserAndRole_AreNullWhenAuthenticationIsDisabled()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        List<QueryResultRow> result = await ExecuteSelect(
            executor, database, dbname, "SELECT current_user(), current_role(), is_superuser()");

        Assert.AreEqual(1, result.Count);
        foreach (ColumnValue value in result[0].Row.Values)
            Assert.AreEqual(ColumnType.Null, value.Type);
    }

    /// <summary>
    /// A view body can call a session function the querying statement never mentions. The snapshot is
    /// decided from the AST, so it has to be reconsidered after view expansion or
    /// <c>SELECT * FROM v</c> would fail on a body that reads perfectly well on its own.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task CurrentDatabase_InsideViewBody_ResolvesForTheQueryingStatement()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        KvTransaction ddlTxn = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: ddlTxn,
            database: dbname,
            sql: "CREATE VIEW robots_with_db AS SELECT name, current_database() AS db FROM robots",
            parameters: null));
        await database.Transactions.CommitAsync(ddlTxn);

        List<QueryResultRow> result = await ExecuteSelect(
            executor, database, dbname, "SELECT db FROM robots_with_db");

        Assert.AreEqual(1, result.Count);
        Assert.AreEqual(dbname, result[0].Row["db"].StrValue);
    }

    /// <summary>
    /// INSERT and UPDATE build their own tickets from the statement, so the session snapshot has to
    /// reach the value/SET expressions the same way it reaches a projection.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task CurrentDatabase_IsUsableInInsertValuesAndUpdateSet()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        KvTransaction insertTxn = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: insertTxn,
            database: dbname,
            sql: "INSERT INTO robots (id, name) VALUES (gen_id(), current_database())",
            parameters: null));
        await database.Transactions.CommitAsync(insertTxn);

        List<QueryResultRow> inserted = await ExecuteSelect(
            executor, database, dbname, $"SELECT name FROM robots WHERE name = '{dbname}'");

        Assert.AreEqual(1, inserted.Count);

        KvTransaction updateTxn = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: updateTxn,
            database: dbname,
            sql: "UPDATE robots SET name = current_database() WHERE name = 'robot'",
            parameters: null));
        await database.Transactions.CommitAsync(updateTxn);

        List<QueryResultRow> updated = await ExecuteSelect(
            executor, database, dbname, $"SELECT name FROM robots WHERE name = '{dbname}'");

        Assert.AreEqual(2, updated.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task SessionFunction_IsRejectedAsColumnDefault()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                txnState: txnState,
                database: dbname,
                sql: "CREATE TABLE owners (id int64 PRIMARY KEY NOT NULL, owner string(64) DEFAULT(current_user()))",
                parameters: null)))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
        Assert.IsTrue(exception.Message.Contains("current_user"), exception.Message);

        await database.Transactions.RollbackAsync(txnState);
    }

    /// <summary>
    /// A CHECK condition is stored and re-evaluated by later inserts, which carry no session, so the
    /// DDL must refuse one that names a session function rather than accept a constraint that can only
    /// fail later.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task SessionFunction_IsRejectedInCheckConstraint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupBasicTable();

        KvTransaction txnState = await database.Transactions.BeginAsync();

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                txnState: txnState,
                database: dbname,
                sql: "CREATE TABLE owners (id int64 PRIMARY KEY NOT NULL, owner string(64), "
                     + "CONSTRAINT owner_is_caller CHECK (owner = current_user()))",
                parameters: null)))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);

        await database.Transactions.RollbackAsync(txnState);
    }
}

/// <summary>
/// <c>current_user()</c> / <c>current_role()</c> with authentication enabled: they report the
/// principal that the transport authenticated for this statement, not a server-wide identity, so two
/// sessions running the same SQL text must see different values.
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test, like the other auth fixtures.
[NonParallelizable]
internal sealed class TestSessionScalarFunctionsWithAuth : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-key",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    private static async Task<Principal> Login(CommandExecutor ex, string user, string password)
        => await ex.ResolvePrincipalAsync((await ex.LoginAsync(user, password)).Token);

    private static Task ServerDdl(CommandExecutor ex, string sql, Principal? principal)
        => ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: principal));

    private static async Task<List<QueryResultRow>> Query(CommandExecutor ex, string db, string sql, Principal? principal)
    {
        DatabaseDescriptor descriptor = await ex.OpenDatabase(db);
        KvTransaction txnState = await descriptor.Transactions.BeginAsync();

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState, db, sql, null, principal));

        List<QueryResultRow> rows = await cursor.ToListAsync();
        await descriptor.Transactions.CommitAsync(txnState);
        return rows;
    }

    private async Task<(string db, CommandExecutor ex, Principal root)> Setup()
    {
        CommandExecutor ex = CreateCommandExecutor();
        string db = "sessionfn" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");
        return (db, ex, root);
    }

    [Test]
    public async Task CurrentUserAndRole_ReportTheAuthenticatedPrincipal()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER alice IDENTIFIED BY 'alice-pw'", root);
        Principal alice = await Login(ex, "alice", "alice-pw");

        List<QueryResultRow> rootRows = await Query(ex, db, "SELECT current_user(), current_role()", root);
        Assert.AreEqual(1, rootRows.Count);
        foreach (ColumnValue value in rootRows[0].Row.Values)
        {
            Assert.AreEqual(ColumnType.String, value.Type);
            Assert.AreEqual("root", value.StrValue);
        }

        // Same SQL text, different session: the value must follow the caller, not the statement.
        List<QueryResultRow> aliceRows = await Query(ex, db, "SELECT current_user(), current_role()", alice);
        Assert.AreEqual(1, aliceRows.Count);
        foreach (ColumnValue value in aliceRows[0].Row.Values)
            Assert.AreEqual("alice", value.StrValue);
    }

    /// <summary>
    /// <c>is_superuser()</c> answers for the session that runs it, so the bootstrap superuser and a
    /// plain user must get different answers from the same SQL text — and a user granted everything on
    /// a database is still not a superuser, since the flag is an identity, not a privilege total.
    /// </summary>
    [Test]
    public async Task IsSuperuser_ReportsTheCallersOwnStatus()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();
        await ServerDdl(ex, "CREATE USER alice IDENTIFIED BY 'alice-pw'", root);
        await ServerDdl(ex, $"GRANT SELECT, INSERT, UPDATE, DELETE ON {db}.* TO alice", root);
        Principal alice = await Login(ex, "alice", "alice-pw");

        List<QueryResultRow> rootRows = await Query(ex, db, "SELECT is_superuser()", root);
        ColumnValue rootValue = rootRows[0].Row.Values.First();
        Assert.AreEqual(ColumnType.Bool, rootValue.Type);
        Assert.IsTrue(rootValue.BoolValue);

        List<QueryResultRow> aliceRows = await Query(ex, db, "SELECT is_superuser()", alice);
        ColumnValue aliceValue = aliceRows[0].Row.Values.First();
        Assert.AreEqual(ColumnType.Bool, aliceValue.Type);
        Assert.IsFalse(aliceValue.BoolValue);
    }

    [Test]
    public async Task CurrentDatabase_ReportsTheStatementDatabase()
    {
        (string db, CommandExecutor ex, Principal root) = await Setup();

        List<QueryResultRow> rows = await Query(ex, db, "SELECT current_database()", root);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(db, rows[0].Row.Values.First().StrValue);
    }
}
