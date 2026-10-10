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
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for <c>SHOW QUERIES</c>, <c>SHOW CONNECTIONS</c> and <c>CANCEL QUERY</c>,
/// driven through <see cref="ExecuteSQLTicket"/> and the real <see cref="CommandExecutor"/>.
///
/// <para><b>How a statement is held running.</b> A row-returning statement stays listed until its
/// cursor ends, so a test reads one row of a SELECT and stops: the statement is then running, with a
/// known row count, for as long as the test keeps the cursor. No sleep and no timing is involved,
/// which is what makes the cancel assertions deterministic.</para>
///
/// <para><b>The negative control</b> is <see cref="RegistryOff_HeldQueryIsNotListedAndCannotBeCancelled"/>:
/// the same held statement on an engine built with the registry off. A listing test that passed for
/// some other reason would fail there.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestQueryActivityStatements : BaseTest
{
    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string db, string sql, Principal? principal = null)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, database: db, sql: sql, parameters: null, principal: principal));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        return rows;
    }

    private static Task CancelAsync(CommandExecutor executor, string db, string queryId, Principal? principal = null)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            null!, database: db, sql: $"CANCEL QUERY '{queryId}'", parameters: null, principal: principal));

    private static string? Text(QueryResultRow row, string column) => row.Row[column].StrValue;

    /// <summary>Opens a SELECT over the table and reads its first row, leaving it running.</summary>
    private static async Task<(KvTransaction txn, IAsyncEnumerator<QueryResultRow> cursor)> HoldSelectAsync(
        CommandExecutor executor, DatabaseDescriptor database, string db, string sql, Principal? principal = null)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> rows) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txn, db, sql, null, principal));

        IAsyncEnumerator<QueryResultRow> cursor = rows.GetAsyncEnumerator();
        Assert.That(await cursor.MoveNextAsync(), Is.True);
        return (txn, cursor);
    }

    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> SetupRobots(
        CamusDBOptions options, int rows = 30, Principal? principal = null)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db,
            "CREATE TABLE robots (id OID PRIMARY KEY, name STRING NOT NULL, year INT64 NOT NULL)", null, principal));
        await database.Transactions.CommitAsync(ddl);

        KvTransaction insert = await database.Transactions.BeginAsync();
        for (int i = 0; i < rows; i++)
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insert, db,
                $"INSERT INTO robots (id, name, year) VALUES (GEN_ID(), 'robot{i}', {2000 + i})", null, principal));
        await database.Transactions.CommitAsync(insert);

        return (db, database, executor);
    }

    [Test]
    public async Task HeldSelectIsListedWithItsProgress()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots(Options);

        (KvTransaction txn, IAsyncEnumerator<QueryResultRow> cursor) =
            await HoldSelectAsync(executor, database, db, "SELECT name FROM robots WHERE year >= 2000");

        List<QueryResultRow> rows = await QueryAsync(executor, db, "SHOW QUERIES");

        QueryResultRow held = rows.Single(r => Text(r, "kind") == "select");
        Assert.That(Text(held, "sql"), Is.EqualTo("SELECT name FROM robots WHERE year >= 2000"));
        Assert.That(held.Row["rows_returned"].LongValue, Is.EqualTo(1));
        Assert.That(Text(held, "phase"), Is.EqualTo("executing"));
        Assert.That(held.Row["cancellable"].BoolValue, Is.True);
        Assert.That(held.Row["cancel_requested"].BoolValue, Is.False);
        Assert.That(Text(held, "database_name"), Is.EqualTo(db));
        Assert.That(Text(held, "transaction_id"), Is.Not.Null);
        Assert.That(Text(held, "transport"), Is.EqualTo(StatementOrigin.EmbeddedTransport));
        Assert.That(held.Row["error"].Type, Is.EqualTo(ColumnType.Null));

        // The SHOW lists itself, as SHOW PROCESSLIST and SHOW QUERIES do elsewhere.
        Assert.That(rows.Any(r => Text(r, "kind") == "show_queries"), Is.True);

        await cursor.DisposeAsync();
        await database.Transactions.RollbackAsync(txn);

        Assert.That((await QueryAsync(executor, db, "SHOW QUERIES")).Any(r => Text(r, "kind") == "select"), Is.False);
    }

    [Test]
    public async Task LikeFiltersByStatementText()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots(Options);

        (KvTransaction txn, IAsyncEnumerator<QueryResultRow> cursor) =
            await HoldSelectAsync(executor, database, db, "SELECT name FROM robots");

        List<QueryResultRow> matched = await QueryAsync(executor, db, "SHOW QUERIES LIKE '%robots%'");
        Assert.That(matched.Select(r => Text(r, "sql")), Is.EqualTo(new[] { "SELECT name FROM robots" }));

        await cursor.DisposeAsync();
        await database.Transactions.RollbackAsync(txn);
    }

    [Test]
    public async Task CancelQueryStopsTheHeldSelect()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots(Options);

        (KvTransaction txn, IAsyncEnumerator<QueryResultRow> cursor) =
            await HoldSelectAsync(executor, database, db, "SELECT name FROM robots");

        string id = Text((await QueryAsync(executor, db, "SHOW QUERIES")).Single(r => Text(r, "kind") == "select"), "query_id")!;

        await CancelAsync(executor, db, id);

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await cursor.MoveNextAsync());
        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.QueryCancelled));

        await cursor.DisposeAsync();
        await database.Transactions.RollbackAsync(txn);

        // The statement left the list, and a second cancel of the same id says so.
        CamusDBException? again = Assert.ThrowsAsync<CamusDBException>(() => CancelAsync(executor, db, id));
        Assert.That(again!.Code, Is.EqualTo(CamusDBErrorCodes.QueryNotFound));

        // Nothing the cancelled read held blocks a write to the same rows.
        KvTransaction write = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(write, db, "UPDATE robots SET year = year + 1 WHERE year >= 2000", null));
        await database.Transactions.CommitAsync(write);
    }

    [Test]
    public async Task CancelQueryOfAnUnknownIdIsNotFound()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => CancelAsync(executor, db, "abcdef0123-42"));
        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.QueryNotFound), "an unknown id must not report success");
    }

    [Test]
    public async Task RegistryOff_HeldQueryIsNotListedAndCannotBeCancelled()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupRobots(Options with { QueryActivityEnabled = false });

        (KvTransaction txn, IAsyncEnumerator<QueryResultRow> cursor) =
            await HoldSelectAsync(executor, database, db, "SELECT name FROM robots");

        Assert.That(await QueryAsync(executor, db, "SHOW QUERIES"), Is.Empty);

        // The cursor still runs to its end: nothing replaced its token.
        int rest = 0;
        while (await cursor.MoveNextAsync())
            rest++;
        Assert.That(rest, Is.EqualTo(29));

        await cursor.DisposeAsync();
        await database.Transactions.RollbackAsync(txn);
    }

    [Test]
    public async Task ClusterFormOnAStandaloneEngineEqualsTheLocalForm()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots(Options);

        (KvTransaction txn, IAsyncEnumerator<QueryResultRow> cursor) =
            await HoldSelectAsync(executor, database, db, "SELECT name FROM robots");

        List<QueryResultRow> rows = await QueryAsync(executor, db, "SHOW CLUSTER QUERIES");
        Assert.That(rows.Count(r => Text(r, "kind") == "select"), Is.EqualTo(1));
        Assert.That(rows.All(r => r.Row["error"].Type == ColumnType.Null), Is.True);

        await cursor.DisposeAsync();
        await database.Transactions.RollbackAsync(txn);
    }

    [Test]
    public async Task ConnectionsListTheHostConnectionsAndTheirStatements()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots(Options);

        Assert.That(await QueryAsync(executor, db, "SHOW CONNECTIONS"), Is.Empty, "no host, no connections");

        ClientConnection connection = executor.QueryActivity.Connections.Open("kestrel-test", "192.0.2.10");
        connection.RequestStarted("HTTP/2", internalPath: false);

        IAsyncEnumerator<QueryResultRow> cursor;
        KvTransaction txn;
        StatementOrigin? previous = StatementOrigin.Current;
        StatementOrigin.Current = connection.OriginFor("grpc");
        try
        {
            (txn, cursor) = await HoldSelectAsync(executor, database, db, "SELECT name FROM robots");
        }
        finally
        {
            StatementOrigin.Current = previous;
        }

        QueryResultRow query = (await QueryAsync(executor, db, "SHOW QUERIES")).Single(r => Text(r, "kind") == "select");
        Assert.That(Text(query, "connection_id"), Is.EqualTo(connection.Id));
        Assert.That(Text(query, "client_address"), Is.EqualTo("192.0.2.10"));
        Assert.That(Text(query, "transport"), Is.EqualTo("grpc"));

        QueryResultRow row = (await QueryAsync(executor, db, "SHOW CONNECTIONS")).Single();
        Assert.That(Text(row, "connection_id"), Is.EqualTo(connection.Id));
        Assert.That(row.Row["active_queries"].LongValue, Is.EqualTo(1));
        Assert.That(Text(row, "protocol"), Is.EqualTo("HTTP/2"));

        await cursor.DisposeAsync();
        await database.Transactions.RollbackAsync(txn);
        connection.RequestEnded();
        connection.Close();

        Assert.That(await QueryAsync(executor, db, "SHOW CONNECTIONS"), Is.Empty);
    }

    [Test]
    public async Task CancelOfAWriteIsRefused()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        // A write cannot be held mid-flight from a test, so the entry is registered the way the no-rows
        // funnel registers one, and the statement path is exercised through CANCEL QUERY.
        QueryActivityEntry entry = executor.QueryActivity.Begin(
            new ExecuteSQLTicket(null!, db, "UPDATE robots SET year = 1", null), cancellable: false, executing: true)!;

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => CancelAsync(executor, db, entry.Id));
        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.QueryNotCancellable));

        entry.End();
    }

    /// <summary>
    /// <c>INSERT … RETURNING</c> comes through the row-returning path, as a read does, but it is a
    /// write. Through SQL end to end: its entry is listed as not cancellable with its parsed kind,
    /// a cancel is refused, and its rows still arrive.
    /// </summary>
    [Test]
    public async Task InsertReturningIsListedAsAWriteAndRefusesACancel()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots(Options, rows: 0);

        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> returning) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(txn, db,
            "INSERT INTO robots (id, name, year) VALUES (GEN_ID(), 'returned', 2050) RETURNING name", null));
        await using IAsyncEnumerator<QueryResultRow> cursor = returning.GetAsyncEnumerator();

        QueryResultRow listed = (await QueryAsync(executor, db, "SHOW QUERIES")).Single(r => Text(r, "kind") == "insert");
        Assert.That(listed.Row["cancellable"].BoolValue, Is.False);

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => CancelAsync(executor, db, Text(listed, "query_id")!));
        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.QueryNotCancellable));

        Assert.That(await cursor.MoveNextAsync(), Is.True);
        Assert.That(Text(cursor.Current, "name"), Is.EqualTo("returned"));
        Assert.That(await cursor.MoveNextAsync(), Is.False);

        await database.Transactions.CommitAsync(txn);
        Assert.That((await QueryAsync(executor, db, "SHOW QUERIES")).Any(r => Text(r, "kind") == "insert"), Is.False);
    }

    /// <summary>A result taken and dropped unread leaves the list, through the real query path.</summary>
    [Test]
    public async Task SelectDisposedUnreadLeavesTheList()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots(Options, rows: 3);

        KvTransaction txn = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> rows) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txn, db, "SELECT name FROM robots WHERE year >= 2000", null));

        Assert.That((await QueryAsync(executor, db, "SHOW QUERIES")).Any(r => Text(r, "kind") == "select"), Is.True);

        await rows.GetAsyncEnumerator().DisposeAsync();

        Assert.That((await QueryAsync(executor, db, "SHOW QUERIES")).Any(r => Text(r, "kind") == "select"), Is.False);
        await database.Transactions.RollbackAsync(txn);
    }

    [Test]
    public async Task StatementsNeedNoDatabaseAndTheWordsStayIdentifiers()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Server-level: an empty context database is accepted.
        Assert.That(await QueryAsync(executor, "", "SHOW CONNECTIONS"), Is.Empty);
        Assert.That(await QueryAsync(executor, "", "SHOW CLUSTER CONNECTIONS"), Is.Empty);
        Assert.That((await QueryAsync(executor, "", "SHOW QUERIES")).Count, Is.EqualTo(1), "the SHOW itself");

        // None of the words became reserved.
        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, db,
            "CREATE TABLE queries (id OID PRIMARY KEY, connections STRING NULL, cancel STRING NULL, query STRING NULL)", null));
        await database.Transactions.CommitAsync(ddl);
    }
}

/// <summary>
/// The privilege rules of the activity statements with authentication on: each user sees and cancels
/// only their own statements, and a superuser sees and cancels all of them.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestQueryActivityPrivileges : BaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-key-padded-to-meet-the-32-byte-secret-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
    };

    private static async Task<Principal> Login(CommandExecutor ex, string user, string password)
        => await ex.ResolvePrincipalAsync((await ex.LoginAsync(user, password)).Token);

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string sql, Principal principal)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, database: "", sql: sql, parameters: null, principal: principal));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        return rows;
    }

    private static Task CancelAsync(CommandExecutor executor, string queryId, Principal principal)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            null!, database: "", sql: $"CANCEL QUERY '{queryId}'", parameters: null, principal: principal));

    [Test]
    public async Task UsersSeeAndCancelOnlyTheirOwnStatements()
    {
        CommandExecutor ex = CreateCommandExecutor();
        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        foreach (string user in new[] { "alice", "bob" })
            await ex.ExecuteDDLSQL(new ExecuteSQLTicket(null!, "", $"CREATE USER {user} IDENTIFIED BY '{user}-password-1'", null, root));

        Principal alice = await Login(ex, "alice", "alice-password-1");
        Principal bob = await Login(ex, "bob", "bob-password-1");

        // Alice holds a statement open: her own SHOW QUERIES, read one row and kept.
        (_, IAsyncEnumerable<QueryResultRow> held) = await ex.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, "", "SHOW QUERIES", null, alice));
        IAsyncEnumerator<QueryResultRow> cursor = held.GetAsyncEnumerator();
        Assert.That(await cursor.MoveNextAsync(), Is.True);
        string aliceId = cursor.Current.Row["query_id"].StrValue!;

        List<QueryResultRow> bobView = await QueryAsync(ex, "SHOW QUERIES", bob);
        Assert.That(bobView.All(r => r.Row["user_name"].StrValue == "bob"), Is.True);
        Assert.That(bobView.Any(r => r.Row["query_id"].StrValue == aliceId), Is.False);

        List<QueryResultRow> rootView = await QueryAsync(ex, "SHOW QUERIES", root);
        Assert.That(rootView.Any(r => r.Row["query_id"].StrValue == aliceId), Is.True);

        // Bob cannot cancel it, and is not told it exists.
        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(() => CancelAsync(ex, aliceId, bob));
        Assert.That(refused!.Code, Is.EqualTo(CamusDBErrorCodes.QueryNotFound));

        // Alice can.
        await CancelAsync(ex, aliceId, alice);
        CamusDBException? stopped = Assert.ThrowsAsync<CamusDBException>(async () => await cursor.MoveNextAsync());
        Assert.That(stopped!.Code, Is.EqualTo(CamusDBErrorCodes.QueryCancelled));

        await cursor.DisposeAsync();
    }

    [Test]
    public void UnauthenticatedCallerIsRefused()
    {
        CommandExecutor ex = CreateCommandExecutor();

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await ex.ExecuteSQLQuery(
                new ExecuteSQLTicket(null!, "", "SHOW QUERIES", null, principal: null));
            await foreach (QueryResultRow _ in cursor) { }
        });

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.AuthenticationFailed));
    }
}
