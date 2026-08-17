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

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Every statement that executes without opening a database must be accepted through <b>both</b> SQL
/// entry points — the DDL one and the no-rows one.
///
/// <para>This matters because a client routes any non-SELECT statement to whichever endpoint it uses
/// for those. A statement that works through one and answers "unknown statement" (or, worse,
/// "database does not exist") through the other is, to that client, indistinguishable from a feature
/// the server does not support.</para>
///
/// <para>The two dispatch lists were maintained separately once and drifted: the no-rows path was
/// missing <c>CREATE DATABASE</c> and <c>CREATE DATABASE … RELINK</c>, so those fell through to the
/// database open and failed. These tests pin the parity so the lists cannot silently diverge again.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestServerLevelStatementParity : BaseTest
{
    private static ExecuteSQLTicket Sql(string database, string sql)
        => new(txnState: null!, database: database, sql: sql, parameters: null);

    [Test]
    public async Task CreateDatabase_IsAcceptedThroughTheNoRowsEndpoint()
    {
        string dbname = "db_" + Guid.NewGuid().ToString("n")[..8];
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(dbname, executor);

        // The regression this pins: CREATE DATABASE used to reach the database open on this path and
        // fail, because only the DDL entry point knew the statement was server-level.
        await executor.ExecuteNonSQLQuery(Sql(dbname, $"CREATE DATABASE {dbname}"));

        DatabaseDescriptor opened = await executor.OpenDatabase(dbname);
        Assert.AreEqual(dbname, opened.Name);
    }

    [Test]
    public async Task CreateDatabaseIfNotExists_IsIdempotentThroughTheNoRowsEndpoint()
    {
        string dbname = "db_" + Guid.NewGuid().ToString("n")[..8];
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(dbname, executor);

        await executor.ExecuteNonSQLQuery(Sql(dbname, $"CREATE DATABASE {dbname}"));
        await executor.ExecuteNonSQLQuery(Sql(dbname, $"CREATE DATABASE IF NOT EXISTS {dbname}"));

        DatabaseDescriptor opened = await executor.OpenDatabase(dbname);
        Assert.AreEqual(dbname, opened.Name);
    }

    [Test]
    public async Task DropAndRenameDatabase_AreAcceptedThroughBothEndpoints()
    {
        CommandExecutor executor = CreateCommandExecutor();

        string viaDdl = "db_" + Guid.NewGuid().ToString("n")[..8];
        string viaNonQuery = "db_" + Guid.NewGuid().ToString("n")[..8];
        string renamed = "db_" + Guid.NewGuid().ToString("n")[..8];
        TrackDatabase(viaDdl, executor);
        TrackDatabase(viaNonQuery, executor);
        TrackDatabase(renamed, executor);

        await executor.ExecuteDDLSQL(Sql(viaDdl, $"CREATE DATABASE {viaDdl}"));
        await executor.ExecuteNonSQLQuery(Sql(viaNonQuery, $"CREATE DATABASE {viaNonQuery}"));

        // RENAME through the no-rows endpoint, DROP through the DDL one: both statements have to
        // work on either side, so exercise the cross pairing rather than each in its own lane.
        await executor.ExecuteNonSQLQuery(Sql(viaDdl, $"RENAME DATABASE {viaDdl} TO {renamed}"));
        DatabaseDescriptor reopened = await executor.OpenDatabase(renamed);
        Assert.AreEqual(renamed, reopened.Name);

        await executor.ExecuteDDLSQL(Sql(viaNonQuery, $"DROP DATABASE {viaNonQuery}"));

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.OpenDatabase(viaNonQuery))!;
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex.Code);
    }

    [Test]
    public async Task CommentOnDatabase_IsAcceptedThroughBothEndpoints()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string dbname = "db_" + Guid.NewGuid().ToString("n")[..8];
        TrackDatabase(dbname, executor);

        await executor.ExecuteDDLSQL(Sql(dbname, $"CREATE DATABASE {dbname}"));

        // Neither call may open a descriptor for the target: the comment lives on the cross-database
        // registry entry, and both entry points must route it the same way.
        await executor.ExecuteDDLSQL(Sql(dbname, $"COMMENT ON DATABASE {dbname} IS 'via ddl'"));
        await executor.ExecuteNonSQLQuery(Sql(dbname, $"COMMENT ON DATABASE {dbname} IS 'via non-query'"));
    }

    [Test]
    public async Task AnInDatabaseStatementStillReachesTheOrdinaryPathOnTheNoRowsEndpoint()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string dbname = "db_" + Guid.NewGuid().ToString("n")[..8];
        TrackDatabase(dbname, executor);
        await executor.ExecuteDDLSQL(Sql(dbname, $"CREATE DATABASE {dbname}"));

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction txn = await database.Transactions.BeginAsync();

        // The shared server-level list must claim only the statements that open no database. A
        // statement it does not claim has to fall through to the ordinary path, transaction and all —
        // otherwise the guard would start swallowing in-database work.
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: txn,
            database: dbname,
            sql: "CREATE TABLE parity_probe (id oid PRIMARY KEY, name string(64))",
            parameters: null));

        await database.Transactions.CommitAsync(txn);

        KvTransaction readTxn = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: readTxn, database: dbname, sql: "SELECT * FROM parity_probe", parameters: null));

        int rows = 0;
        await foreach (QueryResultRow _ in cursor)
            rows++;

        Assert.AreEqual(0, rows);
    }
}
