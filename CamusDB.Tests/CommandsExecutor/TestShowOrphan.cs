
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

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Orphan discovery: <c>SHOW ORPHAN DATABASES</c> / <c>SHOW ORPHAN TABLES</c> surface the
/// ids (and former names) of deferred-dropped objects so an operator can feed them to
/// <c>CREATE ... RELINK TO</c>. Orphans appear after a deferred drop and disappear once recovered.
/// </summary>
internal sealed class TestShowOrphan : BaseTest
{
    private async Task<List<QueryResultRow>> ShowOrphanDatabasesAsync(CommandExecutor executor)
    {
        // Registry-only: no database context, no transaction.
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: "", sql: "SHOW ORPHAN DATABASES", parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        return rows;
    }

    private async Task<List<QueryResultRow>> ShowOrphanTablesAsync(string dbName, DatabaseDescriptor db, CommandExecutor executor)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbName, "SHOW ORPHAN TABLES", null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    private async Task<(string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId)> SetupDbWithTable()
    {
        string dbName = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(dbName, executor);

        DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists: false));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, name STRING)", parameters: null));

        return (dbName, db, executor, db.Schema.Tables["robots"].Id!);
    }

    // -----------------------------------------------------------------------
    // SHOW ORPHAN DATABASES
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task ShowOrphanDatabases_EmptyWhenNoneDropped()
    {
        CommandExecutor executor = CreateCommandExecutor();
        Assert.IsEmpty(await ShowOrphanDatabasesAsync(executor));
    }

    [Test]
    [NonParallelizable]
    public async Task ShowOrphanDatabases_ListsDroppedDatabase()
    {
        (string dbName, _, CommandExecutor executor, _) = await SetupDbWithTable();
        string dbId = (await executor.OpenDatabase(dbName)).Id;

        await executor.DropDatabase(new DropDatabaseTicket(dbName));

        List<QueryResultRow> rows = await ShowOrphanDatabasesAsync(executor);
        List<QueryResultRow> matching = rows.Where(r => r.Row["id"].StrValue == dbId).ToList();
        Assert.AreEqual(1, matching.Count, "dropped database must appear in SHOW ORPHAN DATABASES");
        Assert.AreEqual(dbName, matching[0].Row["former_name"].StrValue);

        // Timestamps are UTC ISO-8601 (not HLC debug text), and expires_at = dropped_at + retention (7d default).
        string droppedAtStr = matching[0].Row["dropped_at"].StrValue!;
        string expiresAtStr = matching[0].Row["expires_at"].StrValue!;
        DateTimeOffset droppedAt = DateTimeOffset.Parse(droppedAtStr, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal);
        DateTimeOffset expiresAt = DateTimeOffset.Parse(expiresAtStr, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal);
        Assert.IsTrue(droppedAtStr.EndsWith("Z", StringComparison.Ordinal), $"dropped_at must be ISO-8601 UTC, was '{droppedAtStr}'");
        Assert.AreEqual(TimeSpan.FromMilliseconds(CamusDB.Core.CamusDBConfig.OrphanRetentionMs), expiresAt - droppedAt,
            "expires_at must be exactly dropped_at + the retention window");
    }

    [Test]
    [NonParallelizable]
    public async Task ShowOrphanDatabases_DisappearsAfterRelink()
    {
        (string dbName, _, CommandExecutor executor, _) = await SetupDbWithTable();
        string dbId = (await executor.OpenDatabase(dbName)).Id;

        await executor.DropDatabase(new DropDatabaseTicket(dbName));
        Assert.IsTrue((await ShowOrphanDatabasesAsync(executor)).Any(r => r.Row["id"].StrValue == dbId));

        string recovered = "db_" + Guid.NewGuid().ToString("n");
        await executor.RelinkDatabase(new RelinkDatabaseTicket(recovered, dbId));
        TrackDatabase(recovered, executor);

        Assert.IsFalse((await ShowOrphanDatabasesAsync(executor)).Any(r => r.Row["id"].StrValue == dbId),
            "recovered database must no longer appear in SHOW ORPHAN DATABASES");
    }

    // -----------------------------------------------------------------------
    // SHOW ORPHAN TABLES
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task ShowOrphanTables_EmptyWhenNoneDropped()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, _) = await SetupDbWithTable();
        Assert.IsEmpty(await ShowOrphanTablesAsync(dbName, db, executor));
    }

    [Test]
    [NonParallelizable]
    public async Task ShowOrphanTables_ListsDroppedTable()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId) = await SetupDbWithTable();

        await executor.DropTable(new DropTableTicket(dbName, "robots", ifExists: false));

        List<QueryResultRow> rows = await ShowOrphanTablesAsync(dbName, db, executor);
        List<QueryResultRow> matching = rows.Where(r => r.Row["id"].StrValue == tableId).ToList();
        Assert.AreEqual(1, matching.Count, "dropped table must appear in SHOW ORPHAN TABLES");
        Assert.AreEqual("robots", matching[0].Row["former_name"].StrValue);
        Assert.IsNotEmpty(matching[0].Row["dropped_at"].StrValue!);
    }

    [Test]
    [NonParallelizable]
    public async Task ShowOrphanTables_DisappearsAfterRelink()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, string tableId) = await SetupDbWithTable();

        await executor.DropTable(new DropTableTicket(dbName, "robots", ifExists: false));
        Assert.IsTrue((await ShowOrphanTablesAsync(dbName, db, executor)).Any(r => r.Row["id"].StrValue == tableId));

        await executor.RelinkTable(new RelinkTableTicket(dbName, "androids", tableId));

        Assert.IsFalse((await ShowOrphanTablesAsync(dbName, db, executor)).Any(r => r.Row["id"].StrValue == tableId),
            "recovered table must no longer appear in SHOW ORPHAN TABLES");
    }
}
