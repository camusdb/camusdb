
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
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Recovery of dropped objects: <c>CREATE DATABASE/TABLE ... RELINK TO '&lt;id&gt;'</c> reattaches an
/// orphaned (deferred-dropped) database or table under a new name, reusing its preserved id and retained
/// data. Covers the recovery round-trip plus the error cases (unknown id, name already taken).
/// </summary>
[NonParallelizable]
internal sealed class TestRelink : BaseTest
{
    private static string NewName() => "db_" + Guid.NewGuid().ToString("n");

    private async Task InsertRow(string dbName, DatabaseDescriptor db, CommandExecutor executor, string insertSql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, insertSql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private async Task<List<QueryResultRow>> SelectAll(string dbName, DatabaseDescriptor db, CommandExecutor executor, string selectSql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbName, selectSql, null));
        List<QueryResultRow> rows = await System.Linq.AsyncEnumerable.ToListAsync(cursor);
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    private async Task<(string dbName, DatabaseDescriptor db, CommandExecutor executor, string dbId, string tableId)> SetupDbWithRows(int rows)
    {
        string dbName = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(dbName, executor);

        DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists: false));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, name STRING)", parameters: null));

        for (int i = 0; i < rows; i++)
            await InsertRow(dbName, db, executor, $"INSERT INTO robots (id, name) VALUES (gen_id(), \"robot {i}\")");

        string tableId = db.Schema.Tables["robots"].Id!;
        return (dbName, db, executor, db.Id, tableId);
    }

    // -----------------------------------------------------------------------
    // RELINK DATABASE
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task RelinkDatabase_RecoversDataUnderNewName()
    {
        (string dbName, _, CommandExecutor executor, string dbId, _) = await SetupDbWithRows(5);

        await executor.DropDatabase(new DropDatabaseTicket(dbName));

        string recovered = NewName();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: recovered,
            sql: $"CREATE DATABASE {recovered} RELINK TO \"{dbId}\"", parameters: null));
        TrackDatabase(recovered, executor);

        // The recovered database opens under the new name with the same id and all its rows.
        DatabaseDescriptor recoveredDb = await executor.OpenDatabase(recovered);
        Assert.AreEqual(dbId, recoveredDb.Id, "relink must reuse the orphan's id");

        List<QueryResultRow> rows = await SelectAll(recovered, recoveredDb, executor, "SELECT name FROM robots");
        Assert.AreEqual(5, rows.Count, "all rows must be recovered after relink");

        // Orphan record is gone.
        Assert.IsNull(await sharedRegistry!.TryGetDatabaseOrphanAsync(dbId), "orphan record must be removed after relink");
    }

    [Test]
    [NonParallelizable]
    public async Task RelinkDatabase_UnknownId_ThrowsOrphanNotFound()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string target = NewName();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.RelinkDatabase(new RelinkDatabaseTicket(target, "ZZZZ")));
        Assert.AreEqual(CamusDBErrorCodes.OrphanNotFound, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task RelinkDatabase_IntoTakenName_ThrowsAlreadyExists()
    {
        (string dbName, _, CommandExecutor executor, string dbId, _) = await SetupDbWithRows(1);
        await executor.DropDatabase(new DropDatabaseTicket(dbName));

        // Occupy a name, then try to relink the orphan into it.
        string taken = NewName();
        await executor.CreateDatabase(new CreateDatabaseTicket(taken, ifNotExists: false));
        TrackDatabase(taken, executor);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.RelinkDatabase(new RelinkDatabaseTicket(taken, dbId)));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, ex!.Code);

        // The orphan is untouched and still relinkable to a free name.
        Assert.IsNotNull(await sharedRegistry!.TryGetDatabaseOrphanAsync(dbId));
    }

    [Test]
    [NonParallelizable]
    public async Task RelinkDatabase_ThenReDrop_KeepsIdStable()
    {
        (string dbName, _, CommandExecutor executor, string dbId, _) = await SetupDbWithRows(2);
        await executor.DropDatabase(new DropDatabaseTicket(dbName));

        string first = NewName();
        DatabaseDescriptor firstDb = await executor.RelinkDatabase(new RelinkDatabaseTicket(first, dbId));
        TrackDatabase(first, executor);
        Assert.AreEqual(dbId, firstDb.Id);

        // Drop again (deferred) and relink under yet another name — the id is stable across the cycle.
        await executor.DropDatabase(new DropDatabaseTicket(first));
        string second = NewName();
        DatabaseDescriptor secondDb = await executor.RelinkDatabase(new RelinkDatabaseTicket(second, dbId));
        TrackDatabase(second, executor);
        Assert.AreEqual(dbId, secondDb.Id);
    }

    // -----------------------------------------------------------------------
    // RELINK TABLE
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task RelinkTable_RecoversRowsUnderNewName()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, _, string tableId) = await SetupDbWithRows(4);

        await executor.DropTable(new DropTableTicket(dbName, "robots", ifExists: false));
        Assert.IsFalse(new CatalogsManager(logger).TableExists(db, "robots"));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: $"CREATE TABLE androids RELINK TO \"{tableId}\"", parameters: null));

        // The recovered table carries the original rows under the new name.
        List<QueryResultRow> rows = await SelectAll(dbName, db, executor, "SELECT name FROM androids");
        Assert.AreEqual(4, rows.Count, "all rows must be recovered after table relink");

        Assert.IsNull(await new CatalogsManager(logger).TryGetTableOrphanAsync(db, tableId),
            "table orphan record must be removed after relink");
    }

    [Test]
    [NonParallelizable]
    public async Task RelinkTable_UnknownId_ThrowsOrphanNotFound()
    {
        (string dbName, _, CommandExecutor executor, _, _) = await SetupDbWithRows(1);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.RelinkTable(new RelinkTableTicket(dbName, "ghost", "ZZZZ")));
        Assert.AreEqual(CamusDBErrorCodes.OrphanNotFound, ex!.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task RelinkTable_IntoTakenName_ThrowsTableAlreadyExists()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor, _, string tableId) = await SetupDbWithRows(1);
        await executor.DropTable(new DropTableTicket(dbName, "robots", ifExists: false));

        // Create a live table occupying the target name.
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName,
            sql: "CREATE TABLE androids (id OBJECT_ID PRIMARY KEY, name STRING)", parameters: null));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.RelinkTable(new RelinkTableTicket(dbName, "androids", tableId)));
        Assert.AreEqual(CamusDBErrorCodes.TableAlreadyExists, ex!.Code);
    }
}
