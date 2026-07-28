
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
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Hardening coverage for recoverable drops: relinking a table that was <c>ALTER</c>ed before the drop
/// must recover rows written under <em>every</em> schema version (not just the drop-time layout), and
/// relink must be idempotent by id so a retried recovery never mints a second live name for one
/// physical keyspace.
/// </summary>
internal sealed class TestRelinkHardening : BaseTest
{
    private async Task InsertRow(string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private async Task<List<QueryResultRow>> SelectAll(string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>
    /// A table altered before the drop is relinked; rows written under the original version and under the
    /// post-ALTER version must all decode — immediately after relink and after a close/reopen. Under the
    /// old Version-0 reattach this threw <c>SystemSpaceCorrupt</c> on the post-ALTER rows.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task RelinkAlteredTable_RecoversRowsFromEveryVersion()
    {
        string dbName = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(dbName, executor);
        DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists: false));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, dbName,
            "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, name STRING)", null));

        // Version 0 rows (before the ALTER).
        await InsertRow(dbName, db, executor, "INSERT INTO robots (id, name) VALUES (gen_id(), \"pre1\")");
        await InsertRow(dbName, db, executor, "INSERT INTO robots (id, name) VALUES (gen_id(), \"pre2\")");

        // ALTER bumps the table's schema version.
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, dbName, "ALTER TABLE robots ADD year INT64", null));

        // Version 1 rows (after the ALTER — they carry the new version).
        await InsertRow(dbName, db, executor, "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"post1\", 2001)");
        await InsertRow(dbName, db, executor, "INSERT INTO robots (id, name, year) VALUES (gen_id(), \"post2\", 2002)");

        string tableId = db.Schema.Tables["robots"].Id!;

        // Deferred drop, then relink under a new name.
        Assert.IsTrue(await executor.DropTable(new DropTableTicket(dbName, "robots", ifExists: false)));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, dbName, $"CREATE TABLE androids RELINK TO \"{tableId}\"", null));

        // Immediate query: rows from BOTH schema versions must decode.
        List<QueryResultRow> immediate = await SelectAll(dbName, db, executor, "SELECT name, year FROM androids");
        Assert.AreEqual(4, immediate.Count, "all rows (pre- and post-ALTER) must decode immediately after relink");

        // Close and reopen the database so the table reloads from disk, then query again.
        await executor.CloseDatabase(new CloseDatabaseTicket(dbName));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbName);
        List<QueryResultRow> afterReopen = await SelectAll(dbName, reopened, executor, "SELECT name, year FROM androids");
        Assert.AreEqual(4, afterReopen.Count, "all rows must still decode after close/reopen");
    }

    /// <summary>
    /// A stale orphan record (relink committed its registration but crashed before deleting the record)
    /// must not let a retry create a second alias. A second relink of the same id to the same name is an
    /// idempotent success; to a different name it is rejected.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task RelinkDatabase_RepeatedById_IsIdempotentAndRejectsSecondName()
    {
        string dbName = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(dbName, executor);
        DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists: false));
        string dbId = db.Id;

        await executor.DropDatabase(new DropDatabaseTicket(dbName));

        string recovered = "db_" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor first = await executor.RelinkDatabase(new RelinkDatabaseTicket(recovered, dbId));
        TrackDatabase(recovered, executor);
        Assert.AreEqual(dbId, first.Id);

        // Simulate the crash window: re-plant a stale orphan record for the now-live id.
        await sharedRegistry!.WriteDatabaseOrphanAsync(new OrphanDatabaseRecord
        {
            Id = dbId,
            FormerName = dbName,
            DroppedAt = default,
        });

        // Retry to the SAME name → idempotent success (same id), and it cleans the stale record.
        DatabaseDescriptor retry = await executor.RelinkDatabase(new RelinkDatabaseTicket(recovered, dbId));
        Assert.AreEqual(dbId, retry.Id);
        Assert.IsNull(await sharedRegistry.TryGetDatabaseOrphanAsync(dbId), "stale orphan must be cleaned by the idempotent retry");

        // Re-plant again and retry to a DIFFERENT name → rejected (no second alias for one id).
        await sharedRegistry.WriteDatabaseOrphanAsync(new OrphanDatabaseRecord { Id = dbId, FormerName = dbName, DroppedAt = default });
        string secondName = "db_" + Guid.NewGuid().ToString("n");
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.RelinkDatabase(new RelinkDatabaseTicket(secondName, dbId)));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, ex!.Code);
    }

    /// <summary>
    /// Table relink is idempotent by id too: once a table id is live under a name, relinking that id to a
    /// second name is rejected (the schema-level id guard), so a stale-orphan retry cannot alias one
    /// table keyspace under two names.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task RelinkTable_SecondAliasForSameId_Rejected()
    {
        string dbName = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(dbName, executor);
        DatabaseDescriptor db = await executor.CreateDatabase(new CreateDatabaseTicket(dbName, ifNotExists: false));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, dbName,
            "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, name STRING)", null));
        string tableId = db.Schema.Tables["robots"].Id!;

        Assert.IsTrue(await executor.DropTable(new DropTableTicket(dbName, "robots", ifExists: false)));
        Assert.IsTrue(await executor.RelinkTable(new RelinkTableTicket(dbName, "androids", tableId)));

        // The id is now live under 'androids'; relinking it to another name is rejected by the id guard
        // (no second alias for one physical keyspace).
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.RelinkTable(new RelinkTableTicket(dbName, "cyborgs", tableId)));
        Assert.AreEqual(CamusDBErrorCodes.TableAlreadyExists, ex!.Code);
    }
}
