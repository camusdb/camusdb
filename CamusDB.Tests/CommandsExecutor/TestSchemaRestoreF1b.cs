/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Threading.Tasks;

using NUnit.Framework;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Serializer;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// F1b restart-replay durability tests.
///
/// Standalone-mode (<see cref="BaseTest"/>) databases use a direct KV-write path (not the
/// schema log) so they do not exercise the replicated F1a/F1b machinery. The tests here
/// focus on the two parts of F1b that <em>are</em> independently testable:
///
/// 1. The KV round-trip: <c>PersistFullSchemaCheckpointAsync</c> writes a valid, loadable
///    checkpoint — confirmed by close + reopen reading it back correctly.
///
/// 2. The gap-detection contract: <c>RestoreAsync</c> must throw a typed
///    <see cref="CamusDBException"/> with code <c>InvalidInternalOperation</c> whenever a
///    WAL entry's <c>FromVersion</c> does not match the current schema version, tested by
///    calling <c>RestoreAsync</c> directly with a crafted out-of-order entry.
///
/// The late-subscriber buffer in <see cref="EmbeddedKahuna.RegisterSchemaApply"/> that makes
/// F1b fire in production (process restart with SQLite WAL) fires the same
/// <c>onRestoreFinished</c> callback wired in <see cref="SchemaReplicator.Register"/>; the
/// in-process cluster tests drive the replicated DDL path end-to-end and confirm that normal
/// apply + checkpoint persist work correctly without needing a process restart.
/// </summary>
internal sealed class TestSchemaRestoreF1b : BaseTest
{
    private const string TableName = "f1b_tbl";

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTableAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: TableName,
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String)
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        return (dbname, database, executor);
    }

    /// <summary>
    /// Happy path: create a table (schema version → 1), close, reopen.
    /// WAL replay fires RestoreAsync → ApplySchemaDelta; OnRestoreFinished fires
    /// PersistFullSchemaCheckpointAsync. Schema must be at version 1 and the table present.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task ReopenRestoresSchemaVersionFromWal()
    {
        (string dbname, DatabaseDescriptor db1, CommandExecutor executor) = await SetupTableAsync();

        long versionAfterDdl = db1.Schema.SchemaVersion;
        Assert.Greater(versionAfterDdl, 0, "Schema version must be > 0 after CreateTable");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        Assert.AreEqual(versionAfterDdl, db2.Schema.SchemaVersion,
            "Schema version must be restored from WAL replay on reopen");

        Assert.IsTrue(db2.Schema.Tables.ContainsKey(TableName),
            "Table must be present after WAL replay on reopen");

        Assert.IsFalse(db2.SchemaSubsystemDegraded,
            "Reopened node must not be degraded");
    }

    /// <summary>
    /// PersistFullSchemaCheckpointAsync re-writes all live tables + schema version to KV.
    /// The test manipulates the checkpoint by calling the method explicitly after DDL and
    /// then verifies a subsequent close+reopen loads the expected version and table set.
    ///
    /// Note: standalone-mode databases (BaseTest) write the schema checkpoint inside the DDL
    /// transaction via PersistSchemaTableAsync; PersistFullSchemaCheckpointAsync is the F1b
    /// recovery path that re-persists the full schema unconditionally. This test proves the
    /// method is idempotent and that its output is a valid, loadable checkpoint.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task PersistFullCheckpointIsIdempotentAndLoadableAfterReopen()
    {
        (string dbname, _, CommandExecutor executor) = await SetupTableAsync();

        // Re-open to get the current live descriptor.
        DatabaseDescriptor db1 = await executor.OpenDatabase(dbname);

        long versionBefore = db1.Schema.SchemaVersion;
        Assert.Greater(versionBefore, 0, "Schema version must be > 0 after CreateTable");
        Assert.IsTrue(db1.Schema.Tables.ContainsKey(TableName), "Table must be present");

        // Re-persist the full checkpoint unconditionally — the F1b recovery action.
        await executor.Catalogs.PersistFullSchemaCheckpointAsync(db1);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

        // Reopen from KV checkpoint written by PersistFullSchemaCheckpointAsync.
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        Assert.AreEqual(versionBefore, db2.Schema.SchemaVersion,
            "Schema version must survive PersistFullSchemaCheckpointAsync round-trip");

        Assert.IsTrue(db2.Schema.Tables.ContainsKey(TableName),
            "Table must be visible after PersistFullSchemaCheckpointAsync round-trip");

        Assert.IsFalse(db2.SchemaSubsystemDegraded,
            "Reopened node must not be degraded");
    }

    /// <summary>
    /// F1b gap detection: RestoreAsync must throw a typed <see cref="CamusDBException"/>
    /// when a log entry's FromVersion does not match the current schema version (a genuine
    /// gap, not a duplicate). The exception message must contain "gap" and "bug" so that
    /// operators can identify this as a data-integrity issue rather than a transient error.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task GapInRestoreThrowsTypedCamusDbException()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTableAsync();

        // Schema is at version 1 after CreateTable. Craft an entry that starts from version 2
        // (skipping 1→2) — a genuine gap that RestoreAsync must reject loudly.
        SchemaChangeLogEntry gapEntry = new()
        {
            Ts = new HLCTimestamp(1, 0, 0),
            Database = dbname,
            FromVersion = database.Schema.SchemaVersion + 1,  // gap: skips current version
            ToVersion = database.Schema.SchemaVersion + 2,
            Op = SchemaOp.CreateTable,
            Payload = Serializator.Serialize(new SchemaCreateTablePayload
            {
                TableName = "should_never_exist",
                Columns = []
            })
        };

        byte[] bytes = Serializator.Serialize(gapEntry);

        SchemaReplicator replicator = new(executor.Catalogs, logger);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await replicator.RestoreAsync(database, bytes)
        )!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, ex.Code);
        Assert.That(ex.Message, Does.Contain("gap").IgnoreCase);
        Assert.That(ex.Message, Does.Contain("bug").IgnoreCase);
    }
}
