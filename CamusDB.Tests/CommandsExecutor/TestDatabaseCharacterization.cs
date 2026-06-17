
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

/**
 * Explicit-database invariants: opening or operating on an unknown database is rejected
 * (no magic-create), the descriptor cache is keyed by the opaque id, and every storage
 * key uses the id for rename-safety (DB2 + DB3). The name is kept for display only.
 *
 * ┌──────────────────────────────────────────────────────────────────────────┐
 * │ ID-vs-NAME AUDIT (post-DB3, 2026-06-17)                                 │
 * │                                                                          │
 * │ USES Id  (storage / routing — rename-safe)                               │
 * │   Filesystem path      DataDirectory/{id}/kv, /wal                      │
 * │   Descriptor cache     DatabaseRegistry.GetOrAdd(id, …)                 │
 * │   CatalogsManager      all key builders receive dbId (database.Id)      │
 * │     "{dbId}/meta/system", "{dbId}/meta/version"                         │
 * │     "{dbId}/meta/table/{tableId}"                                        │
 * │     "{dbId}/meta/history/{tableId}/{version}"                           │
 * │     "{dbId}/meta/coordinator/{table}~{element}"                          │
 * │   SchemaChangeLogEntry .Database = database.Id                           │
 * │   Schema-partition routing                                               │
 * │     SchemaLogPartition("{id}/meta"), AmISchemaLeaderAsync(id, …)        │
 * │     WaitForSchemaLeaderAsync(id, …), StepDownSchemaPartitionAsync(id)   │
 * │     ReplicateSchemaChangeAsync(id, …), WaitForSchemaAcksAsync(id, …)   │
 * │     RecordAndPublishSchemaApplied(id, …), RegisterSchemaApply(id, …)   │
 * │   StatisticsManager                                                      │
 * │     CacheKey  → "{database.Id}:{tableId}"                               │
 * │     KahunaKey → "{database.Id}:stats:{tableId}"                         │
 * │     FlushAllAsync prefix scan → string.Concat(database.Id, ":")         │
 * │                                                                          │
 * │ USES Name  (display only — user-visible, no storage meaning)             │
 * │   Error messages, log strings, diagnostic SchemaDiag output             │
 * └──────────────────────────────────────────────────────────────────────────┘
 */

using NUnit.Framework;
using System.IO;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies the explicit-database lifecycle: unknown databases are rejected rather than
/// auto-created, the descriptor cache is keyed by the opaque id, and the on-disk layout
/// uses the id (not the name).
/// </summary>
internal sealed class TestDatabaseCharacterization : BaseTest
{
    /// <summary>
    /// Opening a never-created database throws DatabaseDoesntExist and creates no directory.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestOpenNeverCreatedDatabase_ThrowsDatabaseDoesntExist()
    {
        string dbname = System.Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.OpenDatabase(dbname));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);

        string expectedPath = Path.Combine(CamusConfig.DataDirectory, dbname);
        Assert.IsFalse(Directory.Exists(expectedPath),
            "No directory must be created for an unknown database name.");

        await Task.CompletedTask;
    }

    /// <summary>
    /// DDL on an unknown database throws DatabaseDoesntExist and creates no directory.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestDdlOnNeverCreatedDatabase_ThrowsDatabaseDoesntExist()
    {
        string dbname = System.Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "dummy",
            columns: [new("id", ColumnType.Id)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false
        );

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            async () => await executor.CreateTable(tableTicket));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code);

        string expectedPath = Path.Combine(CamusConfig.DataDirectory, dbname);
        Assert.IsFalse(Directory.Exists(expectedPath),
            "No directory must be created for an unknown database name.");

        await Task.CompletedTask;
    }

    /// <summary>
    /// The in-memory descriptor cache is keyed by the stable opaque database id, so two
    /// opens of the same database return the same descriptor instance.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestOpenSameDatabaseTwice_ReturnsSameDescriptor()
    {
        (string dbname, DatabaseDescriptor first, CommandExecutor executor) = await CreateDatabase();

        DatabaseDescriptor second = await executor.OpenDatabase(dbname);

        // Same object — the cache is keyed by id and returns the existing lazy.
        Assert.AreSame(first, second,
            "Descriptor cache must return the same instance for the same database.");
    }

    /// <summary>
    /// The on-disk layout is DataDirectory/{id}/kv and /wal, where the id is an opaque
    /// 24-hex string that does NOT equal the database name.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestCreateDatabase_DirectoryUsesId()
    {
        (string dbname, DatabaseDescriptor descriptor, CommandExecutor executor) = await CreateDatabase();

        string id = descriptor.Id;
        Assert.IsNotEmpty(id, "Descriptor must carry a non-empty id after creation");
        Assert.AreNotEqual(dbname, id, "Id must be an opaque value distinct from the name");

        string expectedKv = Path.Combine(CamusConfig.DataDirectory, id, "kv");
        string expectedWal = Path.Combine(CamusConfig.DataDirectory, id, "wal");

        Assert.IsTrue(Directory.Exists(expectedKv),
            "kv sub-directory must exist under DataDirectory/{id}/kv");
        Assert.IsTrue(Directory.Exists(expectedWal),
            "wal sub-directory must exist under DataDirectory/{id}/wal");

        // No directory for the name — name is NOT the identity anymore.
        string namePath = Path.Combine(CamusConfig.DataDirectory, dbname);
        Assert.IsFalse(Directory.Exists(namePath),
            "No directory must exist with the database name as its path component");
    }
}
