/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

using Microsoft.Extensions.Logging;

using Nito.AsyncEx;

using NUnit.Framework;

namespace CamusDB.Tests.Catalogs;

[TestFixture]
public sealed class TestSchemaReplicator
{
    private static readonly ILoggerFactory LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
        b.AddFilter("Camus", LogLevel.Warning));

    [Test]
    public async Task ApplyAsync_ReplayedEntryIsNoOp()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);
        byte[] bytes = Serializator.Serialize(CreateTableEntry(db, 0, 1));

        Assert.True(await replicator.ApplyAsync(database, partitionId, bytes));
        Assert.True(await replicator.ApplyAsync(database, partitionId, bytes));

        Assert.AreEqual(1, database.Schema.SchemaVersion);
        Assert.AreEqual(1, database.Schema.Tables.Count);
        Assert.True(database.Schema.Tables.ContainsKey("robots"));
    }

    [Test]
    public async Task ApplyAsync_OutOfOrderEntryThrows()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);
        byte[] bytes = Serializator.Serialize(CreateTableEntry(db, 2, 3));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => replicator.ApplyAsync(database, partitionId, bytes));

        Assert.NotNull(ex);
        Assert.That(ex!.Message, Does.Contain("out of order"));
        Assert.AreEqual(0, database.Schema.SchemaVersion);
        Assert.AreEqual(0, database.Schema.Tables.Count);
    }

    [Test]
    public async Task ApplyAsync_SameTargetVersionDifferentDeltaThrowsStaleVersion()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);

        Assert.True(await replicator.ApplyAsync(
            database,
            partitionId,
            Serializator.Serialize(CreateTableEntry(db, 0, 1, "robots_a"))
        ));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => replicator.ApplyAsync(
            database,
            partitionId,
            Serializator.Serialize(CreateTableEntry(db, 0, 1, "robots_b"))
        ));

        Assert.NotNull(ex);
        Assert.That(ex!.Message, Does.Contain("out of order"));
        Assert.True(database.Schema.Tables.ContainsKey("robots_a"));
        Assert.False(database.Schema.Tables.ContainsKey("robots_b"));
        Assert.AreEqual(1, database.Schema.SchemaVersion);
    }

    [Test]
    public async Task ApplyAsync_ReplayedSameTargetVersionSameDeltaIsNoOp()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);
        byte[] bytes = Serializator.Serialize(CreateTableEntry(db, 0, 1, "robots_a"));

        Assert.True(await replicator.ApplyAsync(database, partitionId, bytes));
        Assert.True(await replicator.ApplyAsync(database, partitionId, bytes));

        Assert.True(database.Schema.Tables.ContainsKey("robots_a"));
        Assert.AreEqual(1, database.Schema.Tables.Count);
        Assert.AreEqual(1, database.Schema.SchemaVersion);
    }

    [Test]
    public async Task ApplyAsync_RecordsSchemaAckAfterSuccessfulApply()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);

        Assert.True(await replicator.ApplyAsync(
            database,
            partitionId,
            Serializator.Serialize(CreateTableEntry(db, 0, 1))
        ));

        bool acked = await database.Kahuna.WaitForSchemaAcksAsync(
            db,
            1,
            TimeSpan.FromMilliseconds(100),
            liveNodeLease: TimeSpan.FromMinutes(1),
            cancellationToken: CancellationToken.None
        );

        Assert.IsTrue(acked);
    }

    [Test]
    public async Task Register_RecordsLoadedSchemaVersionAck()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        database.Schema.SchemaVersion = 4;

        SchemaReplicator replicator = CreateReplicator();
        replicator.Register(database);

        bool acked = await database.Kahuna.WaitForSchemaAcksAsync(
            db,
            4,
            TimeSpan.FromMilliseconds(100),
            liveNodeLease: TimeSpan.FromMinutes(1),
            cancellationToken: CancellationToken.None
        );

        Assert.IsTrue(acked);
    }

    [Test]
    public async Task ApplyAsync_IgnoresEntriesForOtherDatabases()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);
        byte[] bytes = Serializator.Serialize(CreateTableEntry("other_db", 0, 1));

        Assert.True(await replicator.ApplyAsync(database, partitionId, bytes));

        Assert.AreEqual(0, database.Schema.SchemaVersion);
        Assert.AreEqual(0, database.Schema.Tables.Count);
    }

    [Test]
    public async Task ApplyAsync_AppliesInMemoryWithoutPersistingCheckpoint()
    {
        // ApplyAsync runs inside the schema partition's commit pipeline, so it must only
        // mutate in-memory schema and never issue checkpoint KV writes (which would re-enter
        // the same partition and deadlock). The durable checkpoint is written separately by
        // the proposer (CatalogsManager.ReplicateAndWaitLocalApplyAsync).
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        CatalogsManager catalogs = CreateCatalogs();
        SchemaReplicator replicator = CreateReplicator(catalogs);
        int partitionId = database.Kahuna.SchemaLogPartition(db);
        await database.Kahuna.Raft.WaitForLeader(partitionId, CancellationToken.None);

        byte[] bytes = Serializator.Serialize(CreateTableEntry(db, 0, 1));

        Assert.True(await replicator.ApplyAsync(database, partitionId, bytes));

        // In-memory schema advanced...
        Assert.AreEqual(1, database.Schema.SchemaVersion);
        Assert.True(database.Schema.Tables.ContainsKey("robots"));

        // ...but nothing was persisted, so a fresh descriptor loaded from KV sees nothing.
        DatabaseDescriptor reopened = CreateDescriptor(db, kahuna);
        await catalogs.LoadMetaAsync(reopened);

        Assert.AreEqual(0, reopened.Schema.SchemaVersion);
        Assert.AreEqual(0, reopened.Schema.Tables.Count);
    }

    [Test]
    public async Task ApplyAsync_DropTableInvalidatesOpenDescriptorCache()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);

        Assert.True(await replicator.ApplyAsync(
            database,
            partitionId,
            Serializator.Serialize(CreateTableEntry(db, 0, 1))
        ));

        TableSchema tableSchema = database.Schema.Tables["robots"];
        database.TableDescriptors["robots"] = new AsyncLazy<TableDescriptor>(() => Task.FromResult(
            new TableDescriptor(tableSchema.Id!, tableSchema.Name!, tableSchema, new KvTableStore(kahuna.Kahuna, CamusDBConfig.Ambient, database.Id, tableSchema.Id!))
        ));

        Assert.AreEqual(1, database.TableDescriptors.Count);

        Assert.True(await replicator.ApplyAsync(
            database,
            partitionId,
            Serializator.Serialize(DropTableEntry(db, 1, 2))
        ));

        Assert.AreEqual(0, database.TableDescriptors.Count);
        Assert.False(database.Schema.Tables.ContainsKey("robots"));
    }

    [Test]
    public async Task RestoreAsync_AppliesInMemoryWithoutCheckpointPersist()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();

        database.Schema.Tables["robots"] = new()
        {
            Id = null,
            Name = "robots",
            Version = 0,
            Columns = [new("id-col", "id", ColumnType.Id, true, null)],
            SchemaHistory = [new() { Version = 0, Columns = [new("id-col", "id", ColumnType.Id, true, null)] }]
        };

        byte[] bytes = Serializator.Serialize(AddColumnEntry(db, 0, 1));

        Assert.True(await replicator.RestoreAsync(database, bytes));

        Assert.AreEqual(1, database.Schema.SchemaVersion);
        Assert.AreEqual(1, database.Schema.Tables["robots"].Version);
        Assert.AreEqual(2, database.Schema.Tables["robots"].Columns!.Count);
    }

    [Test]
    public async Task RestoreAsync_ResumesCommittedElementStateTransition()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();

        database.Schema.Tables["robots"] = new()
        {
            Id = "robots-id",
            Name = "robots",
            Version = 0,
            Columns =
            [
                new("id-col", "id", ColumnType.Id, true, null),
                new("enabled-col", "enabled", ColumnType.Bool, false, null, SchemaElementState.DeleteOnly)
            ],
            SchemaHistory =
            [
                new()
                {
                    Version = 0,
                    Columns =
                    [
                        new("id-col", "id", ColumnType.Id, true, null),
                        new("enabled-col", "enabled", ColumnType.Bool, false, null, SchemaElementState.DeleteOnly)
                    ]
                }
            ]
        };

        byte[] bytes = Serializator.Serialize(SetElementStateEntry(
            db,
            0,
            1,
            "enabled",
            SchemaElementState.WriteOnly
        ));

        Assert.True(await replicator.RestoreAsync(database, bytes));

        TableSchema table = database.Schema.Tables["robots"];
        Assert.AreEqual(1, database.Schema.SchemaVersion);
        Assert.AreEqual(1, table.Version);
        Assert.AreEqual(SchemaElementState.WriteOnly, table.Columns!.Single(column => column.Name == "enabled").State);
        Assert.AreEqual(2, table.SchemaHistory!.Count);
    }

    [Test]
    public async Task RestoreAsync_OutOfOrderEntryThrowsGapException()
    {
        // F1b policy: gaps in the committed log are data-corruption or bugs, not silent skips.
        // RestoreAsync must throw CamusDBException so the caller surfaces the inconsistency.
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        byte[] bytes = Serializator.Serialize(CreateTableEntry(db, 2, 3));

        Assert.ThrowsAsync<CamusDBException>(
            async () => await replicator.RestoreAsync(database, bytes),
            "A gap in the committed schema log must throw, not silently skip (F1b fail-loud policy)");

        Assert.AreEqual(0, database.Schema.SchemaVersion);
        Assert.AreEqual(0, database.Schema.Tables.Count);
    }

    [Test]
    public async Task DatabaseDescriptor_DisposesSchemaReplicationSubscriptionBeforeSchema()
    {
        await using EmbeddedKahuna kahuna = new();

        string db = $"db_{Guid.NewGuid():N}";
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        CountingSubscription first = new(() => Assert.DoesNotThrow(() => ProbeSchemaSemaphore(database)));
        CountingSubscription second = new(() => Assert.DoesNotThrow(() => ProbeSchemaSemaphore(database)));

        database.SetSchemaReplicationSubscription(first);
        database.SetSchemaReplicationSubscription(second);

        Assert.AreEqual(1, first.DisposeCount);
        Assert.AreEqual(0, second.DisposeCount);

        database.Dispose();

        Assert.AreEqual(1, second.DisposeCount);
    }

    private static void ProbeSchemaSemaphore(DatabaseDescriptor database)
    {
        if (database.Schema.Semaphore.Wait(0))
            database.Schema.Semaphore.Release();
    }

    private static SchemaReplicator CreateReplicator(CatalogsManager? catalogs = null)
    {
        ILogger<ICamusDB> logger = LoggerFactory.CreateLogger<ICamusDB>();
        return new(catalogs ?? new CatalogsManager(logger), logger);
    }

    private static CatalogsManager CreateCatalogs()
    {
        ILogger<ICamusDB> logger = LoggerFactory.CreateLogger<ICamusDB>();
        return new(logger);
    }

    private static DatabaseDescriptor CreateDescriptor(string db, EmbeddedKahuna kahuna)
    {
        // Use db as both Id and Name so entry.Database == database.Id and
        // SchemaLogPartition(id) == SchemaLogPartition(db) in all replicator tests.
        return new(
            id: db,
            name: db,
            kahuna: kahuna,
            transactions: new KvTransactionsManager(kahuna.Kahuna, CamusDBConfig.Ambient),
            tableDescriptors: new ConcurrentDictionary<string, AsyncLazy<TableDescriptor>>(),
            options: CamusDBConfig.Ambient
        );
    }

    private static string NextSchemaLogDatabaseName(EmbeddedKahuna kahuna)
    {
        for (int i = 0; i < 100; i++)
        {
            string db = $"db_{Guid.NewGuid():N}";
            try
            {
                _ = kahuna.SchemaLogPartition(db);
                return db;
            }
            catch (CamusDBException)
            {
            }
        }

        throw new AssertionException("Could not generate a database name whose schema log partition is not reserved");
    }

    private static SchemaChangeLogEntry CreateTableEntry(string db, long fromVersion, long toVersion, string tableName = "robots")
    {
        return new()
        {
            Database = db,
            FromVersion = fromVersion,
            ToVersion = toVersion,
            Op = SchemaOp.CreateTable,
            Payload = Serializator.Serialize(new SchemaCreateTablePayload
            {
                TableName = tableName,
                Columns =
                [
                    new()
                    {
                        Name = "id",
                        Type = ColumnType.Id,
                        NotNull = true
                    },
                    new()
                    {
                        Name = "name",
                        Type = ColumnType.String
                    }
                ]
            })
        };
    }

    private static SchemaChangeLogEntry AddColumnEntry(string db, long fromVersion, long toVersion)
    {
        return new()
        {
            Database = db,
            FromVersion = fromVersion,
            ToVersion = toVersion,
            Op = SchemaOp.AddColumn,
            Payload = Serializator.Serialize(new SchemaAlterColumnPayload
            {
                TableName = "robots",
                Column = new()
                {
                    Name = "name",
                    Type = ColumnType.String
                }
            })
        };
    }

    private static SchemaChangeLogEntry DropTableEntry(string db, long fromVersion, long toVersion)
    {
        return new()
        {
            Database = db,
            FromVersion = fromVersion,
            ToVersion = toVersion,
            Op = SchemaOp.DropTable,
            Payload = Serializator.Serialize(new SchemaDropTablePayload { TableName = "robots" })
        };
    }

    private static SchemaChangeLogEntry SetElementStateEntry(
        string db,
        long fromVersion,
        long toVersion,
        string elementName,
        SchemaElementState state
    )
    {
        return new()
        {
            Database = db,
            FromVersion = fromVersion,
            ToVersion = toVersion,
            Op = SchemaOp.SetElementState,
            Payload = Serializator.Serialize(new SchemaElementStatePayload
            {
                TableName = "robots",
                ElementName = elementName,
                State = state
            })
        };
    }

    private sealed class CountingSubscription : IDisposable
    {
        private readonly Action onDispose;

        public int DisposeCount { get; private set; }

        public CountingSubscription(Action onDispose)
        {
            this.onDispose = onDispose;
        }

        public void Dispose()
        {
            DisposeCount++;
            onDispose();
        }
    }
}
