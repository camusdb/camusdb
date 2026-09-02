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
using CamusDB.Core.Catalogs.Replication;
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
        byte[] bytes = SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 0, 1));

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
        byte[] bytes = SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 2, 3));

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
            SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 0, 1, "robots_a"))
        ));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => replicator.ApplyAsync(
            database,
            partitionId,
            SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 0, 1, "robots_b"))
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
        byte[] bytes = SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 0, 1, "robots_a"));

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
            SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 0, 1))
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
        byte[] bytes = SchemaChangeLogEntryCodec.Encode(CreateTableEntry("other_db", 0, 1));

        Assert.True(await replicator.ApplyAsync(database, partitionId, bytes));

        Assert.AreEqual(0, database.Schema.SchemaVersion);
        Assert.AreEqual(0, database.Schema.Tables.Count);
    }

    [Test]
    public async Task ApplyAsync_ForeignDatabaseEntryCostsNoDecodeAndNoAllocation()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);
        byte[] bytes = SchemaChangeLogEntryCodec.Encode(CreateTableEntry("other_db", 0, 1));

        // Warm up: the very first call JITs the whole apply path, which allocates once and would
        // swamp the measurement of the steady-state skip.
        Assert.True(await replicator.ApplyAsync(database, partitionId, bytes));

        // The assertion stays outside the measured loop: an NUnit constraint allocates, and it
        // would be counted against the code under test.
        //
        // The bound rather than zero: a debug build emits every async state machine as a class, so
        // each call heap-allocates one whatever its body does. What the bound proves is that
        // nothing else is allocated — decoding this entry costs well over a kilobyte in the entry
        // object, its payload array and the JSON reader's buffers.
        bool skipped = true;
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100; i++)
            skipped &= await replicator.ApplyAsync(database, partitionId, bytes);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;

        Assert.True(skipped);

        Assert.AreEqual(0, database.SchemaEntriesDecoded, "an entry for another database must never be deserialized");
        Assert.LessOrEqual(allocated, 100 * AsyncCallAllocationBound, "dropping an entry for another database must allocate nothing of its own");
        Assert.AreEqual(0, database.Schema.SchemaVersion);
    }

    [Test]
    public async Task ApplyAsync_RedeliveredEntryCostsNoSecondDecode()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);
        byte[] bytes = SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 0, 1));

        // The shape a proposer sees: the same bytes arrive through replication and again through
        // the local apply that lets it observe its own change.
        Assert.True(await replicator.ApplyAsync(database, partitionId, bytes));
        Assert.AreEqual(1, database.SchemaEntriesDecoded);

        Assert.True(await replicator.ApplyAsync(database, partitionId, bytes));

        Assert.AreEqual(1, database.SchemaEntriesDecoded, "the second delivery must be dropped from the frame");
        Assert.AreEqual(1, database.Schema.SchemaVersion);

        bool acked = await database.Kahuna.WaitForSchemaAcksAsync(
            db,
            1,
            TimeSpan.FromMilliseconds(100),
            liveNodeLease: TimeSpan.FromMinutes(1),
            cancellationToken: CancellationToken.None
        );

        Assert.IsTrue(acked, "the dropped delivery must still re-ack the version it names");
    }

    [Test]
    public async Task ApplyAsync_PreFramingForeignDatabaseEntryStillDecodes()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);

        // An entry written before the frame existed carries no header, so it can only be dropped
        // after a full decode. That path has to keep working until log compaction retires them.
        byte[] bytes = Serializator.Serialize(CreateTableEntry("other_db", 0, 1));

        Assert.True(await replicator.ApplyAsync(database, partitionId, bytes));

        Assert.AreEqual(1, database.SchemaEntriesDecoded);
        Assert.AreEqual(0, database.Schema.SchemaVersion);
    }

    [Test]
    public async Task ApplyAsync_CorruptFramedEntryFailsLoudly()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);

        // Header intact, body cut in half: the entry names this database and a version this node has
        // not reached, so it must reach the decode and surface rather than be silently dropped.
        byte[] bytes = SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 0, 1));
        byte[] truncated = bytes.AsSpan(0, bytes.Length - (bytes.Length / 3)).ToArray();

        Assert.ThrowsAsync<CamusDBException>(() => replicator.ApplyAsync(database, partitionId, truncated));

        Assert.AreEqual(0, database.Schema.SchemaVersion);
    }

    [Test]
    public async Task RestoreAsync_ForeignDatabaseEntryCostsNoDecodeAndNoAllocation()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        byte[] bytes = SchemaChangeLogEntryCodec.Encode(CreateTableEntry("other_db", 0, 1));

        Assert.True(await replicator.RestoreAsync(database, bytes));

        bool skipped = true;
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100; i++)
            skipped &= await replicator.RestoreAsync(database, bytes);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;

        Assert.True(skipped);

        Assert.AreEqual(0, database.SchemaEntriesDecoded);
        Assert.LessOrEqual(allocated, 100 * AsyncCallAllocationBound);
        Assert.AreEqual(0, database.Schema.SchemaVersion);
    }

    [Test]
    public async Task RestoreAsync_EntryAlreadyInTheCheckpointCostsNoDecode()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();

        // Replay always starts from the committed tail, so a node whose checkpoint is already ahead
        // of an entry sees it again with nothing left to do.
        database.Schema.SchemaVersion = 4;

        Assert.True(await replicator.RestoreAsync(database, SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 0, 1))));

        Assert.AreEqual(0, database.SchemaEntriesDecoded);
        Assert.AreEqual(4, database.Schema.SchemaVersion);
        Assert.AreEqual(0, database.Schema.Tables.Count);
    }

    [Test]
    public async Task RestoreAsync_ReplaysPreFramingAndFramedEntriesInOneChain()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();

        // A log that spans the format change: entries written by the old build, then by this one.
        Assert.True(await replicator.RestoreAsync(database, Serializator.Serialize(LegacyCreateTableEntry(db, 0, 1, "robots_a"))));
        Assert.True(await replicator.RestoreAsync(database, SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 1, 2, "robots_b"))));

        Assert.AreEqual(2, database.Schema.SchemaVersion);
        Assert.True(database.Schema.Tables.ContainsKey("robots_a"));
        Assert.True(database.Schema.Tables.ContainsKey("robots_b"));
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

        byte[] bytes = SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 0, 1));

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
            SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 0, 1))
        ));

        TableSchema tableSchema = database.Schema.Tables["robots"];
        database.TableDescriptors["robots"] = new AsyncLazy<TableDescriptor>(() => Task.FromResult(
            new TableDescriptor(tableSchema.Id!, tableSchema.Name!, tableSchema, new KvTableStore(kahuna.Kahuna, CamusDBOptions.Default, database.Id, tableSchema.Id!))
        ));

        Assert.AreEqual(1, database.TableDescriptors.Count);

        Assert.True(await replicator.ApplyAsync(
            database,
            partitionId,
            SchemaChangeLogEntryCodec.Encode(DropTableEntry(db, 1, 2))
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

        byte[] bytes = SchemaChangeLogEntryCodec.Encode(AddColumnEntry(db, 0, 1));

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

        byte[] bytes = SchemaChangeLogEntryCodec.Encode(SetElementStateEntry(
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
        byte[] bytes = SchemaChangeLogEntryCodec.Encode(CreateTableEntry(db, 2, 3));

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

    /// <summary>
    /// Bytes one call to an async method may allocate before the measurement means something. A
    /// debug build emits the state machine as a class, so the allocation exists no matter what the
    /// method does; it is two orders of magnitude below the cost of deserializing an entry.
    /// </summary>
    private const long AsyncCallAllocationBound = 256;

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
            transactions: new KvTransactionsManager(kahuna.Kahuna, CamusDBOptions.Default),
            tableDescriptors: new ConcurrentDictionary<string, AsyncLazy<TableDescriptor>>(),
            options: CamusDBOptions.Default
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
            Payload = SchemaChangeLogEntryCodec.EncodePayload(new SchemaCreateTablePayload
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

    /// <summary>
    /// An entry exactly as the engine wrote it before the frame existed: UTF-16 JSON around a
    /// UTF-16 JSON payload. Callers still have to encode it with <c>Serializator.Serialize</c>;
    /// this only builds the object whose payload bytes are in the old form.
    /// </summary>
    private static SchemaChangeLogEntry LegacyCreateTableEntry(string db, long fromVersion, long toVersion, string tableName = "robots")
    {
        SchemaChangeLogEntry entry = CreateTableEntry(db, fromVersion, toVersion, tableName);
        entry.Payload = Serializator.Serialize(entry.GetPayload<SchemaCreateTablePayload>());
        entry.PayloadFormat = SchemaPayloadFormat.Utf16Legacy;
        return entry;
    }

    private static SchemaChangeLogEntry AddColumnEntry(string db, long fromVersion, long toVersion)
    {
        return new()
        {
            Database = db,
            FromVersion = fromVersion,
            ToVersion = toVersion,
            Op = SchemaOp.AddColumn,
            Payload = SchemaChangeLogEntryCodec.EncodePayload(new SchemaAlterColumnPayload
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
            Payload = SchemaChangeLogEntryCodec.EncodePayload(new SchemaDropTablePayload { TableName = "robots" })
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
            Payload = SchemaChangeLogEntryCodec.EncodePayload(new SchemaElementStatePayload
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
