/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kahuna;
using Kahuna.Server.Communication.Internode;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

[TestFixture]
[NonParallelizable]
public sealed class TestSchemaDdlForwarding
{
    private static readonly ILoggerFactory LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
        builder.AddFilter("Camus", LogLevel.Warning));

    private static readonly ILogger<ICamusDB> Logger = LoggerFactory.CreateLogger<ICamusDB>();

    [Test]
    public async Task FollowerDdlWithoutTicketForwarderThrowsLeaderRequired()
    {
        await using ClusterHarness cluster = await ClusterHarness.StartAsync();
        string db = cluster.NextSchemaLogDatabaseName();

        EmbeddedKahuna leader = await cluster.WaitForSchemaLeaderNode(db);
        EmbeddedKahuna follower = cluster.Nodes.First(node => node != leader);
        CommandExecutor followerExecutor = cluster.CreateExecutor(follower);

        try
        {
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
                followerExecutor.CreateTable(CreateRobotsTableTicket(db))
            );

            Assert.NotNull(ex);
            Assert.That(ex!.Message, Does.Contain("DDL must be executed by schema leader"));
            Assert.That(ex.Message, Does.Contain(db));
        }
        finally
        {
            await CleanupDatabaseAsync(db, followerExecutor);
        }
    }

    [Test]
    public async Task FollowerCreateTableIfNotExistsUsesTicketForwarder()
    {
        await using ClusterHarness cluster = await ClusterHarness.StartAsync();
        SimulatedDdlForwarder forwarder = new();
        string db = cluster.NextSchemaLogDatabaseName();

        EmbeddedKahuna leader = await cluster.WaitForSchemaLeaderNode(db);
        EmbeddedKahuna follower = cluster.Nodes.First(node => node != leader);
        CommandExecutor followerExecutor = cluster.CreateExecutor(follower, forwarder);

        try
        {
            CreateTableResult result = await followerExecutor.CreateTable(CreateRobotsTableTicket(db, ifNotExists: true))
                .WaitAsync(TimeSpan.FromSeconds(20));

            Assert.False(result.Success);
            Assert.AreEqual(1, forwarder.CreateTableCalls);
        }
        finally
        {
            await CleanupDatabaseAsync(db, followerExecutor);
        }
    }

    [Test]
    public async Task FollowerAlterTableUsesInPlaceApplyForAlreadyOpenDescriptor()
    {
        await using ClusterHarness cluster = await ClusterHarness.StartAsync();
        SimulatedDdlForwarder forwarder = new();
        string db = cluster.NextSchemaLogDatabaseName();

        EmbeddedKahuna leader = await cluster.WaitForSchemaLeaderNode(db);
        EmbeddedKahuna follower = cluster.Nodes.First(node => node != leader);
        CommandExecutor followerExecutor = cluster.CreateExecutor(follower, forwarder);

        try
        {
            DatabaseDescriptor followerDatabase = await followerExecutor.OpenDatabase(db);
            forwarder.Database = followerDatabase;

            SeedRobotsTable(followerDatabase);

            TableDescriptor openBeforeAlter = await followerExecutor.OpenTable(new OpenTableTicket(db, "robots"));

            Assert.AreEqual(1, followerDatabase.TableDescriptors.Count);
            Assert.AreEqual(2, openBeforeAlter.Schema.Columns!.Count);

            bool altered = await followerExecutor.AlterTable(new AlterTableTicket(
                db,
                "robots",
                AlterTableOperation.AddColumn,
                new ColumnInfo("year", ColumnType.Integer64)
            )).WaitAsync(TimeSpan.FromSeconds(20));

            TableDescriptor openAfterAlter = await followerExecutor.OpenTable(new OpenTableTicket(db, "robots"));

            Assert.True(altered);
            Assert.AreEqual(1, forwarder.AlterTableCalls);
            Assert.AreSame(openBeforeAlter, openAfterAlter);
            Assert.AreEqual(2, followerDatabase.Schema.SchemaVersion);
            Assert.AreEqual(3, openBeforeAlter.Schema.Columns!.Count);
            Assert.True(openBeforeAlter.Schema.Columns.Any(column => column.Name == "year"));
            Assert.AreEqual(1, followerDatabase.TableDescriptors.Count);
        }
        finally
        {
            await CleanupDatabaseAsync(db, followerExecutor);
        }
    }

    private static CreateTableTicket CreateRobotsTableTicket(string db, bool ifNotExists = false)
    {
        return new(
            databaseName: db,
            tableName: "robots",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("name", ColumnType.String)
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)])
            ],
            ifNotExists: ifNotExists
        );
    }

    private static void SeedRobotsTable(DatabaseDescriptor database)
    {
        const string tableId = "000000000000000000000001";

        TableSchema tableSchema = new()
        {
            Id = tableId,
            Name = "robots",
            Version = 0,
            Columns =
            [
                new("000000000000000000000101", "id", ColumnType.Id, true, null),
                new("000000000000000000000102", "name", ColumnType.String, false, null)
            ],
            SchemaHistory = []
        };

        tableSchema.SchemaHistory.Add(new()
        {
            Version = 0,
            Columns = tableSchema.Columns
        });

        database.Schema.SchemaVersion = 1;
        database.Schema.Tables["robots"] = tableSchema;
    }

    private static async Task CleanupDatabaseAsync(string db, CommandExecutor executor)
        => await CleanupDatabaseAsync(db, [executor]);

    private static async Task CleanupDatabaseAsync(string db, IEnumerable<CommandExecutor> executors)
    {
        foreach (CommandExecutor executor in executors)
        {
            try
            {
                await executor.CloseDatabase(new CloseDatabaseTicket(db)).WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (Exception ex)
            {
                TestContext.Progress.WriteLine($"cleanup skipped: {ex.GetType().Name}");
            }
        }

        try
        {
            string dataPath = Path.Combine(CamusConfig.DataDirectory, db);
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
        }
        catch
        {
            // Best-effort test cleanup.
        }
    }

    private sealed class SimulatedDdlForwarder : ISchemaDdlForwarder
    {
        public int CreateTableCalls { get; private set; }

        public int AlterTableCalls { get; private set; }

        public DatabaseDescriptor? Database { get; set; }

        public Task<bool?> ForwardCreateTableAsync(string leader, CreateTableTicket ticket, CancellationToken cancellationToken)
        {
            CreateTableCalls++;
            return Task.FromResult<bool?>(false);
        }

        public Task<bool?> ForwardAlterTableAsync(string leader, AlterTableTicket ticket, CancellationToken cancellationToken)
        {
            AlterTableCalls++;
            DatabaseDescriptor database = Database ?? throw new InvalidOperationException("Forwarder database was not set");

            _ = Task.Run(async () =>
            {
                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                await database.Schema.Semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                try
                {
                    TableSchema table = database.Schema.Tables[ticket.TableName];
                    table.Version++;
                    table.Columns!.Add(new("000000000000000000000103", ticket.Column.Name, ticket.Column.Type, ticket.Column.NotNull, ticket.Column.Default));
                    table.SchemaHistory!.Add(new() { Version = table.Version, Columns = table.Columns });
                    database.Schema.SchemaVersion++;
                }
                finally
                {
                    database.Schema.Semaphore.Release();
                }
            }, cancellationToken);

            return Task.FromResult<bool?>(true);
        }

        public Task<bool?> ForwardAlterIndexAsync(string leader, AlterIndexTicket ticket, CancellationToken cancellationToken)
            => Task.FromResult<bool?>(false);

        public Task<bool?> ForwardDropTableAsync(string leader, DropTableTicket ticket, CancellationToken cancellationToken)
            => Task.FromResult<bool?>(false);
    }

    private sealed class ClusterHarness : IAsyncDisposable
    {
        private ClusterHarness(EmbeddedKahuna[] nodes)
        {
            Nodes = nodes;
        }

        public EmbeddedKahuna[] Nodes { get; }

        public static async Task<ClusterHarness> StartAsync()
        {
            InMemoryCommunication raftCommunication = new();
            MemoryInterNodeCommmunication interNode = new();

            EmbeddedKahuna node1 = CreateClusterNode("node1", 1, 9301, [new("localhost:9302"), new("localhost:9303")], raftCommunication, interNode);
            EmbeddedKahuna node2 = CreateClusterNode("node2", 2, 9302, [new("localhost:9301"), new("localhost:9303")], raftCommunication, interNode);
            EmbeddedKahuna node3 = CreateClusterNode("node3", 3, 9303, [new("localhost:9301"), new("localhost:9302")], raftCommunication, interNode);
            EmbeddedKahuna[] nodes = [node1, node2, node3];

            raftCommunication.SetNodes(nodes.ToDictionary(node => node.Raft.GetLocalEndpoint(), node => node.Raft));
            interNode.SetNodes(nodes.ToDictionary(node => node.Raft.GetLocalEndpoint(), node => node.Kahuna));

            foreach (EmbeddedKahuna node in nodes)
                await node.Raft.UpdateNodes().ConfigureAwait(false);

            await Task.WhenAll(nodes.Select(node => node.StartAsync(CancellationToken.None)))
                .WaitAsync(TimeSpan.FromSeconds(10))
                .ConfigureAwait(false);

            return new(nodes);
        }

        public CommandExecutor CreateExecutor(EmbeddedKahuna node, ISchemaDdlForwarder? forwarder = null)
        {
            CommandValidator validator = new();
            CatalogsManager catalogs = new(Logger);
            return new(validator, catalogs, Logger,
                loggerFactory: LoggerFactory,
                clusterNode: node,
                schemaDdlForwarder: forwarder);
        }

        public string NextSchemaLogDatabaseName()
        {
            for (int i = 0; i < 100; i++)
            {
                string db = $"db_{Guid.NewGuid():N}";
                try
                {
                    _ = Nodes[0].SchemaLogPartition(db);
                    return db;
                }
                catch (CamusDBException)
                {
                }
            }

            throw new AssertionException("Could not generate a database name whose schema log partition is not reserved");
        }

        public async Task<EmbeddedKahuna> WaitForSchemaLeaderNode(string db)
        {
            int partitionId = Nodes[0].SchemaLogPartition(db);
            await Nodes[0].Raft.WaitForLeader(partitionId, CancellationToken.None).ConfigureAwait(false);

            DateTime deadline = DateTime.UtcNow.AddSeconds(5);

            while (DateTime.UtcNow < deadline)
            {
                foreach (EmbeddedKahuna node in Nodes)
                {
                    if (await node.AmISchemaLeaderAsync(db, CancellationToken.None).ConfigureAwait(false))
                        return node;
                }

                await Task.Delay(25).ConfigureAwait(false);
            }

            throw new AssertionException($"No schema leader found for partition {partitionId}");
        }

        public async ValueTask DisposeAsync()
        {
            foreach (EmbeddedKahuna node in Nodes)
            {
                try
                {
                    await node.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    TestContext.Progress.WriteLine($"dispose timed out for {node.Raft.GetLocalNodeName()}");
                }
            }
        }

        private static EmbeddedKahuna CreateClusterNode(
            string nodeName,
            int nodeId,
            int port,
            List<RaftNode> peers,
            InMemoryCommunication raftCommunication,
            MemoryInterNodeCommmunication interNode)
        {
            return new(
                new EmbeddedKahunaOptions
                {
                    NodeName = nodeName,
                    NodeId = nodeId,
                    Host = "localhost",
                    Port = port,
                    Storage = "memory",
                    WalStorage = "memory",
                    InitialPartitions = 3
                },
                interNode,
                raftCommunication,
                new StaticDiscovery(peers)
            );
        }
    }
}
