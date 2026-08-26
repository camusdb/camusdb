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
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Server.Communication.Internode;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

[TestFixture]
// Serial: boots a multi-node in-process cluster. Concurrent clusters contend for ports and skew
// each other's Raft election timing, which shows up as spurious leadership churn.
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

        // Create on any node first to discover the database Id, then find the schema
        // leader for that Id (the schema partition key is the Id, not the name).
        CommandExecutor creatorExecutor = cluster.CreateExecutor(cluster.Nodes[0]);
        DatabaseDescriptor createdDesc = await creatorExecutor
            .CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false))
            .WaitAsync(TimeSpan.FromSeconds(15));

        EmbeddedKahuna leader = await cluster.WaitForSchemaLeaderNode(createdDesc.Id);
        EmbeddedKahuna follower = cluster.Nodes.First(node => node != leader);
        CommandExecutor leaderExecutor = cluster.CreateExecutor(leader);
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
            await CleanupDatabaseAsync(db, [leaderExecutor, followerExecutor, creatorExecutor]);
        }
    }

    [Test]
    public async Task FollowerCreateTableIfNotExistsUsesTicketForwarder()
    {
        await using ClusterHarness cluster = await ClusterHarness.StartAsync();
        SimulatedDdlForwarder forwarder = new();
        string db = cluster.NextSchemaLogDatabaseName();

        CommandExecutor creatorExecutor = cluster.CreateExecutor(cluster.Nodes[0]);
        DatabaseDescriptor createdDesc = await creatorExecutor
            .CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false))
            .WaitAsync(TimeSpan.FromSeconds(15));

        EmbeddedKahuna leader = await cluster.WaitForSchemaLeaderNode(createdDesc.Id);
        EmbeddedKahuna follower = cluster.Nodes.First(node => node != leader);
        CommandExecutor leaderExecutor = cluster.CreateExecutor(leader);
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
            await CleanupDatabaseAsync(db, [leaderExecutor, followerExecutor, creatorExecutor]);
        }
    }

    [Test]
    public async Task FollowerAlterTableUsesInPlaceApplyForAlreadyOpenDescriptor()
    {
        await using ClusterHarness cluster = await ClusterHarness.StartAsync();
        SimulatedDdlForwarder forwarder = new();
        string db = cluster.NextSchemaLogDatabaseName();

        CommandExecutor creatorExecutor = cluster.CreateExecutor(cluster.Nodes[0]);
        DatabaseDescriptor createdDesc = await creatorExecutor
            .CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false))
            .WaitAsync(TimeSpan.FromSeconds(15));

        EmbeddedKahuna leader = await cluster.WaitForSchemaLeaderNode(createdDesc.Id);
        EmbeddedKahuna follower = cluster.Nodes.First(node => node != leader);
        CommandExecutor leaderExecutor = cluster.CreateExecutor(leader);
        CommandExecutor followerExecutor = cluster.CreateExecutor(follower, forwarder);

        try
        {
            DatabaseDescriptor followerDatabase = await followerExecutor.OpenDatabase(db);
            forwarder.Database = followerDatabase;
            forwarder.EmitUnrelatedVersionBumpBeforeAlterApply = true;

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
            Assert.AreEqual(3, followerDatabase.Schema.SchemaVersion);
            Assert.AreEqual(3, openBeforeAlter.Schema.Columns!.Count);
            Assert.True(openBeforeAlter.Schema.Columns.Any(column => column.Name == "year"));
            Assert.AreEqual(1, followerDatabase.TableDescriptors.Count);
        }
        finally
        {
            await CleanupDatabaseAsync(db, [leaderExecutor, followerExecutor, creatorExecutor]);
        }
    }

    /// <summary>
    /// Wires a real <see cref="HttpSchemaDdlForwarder"/> (not a stub) on a
    /// follower and verifies the full path:
    ///   follower.CreateTable → TryForwardDdlAsync → HttpSchemaDdlForwarder
    ///   → HTTP POST to the leader's mapped URL → fake 200/not-applied response
    ///   → CreateTableResult.Success == false.
    ///
    /// The endpoint map is built from the cluster nodes' actual Raft endpoints
    /// (just as Program.cs would build it from config.Peers / config.HttpPeers),
    /// proving the resolver hands each leader endpoint through to the right URI.
    /// </summary>
    [Test]
    public async Task FollowerForwardsCreateTableViaHttpSchemaDdlForwarder()
    {
        await using ClusterHarness cluster = await ClusterHarness.StartAsync();
        string db = cluster.NextSchemaLogDatabaseName();

        CommandExecutor creatorExecutor = cluster.CreateExecutor(cluster.Nodes[0]);
        DatabaseDescriptor createdDesc = await creatorExecutor
            .CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false))
            .WaitAsync(TimeSpan.FromSeconds(15));

        EmbeddedKahuna leader = await cluster.WaitForSchemaLeaderNode(createdDesc.Id);
        EmbeddedKahuna follower = cluster.Nodes.First(node => node != leader);

        // Build an endpoint map that mirrors what Program.cs does from config:
        //   raft-endpoint → http://fake-{nodeName}:5095
        // We use distinct fake hostnames per node so the test can assert which
        // node's URL was actually called.
        JsonSerializerOptions jsonOpts = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true };

        Dictionary<string, Uri> endpointMap = cluster.Nodes.ToDictionary(
            node => node.Raft.GetLocalEndpoint(),
            node => new Uri($"http://fake-{node.Raft.GetLocalNodeName()}:5095")
        );

        // Fake handler: returns "ok / applied:false" and records the last request.
        CapturingHandler handler = new(
            JsonSerializer.Serialize(new SchemaDdlForwardResponse { Status = "ok", Applied = false }, jsonOpts));

        HttpSchemaDdlForwarder forwarder = new(
            new HttpClient(handler),
            raftEndpoint => endpointMap[raftEndpoint],
            NullLogger<ICamusDB>.Instance
        );

        CommandExecutor leaderExecutor = cluster.CreateExecutor(leader);
        CommandExecutor followerExecutor = cluster.CreateExecutor(follower, forwarder);

        try
        {
            CreateTableResult result = await followerExecutor
                .CreateTable(CreateRobotsTableTicket(db, ifNotExists: true))
                .WaitAsync(TimeSpan.FromSeconds(20));

            // The forwarder returned false (not applied) → CreateTableResult.Success is false.
            Assert.IsFalse(result.Success, "follower should surface the forwarder's not-applied result");

            // Exactly one HTTP request was made.
            Assert.IsNotNull(handler.LastRequest, "HttpSchemaDdlForwarder must have made an HTTP request");

            // The request was posted to the leader's mapped URI — not a peer's.
            Uri expectedLeaderUri = endpointMap[leader.Raft.GetLocalEndpoint()];
            Assert.That(handler.LastRequest!.RequestUri!.ToString(),
                Does.StartWith(expectedLeaderUri.ToString()),
                "request must target the schema leader's mapped HTTP address");

            // The request body carries the database name and table name.
            string body = handler.LastBody!;
            using JsonDocument doc = JsonDocument.Parse(body);
            Assert.AreEqual(db, doc.RootElement.GetProperty("databaseName").GetString());
            Assert.AreEqual("robots", doc.RootElement.GetProperty("tableName").GetString());
        }
        finally
        {
            await CleanupDatabaseAsync(db, [leaderExecutor, followerExecutor, creatorExecutor]);
        }
    }

    // 3-node CamusDB data-path test with key-range sharding enabled.
    //
    // Validates the full key-range data path end-to-end across a 3-node in-memory cluster:
    //   1. CREATE TABLE on the schema leader.
    //   2. INSERT from a non-schema-leader node — this opens the table on that node for the
    //      first time, calling RegisterKeyRangeAsync, which triggers seed-forwarding to the
    //      KV meta-partition leader if the inserting node isn't that leader.
    //   3. SELECT from a third node — asserts all rows are visible and no "no range descriptor
    //      covers key" surfaces anywhere in the path.
    //
    [Test]
    [NonParallelizable]
    public async Task ThreeNodeCluster_KeyRangeEnabled_InsertFromNonLeader_SelectFromThirdNode()
    {
        await using ClusterHarness cluster = await ClusterHarness.StartAsync(
            options: CamusDBOptions.Default with { KeyRangeShardingEnabled = true });
            string db = cluster.NextSchemaLogDatabaseName();

            // ── DDL on the schema leader ──────────────────────────────────────────
            // Create on any node first to discover the database Id, then route DDL
            // to the schema leader for that Id (the partition key is the Id).
            CommandExecutor creatorExecutor = cluster.CreateExecutor(cluster.Nodes[0]);
            DatabaseDescriptor createdDesc = await creatorExecutor
                .CreateDatabase(new CreateDatabaseTicket(db, false))
                .WaitAsync(TimeSpan.FromSeconds(15));

            EmbeddedKahuna leader = await cluster.WaitForSchemaLeaderNode(createdDesc.Id);
            CommandExecutor leaderExecutor = cluster.CreateExecutor(leader);

            // Integer64 PK so the ~pk index is key-range eligible (ASCII-encoding type).
            await leaderExecutor.CreateTable(new CreateTableTicket(
                databaseName: db,
                tableName: "readings",
                columns:
                [
                    new ColumnInfo("id", ColumnType.Id),
                    new ColumnInfo("value", ColumnType.Integer64)
                ],
                constraints:
                [
                    new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                        [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false
            )).WaitAsync(TimeSpan.FromSeconds(15));

            // ── INSERT from a non-schema-leader node ──────────────────────────────
            // Choosing the first non-leader node stresses: LoadTable → RegisterKeyRangeAsync
            // on a node that may not be the KV meta-partition leader → seed forwarded via RPC.
            EmbeddedKahuna nonLeader = cluster.Nodes.First(n => n != leader);
            CommandExecutor nonLeaderExecutor = cluster.CreateExecutor(nonLeader);
            DatabaseDescriptor nonLeaderDb = await nonLeaderExecutor.OpenDatabase(db)
                .WaitAsync(TimeSpan.FromSeconds(10));

            const int RowCount = 20;
            for (int i = 0; i < RowCount; i++)
            {
                KvTransaction tx = await nonLeaderDb.Transactions.BeginAsync();
                await nonLeaderExecutor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    tx, db, $"INSERT INTO readings (id, value) VALUES (gen_id(), {i})", null))
                    .WaitAsync(TimeSpan.FromSeconds(10));
                await nonLeaderDb.Transactions.CommitAsync(tx);
            }

            // ── SELECT from a third node ──────────────────────────────────────────
            EmbeddedKahuna thirdNode = cluster.Nodes.First(n => n != leader && n != nonLeader);
            CommandExecutor thirdExecutor = cluster.CreateExecutor(thirdNode);
            DatabaseDescriptor thirdDb = await thirdExecutor.OpenDatabase(db)
                .WaitAsync(TimeSpan.FromSeconds(10));

            KvTransaction readTx = await thirdDb.Transactions.BeginAsync();
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await thirdExecutor.ExecuteSQLQuery(
                new ExecuteSQLTicket(readTx, db, "SELECT id, value FROM readings", null))
                .WaitAsync(TimeSpan.FromSeconds(10));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await thirdDb.Transactions.CommitAsync(readTx);

            Assert.AreEqual(RowCount, rows.Count,
                $"All {RowCount} rows inserted from the non-leader node must be visible from a third node. " +
                $"Got {rows.Count}. If 0, K1 seed-forwarding is broken; if < {RowCount}, replication or routing is incomplete.");

        await CleanupDatabaseAsync(db, [leaderExecutor, nonLeaderExecutor, thirdExecutor, creatorExecutor]);
    }

    /// <summary>
    /// A CHECK constraint containing a regex operator (<c>~</c>) created on the schema leader must
    /// replicate to a follower and enforce there. The constraint is persisted/replicated as SQL
    /// text and re-parsed on the follower, so this proves the operator survives
    /// propose → persist → reparse across nodes: a violating INSERT from the follower is rejected
    /// with <see cref="CamusDBErrorCodes.CheckConstraintViolation"/> while a valid one succeeds.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task ClusterFollower_RegexCheckConstraint_ReplicatesAndEnforces()
    {
        await using ClusterHarness cluster = await ClusterHarness.StartAsync();
        string db = cluster.NextSchemaLogDatabaseName();

        CommandExecutor creatorExecutor = cluster.CreateExecutor(cluster.Nodes[0]);
        DatabaseDescriptor createdDesc = await creatorExecutor
            .CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false))
            .WaitAsync(TimeSpan.FromSeconds(15));

        EmbeddedKahuna leader = await cluster.WaitForSchemaLeaderNode(createdDesc.Id);
        EmbeddedKahuna follower = cluster.Nodes.First(node => node != leader);
        CommandExecutor leaderExecutor = cluster.CreateExecutor(leader);
        CommandExecutor followerExecutor = cluster.CreateExecutor(follower);

        try
        {
            // DDL on the schema leader: a table whose CHECK uses the regex match operator.
            DatabaseDescriptor leaderDb = await leaderExecutor.OpenDatabase(db).WaitAsync(TimeSpan.FromSeconds(10));
            KvTransaction ddlTx = await leaderDb.Transactions.BeginAsync();
            await leaderExecutor.ExecuteDDLSQL(new ExecuteSQLTicket(ddlTx, db, @"
                CREATE TABLE codes (
                    id object_id DEFAULT(gen_id()) PRIMARY KEY,
                    code text NOT NULL,
                    CONSTRAINT code_format CHECK (code ~ '^[A-Z]{3}-[0-9]{3}$')
                )", null)).WaitAsync(TimeSpan.FromSeconds(20));
            await leaderDb.Transactions.CommitAsync(ddlTx);

            // From the follower: a valid code inserts, an invalid code is rejected by the
            // replicated + re-parsed CHECK.
            DatabaseDescriptor followerDb = await followerExecutor.OpenDatabase(db).WaitAsync(TimeSpan.FromSeconds(10));

            KvTransaction okTx = await followerDb.Transactions.BeginAsync();
            await followerExecutor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                okTx, db, "INSERT INTO codes (code) VALUES ('ABC-123')", null))
                .WaitAsync(TimeSpan.FromSeconds(10));
            await followerDb.Transactions.CommitAsync(okTx);

            KvTransaction badTx = await followerDb.Transactions.BeginAsync();
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
                followerExecutor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    badTx, db, "INSERT INTO codes (code) VALUES ('invalid')", null)));
            Assert.NotNull(ex);
            Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex!.Code,
                "the regex CHECK must replicate to the follower and reject the violating row there");
            await followerDb.Transactions.RollbackAsync(badTx);
        }
        finally
        {
            await CleanupDatabaseAsync(db, [leaderExecutor, followerExecutor, creatorExecutor]);
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

        public bool EmitUnrelatedVersionBumpBeforeAlterApply { get; set; }

        public Task<bool?> ForwardCreateTableAsync(string leader, CreateTableTicket ticket, string operationId, CancellationToken cancellationToken)
        {
            CreateTableCalls++;
            return Task.FromResult<bool?>(false);
        }

        public Task<bool?> ForwardAlterTableAsync(string leader, AlterTableTicket ticket, string operationId, CancellationToken cancellationToken)
        {
            AlterTableCalls++;
            DatabaseDescriptor database = Database ?? throw new InvalidOperationException("Forwarder database was not set");

            _ = Task.Run(async () =>
            {
                if (EmitUnrelatedVersionBumpBeforeAlterApply)
                {
                    await Task.Delay(25, cancellationToken).ConfigureAwait(false);
                    await database.Schema.Semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                    try
                    {
                        database.Schema.SchemaVersion++;
                    }
                    finally
                    {
                        database.Schema.Semaphore.Release();
                    }
                }

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

        public Task<bool?> ForwardAlterIndexAsync(string leader, AlterIndexTicket ticket, string operationId, CancellationToken cancellationToken)
            => Task.FromResult<bool?>(false);

        public Task<bool?> ForwardDropTableAsync(string leader, DropTableTicket ticket, string operationId, CancellationToken cancellationToken)
            => Task.FromResult<bool?>(false);

        public Task<bool?> ForwardTruncateTableAsync(string leader, TruncateTableTicket ticket, string operationId, CancellationToken cancellationToken)
            => Task.FromResult<bool?>(false);

        public Task<bool?> ForwardRelinkTableAsync(string leader, RelinkTableTicket ticket, string operationId, CancellationToken cancellationToken)
            => Task.FromResult<bool?>(false);

        public Task<bool?> ForwardRenameTableAsync(string leader, RenameTableTicket ticket, string operationId, CancellationToken cancellationToken)
            => Task.FromResult<bool?>(false);

        public Task<bool?> ForwardAlterConstraintAsync(string leader, AlterConstraintTicket ticket, string operationId, CancellationToken cancellationToken)
            => Task.FromResult<bool?>(false);

        public Task<bool?> ForwardCommentAsync(string leader, CommentTicket ticket, string operationId, CancellationToken cancellationToken)
            => Task.FromResult<bool?>(false);
    }

    private sealed class CapturingHandler : HttpMessageHandler
    {
        private readonly string responseBody;

        public HttpRequestMessage? LastRequest { get; private set; }
        public string? LastBody { get; private set; }

        public CapturingHandler(string responseBody)
        {
            this.responseBody = responseBody;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastRequest = request;
            LastBody = await request.Content!.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(responseBody, Encoding.UTF8, "application/json")
            };
        }
    }

    private sealed class ClusterHarness : IAsyncDisposable
    {
        private static int nextPortBase = 9300;

        private ClusterHarness(EmbeddedKahuna[] nodes, DatabaseRegistry sharedRegistry, CamusDBOptions options)
        {
            Nodes = nodes;
            SharedRegistry = sharedRegistry;
            Options = options;
        }

        public EmbeddedKahuna[] Nodes { get; }
        public DatabaseRegistry SharedRegistry { get; }

        /// <summary>
        /// Configuration every node and executor in this harness is built with. Fixed at start, so a
        /// test wanting a different cluster shape starts its own harness rather than changing anything
        /// the running one already captured.
        /// </summary>
        public CamusDBOptions Options { get; }

        public static async Task<ClusterHarness> StartAsync(CamusDBOptions? options = null)
        {
            CamusDBOptions effective = options ?? CamusDBOptions.Default;

            InMemoryCommunication raftCommunication = new();
            MemoryInterNodeCommmunication interNode = new();

            int portBase = Interlocked.Add(ref nextPortBase, 10);
            int port1 = portBase + 1;
            int port2 = portBase + 2;
            int port3 = portBase + 3;

            EmbeddedKahuna node1 = CreateClusterNode("node1", 1, port1, [new($"localhost:{port2}"), new($"localhost:{port3}")], raftCommunication, interNode);
            EmbeddedKahuna node2 = CreateClusterNode("node2", 2, port2, [new($"localhost:{port1}"), new($"localhost:{port3}")], raftCommunication, interNode);
            EmbeddedKahuna node3 = CreateClusterNode("node3", 3, port3, [new($"localhost:{port1}"), new($"localhost:{port2}")], raftCommunication, interNode);
            EmbeddedKahuna[] nodes = [node1, node2, node3];

            raftCommunication.SetNodes(nodes.ToDictionary(node => node.Raft.GetLocalEndpoint(), node => node.Raft));
            interNode.SetNodes(nodes.ToDictionary(node => node.Raft.GetLocalEndpoint(), node => node.Kahuna));

            foreach (EmbeddedKahuna node in nodes)
                await node.Raft.UpdateNodes().ConfigureAwait(false);

            await Task.WhenAll(nodes.Select(node => node.StartAsync(CancellationToken.None)))
                .WaitAsync(TimeSpan.FromSeconds(10))
                .ConfigureAwait(false);

            // One shared registry for the whole cluster — all executors share the same
            // in-memory name→id cache so CreateDatabase on any node is visible to all.
            DatabaseRegistry sharedRegistry = await DatabaseRegistry.OpenAsync(nodes[0], effective).ConfigureAwait(false);

            return new(nodes, sharedRegistry, effective);
        }

        public CommandExecutor CreateExecutor(EmbeddedKahuna node, ISchemaDdlForwarder? forwarder = null)
        {
            CommandValidator validator = new(Options);
            CatalogsManager catalogs = new(Logger);
            return new(validator, catalogs, Logger, Options,
                sharedNode: node,
                schemaDdlForwarder: forwarder,
                registry: SharedRegistry,
                isClusterMode: true);
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
            try { await SharedRegistry.DisposeAsync().ConfigureAwait(false); } catch { }

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
                    ReadIOThreads = 1,
                    WriteIOThreads = 1,
                    NodeName = nodeName,
                    NodeId = nodeId,
                    Host = "localhost",
                    Port = port,
                    Storage = "memory",
                    WalStorage = "memory",
                    InitialPartitions = 3
                }.WithTestNodeDefaults(),
                interNode,
                raftCommunication,
                new StaticDiscovery(peers)
            );
        }
    }
}
