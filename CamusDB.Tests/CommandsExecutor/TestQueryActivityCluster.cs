/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna.Server.Communication.Internode;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;

using CamusDB.Core;
using Kahuna;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>SHOW CLUSTER QUERIES</c>, <c>SHOW CLUSTER CONNECTIONS</c> and a <c>CANCEL QUERY</c> routed to
/// the node that owns the statement, on a three-node in-process cluster. The peers are reached
/// through <see cref="InProcessClusterActivityTransport"/>, which applies the same node-local list and
/// viewer filter as the host's <c>/internal/activity/*</c> endpoints.
///
/// <para>The held statement is a <c>SHOW QUERIES</c> on node 2 with its one row read and its cursor
/// kept: it needs no database, so the test does not depend on schema replication, and it stays
/// listed for exactly as long as the test keeps the cursor.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestQueryActivityCluster
{
    private static readonly ILogger<ICamusDB> Logger = NullLogger<ICamusDB>.Instance;

    private static int nextPortBase = 9900;

    private EmbeddedKahuna[] nodes = [];

    private CommandExecutor[] executors = [];

    private DatabaseRegistry? registry;

    private InProcessClusterNodes members = new();

    [SetUp]
    public async Task StartClusterAsync()
    {
        CamusDBOptions options = CamusDBOptions.Default;

        InMemoryCommunication raft = new();
        MemoryInterNodeCommmunication interNode = new();

        int portBase = Interlocked.Add(ref nextPortBase, 10);
        int[] ports = [portBase + 1, portBase + 2, portBase + 3];

        nodes = new EmbeddedKahuna[3];
        for (int i = 0; i < 3; i++)
        {
            List<RaftNode> peers = ports.Where((_, j) => j != i).Select(p => new RaftNode($"localhost:{p}")).ToList();
            nodes[i] = new EmbeddedKahuna(
                new EmbeddedKahunaOptions
                {
                    ReadIOThreads = 1,
                    WriteIOThreads = 1,
                    NodeName = $"node{i + 1}",
                    NodeId = i + 1,
                    Host = "localhost",
                    Port = ports[i],
                    Storage = "memory",
                    WalStorage = "memory",
                    InitialPartitions = 3,
                }.WithTestNodeDefaults(),
                interNode,
                raft,
                new StaticDiscovery(peers));
        }

        raft.SetNodes(nodes.ToDictionary(n => n.Raft.GetLocalEndpoint(), n => n.Raft));
        interNode.SetNodes(nodes.ToDictionary(n => n.Raft.GetLocalEndpoint(), n => n.Kahuna));

        foreach (EmbeddedKahuna node in nodes)
            await node.Raft.UpdateNodes();

        await Task.WhenAll(nodes.Select(n => n.StartAsync(CancellationToken.None))).WaitAsync(TimeSpan.FromSeconds(10));

        registry = await DatabaseRegistry.OpenAsync(nodes[0], options);

        members = new InProcessClusterNodes();
        InProcessClusterActivityTransport transport = new(members);

        executors = nodes.Select(node => new CommandExecutor(
            new CommandValidator(options),
            new CatalogsManager(Logger),
            Logger,
            options,
            sharedNode: node,
            registry: registry,
            isClusterMode: true,
            activityTransport: transport)).ToArray();

        for (int i = 0; i < 3; i++)
            members.Register(nodes[i], executors[i]);
    }

    [TearDown]
    public async Task StopClusterAsync()
    {
        foreach (CommandExecutor executor in executors)
            try { await executor.DisposeAsync(); } catch { }

        if (registry is not null)
            try { await registry.DisposeAsync(); } catch { }

        foreach (EmbeddedKahuna node in nodes)
            try { await node.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)); } catch { }
    }

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, "", sql, null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        return rows;
    }

    private static async Task<(IAsyncEnumerator<QueryResultRow> cursor, string id)> HoldAsync(CommandExecutor executor)
    {
        (_, IAsyncEnumerable<QueryResultRow> held) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, "", "SHOW QUERIES", null));

        IAsyncEnumerator<QueryResultRow> cursor = held.GetAsyncEnumerator();
        Assert.That(await cursor.MoveNextAsync(), Is.True);
        return (cursor, cursor.Current.Row["query_id"].StrValue!);
    }

    [Test]
    public async Task OneNodeListsAndCancelsAStatementThatRunsOnAnother()
    {
        (IAsyncEnumerator<QueryResultRow> cursor, string heldId) = await HoldAsync(executors[1]);
        string node2 = nodes[1].Raft.GetLocalEndpoint();

        // The node-local form on node 1 does not see it; the cluster form does, labelled with its node.
        Assert.That((await QueryAsync(executors[0], "SHOW QUERIES")).Any(r => r.Row["query_id"].StrValue == heldId), Is.False);

        List<QueryResultRow> cluster = await QueryAsync(executors[0], "SHOW CLUSTER QUERIES");
        QueryResultRow remote = cluster.Single(r => r.Row["query_id"].StrValue == heldId);
        Assert.That(remote.Row["node"].StrValue, Is.EqualTo(node2));
        Assert.That(cluster.All(r => r.Row["error"].Type == ColumnType.Null), Is.True);

        // The cancel goes to node 2 by the node tag in the id.
        await executors[0].ExecuteNonSQLQuery(new ExecuteSQLTicket(null!, "", $"CANCEL QUERY '{heldId}'", null));

        CamusDBException? stopped = Assert.ThrowsAsync<CamusDBException>(async () => await cursor.MoveNextAsync());
        Assert.That(stopped!.Code, Is.EqualTo(CamusDBErrorCodes.QueryCancelled));
        await cursor.DisposeAsync();

        CamusDBException? gone = Assert.ThrowsAsync<CamusDBException>(() =>
            executors[2].ExecuteNonSQLQuery(new ExecuteSQLTicket(null!, "", $"CANCEL QUERY '{heldId}'", null)));
        Assert.That(gone!.Code, Is.EqualTo(CamusDBErrorCodes.QueryNotFound));
    }

    [Test]
    public async Task ClusterConnectionsGatherEveryNode()
    {
        ClientConnection onNode3 = executors[2].QueryActivity.Connections.Open("kestrel-n3", "198.51.100.3");

        List<QueryResultRow> rows = await QueryAsync(executors[0], "SHOW CLUSTER CONNECTIONS");

        QueryResultRow row = rows.Single();
        Assert.That(row.Row["connection_id"].StrValue, Is.EqualTo(onNode3.Id));
        Assert.That(row.Row["node"].StrValue, Is.EqualTo(nodes[2].Raft.GetLocalEndpoint()));
        Assert.That(row.Row["client_address"].StrValue, Is.EqualTo("198.51.100.3"));

        onNode3.Close();
    }

    [Test]
    public async Task UnreachablePeerGivesAnErrorRowAndTheOthersStillAnswer()
    {
        (IAsyncEnumerator<QueryResultRow> cursor, string heldId) = await HoldAsync(executors[1]);
        string node3 = nodes[2].Raft.GetLocalEndpoint();

        // A member that left the address book fails the way an unreachable host fails.
        members.Unregister(node3);

        List<QueryResultRow> rows = await QueryAsync(executors[0], "SHOW CLUSTER QUERIES");

        QueryResultRow error = rows.Single(r => r.Row["error"].Type != ColumnType.Null);
        Assert.That(error.Row["node"].StrValue, Is.EqualTo(node3));
        Assert.That(error.Row["query_id"].Type, Is.EqualTo(ColumnType.Null));
        Assert.That(rows.Any(r => r.Row["query_id"].StrValue == heldId), Is.True, "node 2 still answered");

        await cursor.DisposeAsync();
    }
}
