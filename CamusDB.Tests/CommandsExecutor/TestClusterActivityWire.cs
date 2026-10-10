/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

using CamusDB.App.Controllers;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Core.Diagnostics;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Both ends of the activity wire in one test: the production <see cref="HttpClusterActivityTransport"/>
/// sends its request through a handler that decodes it, calls the real
/// <see cref="ClusterActivityController"/> action, and encodes the answer the way the host's JSON
/// formatter does (the web defaults). A field that one end spells differently, a record that does
/// not round-trip, or a viewer that is lost on the way fails here.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestClusterActivityWire : BaseTest
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    /// <summary>Routes a transport request to the controller, as the host would.</summary>
    private sealed class ControllerHandler(CommandExecutor executor) : HttpMessageHandler
    {
        public string? LastSecret { get; private set; }

        public string? LastPath { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastPath = request.RequestUri!.AbsolutePath;
            LastSecret = request.Headers.TryGetValues("X-Camus-Node-Secret", out IEnumerable<string>? values) ? values.Single() : null;

            string body = await request.Content!.ReadAsStringAsync(cancellationToken);
            ClusterActivityController controller = new(executor)
            {
                ControllerContext = new ControllerContext { HttpContext = new DefaultHttpContext() },
            };

            object? answer = LastPath switch
            {
                ClusterActivityWire.QueriesPath =>
                    (await controller.ListQueries(JsonSerializer.Deserialize<ClusterActivityWire.ListRequest>(body, Web)!)).Value,
                ClusterActivityWire.ConnectionsPath =>
                    (await controller.ListConnections(JsonSerializer.Deserialize<ClusterActivityWire.ListRequest>(body, Web)!)).Value,
                ClusterActivityWire.CancelPath =>
                    controller.Cancel(JsonSerializer.Deserialize<ClusterActivityWire.CancelRequest>(body, Web)!).Value,
                _ => null,
            };

            if (answer is null)
                return new HttpResponseMessage(HttpStatusCode.NotFound);

            return new HttpResponseMessage(HttpStatusCode.OK) { Content = JsonContent.Create(answer, answer.GetType(), options: Web) };
        }
    }

    private static HttpClusterActivityTransport Transport(ControllerHandler handler)
        => new(new HttpClient(handler),
            new PeerEndpointResolver(["peer:2070"], ["http://peer:5095"], 5095, false, NullLogger<ICamusDB>.Instance),
            "s3cret");

    [Test]
    public async Task ListsAndCancelsAcrossTheWire()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();
        ControllerHandler handler = new(executor);
        HttpClusterActivityTransport transport = Transport(handler);

        Principal alice = new("alice", isSuperuser: false, grants: [], userId: "u-alice");

        (_, IAsyncEnumerable<QueryResultRow> held) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, "", "SHOW QUERIES", null, alice));
        await using IAsyncEnumerator<QueryResultRow> cursor = held.GetAsyncEnumerator();
        Assert.That(await cursor.MoveNextAsync(), Is.True);
        string id = cursor.Current.Row["query_id"].StrValue!;

        ClientConnection connection = executor.QueryActivity.Connections.Open("kestrel-wire", "203.0.113.9");

        // Everything round-trips, including the start time and the booleans.
        List<QueryActivityRow> rows = await transport.ListQueriesAsync("peer:2070", ActivityViewer.All, default);
        QueryActivityRow row = rows.Single(r => r.QueryId == id);
        Assert.That(row.UserName, Is.EqualTo("alice"));
        Assert.That(row.Kind, Is.EqualTo("show_queries"));
        Assert.That(row.Cancellable, Is.True);
        Assert.That(row.StartedAt, Is.Not.EqualTo(default(DateTime)));
        Assert.That(handler.LastSecret, Is.EqualTo("s3cret"));
        Assert.That(handler.LastPath, Is.EqualTo(ClusterActivityWire.QueriesPath));

        // The viewer crosses the wire and the peer filters by it.
        ActivityViewer bob = new(false, "bob", "u-bob");
        Assert.That(await transport.ListQueriesAsync("peer:2070", bob, default), Is.Empty);
        Assert.That(await transport.CancelAsync("peer:2070", id, bob, default), Is.EqualTo(QueryCancelOutcome.NotFound));

        ConnectionActivityRow wireConnection = (await transport.ListConnectionsAsync("peer:2070", ActivityViewer.All, default)).Single();
        Assert.That(wireConnection.ConnectionId, Is.EqualTo(connection.Id));
        Assert.That(wireConnection.ClientAddress, Is.EqualTo("203.0.113.9"));

        // The outcome enum crosses the wire, and the cancel reaches the statement.
        Assert.That(await transport.CancelAsync("peer:2070", id, ActivityViewer.All, default), Is.EqualTo(QueryCancelOutcome.Cancelled));
        CamusDBException? stopped = Assert.ThrowsAsync<CamusDBException>(async () => await cursor.MoveNextAsync());
        Assert.That(stopped!.Code, Is.EqualTo(CamusDBErrorCodes.QueryCancelled));

        connection.Close();
        GC.KeepAlive(db);
    }
}
