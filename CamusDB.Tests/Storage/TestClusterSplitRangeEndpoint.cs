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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Grpc.Core;
using Kommander.Time;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Data;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;

namespace CamusDB.Tests.Storage;

/// <summary>
/// The manual range split of the cluster API, end to end on one node: the endpoint resolves the relation's
/// row space, picks the median row key, and divides the space there through Kahuna. The split key is the
/// endpoint's own choice, because no client can derive a row key from a row's columns, so the test checks
/// the choice against the same median the split fixtures compute from the scanned row ids.
///
/// <para>The endpoint reports a scan for the median that Kahuna gives up on as an outcome the caller
/// can retry. A fault-injection harness asks for a split seconds after it kills a node, when a key that
/// holds an unresolved write intent can keep a page from settling, and it asks again on that outcome.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestClusterSplitRangeEndpoint : KeyRangeSplitFixture
{
    private ClusterController Controller(CommandExecutor executor)
    {
        DefaultHttpContext http = new();
        http.Request.IsHttps = true;
        http.Connection.RemoteIpAddress = IPAddress.Loopback;

        return new ClusterController(executor, new HttpTransactionCoordinator(executor), logger, Options, TestNode!)
        {
            ControllerContext = new ControllerContext { HttpContext = http },
        };
    }

    private static ClusterController WithBody(ClusterController controller, string json)
    {
        controller.ControllerContext.HttpContext.Request.Body = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        return controller;
    }

    [Test]
    public async Task SplitRange_DividesTheRowSpaceAtItsMedianRowKey()
    {
        (string db, CommandExecutor executor, TableDescriptor table, _) = await SetupTableAsync();

        string keySpace = table.Store.RowKeySpace;
        Assert.That(Descriptors(keySpace).Count, Is.EqualTo(1), "the row space starts as one range");

        List<ObjectIdValue> rowIds = await ScanRowIdsAsync(table, executor, db);
        string expectedKey = MedianRowKey(table, rowIds);

        ClusterSplitRangeResponse body = null!;
        int status = 0;

        // Kahuna answers an indeterminate outcome when leadership or a quiesce moves under the call; an
        // idle single node does not do that, but a retry keeps the test from manufacturing a flake.
        for (int attempt = 0; attempt < 3; attempt++)
        {
            JsonResult result = await WithBody(Controller(executor), $"{{\"database\":\"{db}\",\"table\":\"{table.Name}\"}}").SplitRange();
            status = result.StatusCode ?? 0;
            body = (ClusterSplitRangeResponse)result.Value!;

            if (body.Success || body.Determinate)
                break;

            await Task.Delay(200);
        }

        Assert.That(body.Success, Is.True, $"{body.Status}: {body.Reason}");
        Assert.That(status, Is.EqualTo(200));
        Assert.That(body.KeySpace, Is.EqualTo(keySpace));
        Assert.That(body.SplitKey, Is.EqualTo(expectedKey));
        Assert.That(body.KeysSeen, Is.EqualTo(rowIds.Count));
        Assert.That(body.DescriptorsBefore, Is.EqualTo(1));
        Assert.That(body.NewPartitionId, Is.GreaterThan(0));

        List<KahunaRangeDescriptorResponse> after = Descriptors(keySpace);
        Assert.That(after.Count, Is.EqualTo(2));
        Assert.That(after.Any(d => d.StartKey == expectedKey), Is.True, "the upper half starts at the split key");
    }

    [Test]
    public async Task SplitRange_RefusesASpaceLargerThanTheScanBound()
    {
        (string db, CommandExecutor executor, TableDescriptor table, _) = await SetupTableAsync();

        JsonResult result = await WithBody(Controller(executor), $"{{\"database\":\"{db}\",\"table\":\"{table.Name}\",\"maxKeys\":2}}").SplitRange();

        Assert.That(result.StatusCode, Is.EqualTo(409));
        ClusterSplitRangeResponse body = (ClusterSplitRangeResponse)result.Value!;
        Assert.That(body.Success, Is.False);
        Assert.That(body.Status, Is.EqualTo("Refused"));
        Assert.That(body.Reason, Does.Contain("maxKeys"));
        Assert.That(Descriptors(table.Store.RowKeySpace).Count, Is.EqualTo(1), "nothing was divided");
    }

    [Test]
    public async Task SplitRange_NamesAnUnknownIndexAsSuch()
    {
        (string db, CommandExecutor executor, TableDescriptor table, _) = await SetupTableAsync();

        JsonResult result = await WithBody(Controller(executor), $"{{\"database\":\"{db}\",\"table\":\"{table.Name}\",\"index\":\"no_such_index\"}}").SplitRange();

        ClusterSplitRangeResponse body = (ClusterSplitRangeResponse)result.Value!;
        Assert.That(body.Success, Is.False);
        Assert.That(body.Status, Is.EqualTo("Refused"));
        Assert.That(body.Reason, Does.Contain("no_such_index"));
        Assert.That(result.StatusCode, Is.Not.EqualTo(200));
    }

    /// <summary>
    /// Test-only Kahuna that ends the scan of a key space the way Kahuna does when a page keeps
    /// answering one response type past its retry budget: the first key is answered, then the scan
    /// throws. With <see cref="LeaderIsGone"/> the scan throws the transport error of a page whose forward
    /// the partition leader's dead process refused. Scans pass through once <see cref="FailWith"/> is null.
    /// </summary>
    private sealed class ScanGivesUpKahuna(IKahuna inner) : DelegatingKahuna(inner)
    {
        public KeyValueResponseType? FailWith = KeyValueResponseType.MustRetry;

        public bool LeaderIsGone;

        public override async IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(
            HLCTimestamp txId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive,
            int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability, [EnumeratorCancellation] CancellationToken ct,
            string coordinatorKey = "", TransactionOperationId operationId = default,
            KeyValueScanObservationSink? observations = null, KeyValueReadMode readMode = KeyValueReadMode.Default)
        {
            await foreach ((string Key, ReadOnlyKeyValueEntry Entry) item in base.LocateAndScanRange(
                txId, prefix, startKey, startInclusive, endKey, endInclusive, pageSize, readTimestamp, durability, ct,
                coordinatorKey, operationId, observations, readMode))
            {
                yield return item;

                if (FailWith is not null && LeaderIsGone)
                    throw new RpcException(new Status(StatusCode.Unavailable, "Error connecting to subchannel."));

                if (FailWith is KeyValueResponseType type)
                    throw new KahunaServerException(
                        $"Range scan page over '{prefix}' did not settle within 5000 ms (last response: {type}); " +
                        "a key in the page may hold an unresolved write intent. Retry the scan.",
                        type);
            }
        }
    }

    [Test]
    public async Task SplitKeySpace_ReportsAScanKahunaGaveUpOnAsRetryable_AndSplitsOnTheNextCall()
    {
        (string db, CommandExecutor executor, TableDescriptor table, _) = await SetupTableAsync();

        string keySpace = table.Store.RowKeySpace;
        List<ObjectIdValue> rowIds = await ScanRowIdsAsync(table, executor, db);
        ScanGivesUpKahuna kahuna = new(TestNode!.Kahuna);

        ClusterSplitRangeResponse failed = new() { KeySpace = keySpace };
        await ClusterController.SplitKeySpaceAsync(TestNode.Raft, kahuna, keySpace, 1_000_000, failed, CancellationToken.None);

        Assert.That(failed.Success, Is.False);
        Assert.That(failed.Status, Is.EqualTo("MedianScanFailed"));
        Assert.That(failed.Determinate, Is.True, "no split was attempted, so the map did not move");
        Assert.That(failed.SplitKey, Is.Null);
        Assert.That(failed.DescriptorsBefore, Is.EqualTo(1));
        Assert.That(failed.Reason, Does.Contain("did not settle within"), "Kahuna's own message reaches the caller");
        Assert.That(Descriptors(keySpace).Count, Is.EqualTo(1), "nothing was divided");

        kahuna.FailWith = null;
        ClusterSplitRangeResponse retried = null!;

        // The same retry as the endpoint test: an indeterminate outcome is asked again.
        for (int attempt = 0; attempt < 3; attempt++)
        {
            retried = new() { KeySpace = keySpace };
            await ClusterController.SplitKeySpaceAsync(TestNode.Raft, kahuna, keySpace, 1_000_000, retried, CancellationToken.None);

            if (retried.Success || retried.Determinate)
                break;

            await Task.Delay(200);
        }

        Assert.That(retried.Success, Is.True, $"{retried.Status}: {retried.Reason}");
        Assert.That(retried.SplitKey, Is.EqualTo(MedianRowKey(table, rowIds)));
        Assert.That(retried.KeysSeen, Is.EqualTo(rowIds.Count));
        Assert.That(Descriptors(keySpace).Count, Is.EqualTo(2));
    }

    [Test]
    public async Task SplitKeySpace_ReportsAPageThatCouldNotReachItsLeaderAsRetryable()
    {
        (_, _, TableDescriptor table, _) = await SetupTableAsync();

        string keySpace = table.Store.RowKeySpace;
        ScanGivesUpKahuna kahuna = new(TestNode!.Kahuna) { LeaderIsGone = true };

        ClusterSplitRangeResponse failed = new() { KeySpace = keySpace };
        await ClusterController.SplitKeySpaceAsync(TestNode.Raft, kahuna, keySpace, 1_000_000, failed, CancellationToken.None);

        Assert.That(failed.Success, Is.False);
        Assert.That(failed.Status, Is.EqualTo("MedianScanFailed"));
        Assert.That(failed.Determinate, Is.True);
        Assert.That(failed.Reason, Does.Contain("Unavailable"));
        Assert.That(Descriptors(keySpace).Count, Is.EqualTo(1), "nothing was divided");
    }

    [Test]
    public async Task PickMedian_KeepsTheErrorCodeOfADamagedStore()
    {
        (_, _, TableDescriptor table, _) = await SetupTableAsync();

        ScanGivesUpKahuna kahuna = new(TestNode!.Kahuna) { FailWith = KeyValueResponseType.StorageCorrupt };

        CamusDBException failure = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RangeSplitKeyChooser.PickMedianAsync(
                kahuna, table.Store.RowKeySpace, new HashSet<string>(), 1_000_000, CancellationToken.None))!;

        Assert.That(failure.Code, Is.EqualTo(CamusDBErrorCodes.StorageCorrupted), "a retry cannot clear a damaged store");
    }
}
