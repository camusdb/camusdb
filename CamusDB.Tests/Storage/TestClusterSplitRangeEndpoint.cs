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
using System.Threading.Tasks;
using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Kahuna.Shared.Communication.Rest;
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
}
