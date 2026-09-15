/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.Config;
using Grpc.Core;
using Kahuna.Shared.KeyValue;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Pins the boot behaviour of <see cref="ClusterSettingsService.StartAsync"/> against the failure
/// that killed a restarted node, the node came back into a cluster mid-election, its generation read hit the inter-node deadline, and the
/// <c>RpcException</c> escaped <c>Program.Main</c> — the same shape <see cref="StartupLoadRetry"/>
/// was written for on the catalog and registry scans, on a third boot path that had not adopted it.
/// </summary>
[TestFixture]
public sealed class TestClusterSettingsBootRetry
{
    private static RpcException InterNodeDeadline() =>
        new(new Status(StatusCode.Unavailable, "The remote node did not answer within the inter-node request deadline."));

    [Test]
    public async Task TransportFaultDuringBoot_IsRetriedUntilThePeerAnswers()
    {
        int attempts = 0;
        long generation = await ClusterSettingsService.RunBootStepAsync(() =>
        {
            attempts++;
            if (attempts < 3)
                throw InterNodeDeadline();
            return Task.FromResult(41L);
        }, CancellationToken.None);

        Assert.That(generation, Is.EqualTo(41));
        Assert.That(attempts, Is.EqualTo(3), "two deadlines waited out, the third answer taken");
    }

    [Test]
    public void FaultThatOutlivesTheBudget_IsSurfacedUnchanged()
    {
        RpcException ex = Assert.ThrowsAsync<RpcException>(() =>
            ClusterSettingsService.RunBootStepAsync<long>(
                () => throw InterNodeDeadline(), CancellationToken.None, budgetMs: 0))!;
        Assert.That(ex.StatusCode, Is.EqualTo(StatusCode.Unavailable));
    }

    [Test]
    public void Cancellation_IsNeverRetried()
    {
        int attempts = 0;
        Assert.ThrowsAsync<OperationCanceledException>(() =>
            ClusterSettingsService.RunBootStepAsync<long>(() =>
            {
                attempts++;
                throw new OperationCanceledException();
            }, CancellationToken.None));
        Assert.That(attempts, Is.EqualTo(1));
    }

    [Test]
    public void MustRetryAnswer_IsNotReadAsNeverWritten()
    {
        // A partition between leaders answers MustRetry. Reading that as generation 0 would skip the
        // post-subscription re-scan; it must be a fault the boot step retries instead.
        Assert.Throws<InvalidOperationException>(() =>
            ClusterSettingsService.GenerationFromResponse(KeyValueResponseType.MustRetry, null));
        Assert.Throws<InvalidOperationException>(() =>
            ClusterSettingsService.GenerationFromResponse(KeyValueResponseType.Errored, null));
        Assert.That(ClusterSettingsService.GenerationFromResponse(KeyValueResponseType.DoesNotExist, null), Is.EqualTo(0));
    }
}
