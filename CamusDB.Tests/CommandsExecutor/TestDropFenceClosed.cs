
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Tests.Storage;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// In cluster mode the drop-intent marker is the only cross-node fence against a branch-create racing
/// on another node. If it cannot be acquired, the drop must fail CLOSED (retryable, no destructive
/// step) rather than purge the parent unfenced. These tests inject a fence-acquire failure and assert
/// the parent survives intact.
/// </summary>
public sealed class TestDropFenceClosed : SharedNodeBaseTest
{
    /// <summary>Fault fake: makes acquiring the drop-intent fence throw; every other op passes through.</summary>
    private sealed class FenceAcquireThrowsKahuna : DelegatingKahuna
    {
        public FenceAcquireThrowsKahuna(IKahuna inner) : base(inner) { }

        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
            HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision,
            KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken,
            long routedGeneration = 0, string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (key.Contains("dbregistry/drop-intent:", StringComparison.Ordinal))
                throw new InvalidOperationException("injected drop-fence acquire failure");

            return base.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags,
                expiresMs, durability, cancellationToken, routedGeneration, coordinatorKey, operationId);
        }
    }

    [Test]
    [NonParallelizable]
    public async Task ClusterDrop_FailsClosed_WhenFenceAcquireThrows()
    {
        // Create a root database through the normal cluster executor / shared registry.
        (string rootName, _, _) = await CreateDatabase();
        string rootId = sharedRegistry!.Get(rootName)!.Id;

        // Build a second cluster-mode executor whose registry cannot acquire the drop fence.
        FenceAcquireThrowsKahuna fault = new(SharedKahuna);
        await using DatabaseRegistry faultRegistry = await DatabaseRegistry.OpenForTestingAsync(SharedNode, fault, isClusterMode: true);
        CommandExecutor faultExecutor = new(new CommandValidator(), new CatalogsManager(logger), logger,
            sharedNode: SharedNode, registry: faultRegistry, isClusterMode: true);

        // The drop must fail closed with a retryable error.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await faultExecutor.DropDatabase(new DropDatabaseTicket(rootName)));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry),
            "a cluster drop that cannot fence must fail with a retryable error");

        // The parent survives: still registered, no drop-intent marker, and still openable.
        Assert.That(sharedRegistry.Get(rootName), Is.Not.Null, "the parent must remain registered");
        Assert.That(await faultRegistry.HasDropIntentAsync(rootId), Is.False,
            "no drop-intent marker may be left behind after a failed-closed drop");

        CommandExecutor cleanExecutor = CreateCommandExecutor();
        DatabaseDescriptor reopened = await cleanExecutor.OpenDatabase(rootName);
        Assert.That(reopened, Is.Not.Null, "the parent must still be openable after the failed-closed drop");
    }
}
