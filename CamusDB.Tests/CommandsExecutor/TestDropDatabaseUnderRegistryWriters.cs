/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// DROP DATABASE scans the whole registry bucket for live branch descendants. That scan once ran
/// inside a read-write transaction, which pins an OCC snapshot on every registry key it reads; a
/// concurrent CREATE or DROP DATABASE that committed a newer revision of a pinned key between two
/// reads of the same page made Kahuna abort the scan, and the drop failed with a raw
/// <c>Range scan ... over '_system/dbregistry' ... failed with Aborted</c>. These tests drive the
/// descendant scan, and DROP DATABASE itself, while other sessions write the registry.
/// </summary>
[NonParallelizable]
internal sealed class TestDropDatabaseUnderRegistryWriters : BaseTest
{
    /// <summary>
    /// The reported workload: parallel sessions each create a database, create a table in it and
    /// drop it, on one engine. Every drop must succeed and every name must be gone afterwards.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task ParallelCreateAndDrop_EveryDropSucceeds()
    {
        const int Workers = 8;
        const int Rounds = 5;

        CommandExecutor executor = CreateCommandExecutor();
        List<string> created = [];
        object createdLock = new();

        async Task Worker(int worker)
        {
            for (int round = 0; round < Rounds; round++)
            {
                string name = $"drw_{worker}_{round}_{Guid.NewGuid():N}";

                await executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: false));
                lock (createdLock)
                    created.Add(name);

                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                    txnState: null!, database: name,
                    sql: "CREATE TABLE relays (id OBJECT_ID PRIMARY KEY, name STRING)", parameters: null));

                await executor.DropDatabase(new DropDatabaseTicket(name));
            }
        }

        await Task.WhenAll(Enumerable.Range(0, Workers).Select(Worker));

        Assert.AreEqual(Workers * Rounds, created.Count, "every round must create its database");
        foreach (string name in created)
            Assert.IsNull(sharedRegistry!.Get(name), $"'{name}' must not be registered after its drop");
    }

    /// <summary>
    /// The descendant scan itself, looped while a bounded burst of registry writes and deletes runs
    /// against the same bucket. Every scan must complete, and none may report a descendant that does
    /// not exist. The burst is bounded so the test measures robustness, not a latest-read page starved
    /// by an unbounded writer.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task HasLiveDescendants_SurvivesConcurrentRegistryWriters()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string rootName = "drw_root_" + Guid.NewGuid().ToString("N");
        DatabaseDescriptor root = await executor.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        string idA = ObjectIdGenerator.Generate().ToString();
        string idB = ObjectIdGenerator.Generate().ToString();

        Task writer = Task.Run(async () =>
        {
            for (int i = 0; i < 400; i++)
            {
                string id = (i % 2 == 0) ? idA : idB;
                await sharedRegistry!.Orphans.WriteDatabaseOrphanAsync(new OrphanDatabaseRecord
                {
                    Id = id,
                    FormerName = "contended",
                    DroppedAt = HLCTimestamp.Zero,
                });
                await sharedRegistry.Orphans.DeleteDatabaseOrphanAsync(id);
            }
        });

        try
        {
            int scans = 0;
            while (!writer.IsCompleted)
            {
                Assert.IsFalse(await sharedRegistry!.HasLiveDescendantsAsync(root.Id),
                    "a root with no branches has no live descendants");
                scans++;
            }

            await writer;
            Assert.IsFalse(await sharedRegistry!.HasLiveDescendantsAsync(root.Id));
            Assert.Greater(scans, 0, "the scan must run at least once while the writer is active");
        }
        finally
        {
            await sharedRegistry!.Orphans.DeleteDatabaseOrphanAsync(idA);
            await sharedRegistry.Orphans.DeleteDatabaseOrphanAsync(idB);
        }
    }

    /// <summary>
    /// The read-only descendant scan must still see a branch the local cache does not hold: the drop
    /// fence depends on it. A registry loaded before the branch existed answers from its persistent
    /// scan, never from its cache, and must report the branch.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task HasLiveDescendants_SeesBranchMissingFromLocalCache()
    {
        await using DatabaseRegistry staleRegistry = await DatabaseRegistry.OpenAsync(TestNode!, Options);

        CommandExecutor executor = CreateCommandExecutor();
        string rootName = "drw_src_" + Guid.NewGuid().ToString("N");
        DatabaseDescriptor root = await executor.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        string branchName = "drw_br_" + Guid.NewGuid().ToString("N");
        await executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        Assert.IsNull(staleRegistry.Get(branchName), "precondition: the branch is absent from the stale registry's cache");
        Assert.IsTrue(await staleRegistry.HasLiveDescendantsAsync(root.Id),
            "the persistent scan must find a branch the local cache missed");
    }
}
