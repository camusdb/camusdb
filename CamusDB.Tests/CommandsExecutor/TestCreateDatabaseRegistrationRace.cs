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
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Parks several creates of one name after the existence check of <c>CREATE DATABASE</c> and lets
/// them go only once every one of them arrived, so all of them pass the check before any of them
/// registers. That is the interleaving in which every create but one loses the registry's name-key
/// write; without the gate, the creates mostly serialize and the losing path is rarely reached.
/// </summary>
internal sealed class ExistenceCheckGate
{
    private readonly int parties;

    private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private int arrived;

    public ExistenceCheckGate(int parties) => this.parties = parties;

    /// <summary>How many creates reached the gate, i.e. passed the existence check.</summary>
    public int Arrived => Volatile.Read(ref arrived);

    public async Task WaitAsync()
    {
        if (Interlocked.Increment(ref arrived) == parties)
            released.TrySetResult();

        // Bounded, so a create that fails before the gate turns into a test failure, not a hang.
        await released.Task.WaitAsync(TimeSpan.FromSeconds(30));
    }
}

/// <summary>
/// <c>CREATE DATABASE … IF NOT EXISTS</c> on one node: a create that loses the registration race to
/// a concurrent create of the same name must return the winner's database instead of failing with
/// <c>DatabaseAlreadyExists</c>. Without <c>IF NOT EXISTS</c>, exactly one create wins and every
/// other one still fails.
/// </summary>
[NonParallelizable]
internal sealed class TestCreateDatabaseRegistrationRace : BaseTest
{
    private const int Racers = 4;

    private static string NewName() => "db_" + Guid.NewGuid().ToString("n")[..12];

    private static async Task<(List<DatabaseDescriptor> Created, List<CamusDBException> Failed)> RaceAsync(
        IReadOnlyList<Func<Task<DatabaseDescriptor>>> creates)
    {
        Task<DatabaseDescriptor>[] tasks = creates.Select(create => Task.Run(create)).ToArray();

        List<DatabaseDescriptor> created = [];
        List<CamusDBException> failed = [];

        foreach (Task<DatabaseDescriptor> task in tasks)
        {
            try
            {
                created.Add(await task);
            }
            catch (CamusDBException ex)
            {
                failed.Add(ex);
            }
        }

        return (created, failed);
    }

    [Test]
    public async Task ConcurrentCreateIfNotExists_LosersReturnTheWinner()
    {
        string name = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(name, executor);

        ExistenceCheckGate gate = new(Racers);
        executor.TestInterceptAfterDatabaseExistenceCheck = gate.WaitAsync;

        (List<DatabaseDescriptor> created, List<CamusDBException> failed) = await RaceAsync(
            Enumerable.Range(0, Racers)
                .Select(_ => (Func<Task<DatabaseDescriptor>>)(() =>
                    executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: true))))
                .ToList());

        executor.TestInterceptAfterDatabaseExistenceCheck = null;

        Assert.AreEqual(Racers, gate.Arrived, "every create must pass the existence check before any registers");
        Assert.IsEmpty(failed, "no CREATE DATABASE … IF NOT EXISTS may fail: " +
            string.Join("; ", failed.Select(ex => $"{ex.Code}: {ex.Message}")));

        string registeredId = sharedRegistry!.Get(name)!.Id;
        Assert.AreEqual(Racers, created.Count);
        Assert.That(created.Select(d => d.Id), Is.All.EqualTo(registeredId),
            "every create must return the database the registry holds for the name");
    }

    [Test]
    public async Task ConcurrentCreateIfNotExists_ViaSql_AllStatementsSucceed()
    {
        string name = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(name, executor);

        ExistenceCheckGate gate = new(Racers);
        executor.TestInterceptAfterDatabaseExistenceCheck = gate.WaitAsync;

        (List<DatabaseDescriptor> created, List<CamusDBException> failed) = await RaceAsync(
            Enumerable.Range(0, Racers)
                .Select(_ => (Func<Task<DatabaseDescriptor>>)(async () =>
                    (await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                        txnState: null!,
                        database: "",
                        sql: $"CREATE DATABASE IF NOT EXISTS {name}",
                        parameters: null))).Database!))
                .ToList());

        executor.TestInterceptAfterDatabaseExistenceCheck = null;

        Assert.AreEqual(Racers, gate.Arrived, "every statement must pass the existence check before any registers");
        Assert.IsEmpty(failed, "no CREATE DATABASE IF NOT EXISTS statement may fail: " +
            string.Join("; ", failed.Select(ex => $"{ex.Code}: {ex.Message}")));

        string registeredId = sharedRegistry!.Get(name)!.Id;
        Assert.That(created.Select(d => d.Id), Is.All.EqualTo(registeredId));
    }

    [Test]
    public async Task ConcurrentCreateWithoutIfNotExists_ExactlyOneSucceeds()
    {
        string name = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        TrackDatabase(name, executor);

        ExistenceCheckGate gate = new(Racers);
        executor.TestInterceptAfterDatabaseExistenceCheck = gate.WaitAsync;

        (List<DatabaseDescriptor> created, List<CamusDBException> failed) = await RaceAsync(
            Enumerable.Range(0, Racers)
                .Select(_ => (Func<Task<DatabaseDescriptor>>)(() =>
                    executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: false))))
                .ToList());

        executor.TestInterceptAfterDatabaseExistenceCheck = null;

        Assert.AreEqual(Racers, gate.Arrived);
        Assert.AreEqual(1, created.Count, "exactly one plain CREATE DATABASE may win the name");
        Assert.AreEqual(Racers - 1, failed.Count);
        Assert.That(failed.Select(ex => ex.Code), Is.All.EqualTo(CamusDBErrorCodes.DatabaseAlreadyExists));
        Assert.AreEqual(sharedRegistry!.Get(name)!.Id, created[0].Id);
    }

    /// <summary>
    /// The branch path copies the source's metadata and takes a snapshot hold before it registers, so a
    /// loser has real work to undo. It must undo it — no pending-create marker may stay behind — and
    /// then return the winning branch.
    /// </summary>
    [Test]
    public async Task ConcurrentBranchCreateIfNotExists_LosersCompensateAndReturnTheWinner()
    {
        string source = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        DatabaseDescriptor sourceDescriptor = await executor.CreateDatabase(new CreateDatabaseTicket(source, ifNotExists: false));
        TrackDatabase(source, executor);

        string branch = NewName();
        TrackDatabase(branch, executor);

        ExistenceCheckGate gate = new(Racers);
        executor.TestInterceptAfterDatabaseExistenceCheck = gate.WaitAsync;

        (List<DatabaseDescriptor> created, List<CamusDBException> failed) = await RaceAsync(
            Enumerable.Range(0, Racers)
                .Select(_ => (Func<Task<DatabaseDescriptor>>)(() =>
                    executor.CreateDatabase(new CreateDatabaseTicket(branch, ifNotExists: true, branchFrom: source))))
                .ToList());

        executor.TestInterceptAfterDatabaseExistenceCheck = null;

        Assert.AreEqual(Racers, gate.Arrived);
        Assert.IsEmpty(failed, "no CREATE DATABASE … IF NOT EXISTS BRANCH FROM may fail: " +
            string.Join("; ", failed.Select(ex => $"{ex.Code}: {ex.Message}")));

        DatabaseRegistryEntry entry = sharedRegistry!.Get(branch)!;
        Assert.That(created.Select(d => d.Id), Is.All.EqualTo(entry.Id));
        Assert.AreEqual(sourceDescriptor.Id, entry.Ancestors[0].DatabaseId, "the winner must be a branch of the source");

        Assert.IsEmpty(await sharedRegistry.PendingBranches.LoadOrphanBranchIdsAsync(),
            "a losing branch create must clear its pending-create marker after it purges its copied metadata");
    }
}

/// <summary>
/// <c>CREATE DATABASE … IF NOT EXISTS</c> across nodes: two engines with independent registry caches
/// over one shared Kahuna node. Neither cache knows the other's registration, so the loser learns of
/// the winner only from the name-key write that refuses it.
/// </summary>
[NonParallelizable]
internal sealed class TestCreateDatabaseRegistrationRaceCluster : SharedNodeBaseTest
{
    private static string NewName() => "db_" + Guid.NewGuid().ToString("n")[..12];

    private async Task<(DatabaseRegistry Registry, CommandExecutor Executor)> OpenSecondNodeAsync()
    {
        DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(SharedNode, Options, isClusterMode: true);
        CommandExecutor executor = new(new CommandValidator(Options), new CatalogsManager(logger), logger, Options,
            sharedNode: SharedNode, registry: registry, isClusterMode: true);
        return (registry, executor);
    }

    private async Task<(DatabaseDescriptor? Created, CamusDBException? Failed)[]> RaceAcrossNodesAsync(
        CommandExecutor nodeA, CommandExecutor nodeB, string name, bool ifNotExists)
    {
        ExistenceCheckGate gate = new(2);
        nodeA.TestInterceptAfterDatabaseExistenceCheck = gate.WaitAsync;
        nodeB.TestInterceptAfterDatabaseExistenceCheck = gate.WaitAsync;

        static async Task<(DatabaseDescriptor?, CamusDBException?)> Run(CommandExecutor node, string name, bool ifNotExists)
        {
            try
            {
                return (await node.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists)), null);
            }
            catch (CamusDBException ex)
            {
                return (null, ex);
            }
        }

        (DatabaseDescriptor?, CamusDBException?)[] results = await Task.WhenAll(
            Task.Run(() => Run(nodeA, name, ifNotExists)),
            Task.Run(() => Run(nodeB, name, ifNotExists)));

        nodeA.TestInterceptAfterDatabaseExistenceCheck = null;
        nodeB.TestInterceptAfterDatabaseExistenceCheck = null;

        Assert.AreEqual(2, gate.Arrived, "both nodes must pass the existence check before either registers");
        return results;
    }

    [Test]
    public async Task ConcurrentCreateIfNotExists_OnTwoNodes_BothReturnTheWinner()
    {
        string name = NewName();
        CommandExecutor nodeA = CreateCommandExecutor();
        TrackDatabase(name, nodeA);

        (DatabaseRegistry registryB, CommandExecutor nodeB) = await OpenSecondNodeAsync();
        await using DatabaseRegistry ownedRegistryB = registryB;
        await using CommandExecutor ownedNodeB = nodeB;

        (DatabaseDescriptor? Created, CamusDBException? Failed)[] results =
            await RaceAcrossNodesAsync(nodeA, nodeB, name, ifNotExists: true);

        foreach ((DatabaseDescriptor? _, CamusDBException? failed) in results)
            Assert.IsNull(failed, $"no node's CREATE DATABASE … IF NOT EXISTS may fail: {failed?.Code}: {failed?.Message}");

        string idOnA = (await sharedRegistry!.TryResolveEntryAsync(name))!.Id;
        string idOnB = (await registryB.TryResolveEntryAsync(name))!.Id;
        Assert.AreEqual(idOnA, idOnB, "both nodes must resolve the name to one database");
        Assert.That(results.Select(r => r.Created!.Id), Is.All.EqualTo(idOnA),
            "both creates must return the one registered database");
    }

    [Test]
    public async Task ConcurrentCreateWithoutIfNotExists_OnTwoNodes_ExactlyOneSucceeds()
    {
        string name = NewName();
        CommandExecutor nodeA = CreateCommandExecutor();
        TrackDatabase(name, nodeA);

        (DatabaseRegistry registryB, CommandExecutor nodeB) = await OpenSecondNodeAsync();
        await using DatabaseRegistry ownedRegistryB = registryB;
        await using CommandExecutor ownedNodeB = nodeB;

        (DatabaseDescriptor? Created, CamusDBException? Failed)[] results =
            await RaceAcrossNodesAsync(nodeA, nodeB, name, ifNotExists: false);

        Assert.AreEqual(1, results.Count(r => r.Created is not null), "exactly one node may win the name");
        CamusDBException loser = results.Single(r => r.Failed is not null).Failed!;
        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, loser.Code);
    }
}
