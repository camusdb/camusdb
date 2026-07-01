/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;
using Kahuna;
using Kommander.Time;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Tests for BR1: branch ancestry on <see cref="DatabaseRegistryEntry"/> and
/// cross-node entry resolution via <see cref="DatabaseRegistry.TryResolveEntryAsync"/>.
/// </summary>
[NonParallelizable]
internal sealed class TestDatabaseBranchAncestry
{
    private string? tempDir;
    private EmbeddedKahuna? sharedNode;

    [SetUp]
    public async Task Setup()
    {
        tempDir = Path.Combine(Path.GetTempPath(), "camusdb-ancestry-test-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(tempDir);
        CamusConfig.DataDirectory = tempDir;

        sharedNode = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName = "ancestry-test",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        });
        await sharedNode.StartAsync(CancellationToken.None);
        await sharedNode.WaitForLeaderAsync("warmup", CancellationToken.None);
    }

    [TearDown]
    public async Task Teardown()
    {
        if (sharedNode is not null)
            await sharedNode.DisposeAsync();

        if (tempDir is not null && Directory.Exists(tempDir))
            Directory.Delete(tempDir, recursive: true);
    }

    private static string NewId() => ObjectIdGenerator.Generate().ToString();
    private static string NewName() => "db_" + Guid.NewGuid().ToString("n");

    private HLCTimestamp MintTimestamp() =>
        sharedNode!.Raft.HybridLogicalClock.SendOrLocalEvent(sharedNode!.Raft.GetLocalNodeId());

    // -----------------------------------------------------------------------
    // Root databases have empty ancestry
    // -----------------------------------------------------------------------

    [Test]
    public async Task Root_HasEmptyAncestors()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!);

        string name = NewName();
        DatabaseRegistryEntry entry = await registry.RegisterAsync(name, NewId());

        Assert.IsNotNull(entry.Ancestors);
        Assert.AreEqual(0, entry.Ancestors.Count, "root must have empty ancestry");
    }

    [Test]
    public async Task Root_EmptyAncestors_SurviveReopen()
    {
        string name = NewName();
        string id = NewId();

        DatabaseRegistry first = await DatabaseRegistry.OpenAsync(sharedNode!);
        await first.RegisterAsync(name, id);
        await first.DisposeAsync();

        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync(sharedNode!);
        DatabaseRegistryEntry? loaded = second.GetById(id);

        Assert.IsNotNull(loaded, "entry must survive reopen");
        Assert.IsNotNull(loaded!.Ancestors);
        Assert.AreEqual(0, loaded.Ancestors.Count, "reloaded root must still have empty ancestry");
    }

    // -----------------------------------------------------------------------
    // Branch entries carry ancestry
    // -----------------------------------------------------------------------

    [Test]
    public async Task Branch_AncestorsPersisted()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!);

        string parentId = NewId();
        HLCTimestamp forkT = MintTimestamp();

        List<DatabaseBranchAncestor> ancestors =
        [
            new DatabaseBranchAncestor { DatabaseId = parentId, ForkTimestamp = forkT }
        ];

        string branchName = NewName();
        DatabaseRegistryEntry entry = await registry.RegisterAsync(branchName, NewId(), ancestors);

        Assert.AreEqual(1, entry.Ancestors.Count);
        Assert.AreEqual(parentId, entry.Ancestors[0].DatabaseId);
        Assert.AreEqual(forkT, entry.Ancestors[0].ForkTimestamp);
    }

    [Test]
    public async Task Branch_AncestorsSurviveReopen()
    {
        string parentId = NewId();
        HLCTimestamp forkT = MintTimestamp();
        string branchName = NewName();
        string branchId = NewId();

        DatabaseRegistry first = await DatabaseRegistry.OpenAsync(sharedNode!);
        await first.RegisterAsync(branchName, branchId,
            [new DatabaseBranchAncestor { DatabaseId = parentId, ForkTimestamp = forkT }]);
        await first.DisposeAsync();

        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync(sharedNode!);
        DatabaseRegistryEntry? loaded = second.GetById(branchId);

        Assert.IsNotNull(loaded);
        Assert.AreEqual(1, loaded!.Ancestors.Count);
        Assert.AreEqual(parentId, loaded.Ancestors[0].DatabaseId);
        Assert.AreEqual(forkT, loaded.Ancestors[0].ForkTimestamp);
    }

    // -----------------------------------------------------------------------
    // Deep chain: ancestry is [(immediate parent, T1)] + grandparent.Ancestors
    // -----------------------------------------------------------------------

    [Test]
    public async Task DeepChain_ThreeLevels_AncestryPreserved()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!);

        // Level A: root
        string aId = NewId();
        await registry.RegisterAsync(NewName(), aId);

        // Level B: branch of A
        HLCTimestamp forkTA = MintTimestamp();
        string bId = NewId();
        List<DatabaseBranchAncestor> bAncestors =
        [
            new DatabaseBranchAncestor { DatabaseId = aId, ForkTimestamp = forkTA }
        ];
        await registry.RegisterAsync(NewName(), bId, bAncestors);

        // Level C: branch of B — ancestors = [(B, forkTB)] + B.Ancestors = [(B, forkTB), (A, forkTA)]
        HLCTimestamp forkTB = MintTimestamp();
        string cId = NewId();
        List<DatabaseBranchAncestor> cAncestors =
        [
            new DatabaseBranchAncestor { DatabaseId = bId, ForkTimestamp = forkTB },
            .. bAncestors
        ];
        await registry.RegisterAsync(NewName(), cId, cAncestors);

        DatabaseRegistryEntry? cEntry = registry.GetById(cId);
        Assert.IsNotNull(cEntry);
        Assert.AreEqual(2, cEntry!.Ancestors.Count, "C must have 2 ancestors");
        Assert.AreEqual(bId, cEntry.Ancestors[0].DatabaseId, "nearest ancestor must be B");
        Assert.AreEqual(forkTB, cEntry.Ancestors[0].ForkTimestamp);
        Assert.AreEqual(aId, cEntry.Ancestors[1].DatabaseId, "second ancestor must be A");
        Assert.AreEqual(forkTA, cEntry.Ancestors[1].ForkTimestamp);
    }

    // -----------------------------------------------------------------------
    // Rename preserves ancestry
    // -----------------------------------------------------------------------

    [Test]
    public async Task Rename_PreservesAncestors()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!);

        string parentId = NewId();
        HLCTimestamp forkT = MintTimestamp();
        string branchId = NewId();
        string originalName = NewName();
        string newName = NewName();

        await registry.RegisterAsync(originalName, branchId,
            [new DatabaseBranchAncestor { DatabaseId = parentId, ForkTimestamp = forkT }]);

        await registry.RenameAsync(originalName, newName);

        DatabaseRegistryEntry? after = registry.GetById(branchId);
        Assert.IsNotNull(after);
        Assert.AreEqual(newName, after!.Name, "name must be updated");
        Assert.AreEqual(branchId, after.Id, "id must be unchanged");
        Assert.AreEqual(1, after.Ancestors.Count, "ancestry must be unchanged after rename");
        Assert.AreEqual(parentId, after.Ancestors[0].DatabaseId);
        Assert.AreEqual(forkT, after.Ancestors[0].ForkTimestamp);
    }

    [Test]
    public async Task Rename_PreservesAncestors_SurvivesReopen()
    {
        string parentId = NewId();
        HLCTimestamp forkT = MintTimestamp();
        string branchId = NewId();
        string originalName = NewName();
        string newName = NewName();

        DatabaseRegistry first = await DatabaseRegistry.OpenAsync(sharedNode!);
        await first.RegisterAsync(originalName, branchId,
            [new DatabaseBranchAncestor { DatabaseId = parentId, ForkTimestamp = forkT }]);
        await first.RenameAsync(originalName, newName);
        await first.DisposeAsync();

        await using DatabaseRegistry second = await DatabaseRegistry.OpenAsync(sharedNode!);
        DatabaseRegistryEntry? loaded = second.GetById(branchId);

        Assert.IsNotNull(loaded);
        Assert.AreEqual(newName, loaded!.Name);
        Assert.AreEqual(1, loaded.Ancestors.Count, "ancestry must survive rename+reopen");
        Assert.AreEqual(parentId, loaded.Ancestors[0].DatabaseId);
        Assert.AreEqual(forkT, loaded.Ancestors[0].ForkTimestamp);
    }

    // -----------------------------------------------------------------------
    // TryResolveEntryAsync: cross-node live-KV fallback returns full entry with ancestry
    // -----------------------------------------------------------------------

    [Test]
    public async Task TryResolveEntryAsync_CacheHit_ReturnsEntry()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!);

        string parentId = NewId();
        HLCTimestamp forkT = MintTimestamp();
        string branchId = NewId();
        string name = NewName();

        await registry.RegisterAsync(name, branchId,
            [new DatabaseBranchAncestor { DatabaseId = parentId, ForkTimestamp = forkT }]);

        DatabaseRegistryEntry? resolved = await registry.TryResolveEntryAsync(name);

        Assert.IsNotNull(resolved);
        Assert.AreEqual(branchId, resolved!.Id);
        Assert.AreEqual(1, resolved.Ancestors.Count);
        Assert.AreEqual(parentId, resolved.Ancestors[0].DatabaseId);
    }

    [Test]
    public async Task TryResolveEntryAsync_LiveKvFallback_ReturnsEntryWithAncestors()
    {
        // r1 registers a branch; r2 (opened before the write) has a stale cache.
        // TryResolveEntryAsync on r2 must fall back to Kahuna and return the full entry.
        await using EmbeddedKahuna node = new(new EmbeddedKahunaOptions
        {
            NodeName = "ancestry-fallback-test-" + Guid.NewGuid().ToString("n"),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        });
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync("warmup", CancellationToken.None);

        await using DatabaseRegistry r1 = await DatabaseRegistry.OpenAsync(node);
        await using DatabaseRegistry r2 = await DatabaseRegistry.OpenAsync(node);

        string parentId = NewId();
        HLCTimestamp forkT = node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());
        string branchId = NewId();
        string name = NewName();

        await r1.RegisterAsync(name, branchId,
            [new DatabaseBranchAncestor { DatabaseId = parentId, ForkTimestamp = forkT }]);

        // r2 cache is stale — sync lookup misses.
        Assert.IsFalse(r2.TryResolveId(name, out _), "r2 cache must not reflect r1's write");

        // Async fallback must find the entry with the full ancestry.
        DatabaseRegistryEntry? resolved = await r2.TryResolveEntryAsync(name);

        Assert.IsNotNull(resolved, "TryResolveEntryAsync must find the entry via live KV");
        Assert.AreEqual(branchId, resolved!.Id);
        Assert.AreEqual(1, resolved.Ancestors.Count, "ancestry must survive the live KV round-trip");
        Assert.AreEqual(parentId, resolved.Ancestors[0].DatabaseId);
        Assert.AreEqual(forkT, resolved.Ancestors[0].ForkTimestamp);

        // Cache must be warmed after the live read.
        Assert.IsTrue(r2.TryResolveId(name, out string cached), "cache must be backfilled after live read");
        Assert.AreEqual(branchId, cached);
    }

    [Test]
    public async Task TryResolveEntryAsync_UnknownName_ReturnsNull()
    {
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!);

        DatabaseRegistryEntry? result = await registry.TryResolveEntryAsync("nonexistent-db");
        Assert.IsNull(result);
    }
}

/// <summary>
/// End-to-end tests that open databases through <see cref="CommandExecutor"/> and
/// verify that <see cref="DatabaseDescriptor.Ancestors"/> is populated correctly by
/// <see cref="DatabaseOpener"/> — the production path that BR3 will consume.
/// </summary>
[NonParallelizable]
internal sealed class TestDatabaseBranchAncestryEndToEnd : BaseTest
{
    private static string NewName() => "db_" + Guid.NewGuid().ToString("n");

    // -----------------------------------------------------------------------
    // Root database opened via executor has empty ancestry
    // -----------------------------------------------------------------------

    [Test]
    public async Task RootDatabase_OpenedViaExecutor_AncestorsEmpty()
    {
        (string dbname, DatabaseDescriptor descriptor, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(dbname, executor);

        Assert.IsNotNull(descriptor.Ancestors, "Ancestors must never be null");
        Assert.AreEqual(0, descriptor.Ancestors.Count, "root database must have empty ancestry");
    }

    // -----------------------------------------------------------------------
    // Branch entry opened via executor surfaces ancestry from registry
    // -----------------------------------------------------------------------

    [Test]
    public async Task BranchEntry_OpenedViaExecutor_AncestorsSurfacedOnDescriptor()
    {
        // Create a real root database so its schema metadata exists.
        (string rootName, DatabaseDescriptor rootDescriptor, CommandExecutor rootExecutor) = await CreateDatabase();
        TrackDatabase(rootName, rootExecutor);

        string parentId = rootDescriptor.Id;

        // Mint a fork timestamp from the same HLC that the opener will use.
        HLCTimestamp forkT = TestNode!.Raft.HybridLogicalClock.SendOrLocalEvent(
            TestNode!.Raft.GetLocalNodeId());

        // Register a branch entry directly in the registry.
        // (CREATE DATABASE … BRANCH FROM … is BR2; here we bypass the SQL surface and
        // inject the branch entry to verify the DatabaseOpener wiring in isolation.)
        string branchId = await sharedRegistry!.AllocateIdAsync();
        string branchName = NewName();
        await sharedRegistry.RegisterAsync(branchName, branchId,
            [new DatabaseBranchAncestor { DatabaseId = parentId, ForkTimestamp = forkT }]);

        // Open the branch through the executor. The branch has no schema metadata,
        // so LoadMetaAsync will succeed with an empty schema (the missing version key
        // is treated as a fresh database) — that is the correct behaviour for a just-
        // branched database before any DDL.
        CommandExecutor branchExecutor = CreateCommandExecutor();
        DatabaseDescriptor branchDescriptor = await branchExecutor.OpenDatabase(branchName);
        TrackDatabase(branchName, branchExecutor);

        Assert.IsNotNull(branchDescriptor.Ancestors, "Ancestors must never be null on a branch descriptor");
        Assert.AreEqual(1, branchDescriptor.Ancestors.Count,
            "branch descriptor must carry the one ancestor registered in the registry");
        Assert.AreEqual(parentId, branchDescriptor.Ancestors[0].DatabaseId,
            "ancestor DatabaseId must match the parent id");
        Assert.AreEqual(forkT, branchDescriptor.Ancestors[0].ForkTimestamp,
            "ancestor ForkTimestamp must match the registered fork timestamp");
    }
}
