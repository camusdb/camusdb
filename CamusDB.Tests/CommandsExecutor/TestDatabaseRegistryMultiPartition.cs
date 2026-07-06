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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;
using Kahuna;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Exercises <see cref="DatabaseRegistry"/> against a node with more than one user partition
/// (<c>InitialPartitions = 3</c>, giving hash-pool partitions 1..3). Every other registry/command
/// test runs single-partition, so the multi-partition registry-open and reload path is otherwise
/// uncovered.
///
/// <para>Regression guard: the registry load must not depend on a routed transaction rollback. A
/// read-write transaction hash-routes its commit/rollback by transaction id into <c>[1,
/// InitialPartitions]</c>; when the registry load used one, its rollback could route to a partition
/// and (during startup) throw <c>RaftException: Invalid partition</c>, permanently faulting the
/// cached registry. The load now runs as a zero-identity read-only snapshot with no rollback to
/// route, so opening/reloading the registry must succeed regardless of partition count.</para>
/// </summary>
[NonParallelizable]
internal sealed class TestDatabaseRegistryMultiPartition
{
    private string? tempDir;
    private EmbeddedKahuna? sharedNode;

    [SetUp]
    public async Task Setup()
    {
        tempDir = Path.Combine(Path.GetTempPath(), "camusdb-registry-mp-test-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(tempDir);
        CamusConfig.DataDirectory = tempDir;

        sharedNode = new EmbeddedKahuna(new EmbeddedKahunaOptions
        {
            NodeName = "registry-mp-test",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 3
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
    private static string NewName() => Guid.NewGuid().ToString("n");

    [Test]
    public async Task OpenRegistry_ThreePartitions_Succeeds()
    {
        // Opening the registry runs the read-only load scan; it must not throw with >1 partition.
        await using DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!);
        Assert.IsNotNull(registry);
    }

    [Test]
    public async Task RegisterMany_ThenReopen_LoadsAllEntries()
    {
        // Register several databases whose ids/keys spread across the 3-partition hash pool.
        Dictionary<string, string> expected = new();
        await using (DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(sharedNode!))
        {
            for (int i = 0; i < 12; i++)
            {
                string name = NewName();
                string id = NewId();
                await registry.RegisterAsync(name, id);
                expected[name] = id;
            }
        }

        // Reopen: a fresh OpenAsync runs LoadAsync (the read-only scan) from scratch. Every
        // registered database must be reloaded — proving the zero-identity load reads correctly
        // across all partitions with no routed rollback.
        await using DatabaseRegistry reopened = await DatabaseRegistry.OpenAsync(sharedNode!);

        List<DatabaseRegistryEntry> loaded = reopened.List().ToList();
        Assert.AreEqual(expected.Count, loaded.Count, "reopened registry must load every registered database");

        foreach ((string name, string id) in expected)
        {
            Assert.IsTrue(reopened.TryResolveId(name, out string resolvedId), $"missing database {name} after reload");
            Assert.AreEqual(id, resolvedId, $"wrong id for {name} after reload");
        }
    }
}
