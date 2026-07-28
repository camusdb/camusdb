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

using CamusDB.Core.Auth;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Cross-node auth-catalog cache coherence: a user or grant created, dropped, or changed through ONE
/// node's <see cref="AuthCatalog"/> must become visible (or stop being visible) through ANOTHER node
/// whose in-memory cache was warmed with the old state. Two <see cref="AuthCatalog"/> instances over
/// the same shared node stand in for two cluster nodes — each keeps its own cache but shares the
/// Raft-replicated persistent store, which is exactly the surface the generation stamp closes. Mirrors
/// <see cref="TestRegistryCoherence"/> for the database registry.
/// </summary>
[NonParallelizable]
internal sealed class TestAuthCatalogCoherence : BaseTest
{
    // Cluster mode: two catalogs stand in for two cluster nodes, so each must run the cross-node
    // generation revalidation on a read — the path under test. In standalone mode a read is trusted
    // without revalidation, which is not what these tests exercise.
    private async Task<(AuthCatalog a, AuthCatalog b)> TwoNodesAsync()
        => (await AuthCatalog.OpenAsync(TestNode!, isClusterMode: true),
            await AuthCatalog.OpenAsync(TestNode!, isClusterMode: true));

    private static GrantScope DbScope(string databaseId) =>
        new() { Kind = GrantScopeKind.Database, DatabaseId = databaseId, DatabaseName = databaseId };

    /// <summary>A user created on node A must become visible on node B after B's stale generation revalidates.</summary>
    [Test]
    [NonParallelizable]
    public async Task CreateUser_OnOneNode_VisibleOnAnother()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");

        // Warm B with a miss (loads B's cache at the current generation).
        Assert.IsNull(await b.TryGetUserAsync(user));

        await a.CreateUserAsync(user, PasswordHasher.Hash("x"), ifNotExists: false);

        UserRecord? seen = await b.TryGetUserAsync(user);
        Assert.IsNotNull(seen, "a user created on another node must become visible after revalidation");
        Assert.AreEqual(user, seen!.Name);
    }

    /// <summary>A grant added on node A must be visible on node B.</summary>
    [Test]
    [NonParallelizable]
    public async Task Grant_OnOneNode_VisibleOnAnother()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");
        await a.CreateUserAsync(user, null, ifNotExists: false);

        // Warm B (sees the user, no grants yet).
        Assert.IsEmpty(await b.ListGrantsAsync(user));

        await a.GrantAsync(user, DbScope("db1"), Privilege.Select | Privilege.Insert, revoke: false);

        // A cold catalog must load the grant from KV — proves it was durably persisted, not just held
        // in a cache. (This is the check that first caught the grant key landing in the wrong routing
        // bucket because its leaf contained a '/'.)
        AuthCatalog c = await AuthCatalog.OpenAsync(TestNode!, isClusterMode: true);
        Assert.AreEqual(1, (await c.ListGrantsAsync(user)).Count, "cold node must load the persisted grant from KV");

        IReadOnlyList<GrantRecord> grants = await b.ListGrantsAsync(user);
        Assert.AreEqual(1, grants.Count, "a grant added on another node must be visible after revalidation");
        Assert.AreEqual(Privilege.Select | Privilege.Insert, grants[0].Privileges);
    }

    /// <summary>A user dropped on node A must stop resolving through node B's stale cache hit.</summary>
    [Test]
    [NonParallelizable]
    public async Task DropUser_OnOneNode_RevalidatesGoneOnAnother()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");
        await a.CreateUserAsync(user, null, ifNotExists: false);

        // Warm B with a real hit.
        Assert.IsNotNull(await b.TryGetUserAsync(user));

        await a.DropUserAsync(user, ifExists: false);

        Assert.IsNull(await b.TryGetUserAsync(user),
            "a user dropped on another node must no longer resolve from a stale cache hit");
    }

    /// <summary>A revoke on node A must shrink the privilege mask node B sees.</summary>
    [Test]
    [NonParallelizable]
    public async Task Revoke_OnOneNode_UpdatesMaskOnAnother()
    {
        (AuthCatalog a, AuthCatalog b) = await TwoNodesAsync();

        string user = "coh_" + Guid.NewGuid().ToString("n");
        await a.CreateUserAsync(user, null, ifNotExists: false);
        await a.GrantAsync(user, DbScope("db1"), Privilege.Select | Privilege.Insert, revoke: false);

        // Warm B with the full mask.
        Assert.AreEqual(Privilege.Select | Privilege.Insert, (await b.ListGrantsAsync(user)).Single().Privileges);

        await a.GrantAsync(user, DbScope("db1"), Privilege.Insert, revoke: true);

        Assert.AreEqual(Privilege.Select, (await b.ListGrantsAsync(user)).Single().Privileges,
            "a revoke on another node must be reflected after revalidation");
    }
}
