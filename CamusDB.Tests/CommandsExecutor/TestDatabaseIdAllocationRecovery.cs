
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Shared.Sequences;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Database-id allocation must not hand out an id that a registered database still holds.
///
/// <para>The id counter is a persistent monotonic Kahuna sequence, so in the normal case it cannot
/// repeat. It can, however, be <i>re-created</i> rather than resumed — the ensure step reports that it
/// created the sequence rather than that it already existed — and a counter restarting from zero
/// re-issues ids that live databases are still using. That surfaced as an intermittent
/// <c>Database id 'H' is already registered under name '…'</c> failing a <c>CREATE DATABASE</c> in an
/// unrelated test's set-up, which is a miserable thing to debug from the symptom.</para>
///
/// <para>These tests pin the recovery rather than the mechanism: whatever the counter does, allocation
/// must return an id that is free, and registration must not collide with a live one.</para>
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node per test. Running node-booting fixtures concurrently
// multiplies live nodes and is what exhausted memory in the suite before they were serialized.
[NonParallelizable]
public sealed class TestDatabaseIdAllocationRecovery : BaseTest
{
    /// <summary>
    /// Allocated ids are unique across many allocations against one registry — the property the
    /// collision violated.
    /// </summary>
    [Test]
    public async Task AllocatedIdsAreNeverReusedWithinARegistry()
    {
        DatabaseRegistry registry = sharedRegistry!;

        HashSet<string> seen = new(StringComparer.Ordinal);

        for (int i = 0; i < 40; i++)
        {
            string id = await registry.AllocateIdAsync();

            Assert.IsTrue(seen.Add(id), $"id '{id}' was allocated twice");
        }
    }

    /// <summary>
    /// The recovery itself, driven by the actual fault: the id counter is deleted behind the registry,
    /// so the next ensure re-creates it from zero and it starts re-issuing ids that live databases
    /// still hold. Allocation must skip those rather than hand back a live id.
    ///
    /// <para>Without the skip this fails exactly as it did in the wild — the re-registration throws
    /// "Database id '…' is already registered under name '…'".</para>
    /// </summary>
    [Test]
    public async Task AllocationRecoversWhenTheIdCounterIsResetBehindTheRegistry()
    {
        DatabaseRegistry registry = sharedRegistry!;
        IKahuna kahuna = TestNode!.Kahuna;

        // Register a run of databases so the registry holds a block of live ids.
        List<string> registered = new();

        for (int i = 0; i < 8; i++)
        {
            string id = await registry.AllocateIdAsync();

            await registry.RegisterAsync($"idalloc_{Guid.NewGuid():N}", id);
            registered.Add(id);
        }

        // The fault: the counter disappears, so the next ensure creates it afresh at zero.
        SequenceResponseType deleted = await kahuna.LocateAndDeleteSequence(
            registry.IdSequenceKeyForTests, SequenceDurability.Persistent, default);

        Assert.AreEqual(SequenceResponseType.Success, deleted,
            "sanity: the test must actually have removed the id counter");

        // Allocation must still yield free ids, and registering them must not collide.
        for (int i = 0; i < 8; i++)
        {
            string id = await registry.AllocateIdAsync();

            CollectionAssert.DoesNotContain(registered, id,
                "allocation returned an id that is already registered after the counter reset");

            Assert.DoesNotThrowAsync(
                async () => await registry.RegisterAsync($"idalloc_after_{Guid.NewGuid():N}", id),
                $"registering freshly allocated id '{id}' must not collide with a live database");

            registered.Add(id);
        }
    }
}
