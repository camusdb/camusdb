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
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Tests.Storage;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Database ids, table ids, and the registry's cross-node generation stamp all come from persistent
/// monotonic counters in the storage layer, and every call against one can answer <c>MustRetry</c>:
/// the Raft partition owning the counter has no confirmed leader at that instant because a node is
/// still joining, an election is in flight, or leadership moved while the request was being
/// forwarded. It carries no state change and clears in milliseconds.
///
/// <para>That makes it a condition to ride out, never one to surface. Every DDL statement that names
/// a new relation — CREATE TABLE, CREATE VIEW, CTAS, a materialized-view refresh — allocates an id
/// first, so a single unretried <c>MustRetry</c> fails a user's statement over a routine leadership
/// blip. These tests pin the retry to the counter's <em>creation</em> as much as its advance: the
/// create is reached only on a counter's first use, which is exactly when a freshly started node is
/// most likely to still be electing.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestSequenceAllocationUnderLeadershipBlips : BaseTest
{
    private const string TableSequence = "_system/tableseq";
    private const string GenerationKey = "_system/dbregistry/generation";

    /// <summary>
    /// Answers <see cref="SequenceResponseType.MustRetry"/> for the next N calls against one named
    /// counter, then falls through to the real node. Scoped to a single sequence name, and armed
    /// explicitly rather than at construction, so a test can blip the one call it is about without its
    /// own setup consuming the blips first.
    /// </summary>
    private sealed class LeadershipBlipKahuna : DelegatingKahuna
    {
        private readonly string sequenceName;
        private int createBlips;
        private int advanceBlips;

        public int CreateCalls;
        public int AdvanceCalls;
        public int SuccessfulAdvances;

        public LeadershipBlipKahuna(IKahuna inner, string sequenceName) : base(inner)
            => this.sequenceName = sequenceName;

        public void Arm(int creates, int advances)
        {
            Volatile.Write(ref createBlips, creates);
            Volatile.Write(ref advanceBlips, advances);
        }

        private static bool Blip(ref int remaining)
            => Interlocked.Decrement(ref remaining) >= 0;

        public override Task<(SequenceResponseType, long)> LocateAndCreateSequence(
            string name, long initialValue, long increment, long? maxValue,
            SequenceDurability durability, CancellationToken cancellationToken)
        {
            if (!string.Equals(name, sequenceName, StringComparison.Ordinal))
                return base.LocateAndCreateSequence(
                    name, initialValue, increment, maxValue, durability, cancellationToken);

            Interlocked.Increment(ref CreateCalls);

            if (Blip(ref createBlips))
                return Task.FromResult((SequenceResponseType.MustRetry, -1L));

            return base.LocateAndCreateSequence(
                name, initialValue, increment, maxValue, durability, cancellationToken);
        }

        public override async Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(
            string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
        {
            if (!string.Equals(name, sequenceName, StringComparison.Ordinal))
                return await base.LocateAndNextSequenceValue(name, idempotencyKey, durability, cancellationToken);

            Interlocked.Increment(ref AdvanceCalls);

            if (Blip(ref advanceBlips))
                return (SequenceResponseType.MustRetry, default);

            (SequenceResponseType type, SequenceAllocation allocation) =
                await base.LocateAndNextSequenceValue(name, idempotencyKey, durability, cancellationToken);

            if (type == SequenceResponseType.Success)
                Interlocked.Increment(ref SuccessfulAdvances);

            return (type, allocation);
        }
    }

    /// <summary>
    /// Removes a counter so the next allocation genuinely takes the first-use path and has to create it.
    /// Safe because each test gets its own embedded node.
    /// </summary>
    private async Task DeleteSequenceAsync(string name)
        => await TestNode!.Kahuna.LocateAndDeleteSequence(
            name, SequenceDurability.Persistent, CancellationToken.None);

    /// <summary>
    /// The blip that reaches production: a node whose table-id counter does not exist yet, electing while
    /// the first CREATE TABLE tries to create it. The allocation must ride it out and return an id.
    /// </summary>
    [Test]
    public async Task ATableIdIsAllocatedThroughABlipOnTheCountersCreation()
    {
        await DeleteSequenceAsync(TableSequence);

        LeadershipBlipKahuna kahuna = new(TestNode!.Kahuna, TableSequence);
        DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(
            TestNode!, kahuna, Options, isClusterMode: true);

        try
        {
            kahuna.Arm(creates: 3, advances: 0);

            string tableId = await registry.AllocateTableIdAsync();

            Assert.IsNotEmpty(tableId, "a leadership blip during creation must not lose the allocation");
            Assert.AreEqual(
                4, Volatile.Read(ref kahuna.CreateCalls),
                "precondition: the create path was reached and retried past all three blips");
        }
        finally
        {
            await registry.DisposeAsync();
        }
    }

    /// <summary>
    /// The same for a database id, and for a blip on the advance rather than the creation — the two
    /// calls behind one allocation must be equally patient.
    /// </summary>
    [Test]
    public async Task ADatabaseIdIsAllocatedThroughABlipOnTheCountersAdvance()
    {
        LeadershipBlipKahuna kahuna = new(TestNode!.Kahuna, "_system/dbregistry/seq");

        DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(
            TestNode!, kahuna, Options, isClusterMode: true);

        try
        {
            kahuna.Arm(creates: 0, advances: 3);

            string databaseId = await registry.AllocateIdAsync();

            Assert.IsNotEmpty(databaseId);
            Assert.GreaterOrEqual(
                Volatile.Read(ref kahuna.AdvanceCalls), 4,
                "precondition: the advance was retried past all three blips");
        }
        finally
        {
            await registry.DisposeAsync();
        }
    }

    /// <summary>
    /// A counter that stays unreachable for the whole retry window is reported as transient
    /// unavailability, not as a corrupt system keyspace.
    ///
    /// <para>The distinction is the whole point of the error code: nothing was allocated and nothing was
    /// written, so the caller's correct response is to re-issue the statement. Reporting it as corruption
    /// tells an operator their system space is damaged and gives the client a 500 it will not retry.</para>
    /// </summary>
    [Test]
    public async Task AnUnreachableCounterIsReportedAsTransientNotAsCorruption()
    {
        LeadershipBlipKahuna kahuna = new(TestNode!.Kahuna, TableSequence);

        // The registry captures its options when it is built, so the shortened budget has to be in place
        // here — setting it afterwards would be a no-op and this test would sit out the full default.
        DatabaseRegistry registry = await DatabaseRegistry.OpenForTestingAsync(
            TestNode!, kahuna, Options with { SequenceRetryBudgetMs = 300 }, isClusterMode: true);

        try
        {
            kahuna.Arm(creates: 0, advances: int.MaxValue);

            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
                async () => await registry.AllocateTableIdAsync());

            Assert.AreEqual(CamusDBErrorCodes.SequenceUnavailable, ex!.Code);
            Assert.AreEqual(503, CamusDBErrorCodes.GetHttpStatus(ex.Code), "the caller must be told to retry");
        }
        finally
        {
            await registry.DisposeAsync();
        }
    }

    /// <summary>
    /// Blips the write that publishes the registry's generation stamp, so a test can hold one node's
    /// partition down while another node's cache depends on the stamp moving.
    /// </summary>
    private sealed class GenerationBlipKahuna : DelegatingKahuna
    {
        private readonly string generationKey;
        private int blips;

        public int SetCalls;

        public GenerationBlipKahuna(IKahuna inner, string generationKey) : base(inner)
            => this.generationKey = generationKey;

        /// <summary>Arms the blips and resets the call count, so a test measures only the operation it
        /// arms for rather than the writes its own setup already made.</summary>
        public void Arm(int count)
        {
            Volatile.Write(ref SetCalls, 0);
            Volatile.Write(ref blips, count);
        }

        public override Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
            HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue,
            long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability,
            CancellationToken cancellationToken, long routedGeneration = 0,
            string coordinatorKey = "", TransactionOperationId operationId = default)
        {
            if (!string.Equals(key, generationKey, StringComparison.Ordinal))
                return base.LocateAndTrySetKeyValue(
                    transactionId, key, value, compareValue, compareRevision, flags, expiresMs,
                    durability, cancellationToken, routedGeneration, coordinatorKey, operationId);

            Interlocked.Increment(ref SetCalls);

            if (Interlocked.Decrement(ref blips) >= 0)
                return Task.FromResult((KeyValueResponseType.MustRetry, 0L, HLCTimestamp.Zero));

            return base.LocateAndTrySetKeyValue(
                transactionId, key, value, compareValue, compareRevision, flags, expiresMs,
                durability, cancellationToken, routedGeneration, coordinatorKey, operationId);
        }
    }

    /// <summary>
    /// A blip while publishing the generation stamp must not cost cross-node coherence.
    ///
    /// <para>This is the same defect as the id allocations above with a far quieter failure. The bump is
    /// deliberately best-effort — the mutation it follows has already committed and must not be undone —
    /// so giving up raises nothing, logs nothing, and fails no statement. The cost lands on a different
    /// node, which goes on serving a name that was already dropped, with nothing said anywhere.</para>
    ///
    /// <para>The observer is deliberately brought fully current before the blipped drop. A node that has
    /// not adopted the generation revalidates on every hit anyway, and would resolve the drop correctly
    /// whether or not the stamp ever moved — which is exactly the defect this has to be able to see.</para>
    /// </summary>
    [Test]
    public async Task ABlipWhilePublishingTheGenerationDoesNotStrandOtherNodesOnAStaleCache()
    {
        DatabaseRegistry observer = await DatabaseRegistry.OpenAsync(TestNode!, Options, isClusterMode: true);

        GenerationBlipKahuna kahuna = new(TestNode!.Kahuna, GenerationKey);

        DatabaseRegistry mutator = await DatabaseRegistry.OpenForTestingAsync(
            TestNode!, kahuna, Options, isClusterMode: true);

        try
        {
            string name = "blip_" + Guid.NewGuid().ToString("n");
            string id = await mutator.AllocateIdAsync();
            await mutator.RegisterAsync(name, id);

            Assert.AreEqual(
                id, (await observer.TryResolveEntryAsync(name))!.Id,
                "precondition: the observer has the name cached");

            // Brings the observer's cache to the current generation, so from here its hit is trusted
            // outright and only a moved stamp can dislodge it.
            await observer.GetBackgroundSnapshotAsync();

            kahuna.Arm(3);

            await mutator.UnregisterAsync(name);

            Assert.AreEqual(
                4, Volatile.Read(ref kahuna.SetCalls),
                "precondition: the drop's stamp write hit the blips and retried past them");

            Assert.IsNull(
                await observer.TryResolveEntryAsync(name),
                "a name dropped on another node must stop resolving even when the stamp write had to " +
                "retry its way past a leadership blip");
        }
        finally
        {
            await mutator.DisposeAsync();
            await observer.DisposeAsync();
        }
    }
}
