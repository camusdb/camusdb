
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Shared.Communication.Rest;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Shared ground for the fixtures that let a range divide itself: an engine whose Kahuna node has the
/// load branch of range auto-split switched on, and the helpers for driving load and reading the
/// outcome counters back.
///
/// <para>The knobs are set on the node rather than on <see cref="CamusDBOptions"/>, because that is
/// where they live. A node fixes its configuration when it is constructed, so
/// <see cref="ConfigureNodeOptions"/> is the only point at which they take effect.</para>
/// </summary>
public abstract class KeyRangeLoadSplitFixture : KeyRangeSplitFixture
{
    protected const string SplitsMetric = "kahuna.range.splits";

    protected const string SettleSkipsMetric = "kahuna.range.split.settle_skips";

    protected const string IndivisibleRefusalsMetric = "kahuna.range.split.indivisible_refusals";

    /// <summary>How long to keep writing before checking what the splitter did.</summary>
    protected static readonly TimeSpan LoadDuration = TimeSpan.FromSeconds(6);

    /// <summary>
    /// Test timings, all far below their production defaults. The load predicate must hold for a whole
    /// window before a split is considered, so the default 15 s window and 5 s poll would turn every
    /// test here into a half-minute wait.
    /// </summary>
    protected override void ConfigureNodeOptions(EmbeddedKahunaOptions options)
    {
        // Any write rate at all counts as hot. These fixtures are about the decision path, not about
        // calibrating a realistic threshold.
        options.RangeSplitLoadThreshold = 0.001;

        // A memory WAL drains as fast as it fills, so a queue-depth gate above zero would never pass
        // and the predicate could never hold — a test would time out instead of failing honestly.
        options.RangeSplitLoadMinQueueDepth = 0;

        options.RangeSplitLoadWindow = TimeSpan.FromMilliseconds(300);
        options.RangeSplitLoadPollInterval = TimeSpan.FromMilliseconds(100);

        // Kahuna requires the settle window to cover the leader-stability window, so the two move down
        // together. The settle window still has to be long enough to observe, because one test here
        // asserts that a fresh child really is left alone for it.
        options.RangeSplitSettleWindow = TimeSpan.FromSeconds(1);
        options.MinLeaderStability = TimeSpan.FromSeconds(1);
    }

    /// <summary>
    /// Inserts in small committed batches for the whole budget and returns the amounts that were
    /// actually acknowledged. Each commit replays its intents as ordinary KV writes, which is what
    /// raises the partition's log rate and feeds the write-frequency histogram the split key comes
    /// from.
    ///
    /// <para>An aborted batch is counted and skipped, not failed, and its amounts are not returned.
    /// The split checker samples the same key range these writes stage into, so a batch can lose a
    /// conflict or miss its deadline while a boundary moves — the retry contract a real client works
    /// under. The counts are returned so the caller can prove the load was real rather than entirely
    /// refused.</para>
    /// </summary>
    protected static async Task<(int Committed, int Aborted, List<long> Amounts)> DriveWriteLoadAsync(
        string db, CommandExecutor executor, TimeSpan budget)
    {
        const int BatchSize = 10;

        Stopwatch elapsed = Stopwatch.StartNew();
        List<long> amounts = [];
        int committed = 0;
        int aborted = 0;

        for (int batch = 0; elapsed.Elapsed < budget; batch++)
        {
            int startingAt = RowCount + (batch * BatchSize);

            try
            {
                await InsertRowsAsync(db, executor, count: BatchSize, startingAt: startingAt);
                committed++;

                // InsertRowsAsync writes amount = startingAt + i, so a batch's amounts are known once
                // it commits. They identify the rows by a value the caller supplied, which the minted
                // KV row id does not.
                for (int i = 0; i < BatchSize; i++)
                    amounts.Add(startingAt + i);
            }
            catch (CamusDBException)
            {
                aborted++;
            }
        }

        return (committed, aborted, amounts);
    }

    /// <summary>
    /// Polls the descriptor set until it reaches <paramref name="atLeast"/> or the budget runs out. The
    /// split checker runs on its own timer, so the map usually changes after the load stops rather than
    /// during it.
    /// </summary>
    protected async Task<int> WaitForDescriptorsAsync(string keySpace, int atLeast, TimeSpan budget)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        int count = Descriptors(keySpace).Count;

        while (count < atLeast && elapsed.Elapsed < budget)
        {
            await Task.Delay(250);
            count = Descriptors(keySpace).Count;
        }

        return count;
    }

    /// <summary>Polls a counter until it passes <paramref name="baseline"/>, and returns the last value read.</summary>
    protected static async Task<double> WaitForCounterAboveAsync(
        CommandExecutor executor, string db, string metric, double baseline, TimeSpan budget)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        double total = baseline;

        while (elapsed.Elapsed < budget)
        {
            total = await ReadCounterTotalAsync(executor, db, metric);

            if (total > baseline)
                return total;

            await Task.Delay(250);
        }

        return total;
    }

    /// <summary>
    /// Reads one counter's total out of <c>SHOW ENGINE STATS</c>, summed over every tag-set, and
    /// returns 0 when the counter has never fired.
    ///
    /// <para>Summing matters: Kahuna tags each of these counters with the key space, so one row per
    /// space appears and a test that read the first row would assert against whichever space happened
    /// to sort first. The read goes through the SQL statement rather than the collector, so what is
    /// proven is the path an operator actually uses.</para>
    /// </summary>
    protected static async Task<double> ReadCounterTotalAsync(CommandExecutor executor, string db, string metric)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: db, sql: "SHOW ENGINE STATS", parameters: null));

        double total = 0;

        await foreach (QueryResultRow row in cursor)
        {
            if (!row.Row.TryGetValue("metric", out ColumnValue? name) || name.StrValue != metric)
                continue;

            if (row.Row.TryGetValue("total", out ColumnValue? value) && value.Type != ColumnType.Null)
                total += value.FloatValue;
        }

        return total;
    }
}

/// <summary>
/// Proves a hot range divides itself with no administrative call, driven only by configuration.
///
/// <para>Every other split fixture in this suite calls <c>SplitRangeAtKeyWithOutcomeAsync</c> by hand
/// and therefore proves only that a split <i>can</i> be performed. This one sets the load thresholds,
/// writes, and waits — so a regression that leaves the knobs unreachable, stops the load-check actor
/// from starting, or stops CamusDB's writes from reaching the write-frequency histogram fails here and
/// nowhere else.</para>
///
/// <para>The split is worthless if it loses data, so the fixture also reads every acknowledged row
/// back afterwards. A boundary moving underneath concurrent writes is exactly where a lost row would
/// come from.</para>
/// </summary>
[TestFixture]
// Serial: boots an embedded node, drives sustained writes, and asserts on the process-wide Kahuna
// meter, all of which a fixture running alongside would disturb.
[NonParallelizable]
public sealed class TestKeyRangeLoadSplitStandalone : KeyRangeLoadSplitFixture
{
    [Test]
    public async Task SustainedWriteLoad_SplitsTheRowSpace_WithNoAdminCall()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> initialIds) = await SetupTableAsync();

        string keySpace = table.Store.RowKeySpace;

        Assert.That(RoutingMode(keySpace), Is.EqualTo("KeyRange"),
            "A hash-routed space has no descriptor for the split checker to act on, so this test " +
            "would pass without exercising the load branch at all");

        Assert.That(Descriptors(keySpace), Has.Count.EqualTo(1),
            "A freshly registered space starts as one whole-space range");

        double splitsBefore = await ReadCounterTotalAsync(executor, db, SplitsMetric);

        // Sustained traffic, not one burst: the predicate is sampled every poll interval and must hold
        // across a whole window, so a single batch can finish between two samples.
        (int committed, int aborted, List<long> loadAmounts) = await DriveWriteLoadAsync(db, executor, LoadDuration);

        Assert.That(committed, Is.GreaterThan(0),
            $"Every write batch was aborted ({aborted} of them), so the partition never went hot and " +
            "nothing below would prove anything");

        int descriptors = await WaitForDescriptorsAsync(keySpace, atLeast: 2, budget: TimeSpan.FromSeconds(15));

        Assert.That(descriptors, Is.GreaterThan(1),
            "The row space never divided. No test here calls the split API, so either the load knobs " +
            "did not reach the node, the load-check actor never started, or CamusDB's writes are not " +
            "reaching the write-frequency histogram the split key comes from.");

        List<KahunaRangeDescriptorResponse> after = Descriptors(keySpace);
        AssertCoversSpaceContiguously(after, keySpace);

        Assert.That(after.Select(d => d.PartitionId).Distinct().Count(), Is.GreaterThan(1),
            "A child range that stays on its parent's partition relieves nothing — the whole point of " +
            "the split is a second Raft leader for the hot half");

        Assert.That(await ReadCounterTotalAsync(executor, db, SplitsMetric), Is.GreaterThan(splitsBefore),
            $"The descriptor set grew, so '{SplitsMetric}' must have moved with it");

        // Data must survive the boundary moving underneath concurrent writes. The check is on the
        // amount column rather than on KV row ids, because the inserter mints a row id independently
        // of the id column and the two sequences cannot be compared.
        HashSet<long> expected = [.. Enumerable.Range(0, initialIds.Count).Select(value => (long)value)];
        expected.UnionWith(loadAmounts);

        List<long> readBack = await ScanAmountsAsync(executor, db);

        Assert.That(readBack, Has.Count.EqualTo(expected.Count),
            "Every acknowledged row must still be readable after the splits, and none may be duplicated");

        foreach (long amount in expected)
            Assert.That(readBack, Does.Contain(amount), $"The row with amount={amount} was acknowledged and is now missing");

        Assert.That(await ScanRowIdsAsync(table, executor, db), Has.Count.EqualTo(expected.Count),
            "The row key space must hold exactly one KV row per acknowledged insert");
    }

    [Test]
    public async Task AFreshChildRange_IsLeftAloneForItsSettleWindow()
    {
        // A child inherits a filtered write histogram, so it starts warm and would re-split at once
        // without the settle window. Its skip counter is what tells an operator "it just split, wait"
        // apart from "it is not hot".
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        string keySpace = table.Store.RowKeySpace;
        double skipsBefore = await ReadCounterTotalAsync(executor, db, SettleSkipsMetric);

        (int committed, int aborted, List<long> _) = await DriveWriteLoadAsync(db, executor, LoadDuration);

        Assert.That(committed, Is.GreaterThan(0), $"Every write batch was aborted ({aborted} of them)");

        await WaitForDescriptorsAsync(keySpace, atLeast: 2, budget: TimeSpan.FromSeconds(15));

        double skipsAfter = await WaitForCounterAboveAsync(
            executor, db, SettleSkipsMetric, skipsBefore, TimeSpan.FromSeconds(10));

        Assert.That(skipsAfter, Is.GreaterThan(skipsBefore),
            $"'{SettleSkipsMetric}' never moved, so a fresh child was re-evaluated immediately after " +
            "its split and the settle window is not being honored");
    }

    /// <summary>Reads every row's <c>amount</c> through an ordinary autocommit SELECT.</summary>
    private static async Task<List<long>> ScanAmountsAsync(CommandExecutor executor, string db)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), db, "SELECT amount FROM readings", null));

        List<long> amounts = [];

        await foreach (QueryResultRow row in cursor)
            amounts.Add(row.Row["amount"].LongValue);

        return amounts;
    }
}

/// <summary>
/// Proves that load concentrated on a single key is refused rather than split.
///
/// <para>A split moves a boundary. When one key carries the whole write rate, no boundary puts less
/// than all of it on one child, so splitting adds a Raft group and relieves nothing. Kahuna's
/// imbalance guard refuses that shape, and its counter is what tells an operator the workload — not
/// the configuration — is why nothing is splitting.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestKeyRangeHotKeyRefusalStandalone : KeyRangeLoadSplitFixture
{
    [Test]
    public async Task LoadOnASingleKey_IsRefusedAsIndivisible()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        string keySpace = table.Store.RowKeySpace;

        // The concentrated phase starts at once, with no settling pause. The seeded rows enter the
        // write histogram with a weight of one each, so a pause would let their even distribution
        // drive the first split decisions; starting immediately lets the hot key outweigh them
        // within the first debounce window, which is the shape under test.
        double refusalsBefore = await ReadCounterTotalAsync(executor, db, IndivisibleRefusalsMetric);
        int descriptorsBefore = Descriptors(keySpace).Count;

        await HammerOneRowAsync(db, executor, LoadDuration);

        double refusalsAfter = await WaitForCounterAboveAsync(
            executor, db, IndivisibleRefusalsMetric, refusalsBefore, TimeSpan.FromSeconds(15));

        Assert.That(refusalsAfter, Is.GreaterThan(refusalsBefore),
            $"'{IndivisibleRefusalsMetric}' never moved. A range whose writes all land on one key must " +
            "be refused by the imbalance guard rather than split.");

        Assert.That(Descriptors(keySpace), Has.Count.EqualTo(descriptorsBefore),
            "Concentrated load must not divide the range: a split cannot relieve a single hot key, so " +
            "the boundary must stay where it was");
    }

    /// <summary>
    /// Rewrites the same row for the whole budget, so the write-frequency histogram collapses onto one
    /// key and every candidate boundary becomes maximally lopsided.
    /// </summary>
    private static async Task HammerOneRowAsync(string db, CommandExecutor executor, TimeSpan budget)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        Stopwatch elapsed = Stopwatch.StartNew();

        for (long value = 0; elapsed.Elapsed < budget; value++)
        {
            KvTransaction? tx = null;

            try
            {
                tx = await database.Transactions.BeginAsync();

                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    tx, db, $"UPDATE readings SET amount = {value} WHERE label = 'reading-0'", null));

                await database.Transactions.CommitAsync(tx);
            }
            catch (CamusDBException)
            {
                // The same retry contract as the insert loop: a conflict or a missed deadline is a
                // skipped iteration, not a failure — but the dead transaction must still be rolled
                // back, or its staged intents stay planted and block every later snapshot read of
                // the range (including the split machinery this fixture exists to exercise).
                if (tx is not null)
                    await database.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        }
    }
}
