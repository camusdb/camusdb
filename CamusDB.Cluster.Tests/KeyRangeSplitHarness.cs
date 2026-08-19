/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Shared.Communication.Rest;

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Drives and verifies Kahuna range splits from CamusDB's own test fixtures, so the data path can be
/// exercised across a range boundary instead of only within one whole-space range.
///
/// <para>Everything here goes through the same public surface a CamusDB deployment would use —
/// <c>IKahuna.SplitRangeAtKeyWithOutcomeAsync</c> to divide a space and <c>IKahuna.GetRangeMap</c> to
/// read the result — rather than any test-only seam. Kahuna's deterministic internal seams
/// (<c>ForceSplitAtKeyAsync</c>, the threshold-parameterized auto-split trigger) are <c>internal</c>
/// to <c>Kahuna.Core</c> and not visible here, and reaching for them would also stop the fixture from
/// proving that the path an operator can actually reach works.</para>
///
/// <para><b>Why every method asserts instead of returning a status.</b> A split has several ways to
/// be refused without throwing: the node asked is not the range-map meta-partition leader, no
/// descriptor covers the key, the key would produce an empty half, or a periodic checker is mid-split.
/// A refusal leaves the space as one range, and a test that then "verifies behavior across a split"
/// silently verifies behavior within a single range and passes for the wrong reason. So each step
/// here reads the descriptor set back and fails loudly if the split did not land.</para>
/// </summary>
internal static class KeyRangeSplitHarness
{
    /// <summary>
    /// How long to wait for a descriptor set to converge on a node. Descriptors are replicated
    /// through the meta partition, so a follower applies them slightly after the committing leader.
    /// </summary>
    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromSeconds(20);

    private const int ConvergencePollMs = 100;

    /// <summary>
    /// Splits <paramref name="keySpace"/> at <paramref name="splitKey"/> and does not return until
    /// every node in the cluster has applied the resulting descriptors.
    ///
    /// <para>Only the meta-partition leader may commit the split, and which node that is changes with
    /// elections, so this offers the split to each node in turn and takes the first acceptance —
    /// mirroring how a client would retry against the leader hint. A node that refuses for lack of
    /// leadership has done nothing, so trying the next one is safe.</para>
    ///
    /// <para>Outcomes that Kahuna reports as <i>indeterminate</i> (cutover or quiesce interrupted,
    /// a concurrent split) are retried rather than failed: the map may still change moments after such
    /// a call returns, so treating one as failure would manufacture a flaky test. The loop stops as
    /// soon as the descriptor set actually shows the split, whichever call caused it.</para>
    /// </summary>
    /// <returns>The number of descriptors covering the space after the split.</returns>
    public static async Task<int> SplitAtAsync(
        InProcessSchemaCluster cluster, string keySpace, string splitKey)
    {
        int before = DescriptorsOn(cluster.Nodes[0], keySpace).Count;

        Assert.That(before, Is.GreaterThan(0),
            $"Key space '{keySpace}' has no descriptor to split — it was never registered or never seeded. " +
            "Check that the cluster was started with KeyRangeShardingEnabled and that the table has been opened.");

        List<string> refusals = [];

        // Three passes over the cluster: enough to ride out one leadership change plus an
        // indeterminate cutover, without hanging a fixture on a genuinely impossible split.
        for (int attempt = 0; attempt < 3; attempt++)
        {
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                KahunaSplitRangeResponse response =
                    await node.Kahuna.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);

                if (response.Success)
                    return await WaitForDescriptorsAsync(cluster, keySpace, atLeast: before + 1);

                refusals.Add($"node {node.Index}: {response.Status} ({response.Reason})");

                // An indeterminate outcome may still have moved the map. Re-read before concluding
                // anything — this is exactly the case the response's Determinate flag exists to mark.
                if (!response.Determinate && DescriptorsOn(node, keySpace).Count > before)
                    return await WaitForDescriptorsAsync(cluster, keySpace, atLeast: before + 1);
            }

            await Task.Delay(200);
        }

        Assert.Fail(
            $"No node committed a split of '{keySpace}' at '{splitKey}'. Outcomes: {string.Join("; ", refusals)}. " +
            "'BelowMinRangeSize' means no key sorts on one side of the split key; 'NoRange' means the space " +
            "is unregistered; 'NotLeader' on every node means the meta partition has no elected leader.");

        return 0;
    }

    /// <summary>
    /// Like <see cref="SplitAtAsync"/>, but keeps offering the split around the cluster for up to
    /// <paramref name="budget"/>.
    ///
    /// <para>Needed whenever writes are in flight against the space. Kahuna checks both halves are
    /// non-empty before dividing a range, and a scan whose window contains an uncommitted write cannot
    /// be served — so the check answers "indeterminate" and the split declines for as long as some
    /// transaction happens to be staging a write there. Retrying is the documented response; a
    /// three-pass budget is simply not enough to ride it out under continuous traffic.</para>
    /// </summary>
    /// <returns>
    /// The number of descriptors covering the space, and the index of the node whose call committed
    /// the split (<c>-1</c> if the map changed without any single call reporting success).
    /// </returns>
    public static async Task<(int Descriptors, int CommittedBy)> SplitAtWithinAsync(
        InProcessSchemaCluster cluster, string keySpace, string splitKey, TimeSpan budget)
    {
        int before = DescriptorsOn(cluster.Nodes[0], keySpace).Count;

        Assert.That(before, Is.GreaterThan(0),
            $"Key space '{keySpace}' has no descriptor to split — it was never registered or never seeded");

        List<string> refusals = [];
        DateTime deadline = DateTime.UtcNow + budget;

        while (DateTime.UtcNow < deadline)
        {
            foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
            {
                KahunaSplitRangeResponse response =
                    await node.Kahuna.Kahuna.SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);

                if (response.Success)
                    return (await WaitForDescriptorsAsync(cluster, keySpace, atLeast: before + 1), node.Index);

                refusals.Add($"node {node.Index}: {response.Status} ({response.Reason})");

                if (DescriptorsOn(node, keySpace).Count > before)
                    return (await WaitForDescriptorsAsync(cluster, keySpace, atLeast: before + 1), -1);
            }

            await Task.Delay(100);
        }

        Assert.Fail(
            $"No node committed a split of '{keySpace}' at '{splitKey}' within {budget.TotalSeconds}s. " +
            $"Last outcomes: {string.Join("; ", refusals.TakeLast(cluster.Nodes.Length))}");

        return (0, -1);
    }

    /// <summary>
    /// Blocks until every node's applied range map reports at least <paramref name="atLeast"/>
    /// descriptors for the space, then asserts the descriptors tile it without gap or overlap.
    ///
    /// <para>Waiting on <b>every</b> node matters: descriptors reach followers by replication, and a
    /// read issued against a node that has not applied the cutover yet would route by the pre-split
    /// map. A test that only checks the committing node would not notice that.</para>
    /// </summary>
    public static async Task<int> WaitForDescriptorsAsync(
        InProcessSchemaCluster cluster, string keySpace, int atLeast)
    {
        foreach (InProcessSchemaCluster.Node node in cluster.Nodes)
        {
            List<KahunaRangeDescriptorResponse> descriptors = [];
            DateTime deadline = DateTime.UtcNow + ConvergenceTimeout;

            while (DateTime.UtcNow < deadline)
            {
                descriptors = DescriptorsOn(node, keySpace);
                if (descriptors.Count >= atLeast)
                    break;

                await Task.Delay(ConvergencePollMs);
            }

            Assert.That(descriptors.Count, Is.GreaterThanOrEqualTo(atLeast),
                $"Node {node.Index} still reports {descriptors.Count} descriptor(s) for '{keySpace}' " +
                $"after {ConvergenceTimeout.TotalSeconds}s; expected at least {atLeast}. " +
                "The split committed but did not replicate to this node.");

            AssertCoversSpaceContiguously(descriptors, keySpace, node.Index);

            // Routing mode is node-local and never replicated, so a node that applied the descriptors
            // but never registered the space would still hash-route it — and would read the wrong
            // partition. Registration happens when the node opens the table.
            string routingMode = RoutingModeOn(node, keySpace);
            Assert.That(routingMode, Is.EqualTo("KeyRange"),
                $"Node {node.Index} holds the descriptors for '{keySpace}' but routes it as " +
                $"'{routingMode}'; the space was never registered on this node.");
        }

        return DescriptorsOn(cluster.Nodes[0], keySpace).Count;
    }

    /// <summary>
    /// The descriptors one node has applied for a key space, ordered by start key. This is node-local
    /// state, which is the point: a caller comparing two nodes is asking whether replication landed.
    /// </summary>
    public static List<KahunaRangeDescriptorResponse> DescriptorsOn(
        InProcessSchemaCluster.Node node, string keySpace)
    {
        KahunaRangeMapResponse map = node.Kahuna.Kahuna.GetRangeMap(keySpace);

        return map.KeySpaces
            .Where(space => space.KeySpace == keySpace)
            .SelectMany(space => space.Descriptors)
            .ToList();
    }

    /// <summary>How this node routes the space: <c>"KeyRange"</c> once registered, else <c>"Hash"</c>.</summary>
    public static string RoutingModeOn(InProcessSchemaCluster.Node node, string keySpace)
        => node.Kahuna.Kahuna.GetRangeMap(keySpace)
               .KeySpaces.FirstOrDefault(space => space.KeySpace == keySpace)?.RoutingMode ?? "(absent)";

    /// <summary>
    /// Asserts the descriptors form one unbroken cover of the space: the first starts at −infinity,
    /// the last ends at +infinity, and each one's end key is the next one's start key.
    ///
    /// <para>A gap loses every key inside it — reads return nothing and writes route nowhere — and an
    /// overlap gives two partitions a claim on the same key. Both are silent at the SQL layer, which
    /// is why the shape is asserted directly rather than inferred from a query returning the right
    /// number of rows.</para>
    ///
    /// <para>Bounds are compared with <see cref="StringComparer.Ordinal"/> because that is how Kahuna
    /// orders them; a culture-aware comparison reports gaps that do not exist.</para>
    /// </summary>
    public static void AssertCoversSpaceContiguously(
        IReadOnlyList<KahunaRangeDescriptorResponse> descriptors, string keySpace, int nodeIndex)
    {
        Assert.That(descriptors, Is.Not.Empty, $"Node {nodeIndex}: '{keySpace}' has no descriptors");

        Assert.That(descriptors[0].StartKey, Is.Null,
            $"Node {nodeIndex}: the first descriptor of '{keySpace}' must start at -infinity, " +
            $"otherwise keys below '{descriptors[0].StartKey}' are unroutable");

        Assert.That(descriptors[^1].EndKey, Is.Null,
            $"Node {nodeIndex}: the last descriptor of '{keySpace}' must end at +infinity, " +
            $"otherwise keys above '{descriptors[^1].EndKey}' are unroutable");

        for (int i = 1; i < descriptors.Count; i++)
        {
            Assert.That(descriptors[i].StartKey, Is.EqualTo(descriptors[i - 1].EndKey),
                $"Node {nodeIndex}: descriptors {i - 1} and {i} of '{keySpace}' do not meet — " +
                $"'{descriptors[i - 1].EndKey}' then '{descriptors[i].StartKey}'. " +
                "A gap drops keys; an overlap gives two partitions the same key.");
        }
    }

    /// <summary>
    /// Asserts the space is served by more than one Raft partition. Two descriptors owned by the same
    /// partition would still exercise the merge path in a scan, but not the cross-partition routing
    /// this feature exists to establish, so a fixture claiming to test the latter should check this.
    /// </summary>
    public static void AssertSpansMultiplePartitions(
        IReadOnlyList<KahunaRangeDescriptorResponse> descriptors, string keySpace)
    {
        // Built before the assert rather than inside its message, because NUnit evaluates the message
        // argument eagerly — indexing descriptors[0] in it would throw on an empty set instead of
        // reporting the failure.
        string owners = string.Join(", ", descriptors.Select(d => d.PartitionId).Distinct());
        int distinct = descriptors.Select(d => d.PartitionId).Distinct().Count();

        Assert.That(distinct, Is.GreaterThan(1),
            $"'{keySpace}' has {descriptors.Count} descriptor(s) served by partition(s) {owners}; " +
            "the split did not move a range onto another partition.");
    }

    // -----------------------------------------------------------------------
    // Split-key construction
    // -----------------------------------------------------------------------

    /// <summary>
    /// The actual KV row ids of every row a table holds, read through the store.
    ///
    /// <para>These are <b>not</b> the values of the <c>id</c> column. A row's KV key is built from an
    /// id the inserter mints for it, independently of any column the caller supplied, so a split key
    /// derived from column values would name a key that belongs to no row.</para>
    /// </summary>
    public static async Task<List<ObjectIdValue>> ScanRowIdsAsync(
        InProcessSchemaCluster.Node node, TableDescriptor table)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();

        List<ObjectIdValue> rowIds = [];

        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanRows(tx))
            rowIds.Add(rowId);

        await node.Database.Transactions.CommitAsync(tx);

        return rowIds;
    }

    /// <summary>
    /// The row key of the median row in ordinal order — a split key with real rows guaranteed on both
    /// sides, which is what Kahuna requires before it will divide a range.
    ///
    /// <para>Ordinal ordering is the right comparison because a row key ends in fixed-width big-endian
    /// hex, so ordering the key strings ordinally is the same as ordering the ids.</para>
    /// </summary>
    public static string MedianRowKey(TableDescriptor table, IReadOnlyList<ObjectIdValue> rowIds)
    {
        Assert.That(rowIds.Count, Is.GreaterThanOrEqualTo(2),
            "A split needs at least one row on each side; fewer than two rows cannot produce a valid split key");

        List<ObjectIdValue> sorted = rowIds
            .OrderBy(id => id.ToString(), StringComparer.Ordinal)
            .ToList();

        return table.Store.RowPointKey(sorted[sorted.Count / 2]);
    }

    /// <summary>
    /// The full index key at which to split a secondary index's space, built the same way the store
    /// builds one: the index bucket prefix, a separator, and the ordered encoding of the key value.
    ///
    /// <para><paramref name="directions"/> must match the index's declared column directions — a
    /// descending column inverts its encoding, so encoding with the wrong directions produces a key
    /// that sorts outside the live range and the split is refused as out of bounds.</para>
    /// </summary>
    public static string IndexKeyAt(
        TableDescriptor table, string indexId, CompositeColumnValue key, OrderType[]? directions = null)
        => table.Store.IndexKeySpace(indexId) + "/" + KeyEncoder.Encode(key, directions);
}
