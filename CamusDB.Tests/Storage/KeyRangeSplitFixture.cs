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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Shared ground for standalone (non-cluster) key-range split fixtures: an engine with key-range
/// routing on and enough partitions for a range to move to, a filled table whose row space and
/// Integer64 index space are both range-routed, and the split/read-back helpers.
///
/// <para>Standalone is a genuinely different path, not a smaller cluster. It runs the
/// <c>isClusterMode == false</c> branch throughout: DDL applies directly instead of being proposed
/// through the schema log, and the single node leads every partition rather than winning an election
/// for it. A split proven on a three-node cluster therefore says nothing about this path.</para>
///
/// <para>Derived fixtures assert on the range descriptors Kahuna reports, not only on queries
/// returning the expected rows. A silently refused split leaves the space as one range, which every
/// query still reads correctly — so row assertions alone cannot tell "the split worked" from "the
/// split never happened".</para>
/// </summary>
public abstract class KeyRangeSplitFixture : BaseTest
{
    protected const int RowCount = 40;

    /// <summary>
    /// Key-range routing needs somewhere for a child range to live. A split allocates a fresh
    /// partition as it goes, so this is the starting pool rather than a ceiling.
    /// </summary>
    protected override int NodeInitialPartitions => 2;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { KeyRangeShardingEnabled = true };

    // -----------------------------------------------------------------------
    // Setup
    // -----------------------------------------------------------------------

    protected async Task<(string db, CommandExecutor executor, TableDescriptor table, List<string> ids)>
        SetupTableAsync()
    {
        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "readings",
            columns:
            [
                new ColumnInfo("id",     ColumnType.Id),
                new ColumnInfo("label",  ColumnType.String, notNull: true),
                new ColumnInfo("amount", ColumnType.Integer64),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)]),
                // Integer64 rather than String on purpose: only indexes whose key columns all use the
                // non-String ordered encoding are registered for key-range routing, so a String-keyed
                // index would stay hash-routed and have no range to split.
                new ConstraintInfo(ConstraintType.IndexMulti, "amount_idx",
                    [new ColumnIndexInfo("amount", OrderType.Ascending)]),
            ],
            ifNotExists: false
        ));

        List<string> ids = await InsertRowsAsync(db, executor, RowCount);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(db, "readings"));

        return (db, executor, table, ids);
    }

    /// <summary>
    /// Inserts <paramref name="count"/> rows and returns their ids.
    ///
    /// <para><paramref name="startingAt"/> offsets the generated <c>label</c> and <c>amount</c>
    /// values so a second call adds rows that extend the first batch instead of colliding with it —
    /// which matters because <c>label</c> carries a unique index in some fixtures, and because a
    /// repeated <c>amount</c> would make an index range scan's expected row count ambiguous.</para>
    /// </summary>
    protected static async Task<List<string>> InsertRowsAsync(
        string db, CommandExecutor executor, int count, int startingAt = 0)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        List<string> ids = [];

        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            for (int i = 0; i < count; i++)
            {
                string id = ObjectIdGenerator.Generate().ToString();
                ids.Add(id);

                await executor.Insert(new InsertTicket(
                    txnState: tx, databaseName: db, tableName: "readings",
                    values: new() { new() {
                        { "id",     new(ColumnType.Id,        id) },
                        { "label",  new(ColumnType.String,    $"reading-{startingAt + i}") },
                        { "amount", new(ColumnType.Integer64, (long)(startingAt + i)) },
                    }}));
            }

            await database.Transactions.CommitAsync(tx);
            return ids;
        }
        catch
        {
            // Roll the failed batch back before surfacing the error. A batch refused mid-flight —
            // by a split's quiesce fence, for example — still holds staged intents and key locks;
            // abandoning it leaves those planted for the reaper to find much later, and every
            // snapshot read of the range (including the next split's own copy) waits on them
            // until then. The caller decides whether the failure ends the test.
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    // -----------------------------------------------------------------------
    // Split helpers — the standalone analogue of the cluster harness. One node
    // leads every partition, so there is no leader to search for.
    // -----------------------------------------------------------------------

    protected List<KahunaRangeDescriptorResponse> Descriptors(string keySpace)
        => TestNode!.Kahuna.GetRangeMap(keySpace)
                    .KeySpaces.Where(space => space.KeySpace == keySpace)
                    .SelectMany(space => space.Descriptors)
                    .ToList();

    protected string RoutingMode(string keySpace)
        => TestNode!.Kahuna.GetRangeMap(keySpace)
                    .KeySpaces.FirstOrDefault(space => space.KeySpace == keySpace)?.RoutingMode ?? "(absent)";

    /// <summary>
    /// Splits a space and fails the test unless a new range actually appeared.
    ///
    /// <para>Outcomes Kahuna marks indeterminate (an interrupted cutover, a concurrent split) are
    /// retried rather than treated as failure: after one of those the map may still change, so
    /// failing immediately would report a flake as a defect.</para>
    /// </summary>
    protected async Task<int> SplitAtAsync(string keySpace, string splitKey)
    {
        int before = Descriptors(keySpace).Count;

        Assert.That(before, Is.GreaterThan(0),
            $"'{keySpace}' has no descriptor to split — the space was never registered for key-range " +
            "routing, so this test would otherwise pass without exercising a split at all");

        KahunaSplitRangeResponse response = new();

        for (int attempt = 0; attempt < 3; attempt++)
        {
            response = await TestNode!.Kahuna
                .SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);

            if (response.Success || Descriptors(keySpace).Count > before)
                break;

            if (response.Determinate)
                break;

            await Task.Delay(200);
        }

        List<KahunaRangeDescriptorResponse> after = Descriptors(keySpace);

        Assert.That(after.Count, Is.GreaterThan(before),
            $"Splitting '{keySpace}' at '{splitKey}' left {after.Count} range(s): " +
            $"{response.Status} ({response.Reason}). 'BelowMinRangeSize' means no key sorts on one " +
            "side of the split key; 'NoRange' means the space is unregistered.");

        AssertCoversSpaceContiguously(after, keySpace);

        return after.Count;
    }

    /// <summary>
    /// Splits a space, retrying for up to <paramref name="budget"/> instead of the handful of attempts
    /// <see cref="SplitAtAsync"/> allows.
    ///
    /// <para>Needed whenever writes are in flight against the space being split. Kahuna checks both
    /// halves are non-empty before dividing a range, and a scan whose window contains an uncommitted
    /// write cannot be served — so the check answers "indeterminate" and the split declines for as long
    /// as a transaction happens to be staging a write there. That is transient and retrying is the
    /// documented response, but a three-attempt budget is not enough to ride it out under continuous
    /// traffic.</para>
    /// </summary>
    protected async Task<int> SplitAtWithinAsync(string keySpace, string splitKey, TimeSpan budget)
    {
        int before = Descriptors(keySpace).Count;

        Assert.That(before, Is.GreaterThan(0),
            $"'{keySpace}' has no descriptor to split — the space was never registered for key-range routing");

        KahunaSplitRangeResponse response = new();
        DateTime deadline = DateTime.UtcNow + budget;

        while (DateTime.UtcNow < deadline)
        {
            response = await TestNode!.Kahuna
                .SplitRangeAtKeyWithOutcomeAsync(keySpace, splitKey, CancellationToken.None);

            if (response.Success || Descriptors(keySpace).Count > before)
                break;

            await Task.Delay(100);
        }

        List<KahunaRangeDescriptorResponse> after = Descriptors(keySpace);

        Assert.That(after.Count, Is.GreaterThan(before),
            $"No split of '{keySpace}' at '{splitKey}' landed within {budget.TotalSeconds}s; " +
            $"last outcome {response.Status} ({response.Reason})");

        AssertCoversSpaceContiguously(after, keySpace);

        return after.Count;
    }

    /// <summary>
    /// Asserts the descriptors tile the space end to end. A gap makes the keys inside it unroutable
    /// and an overlap gives two partitions a claim on the same key; both are invisible at the SQL
    /// layer until a read silently returns fewer rows, so the shape is checked directly.
    ///
    /// <para>Bounds compare with <see cref="StringComparer.Ordinal"/> because that is the order Kahuna
    /// maintains them in.</para>
    /// </summary>
    protected static void AssertCoversSpaceContiguously(
        IReadOnlyList<KahunaRangeDescriptorResponse> descriptors, string keySpace)
    {
        Assert.That(descriptors, Is.Not.Empty, $"'{keySpace}' has no descriptors");

        Assert.That(descriptors[0].StartKey, Is.Null,
            $"The first descriptor of '{keySpace}' must start at -infinity");
        Assert.That(descriptors[^1].EndKey, Is.Null,
            $"The last descriptor of '{keySpace}' must end at +infinity");

        for (int i = 1; i < descriptors.Count; i++)
            Assert.That(descriptors[i].StartKey, Is.EqualTo(descriptors[i - 1].EndKey),
                $"Descriptors {i - 1} and {i} of '{keySpace}' do not meet — " +
                $"'{descriptors[i - 1].EndKey}' then '{descriptors[i].StartKey}'");
    }

    /// <summary>
    /// The actual KV row ids of every row in the table, in ascending order.
    ///
    /// <para>These are <b>not</b> the values of the <c>id</c> column. A row's KV key is built from an
    /// id the inserter mints for it, independently of any column the caller supplied — so a split key
    /// derived from column values would be a key that does not name a real row, and a comparison
    /// against column values would compare two unrelated id sequences.</para>
    /// </summary>
    protected static async Task<List<ObjectIdValue>> ScanRowIdsAsync(
        TableDescriptor table, CommandExecutor executor, string db)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        List<ObjectIdValue> rowIds = [];

        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanRows(tx))
            rowIds.Add(rowId);

        await database.Transactions.CommitAsync(tx);

        return rowIds;
    }

    /// <summary>
    /// The row key of the median row, which is a split key guaranteed to have real rows on both
    /// sides — the condition Kahuna requires before it will divide a range.
    /// </summary>
    protected static string MedianRowKey(TableDescriptor table, IReadOnlyList<ObjectIdValue> rowIds)
    {
        Assert.That(rowIds.Count, Is.GreaterThanOrEqualTo(2),
            "A split needs a row on each side of the key; fewer than two rows cannot produce one");

        List<ObjectIdValue> sorted = rowIds
            .OrderBy(id => id.ToString(), StringComparer.Ordinal)
            .ToList();

        return table.Store.RowPointKey(sorted[sorted.Count / 2]);
    }

    /// <summary>
    /// Splits this table's row space at its median row and returns the split key.
    /// </summary>
    protected async Task<string> SplitRowSpaceAtMedianAsync(
        TableDescriptor table, CommandExecutor executor, string db)
    {
        string splitKey = MedianRowKey(table, await ScanRowIdsAsync(table, executor, db));
        await SplitAtAsync(table.Store.RowKeySpace, splitKey);
        return splitKey;
    }

    protected static string IndexKvId(TableDescriptor table, string indexName)
    {
        TableIndexSchema index = table.Indexes[indexName];

        Assert.That(index.Id, Is.Not.Null.And.Not.Empty,
            $"Index '{indexName}' has no immutable id; its key space cannot be addressed");

        return index.Id!;
    }
}
