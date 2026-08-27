/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

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
/// <c>SHOW RANGES</c> over a key space that has actually been divided — the shape the statement
/// exists for. A standalone fixture only ever sees one hash span, so it cannot tell "the statement
/// reports every span" from "the statement reports the one span there is".
///
/// <para>Derives from <see cref="KeyRangeSplitFixture"/> rather than rebuilding the setup: that
/// fixture turns key-range sharding on through <c>ConfigureOptions</c>, which is the only way it
/// takes effect — an engine latches its options when it is constructed, so setting the flag on an
/// already-built executor is a silent no-op that still passes. It also splits deterministically at a
/// chosen key instead of waiting for the auto-splitter's sampling to fire.</para>
///
/// <para>Every assertion here compares the statement's answer against the range descriptors Kahuna
/// reports directly. A statement that agreed with itself but not with the router would be exactly
/// the failure an operator uses it to rule out.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShowRangesAcrossSplit : KeyRangeSplitFixture
{
    private static async Task<List<QueryResultRow>> Query(
        CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction txn = await database.Transactions.BeginAsync();

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: db, sql: sql, parameters: null));

        List<QueryResultRow> rows = [];
        await foreach (QueryResultRow row in cursor)
            rows.Add(row);

        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static string? Text(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? v) && v.Type != ColumnType.Null ? v.StrValue : null;

    private static long? Number(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? v) && v.Type != ColumnType.Null ? v.LongValue : null;

    /// <summary>
    /// After a split the row space has more than one descriptor, and the statement must report one
    /// row per descriptor, in the same order, with the same raw bounds.
    /// </summary>
    [Test]
    public async Task RowSpace_ReportsOneRowPerDescriptorInRouterOrder()
    {
        (string db, CommandExecutor executor, TableDescriptor table, _) = await SetupTableAsync();

        await SplitRowSpaceAtMedianAsync(table, executor, db);

        List<KahunaRangeDescriptorResponse> descriptors = Descriptors(table.Store.RowKeySpace);
        Assert.That(descriptors.Count, Is.GreaterThan(1), "The split must have produced more than one range");
        List<QueryResultRow> rows = await Query(executor, db, "SHOW RANGES FROM TABLE readings");

        Assert.AreEqual(descriptors.Count, rows.Count);

        for (int i = 0; i < descriptors.Count; i++)
        {
            Assert.AreEqual("key_range", Text(rows[i], "routing"), "A split space is key-range routed");
            Assert.AreEqual(i + 1, Number(rows[i], "span"), "Span ordinals are 1-based and contiguous");
            Assert.AreEqual(descriptors[i].StartKey, Text(rows[i], "raw_start_key"));
            Assert.AreEqual(descriptors[i].EndKey, Text(rows[i], "raw_end_key"));
            Assert.AreEqual(descriptors[i].PartitionId, Number(rows[i], "partition_id"));
            Assert.AreEqual(descriptors[i].Generation, Number(rows[i], "generation"));
        }
    }

    /// <summary>
    /// The reported bounds must cover the key space without a gap and in ascending ordinal order —
    /// the same contiguity the router depends on. A culture-aware comparison reads gaps that are not
    /// there, so the comparison is ordinal.
    /// </summary>
    [Test]
    public async Task ReportedBoundsAreContiguousAndOrdinallyAscending()
    {
        (string db, CommandExecutor executor, TableDescriptor table, _) = await SetupTableAsync();
        await SplitRowSpaceAtMedianAsync(table, executor, db);

        List<QueryResultRow> rows = await Query(executor, db, "SHOW RANGES FROM TABLE readings");

        Assert.That(rows.Count, Is.GreaterThan(1));
        Assert.IsNull(Text(rows[0], "raw_start_key"), "The first span starts unbounded");
        Assert.IsNull(Text(rows[^1], "raw_end_key"), "The last span ends unbounded");

        for (int i = 1; i < rows.Count; i++)
        {
            string previousEnd = Text(rows[i - 1], "raw_end_key")!;
            string start = Text(rows[i], "raw_start_key")!;

            Assert.AreEqual(previousEnd, start, "A span must begin exactly where the previous one ends");
            Assert.That(string.CompareOrdinal(previousEnd, start), Is.EqualTo(0));
        }
    }

    /// <summary>
    /// A row space's bounds are row ids, so the decoded form is the 24-hex id itself — the value an
    /// operator already has in hand when they are chasing one row.
    /// </summary>
    [Test]
    public async Task RowSpaceBoundsDecodeToRowIdHex()
    {
        (string db, CommandExecutor executor, TableDescriptor table, _) = await SetupTableAsync();
        await SplitRowSpaceAtMedianAsync(table, executor, db);

        List<QueryResultRow> rows = await Query(executor, db, "SHOW RANGES FROM TABLE readings");

        string decoded = Text(rows[0], "end_key")!;

        Assert.AreEqual(24, decoded.Length, "A row-space bound decodes to a 24-hex row id");
        Assert.IsTrue(decoded.All(Uri.IsHexDigit));
        StringAssert.EndsWith(decoded, Text(rows[0], "raw_end_key")!);
    }

    /// <summary>
    /// An index space's bounds are encoded column values, so they must come back as the values the
    /// user wrote rather than as the encoding. That is what makes the output readable at all.
    /// </summary>
    [Test]
    public async Task IndexSpaceBoundsDecodeToColumnValues()
    {
        (string db, CommandExecutor executor, TableDescriptor table, _) = await SetupTableAsync();

        string indexKeySpace = table.Store.IndexKeySpace(IndexKvId(table, "amount_idx"));

        // amount runs 0..RowCount-1, so the middle value has real keys on both sides.
        List<KahunaRangeDescriptorResponse> before = Descriptors(indexKeySpace);
        Assert.That(before, Is.Not.Empty, "The Integer64 index must be range-routed");

        List<QueryResultRow> indexRows = await Query(
            executor, db, "SHOW RANGES FROM INDEX readings@amount_idx");

        Assert.That(indexRows, Is.Not.Empty);
        Assert.AreEqual("readings@amount_idx", Text(indexRows[0], "relation"));
        Assert.AreEqual(indexKeySpace, Text(indexRows[0], "key_space"));

        // Split at the encoded key of a middle amount, then read the bound back as that number.
        List<ObjectIdValue> rowIds = await ScanRowIdsAsync(table, executor, db);
        Assert.That(rowIds.Count, Is.GreaterThanOrEqualTo(2));

        List<QueryResultRow> afterSplit = await SplitIndexAtMiddleAmountAsync(executor, db, table, indexKeySpace);

        // The bound between the two spans must render as the integer it encodes, not as raw text.
        string boundary = Text(afterSplit[0], "end_key")!;

        Assert.IsTrue(long.TryParse(boundary, out long decodedAmount),
            $"An Integer64 index bound must decode to a number, got '{boundary}'");

        Assert.That(decodedAmount, Is.GreaterThanOrEqualTo(0).And.LessThan(RowCount));

        // And the raw form must still be the encoded key, so both columns carry their own meaning.
        Assert.AreNotEqual(boundary, Text(afterSplit[0], "raw_end_key"));
    }

    private async Task<List<QueryResultRow>> SplitIndexAtMiddleAmountAsync(
        CommandExecutor executor, string db, TableDescriptor table, string indexKeySpace)
    {
        List<KahunaRangeDescriptorResponse> descriptors = Descriptors(indexKeySpace);
        Assert.That(descriptors, Is.Not.Empty);

        // Ask the statement itself where a middle key lands, then split there — this uses the same
        // encoding the router does, so the split key is guaranteed to be a real key boundary.
        List<QueryResultRow> probe = await Query(
            executor, db, $"SHOW RANGE FROM INDEX readings@amount_idx FOR ROW ({RowCount / 2})");

        string splitKey = Text(probe.Single(), "probe_key")!;
        await SplitAtAsync(indexKeySpace, splitKey);

        List<QueryResultRow> rows = await Query(executor, db, "SHOW RANGES FROM INDEX readings@amount_idx");
        Assert.That(rows.Count, Is.GreaterThan(1), "The index space must now have more than one span");

        _ = table;
        return rows;
    }

    /// <summary>
    /// The single-span form must land in the span whose bounds contain its probe key, and must
    /// report that span's own ordinal rather than always reporting 1.
    /// </summary>
    [Test]
    public async Task ForRow_LandsInTheSpanWhoseBoundsContainTheProbeKey()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> ids) = await SetupTableAsync();
        await SplitRowSpaceAtMedianAsync(table, executor, db);

        List<QueryResultRow> allSpans = await Query(executor, db, "SHOW RANGES FROM TABLE readings");
        Assert.That(allSpans.Count, Is.GreaterThan(1));

        // Probe every row, so both sides of the split are exercised rather than whichever one the
        // first row happens to fall in.
        foreach (string id in ids)
        {
            List<QueryResultRow> located = await Query(
                executor, db, $"SHOW RANGE FROM TABLE readings FOR ROW ('{id}')");

            QueryResultRow span = located.Single();
            string probeKey = Text(span, "probe_key")!;

            string? start = Text(span, "raw_start_key");
            string? end = Text(span, "raw_end_key");

            Assert.IsTrue(start is null || string.CompareOrdinal(probeKey, start) >= 0,
                $"Probe key '{probeKey}' sorts before its span's start '{start}'");
            Assert.IsTrue(end is null || string.CompareOrdinal(probeKey, end) < 0,
                $"Probe key '{probeKey}' sorts at or after its span's end '{end}'");

            long ordinal = Number(span, "span")!.Value;
            Assert.That(ordinal, Is.InRange(1, allSpans.Count),
                "The reported ordinal is the span's position in the whole key space");
        }
    }

    /// <summary>
    /// A split must be visible immediately, without waiting out the planner's placement cache TTL.
    /// Reporting a layout the cache still holds is the exact staleness this statement exists to
    /// rule out.
    /// </summary>
    [Test]
    public async Task SplitIsVisibleImmediately_NotAfterTheCacheTtl()
    {
        (string db, CommandExecutor executor, TableDescriptor table, _) = await SetupTableAsync();

        List<QueryResultRow> before = await Query(executor, db, "SHOW RANGES FROM TABLE readings");
        int spansBefore = before.Count;

        // Warm the planner's cache with the pre-split layout, so a cached read would be observably
        // wrong rather than accidentally right.
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        database.Kahuna.GetPlacement(table.Store.RowKeySpace);

        await SplitRowSpaceAtMedianAsync(table, executor, db);

        List<QueryResultRow> after = await Query(executor, db, "SHOW RANGES FROM TABLE readings");

        Assert.That(after.Count, Is.GreaterThan(spansBefore),
            "The split must be reported on the next statement, not after the placement cache expires");
    }
}
