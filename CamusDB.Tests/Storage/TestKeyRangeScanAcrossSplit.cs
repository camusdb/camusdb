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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Reads must be unaffected by how many ranges a key space is divided into. Once a space splits, the
/// rows a single partition used to serve are spread over several, and answering a scan means
/// resolving every range the scan's bounds touch and merging their output back into one ordered
/// stream. Kahuna does that merging; this fixture establishes that CamusDB's scans actually inherit
/// it, which no test previously showed.
///
/// <para><b>Why the row-count assertions are meaningful.</b> A scan that failed to merge would not
/// error — it would quietly return the rows of one range and stop. That is a smaller number, not a
/// crash, so <see cref="FullScan_WithoutFanOut_WouldReturnFewerRows_EstablishingTheAssertionsBite"/>
/// measures what a single range actually holds and asserts it is strictly less than the whole table.
/// Without that, "the scan returned 40 rows" would not distinguish a working merge from a table that
/// never really split.</para>
///
/// <para>Ordering is asserted alongside the row set because the planner elides sorts it believes the
/// storage layer already provides. A merge that returned every row but interleaved the children
/// wrongly would still satisfy a count assertion while breaking <c>ORDER BY</c>.</para>
/// </summary>
[TestFixture]
// Serial: boots an embedded Kahuna node with several partitions and drives Raft-committed range
// splits, whose timing is disturbed by other node-booting fixtures running alongside.
[NonParallelizable]
public sealed class TestKeyRangeScanAcrossSplit : KeyRangeSplitFixture
{
    // -----------------------------------------------------------------------
    // Query helpers
    // -----------------------------------------------------------------------

    private static async Task<List<QueryResultRow>> RunSqlAsync(
        string db, CommandExecutor executor, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));

            List<QueryResultRow> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(row);

            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    private static List<string> IdsOf(IEnumerable<QueryResultRow> rows)
        => rows.Select(row => row.Row["id"].StrValue!).ToList();

    private static List<long> AmountsOf(IEnumerable<QueryResultRow> rows)
        => rows.Select(row => row.Row["amount"].LongValue).ToList();

    // -----------------------------------------------------------------------
    // 1. Sensitivity control. Establishes that a scan confined to one child
    //    range returns strictly fewer rows than the table holds, so every
    //    "returned all rows" assertion below is actually distinguishing a
    //    working fan-out from a broken one.
    // -----------------------------------------------------------------------

    [Test]
    public async Task FullScan_WithoutFanOut_WouldReturnFewerRows_EstablishingTheAssertionsBite()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        List<ObjectIdValue> sorted = (await ScanRowIdsAsync(table, executor, db))
            .OrderBy(id => id.ToString(), StringComparer.Ordinal)
            .ToList();

        ObjectIdValue boundary = sorted[sorted.Count / 2];

        await SplitAtAsync(table.Store.RowKeySpace, table.Store.RowPointKey(boundary));

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        // Read only the lower child's span by bounding the scan at the split key. This is the number
        // of rows a scan that resolved a single range would return.
        int lowerChildRows = 0;
        await foreach ((ObjectIdValue _, ReadOnlyMemory<byte> _) in
                       table.Store.ScanRows(tx, untilRowId: boundary))
            lowerChildRows++;

        await database.Transactions.CommitAsync(tx);

        Assert.That(lowerChildRows, Is.GreaterThan(0),
            "The lower child range must hold rows, or the split key was not really inside the data");

        Assert.That(lowerChildRows, Is.LessThan(RowCount),
            $"One child range holds {lowerChildRows} of {RowCount} rows. If this were equal to the " +
            "table size, a scan that ignored the other child would still look correct and every " +
            "assertion in this fixture would be vacuous.");
    }

    // -----------------------------------------------------------------------
    // 2. A full table scan over a split space returns every row, once, in order.
    // -----------------------------------------------------------------------

    [Test]
    public async Task FullScan_AfterSplit_ReturnsEveryRowExactlyOnceInRowIdOrder()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> ids) = await SetupTableAsync();

        List<string> before = IdsOf(await RunSqlAsync(db, executor, "SELECT id FROM readings"));

        await SplitRowSpaceAtMedianAsync(table, executor, db);

        List<string> after = IdsOf(await RunSqlAsync(db, executor, "SELECT id FROM readings"));

        Assert.That(after, Has.Count.EqualTo(RowCount),
            "A split must not change how many rows a full scan returns");

        Assert.That(after.Distinct().Count(), Is.EqualTo(RowCount),
            "A row must not be returned twice — overlapping child ranges would duplicate the rows " +
            "in the overlap");

        Assert.That(after.OrderBy(x => x, StringComparer.Ordinal), Is.EqualTo(ids.OrderBy(x => x, StringComparer.Ordinal)),
            "The scan must return exactly the rows that were inserted");

        // The merged stream must stay globally ordered, not merely complete: the planner elides sorts
        // it believes storage already provides, so a correctly-populated but wrongly-interleaved
        // stream would silently produce mis-ordered query results.
        Assert.That(after, Is.EqualTo(after.OrderBy(x => x, StringComparer.Ordinal).ToList()),
            "Rows must arrive in ascending row-id order across the child ranges, not grouped by range");

        Assert.That(after, Is.EqualTo(before),
            "The post-split scan must produce the identical sequence the pre-split scan produced");
    }

    // -----------------------------------------------------------------------
    // 3. The split lands while a scan is midway through, and — critically —
    //    with more pages still to fetch. This is what the fan-out's
    //    deliberately generation-free cursor exists for: every page
    //    re-resolves against the live range map rather than against the map
    //    the scan started under.
    // -----------------------------------------------------------------------

    /// <summary>
    /// Rows this test adds so the scan spans more than one storage page. The store fetches
    /// <c>512</c> keys per page, so a table smaller than that is read in a single round trip and a
    /// split triggered "mid-enumeration" would in fact land after all the data had already been
    /// fetched — proving only that an in-memory list survives, which is not the claim.
    /// </summary>
    private const int PagedRowCount = 700;

    [Test]
    public async Task FullScan_SplitLandingBetweenPages_StillReturnsEveryRowInOrder()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> ids) = await SetupTableAsync();

        ids.AddRange(await InsertRowsAsync(db, executor, PagedRowCount - RowCount, startingAt: RowCount));

        Assert.That(ids, Has.Count.EqualTo(PagedRowCount));

        // The row ids the table actually holds, captured before the scan starts. These are the KV row
        // ids, which the inserter mints independently of the `id` column, so they are what a scan
        // yields and what the split key is built from.
        List<string> expectedRowIds = (await ScanRowIdsAsync(table, executor, db))
            .Select(id => id.ToString())
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToList();

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        List<string> seen = [];
        bool split = false;

        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> _) in table.Store.ScanRows(tx))
        {
            seen.Add(rowId.ToString());

            // Divide the space underneath the open cursor once the first page is consumed but well
            // before the last row, so the pages that follow must resolve against the new range map.
            if (!split && seen.Count == 520)
            {
                await SplitRowSpaceAtMedianAsync(table, executor, db);
                split = true;
            }
        }

        await database.Transactions.CommitAsync(tx);

        Assert.That(split, Is.True, "The split must have been triggered inside the enumeration");

        Assert.That(seen, Has.Count.EqualTo(PagedRowCount),
            "A split that landed between two pages must not cost the scan any rows");

        Assert.That(seen.Distinct().Count(), Is.EqualTo(PagedRowCount),
            "A split that landed between two pages must not make the scan repeat rows");

        Assert.That(seen, Is.EqualTo(seen.OrderBy(x => x, StringComparer.Ordinal).ToList()),
            "The scan must stay ordered across the boundary that appeared underneath it");

        Assert.That(seen, Is.EqualTo(expectedRowIds),
            "The scan must return exactly the row ids the table holds");
    }

    // -----------------------------------------------------------------------
    // 4. A bounded index range scan whose bounds straddle a split of the index
    //    space. Index keys route in their own space, so the row-space results
    //    above say nothing about them.
    // -----------------------------------------------------------------------

    [Test]
    public async Task BoundedIndexRangeScan_StraddlingASplitOfTheIndexSpace_ReturnsTheWholeRange()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        string indexId = IndexKvId(table, "amount_idx");
        string indexSpace = table.Store.IndexKeySpace(indexId);

        const int low = 5;
        const int high = RowCount - 5;

        List<long> before = AmountsOf(await RunSqlAsync(
            db, executor, $"SELECT amount FROM readings WHERE amount >= {low} AND amount <= {high} ORDER BY amount"));

        Assert.That(before, Has.Count.EqualTo(high - low + 1),
            "The bounded range must be fully populated before the split, or the comparison below is empty");

        // Split the index space in the middle of the queried range, so the bounds genuinely straddle
        // the new boundary rather than falling entirely inside one child.
        string splitKey = indexSpace + "/" + KeyEncoder.Encode(
            new CompositeColumnValue(new ColumnValue(ColumnType.Integer64, (long)(RowCount / 2))), null);

        await SplitAtAsync(indexSpace, splitKey);

        List<long> after = AmountsOf(await RunSqlAsync(
            db, executor, $"SELECT amount FROM readings WHERE amount >= {low} AND amount <= {high} ORDER BY amount"));

        Assert.That(after, Is.EqualTo(before),
            "A bounded index scan must return the same rows, in the same order, once its space is split " +
            "in the middle of the queried range");

        Assert.That(after, Is.EqualTo(after.OrderBy(x => x).ToList()),
            "The index scan must stay in ascending key order across the child ranges");
    }

    // -----------------------------------------------------------------------
    // 5. An index built over an already-split row space. The backfill reads
    //    rows through the same scan path, so if fan-out were missing the index
    //    would be built from one range's rows and silently miss the rest.
    // -----------------------------------------------------------------------

    [Test]
    public async Task IndexBackfill_OverASplitRowSpace_IndexesEveryRow()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        await SplitRowSpaceAtMedianAsync(table, executor, db);

        // Build the index after the split, so its backfill scan must cross the boundary.
        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: db,
            tableName: "readings",
            indexName: "label_idx",
            columns: [new ColumnIndexInfo("label", OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex,
            includeColumns: null));

        // Read the index itself rather than issuing a SELECT and hoping the planner chose it. A query
        // that fell back to a table scan plus a sort would return every row whether or not the index
        // was completely built, so it could not detect the failure this test is about.
        TableDescriptor reopened = await executor.OpenTable(new OpenTableTicket(db, "readings"));
        string indexId = IndexKvId(reopened, "label_idx");

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        List<string> indexed = [];
        await foreach ((CompositeColumnValue key, ObjectIdValue _, ReadOnlyMemory<byte> _) in
                       reopened.Store.ScanIndex(tx, indexId, [ColumnType.String], from: null, to: null, unique: false))
            indexed.Add(key.Values[0].StrValue!);

        await database.Transactions.CommitAsync(tx);

        Assert.That(indexed, Has.Count.EqualTo(RowCount),
            "Every row must appear in an index backfilled over a split row space. A backfill that saw " +
            "only one child range leaves the index short while a table scan still returns everything, " +
            "which is exactly how such a bug stays hidden.");

        Assert.That(indexed.Distinct().Count(), Is.EqualTo(RowCount),
            "The backfill must not index a row twice");

        Assert.That(indexed, Is.EqualTo(indexed.OrderBy(x => x, StringComparer.Ordinal).ToList()),
            "The index scan must yield keys in ascending order");
    }

    // -----------------------------------------------------------------------
    // 6. A unique index built over a split row space. Uniqueness is enforced
    //    per key, so a backfill that missed a range would also fail to notice
    //    a duplicate in it — and would then accept writes it should reject.
    // -----------------------------------------------------------------------

    [Test]
    public async Task UniqueIndexBackfill_OverASplitRowSpace_StillRejectsADuplicate()
    {
        (string db, CommandExecutor executor, TableDescriptor table, List<string> _) = await SetupTableAsync();

        await SplitRowSpaceAtMedianAsync(table, executor, db);

        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: db,
            tableName: "readings",
            indexName: "label_unique",
            columns: [new ColumnIndexInfo("label", OrderType.Ascending)],
            operation: AlterIndexOperation.AddUniqueIndex,
            includeColumns: null));

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        // "reading-0" already exists. If the backfill had covered only one child range, the entry for
        // this label might be missing and the insert would be accepted.
        CamusDBException? failure = null;

        try
        {
            await executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "readings",
                values: new() { new() {
                    { "id",     new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                    { "label",  new(ColumnType.String,    "reading-0") },
                    { "amount", new(ColumnType.Integer64, 999L) },
                }}));

            await database.Transactions.CommitAsync(tx);
        }
        catch (CamusDBException exception)
        {
            failure = exception;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.That(failure, Is.Not.Null,
            "Inserting a duplicate of a value that lives in the lower child range must be rejected; " +
            "acceptance would mean the backfill never saw that range");

        Assert.That(failure!.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
    }
}
