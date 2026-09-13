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
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using Kahuna.Shared.KeyValue;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Behavioral guard for the paged primary-row fetch inside the non-unique probe of an index
/// nested-loop join.
///
/// <para>
/// For every outer row the probe walks the index equality range and used to fetch each matching
/// primary row with its own round trip. It now buffers the matching row ids and resolves them one
/// page at a time. The page boundary must be invisible: the same join at page sizes 1, 2 and 64 must
/// return the same rows, in the same order, with the same multiplicity and the same residual
/// filtering, and the plain nested-loop join is the oracle for that result. The page size is set
/// through <c>IndexScanFetchBatchSize</c> on the engine that runs the query, never after the engine
/// is built.
/// </para>
///
/// <para>
/// The fetch shape is asserted with the store-level counters: a probe over M matches at page size B
/// must issue ceil(M / B) batched reads and no single-row reads.
/// </para>
/// </summary>
[NonParallelizable]
public sealed class TestIndexNestedLoopProbeBatchFetch : SharedNodeBaseTest
{
    private static readonly int[] PageSizes = [1, 2, 64];

    /// <summary>The shape of the non-unique index on <c>line_items</c> for one fixture.</summary>
    private enum IndexShape
    {
        /// <summary>A single ascending column: <c>(order_id)</c>.</summary>
        OrderId,

        /// <summary>A descending column: <c>(order_id DESC)</c>, which flips the probe's stop rule.</summary>
        OrderIdDescending,
    }

    private sealed record Fixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor,
        IReadOnlyDictionary<string, string> OrderIds);

    /// <summary>
    /// orders     : id (pk), name string NOT NULL
    /// shipments  : id (pk), order_id id (nullable), label string NOT NULL — a left side whose
    ///              lookup column is not unique, so one order can be probed several times
    /// line_items : id (pk), order_id id (nullable), product string NOT NULL, qty int64 (nullable),
    ///              note string NOT NULL, with a non-unique index on order_id (see
    ///              <see cref="IndexShape"/>)
    ///
    /// <paramref name="fanOut"/> lists, per order name, how many line items reference it. Products
    /// are numbered in insertion order, and every third note is "drop" so a residual filter on note
    /// rejects rows in the middle of a page. A NULL qty every seventh row keeps the nullable column
    /// honest.
    ///
    /// The join planner probes only a single-column index, so a composite index never reaches the
    /// correlated probe and is not a shape here.
    /// </summary>
    private async Task<Fixture> Setup(
        CamusDBOptions options,
        IReadOnlyList<(string order, int matches)> fanOut,
        IndexShape shape = IndexShape.OrderId,
        IReadOnlyList<(string order, int shipments)>? shipments = null,
        int unreferencedLineItems = 0)
    {
        // The cost model prefers a hash join for these small fixtures, and the join-order optimizer
        // would happily swap the sides so the probe lands on the orders primary key. The subject here
        // is the fetch path of the non-unique correlated probe, not the algorithm or order choice, so
        // both are pinned: join order stays as written and the index probe is forced. The plan
        // assertions below still confirm the shape on every engine.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(options with { CostBasedJoinOrderEnabled = false });

        executor.Statistics.ForceIndexNestedLoopForTesting = true;

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "orders",
            columns:
            [
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "shipments",
            columns:
            [
                new("id", ColumnType.Id),
                new("order_id", ColumnType.Id),
                new("label", ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        ColumnIndexInfo[] indexColumns = shape switch
        {
            IndexShape.OrderId           => [new("order_id", OrderType.Ascending)],
            IndexShape.OrderIdDescending => [new("order_id", OrderType.Descending)],
            _                            => throw new ArgumentOutOfRangeException(nameof(shape)),
        };

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "line_items",
            columns:
            [
                new("id", ColumnType.Id),
                new("order_id", ColumnType.Id),
                new("product", ColumnType.String, notNull: true),
                new("qty", ColumnType.Integer64),
                new("note", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                new(ConstraintType.IndexMulti, "li_order_id_idx", indexColumns),
            ],
            ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        Dictionary<string, string> orderIds = new();
        List<Dictionary<string, ColumnValue>> orders = [];
        List<Dictionary<string, ColumnValue>> items = [];

        foreach ((string order, int matches) in fanOut)
        {
            string orderId = ObjectIdGenerator.Generate().ToString();
            orderIds[order] = orderId;

            orders.Add(new()
            {
                { "id", new(ColumnType.Id, orderId) },
                { "name", new(ColumnType.String, order) },
            });

            for (int i = 0; i < matches; i++)
                items.Add(LineItem(new ColumnValue(ColumnType.Id, orderId), order, i, matches));
        }

        for (int i = 0; i < unreferencedLineItems; i++)
            items.Add(LineItem(ColumnValue.Null, "none", i, unreferencedLineItems));

        await executor.Insert(new InsertTicket(txn, dbname, "orders", orders));

        // Large fan-outs are inserted in slices so one ticket stays a reasonable size.
        const int insertSlice = 1024;
        for (int offset = 0; offset < items.Count; offset += insertSlice)
            await executor.Insert(new InsertTicket(txn, dbname, "line_items", items.Skip(offset).Take(insertSlice).ToList()));

        if (shipments is not null)
        {
            List<Dictionary<string, ColumnValue>> rows = [];

            foreach ((string order, int count) in shipments)
            {
                for (int i = 0; i < count; i++)
                {
                    rows.Add(new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "order_id", orderIds.TryGetValue(order, out string? id) ? new ColumnValue(ColumnType.Id, id) : ColumnValue.Null },
                        { "label", new(ColumnType.String, $"{order}-s{i}") },
                    });
                }
            }

            await executor.Insert(new InsertTicket(txn, dbname, "shipments", rows));
        }

        await database.Transactions.CommitAsync(txn);

        return new Fixture(dbname, database, executor, orderIds);
    }

    private static Dictionary<string, ColumnValue> LineItem(ColumnValue orderId, string order, int i, int total)
    {
        return new()
        {
            { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
            { "order_id", orderId },
            { "product", new(ColumnType.String, $"{order}-p{i:D4}") },
            { "qty", i % 7 == 6 ? ColumnValue.Null : new ColumnValue(ColumnType.Integer64, (long)(total - i)) },
            { "note", new(ColumnType.String, i % 3 == 1 ? "drop" : "keep") },
        };
    }

    private static async Task<List<QueryResultRow>> Run(
        Fixture fixture,
        string sql,
        KvTransaction? txn = null,
        CancellationToken cancellationToken = default)
    {
        bool ownsTxn = txn is null;
        txn ??= await fixture.Database.Transactions.BeginAsync();

        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: txn,
                database: fixture.DbName,
                sql: sql,
                parameters: null,
                cancellationToken: cancellationToken));

            List<QueryResultRow> rows = [];

            await foreach (QueryResultRow row in cursor.WithCancellation(cancellationToken))
                rows.Add(row);

            return rows;
        }
        finally
        {
            if (ownsTxn)
                await fixture.Database.Transactions.CommitAsync(txn);
        }
    }

    private static async Task NonQuery(Fixture fixture, string sql, KvTransaction? txn = null)
    {
        bool ownsTxn = txn is null;
        txn ??= await fixture.Database.Transactions.BeginAsync();

        await fixture.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txn, fixture.DbName, sql, null));

        if (ownsTxn)
            await fixture.Database.Transactions.CommitAsync(txn);
    }

    /// <summary>Renders a result set as an order-sensitive list of strings, so parity is exact.</summary>
    private static List<string> Render(IReadOnlyList<QueryResultRow> rows, params string[] columns)
    {
        List<string> rendered = new(rows.Count);

        foreach (QueryResultRow row in rows)
            rendered.Add(string.Join("|", columns.Select(c => row.Row.TryGetValue(c, out ColumnValue? v) ? Format(v) : "<absent>")));

        return rendered;
    }

    /// <summary>Plain text for a cell, so assertions can match on a literal like <c>|keep</c>.</summary>
    private static string Format(ColumnValue value) => value.Type switch
    {
        ColumnType.Null      => "<null>",
        ColumnType.String    => value.StrValue ?? "<null>",
        ColumnType.Integer64 => value.LongValue.ToString(),
        _                    => value.ToString(),
    };

    private static async Task<string> ExplainDetails(Fixture fixture, string sql)
    {
        List<QueryResultRow> planRows = await Run(fixture, "EXPLAIN " + sql);

        return string.Join(
            "\n",
            planRows.Select(r => string.Join(" ", r.Row.Values.Select(v => v.StrValue ?? v.ToString()))));
    }

    /// <summary>
    /// Asserts the query plans an index nested-loop join whose inner side is <c>line_items</c>
    /// through its non-unique index. Every parity assertion in this fixture depends on this shape:
    /// a hash join or a leaf scan would exercise a different fetch path and prove nothing here.
    /// The shape is forced in <see cref="Setup"/>; this check proves the force took effect.
    /// </summary>
    private static async Task AssertPlansCorrelatedProbe(Fixture fixture, string sql, bool expectResidual = false)
    {
        string plan = await ExplainDetails(fixture, sql);

        StringAssert.Contains("index-nested-loop-join", plan, "the join must plan an index nested-loop join:\n" + plan);
        StringAssert.Contains("index=li_order_id_idx", plan, "the inner side must be probed through the non-unique index:\n" + plan);
        StringAssert.DoesNotContain("index-range-scan", plan, "a leaf scan would bypass the correlated probe:\n" + plan);
        StringAssert.DoesNotContain("index-in-list", plan, "a leaf scan would bypass the correlated probe:\n" + plan);

        if (expectResidual)
            StringAssert.Contains("right-filter=", plan, "the non-indexed predicate must stay a residual filter on the probe:\n" + plan);
    }

    /// <summary>
    /// Runs the same query through the paged probe and through a forced plain nested-loop join on
    /// the same engine, and returns both renderings. The plain nested loop reads the inner table in
    /// primary order per outer row, which is also the index order of a single-column probe, so a
    /// caller may compare the two exactly or as sorted multisets, depending on the index shape.
    /// </summary>
    private static async Task<(List<string> probed, List<string> oracle)> RunWithOracle(Fixture fixture, string sql, string[] columns)
    {
        List<string> probed = Render(await Run(fixture, sql), columns);

        fixture.Executor.Statistics.ForceNestedLoopForTesting = true;
        try
        {
            List<string> oracle = Render(await Run(fixture, sql), columns);
            return (probed, oracle);
        }
        finally
        {
            fixture.Executor.Statistics.ForceNestedLoopForTesting = false;
        }
    }

    private static async Task<KvTableStore> LineItemsStore(Fixture fixture)
    {
        TableDescriptor table = await fixture.Executor.OpenTable(new OpenTableTicket(fixture.DbName, "line_items"));
        return table.Store;
    }

    /// <summary>
    /// One line item as a test sees it: the storage row id (what a lock key or a direct store call
    /// needs), the <c>id</c> column (what a SQL predicate needs; it is not the storage row id), and
    /// the product, which encodes the insertion position.
    /// </summary>
    private sealed record ItemRef(ObjectIdValue RowId, string Id, string Product);

    private static async Task<List<ItemRef>> LineItems(Fixture fixture, string order)
    {
        List<QueryResultRow> rows = await Run(fixture,
            $"SELECT id, product FROM line_items WHERE order_id = \"{fixture.OrderIds[order]}\" ORDER BY product");

        return rows.Select(r => new ItemRef(r.RowId, r.Row["id"].StrValue!, r.Row["product"].StrValue!)).ToList();
    }

    private const string JoinSql =
        "SELECT o.name, li.product, li.qty, li.note FROM orders o JOIN line_items li ON li.order_id = o.id";

    private static readonly string[] JoinColumns = ["name", "product", "qty", "note"];

    // ── The plan under test ───────────────────────────────────────────────────

    [Test]
    public async Task ExplainConfirmsTheCorrelatedProbe()
    {
        Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = 2 }, [("A", 3), ("B", 0)]);

        await AssertPlansCorrelatedProbe(fixture, JoinSql);
        await AssertPlansCorrelatedProbe(fixture, JoinSql + " WHERE li.note = \"keep\"", expectResidual: true);
    }

    // ── Fan-out per outer key, at every page size, against the oracle ────────

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(63)]
    [TestCase(64)]
    [TestCase(65)]
    [TestCase(4096)]
    public async Task FanOut_PagesIdenticallyAtEveryPageSize_AndIssuesOneBatchPerPage(int matches)
    {
        List<string>? reference = null;

        foreach (int pageSize in PageSizes)
        {
            // Very large fan-outs skip the one-row page: 4,096 single-id batches per probe is a slow
            // walk that proves nothing the 63/64/65 cases do not already prove for page size 1.
            if (matches > 1024 && pageSize == 1)
                continue;

            Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = pageSize }, [("A", matches), ("B", 0)]);
            await AssertPlansCorrelatedProbe(fixture, JoinSql);

            KvTableStore store = await LineItemsStore(fixture);
            long batchBefore = store.PrimaryRowBatchReadCalls;
            long pointBefore = store.PrimaryRowPointReadCalls;

            List<string> probed = Render(await Run(fixture, JoinSql), JoinColumns);

            long expectedBatches = (matches + pageSize - 1) / pageSize;
            Assert.AreEqual(expectedBatches, store.PrimaryRowBatchReadCalls - batchBefore,
                $"a probe over {matches} matches at page size {pageSize} must issue ceil(M / B) batched reads");
            Assert.AreEqual(0, store.PrimaryRowPointReadCalls - pointBefore,
                "the paged probe must not fall back to single-row reads");

            Assert.AreEqual(matches, probed.Count, $"page size {pageSize} changed the row count");

            if (reference is null)
            {
                (_, List<string> oracle) = await RunWithOracle(fixture, JoinSql, JoinColumns);
                Assert.AreEqual(oracle, probed, "the paged probe must match the plain nested-loop join row for row");
                reference = probed;
            }
            else
            {
                Assert.AreEqual(reference, probed, $"page size {pageSize} changed the result");
            }
        }
    }

    // ── Repeated outer keys keep per-outer-row multiplicity and order ─────────

    [Test]
    public async Task RepeatedOuterKeys_EmitEveryMatchOncePerOuterRow()
    {
        const string sql =
            "SELECT s.label, li.product FROM shipments s JOIN line_items li ON li.order_id = s.order_id";
        string[] columns = ["label", "product"];

        List<string>? reference = null;

        foreach (int pageSize in PageSizes)
        {
            Fixture fixture = await Setup(
                Options with { IndexScanFetchBatchSize = pageSize },
                [("A", 5), ("B", 3)],
                shipments: [("A", 3), ("B", 1), ("A", 1)]);

            await AssertPlansCorrelatedProbe(fixture, sql);

            (List<string> probed, List<string> oracle) = await RunWithOracle(fixture, sql, columns);

            Assert.AreEqual(4 * 5 + 1 * 3, probed.Count, "every shipment must pair with every line item of its order");
            Assert.AreEqual(oracle, probed, "the paged probe must repeat a probe's rows once per outer row, in order");

            reference ??= probed;
            Assert.AreEqual(reference, probed, $"page size {pageSize} changed the result");
        }
    }

    // ── NULL lookup keys ──────────────────────────────────────────────────────

    [Test]
    public async Task NullLookupKeys_MatchNothing_AtEveryPageSize()
    {
        const string sql =
            "SELECT s.label, li.product FROM shipments s JOIN line_items li ON li.order_id = s.order_id";
        string[] columns = ["label", "product"];

        foreach (int pageSize in PageSizes)
        {
            // Two shipments with a NULL order_id, three line items with a NULL order_id: a NULL never
            // equals a NULL, so neither side contributes a row.
            Fixture fixture = await Setup(
                Options with { IndexScanFetchBatchSize = pageSize },
                [("A", 4)],
                shipments: [("A", 1), ("missing", 2)],
                unreferencedLineItems: 3);

            await AssertPlansCorrelatedProbe(fixture, sql);

            (List<string> probed, List<string> oracle) = await RunWithOracle(fixture, sql, columns);

            Assert.AreEqual(4, probed.Count, "only the shipment with a real order id pairs with rows");
            Assert.AreEqual(oracle, probed);
        }
    }

    // ── Descending index ──────────────────────────────────────────────────────

    [Test]
    public async Task DescendingFirstColumn_StopsAtTheProbeKey_AtEveryPageSize()
    {
        List<string>? reference = null;

        foreach (int pageSize in PageSizes)
        {
            Fixture fixture = await Setup(
                Options with { IndexScanFetchBatchSize = pageSize },
                [("A", 5), ("B", 7), ("C", 3)],
                IndexShape.OrderIdDescending);

            await AssertPlansCorrelatedProbe(fixture, JoinSql);

            (List<string> probed, List<string> oracle) = await RunWithOracle(fixture, JoinSql, JoinColumns);

            Assert.AreEqual(15, probed.Count);
            Assert.AreEqual(oracle, probed, "a descending first column must still stop at the probe key and match the oracle");

            reference ??= probed;
            Assert.AreEqual(reference, probed, $"page size {pageSize} changed the result");
        }
    }

    // ── Residual filter and a missing row inside a page ───────────────────────

    [Test]
    public async Task ResidualFilter_RejectsRowsInsideAPage_AtEveryPageSize()
    {
        string sql = JoinSql + " WHERE li.note = \"keep\"";
        List<string>? reference = null;

        foreach (int pageSize in PageSizes)
        {
            Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = pageSize }, [("A", 10), ("B", 4)]);

            await AssertPlansCorrelatedProbe(fixture, sql, expectResidual: true);

            (List<string> probed, List<string> oracle) = await RunWithOracle(fixture, sql, JoinColumns);

            // Every third line item carries note "drop": 10 → 7 kept, 4 → 3 kept.
            Assert.AreEqual(10, probed.Count, "the residual filter must reject the 'drop' rows");
            Assert.IsTrue(probed.All(v => v.EndsWith("|keep", StringComparison.Ordinal)));
            Assert.AreEqual(oracle, probed);

            reference ??= probed;
            Assert.AreEqual(reference, probed, $"page size {pageSize} changed the result");
        }
    }

    [Test]
    public async Task MissingRowBehindAnIndexEntry_IsSkippedWithoutShiftingThePage()
    {
        List<string>? reference = null;

        foreach (int pageSize in PageSizes)
        {
            Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = pageSize }, [("A", 6)]);

            // Delete the primary row only, leaving its index entry behind: the probe must skip the
            // absent row and keep every other row at its place.
            List<ItemRef> items = await LineItems(fixture, "A");
            KvTableStore store = await LineItemsStore(fixture);

            KvTransaction del = await fixture.Database.Transactions.BeginAsync();
            await store.DeleteRow(del, items[2].RowId);
            await fixture.Database.Transactions.CommitAsync(del);

            List<string> probed = Render(await Run(fixture, JoinSql), JoinColumns);

            Assert.AreEqual(5, probed.Count, "the orphaned index entry must not produce a row");
            Assert.IsFalse(probed.Any(v => v.Contains(items[2].Product, StringComparison.Ordinal)), "the deleted row must be absent");

            reference ??= probed;
            Assert.AreEqual(reference, probed, $"page size {pageSize} changed the result");
        }
    }

    // ── Serializable read-write: the lock set after full consumption ──────────

    [Test]
    public async Task SerializableReadWrite_LocksExactlyTheMatchedRows_AtEveryPageSize()
    {
        const string sql = JoinSql + " WHERE o.name = \"A\"";

        foreach (int pageSize in PageSizes)
        {
            // 20 matches stays under the lock escalation threshold, so every lock is a point lock.
            Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = pageSize }, [("A", 20), ("C", 3)]);

            await AssertPlansCorrelatedProbe(fixture, sql);

            List<ObjectIdValue> matched = (await LineItems(fixture, "A")).Select(i => i.RowId).ToList();
            List<ObjectIdValue> unmatched = (await LineItems(fixture, "C")).Select(i => i.RowId).ToList();
            KvTableStore store = await LineItemsStore(fixture);

            KvTransaction txn = await fixture.Database.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

            try
            {
                List<QueryResultRow> rows = await Run(fixture, sql, txn);
                Assert.AreEqual(20, rows.Count);

                Assert.IsFalse(txn.HasWholeBucketLock(store.RowKeySpace), "no escalation below the threshold");

                foreach (ObjectIdValue rowId in matched)
                    Assert.IsTrue(txn.HasPointLock(store.RowKeySpace, store.RowPointKey(rowId), RangeLockMode.Shared),
                        $"page size {pageSize}: every fetched row must hold the shared point lock a per-row read took");

                foreach (ObjectIdValue rowId in unmatched)
                    Assert.IsFalse(txn.HasPointLock(store.RowKeySpace, store.RowPointKey(rowId)),
                        $"page size {pageSize}: a row the probe never matched must not be locked");

                Assert.AreEqual(matched.Count, txn.CountPointLocksForBucket(store.RowKeySpace),
                    $"page size {pageSize}: the row bucket must hold one point lock per matched row and nothing else");
            }
            finally
            {
                await fixture.Database.Transactions.RollbackIfNotCompletedAsync(txn);
            }
        }
    }

    /// <summary>
    /// The lock-timing consequence of paging, pinned down so it is a decision and not a surprise:
    /// under Serializable read-write a flushed page holds shared point locks on every row it
    /// fetched, including rows the consumer has not asked for yet, while rows of a page not yet
    /// flushed are unlocked. A concurrent writer of a fetched row is refused; a writer of an
    /// unfetched row proceeds, exactly as it did against the per-row read before that row was read.
    /// </summary>
    [Test]
    public async Task SerializableReadWrite_ConcurrentWriterOnAFetchedRowIsRefused_OnAnUnfetchedRowProceeds()
    {
        Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = 2 }, [("A", 4)]);
        List<ItemRef> items = await LineItems(fixture, "A");
        KvTableStore store = await LineItemsStore(fixture);

        KvTransaction reader = await fixture.Database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);

        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(reader, fixture.DbName, JoinSql, null));

            await using IAsyncEnumerator<QueryResultRow> enumerator = cursor.GetAsyncEnumerator();

            Assert.IsTrue(await enumerator.MoveNextAsync(), "the first row of the first page");

            // Page size 2: rows 0 and 1 are fetched and locked, rows 2 and 3 are not fetched yet.
            Assert.IsTrue(reader.HasPointLock(store.RowKeySpace, store.RowPointKey(items[1].RowId)));
            Assert.IsFalse(reader.HasPointLock(store.RowKeySpace, store.RowPointKey(items[3].RowId)));

            KvTransaction writerOfFetched = await fixture.Database.Transactions.BeginAsync();
            CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            {
                await NonQuery(fixture, $"UPDATE line_items SET note = \"late\" WHERE id = \"{items[1].Id}\"", writerOfFetched);
                await fixture.Database.Transactions.CommitAsync(writerOfFetched);
            });
            Assert.That(refused?.Code, Is.AnyOf(CamusDBErrorCodes.TransactionConflict, CamusDBErrorCodes.TransactionMustRetry),
                "a write to a row the page already fetched must be refused while the reader is live");
            await fixture.Database.Transactions.RollbackIfNotCompletedAsync(writerOfFetched);

            KvTransaction writerOfUnfetched = await fixture.Database.Transactions.BeginAsync();
            await NonQuery(fixture, $"UPDATE line_items SET note = \"late\" WHERE id = \"{items[3].Id}\"", writerOfUnfetched);
            await fixture.Database.Transactions.CommitAsync(writerOfUnfetched);

            int remaining = 0;
            while (await enumerator.MoveNextAsync())
                remaining++;

            Assert.AreEqual(3, remaining, "the reader must still see every row of the probe");
        }
        finally
        {
            await fixture.Database.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }

    // ── Read-your-writes inside one transaction ───────────────────────────────

    [Test]
    public async Task ReadYourWrites_UncommittedRowsAndUpdatesAreVisibleThroughThePage()
    {
        Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = 2 }, [("A", 5)]);
        string orderId = fixture.OrderIds["A"];
        List<ItemRef> items = await LineItems(fixture, "A");

        KvTransaction txn = await fixture.Database.Transactions.BeginAsync();

        try
        {
            for (int i = 0; i < 3; i++)
            {
                await NonQuery(fixture,
                    $"INSERT INTO line_items (id, order_id, product, qty, note) VALUES (gen_id(), \"{orderId}\", \"A-new{i}\", {100 + i}, \"keep\")",
                    txn);
            }

            await NonQuery(fixture, $"UPDATE line_items SET note = \"mine\" WHERE id = \"{items[1].Id}\"", txn);

            List<string> probed = Render(await Run(fixture, JoinSql, txn), JoinColumns);

            Assert.AreEqual(8, probed.Count, "the three uncommitted inserts must join in the same transaction");
            Assert.AreEqual(3, probed.Count(v => v.Contains("A-new", StringComparison.Ordinal)));
            Assert.AreEqual(1, probed.Count(v => v.EndsWith("|mine", StringComparison.Ordinal)),
                "the uncommitted update must be the version the page reads");
        }
        finally
        {
            await fixture.Database.Transactions.RollbackIfNotCompletedAsync(txn);
        }
    }

    // ── Optimistic read-set validation ────────────────────────────────────────

    [Test]
    public async Task OptimisticTransaction_CommitAbortsWhenAPagedRowWasModifiedConcurrently()
    {
        Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = 2 }, [("A", 5)]);
        List<ItemRef> items = await LineItems(fixture, "A");
        string orderId = fixture.OrderIds["A"];

        // The optimistic reader folds every batched row read into its read set.
        KvTransaction reader = await fixture.Database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted,
            locking: KeyValueTransactionLocking.Optimistic);

        try
        {
            List<QueryResultRow> rows = await Run(fixture, JoinSql, reader);
            Assert.AreEqual(5, rows.Count);

            // A peer modifies a row the page read, and commits.
            await NonQuery(fixture, $"UPDATE line_items SET note = \"peer\" WHERE id = \"{items[3].Id}\"");

            // The reader writes a disjoint key and commits: its read set is stale, so commit must abort.
            await NonQuery(fixture,
                $"INSERT INTO line_items (id, order_id, product, qty, note) VALUES (gen_id(), \"{orderId}\", \"A-late\", 1, \"keep\")",
                reader);

            Assert.ThrowsAsync<CamusDBException>(async () => await fixture.Database.Transactions.CommitAsync(reader),
                "an optimistic commit must abort when a row the paged probe read was modified by a committed peer");
        }
        finally
        {
            await fixture.Database.Transactions.RollbackIfNotCompletedAsync(reader);
        }

        List<QueryResultRow> after = await Run(fixture, "SELECT product FROM line_items");
        Assert.AreEqual(5, after.Count, "the aborted transaction's insert must not persist");
    }

    // ── Branch ancestry ───────────────────────────────────────────────────────

    [Test]
    public async Task BranchInheritsParentRows_AndPagesIdenticallyAtEveryPageSize()
    {
        List<string>? reference = null;

        foreach (int pageSize in PageSizes)
        {
            Fixture root = await Setup(Options with { IndexScanFetchBatchSize = pageSize }, [("A", 9), ("B", 2)]);
            List<ItemRef> rootItems = await LineItems(root, "A");

            string branchName = "b_" + Guid.NewGuid().ToString("n");
            DatabaseDescriptor branchDb = await root.Executor.CreateDatabase(
                new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: root.DbName));
            TrackDatabase(branchName, root.Executor);

            Fixture branch = new(branchName, branchDb, root.Executor, root.OrderIds);

            // One inherited row overridden, one tombstoned, two added: the page resolves inherited ids
            // through the ancestry and must honour the branch-level override and tombstone.
            await NonQuery(branch, $"UPDATE line_items SET note = \"branch\" WHERE id = \"{rootItems[4].Id}\"");
            await NonQuery(branch, $"DELETE FROM line_items WHERE id = \"{rootItems[6].Id}\"");
            for (int i = 0; i < 2; i++)
            {
                await NonQuery(branch,
                    $"INSERT INTO line_items (id, order_id, product, qty, note) VALUES (gen_id(), \"{branch.OrderIds["A"]}\", \"A-branch{i}\", {50 + i}, \"keep\")");
            }

            await AssertPlansCorrelatedProbe(branch, JoinSql);

            (List<string> probed, List<string> oracle) = await RunWithOracle(branch, JoinSql, JoinColumns);

            Assert.AreEqual(9 - 1 + 2 + 2, probed.Count, "inherited rows minus the tombstone plus the branch rows, plus B");
            Assert.AreEqual(1, probed.Count(v => v.EndsWith("|branch", StringComparison.Ordinal)), "the branch override must win");
            Assert.IsFalse(probed.Any(v => v.Contains(rootItems[6].Product, StringComparison.Ordinal)), "the tombstoned row must not resurface");
            Assert.AreEqual(oracle, probed);

            reference ??= probed;
            Assert.AreEqual(reference, probed, $"page size {pageSize} changed the branch result");

            // The parent is untouched.
            List<string> parent = Render(await Run(root, JoinSql), JoinColumns);
            Assert.AreEqual(11, parent.Count);
            Assert.IsFalse(parent.Any(v => v.EndsWith("|branch", StringComparison.Ordinal)));
        }
    }

    // ── Cancellation inside a page ────────────────────────────────────────────

    [Test]
    public async Task CancellationMidPage_StopsTheProbeBeforeTheNextRow()
    {
        Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = 4 }, [("A", 8)]);

        using CancellationTokenSource cts = new();

        KvTransaction txn = await fixture.Database.Transactions.BeginAsync();

        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await fixture.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txn, fixture.DbName, JoinSql, null, cancellationToken: cts.Token));

            await using IAsyncEnumerator<QueryResultRow> enumerator = cursor.GetAsyncEnumerator(cts.Token);

            Assert.IsTrue(await enumerator.MoveNextAsync(), "the first row of a four-row page");

            // The page still holds three fetched rows: the cancel must be observed before the next one.
            cts.Cancel();

            Assert.CatchAsync<OperationCanceledException>(async () => await enumerator.MoveNextAsync());
        }
        finally
        {
            await fixture.Database.Transactions.RollbackIfNotCompletedAsync(txn);
        }
    }

    [Test]
    public async Task CancelledTicketTokenStopsTheProbe()
    {
        Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = 2 }, [("A", 6)]);

        using CancellationTokenSource cts = new();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () => await Run(fixture, JoinSql, cancellationToken: cts.Token));
    }

    // ── First-row cost and the one-match control ──────────────────────────────

    [Test]
    public async Task LimitOne_FetchesExactlyOnePage()
    {
        Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = 64 }, [("A", 4096)]);
        KvTableStore store = await LineItemsStore(fixture);

        long batchBefore = store.PrimaryRowBatchReadCalls;
        long pointBefore = store.PrimaryRowPointReadCalls;

        List<QueryResultRow> rows = await Run(fixture, JoinSql + " LIMIT 1");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(1, store.PrimaryRowBatchReadCalls - batchBefore, "the first row must cost one page, not the whole probe");
        Assert.AreEqual(0, store.PrimaryRowPointReadCalls - pointBefore);
    }

    [Test]
    public async Task OneMatchPerProbe_CostsOneBatchedReadPerOuterRow()
    {
        List<(string order, int matches)> fanOut = Enumerable.Range(0, 12).Select(i => ($"O{i:D2}", 1)).ToList();
        Fixture fixture = await Setup(Options with { IndexScanFetchBatchSize = 64 }, fanOut);
        await AssertPlansCorrelatedProbe(fixture, JoinSql);

        KvTableStore store = await LineItemsStore(fixture);
        long batchBefore = store.PrimaryRowBatchReadCalls;
        long pointBefore = store.PrimaryRowPointReadCalls;

        (List<string> probed, List<string> oracle) = await RunWithOracle(fixture, JoinSql, JoinColumns);

        Assert.AreEqual(12, probed.Count);
        Assert.AreEqual(oracle, probed);

        // The oracle run above issues no primary point reads either: it scans the inner table.
        Assert.AreEqual(12, store.PrimaryRowBatchReadCalls - batchBefore, "one probe, one match, one batched read");
        Assert.AreEqual(0, store.PrimaryRowPointReadCalls - pointBefore);
    }
}
