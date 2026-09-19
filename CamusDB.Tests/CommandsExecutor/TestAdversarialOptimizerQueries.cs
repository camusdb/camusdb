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

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Adversarial end-to-end SQL queries that stress the planner's resolution and the execution
/// pipeline's correctness at the same time.
///
/// The fixture is deliberately hostile rather than realistic:
///   - nullable join keys on both sides of every join (orphan orders/items that must never match),
///   - nullable filter columns (three-valued logic must drop them from =, !=, ranges, NOT),
///   - duplicated and tied values in every indexed column (ORDER BY ties, index scan ranges that
///     hit equal keys, OR branches whose row sets overlap),
///   - negative and zero numbers (sign-sensitive key encodings and bounds),
///   - an empty-string city and a case-variant duplicate 'lima'/'Lima' (string ordering and
///     case-sensitive equality),
///   - a self-referencing column with NULLs (self-join plus NOT IN with NULL in the subquery).
///
/// Every expectation below was derived by hand from the fixture tables using standard SQL
/// semantics (UNKNOWN excluded by WHERE, aggregates ignore NULLs, NOT IN over a set containing
/// NULL yields no rows). A query only passes by producing exactly those rows — a plan that picks
/// a wrong index, mis-intersects range bounds, drops a pushdown predicate, double-counts an OR
/// branch, or mishandles a NULL join key returns something else.
/// </summary>
public sealed class TestAdversarialOptimizerQueries : SharedNodeBaseTest
{
    // ── Setup ─────────────────────────────────────────────────────────────────

    private sealed record Fixture(
        string DbName,
        DatabaseDescriptor Database,
        CommandExecutor Executor);

    /// <summary>
    /// regions(1 'north', 2 'south', 3 'west');
    /// customers 1..11 with NULL city/tier/region_id/referred_by, credit ties at 500/-50,
    /// 'Oslo' repeated four times, '' and 'lima'/'Lima' as string traps;
    /// orders 100..119 with two orphan rows (NULL customer_id), NULL totals/years, total ties
    /// at 120 (five times), 700, 300, 60, a -30 and a 0;
    /// order_items 500..516 with two orphan rows (NULL order_id), qty ties at 10 and 2, a 0.
    /// </summary>
    private async Task<Fixture> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE regions (id int64 primary key, name string(32) not null)");

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE customers (" +
            "id int64 primary key, name string(64) not null, city string(32), " +
            "region_id int64, tier int64, credit int64, referred_by int64)");

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE orders (" +
            "id int64 primary key, customer_id int64, status string(16) not null, " +
            "total int64, year int64)");

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE order_items (" +
            "id int64 primary key, order_id int64, sku string(32) not null, qty int64)");

        await ExecDdl(database, executor, dbname, "CREATE INDEX customers_city_idx ON customers (city)");
        await ExecDdl(database, executor, dbname, "CREATE INDEX customers_tier_credit_idx ON customers (tier, credit)");
        await ExecDdl(database, executor, dbname, "CREATE INDEX orders_customer_idx ON orders (customer_id)");
        await ExecDdl(database, executor, dbname, "CREATE INDEX orders_status_total_idx ON orders (status, total)");
        await ExecDdl(database, executor, dbname, "CREATE INDEX orders_total_idx ON orders (total)");
        await ExecDdl(database, executor, dbname, "CREATE INDEX orders_year_idx ON orders (year)");
        await ExecDdl(database, executor, dbname, "CREATE INDEX order_items_order_idx ON order_items (order_id)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO regions (id, name) VALUES (1, 'north'), (2, 'south'), (3, 'west')");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO customers (id, name, city, region_id, tier, credit, referred_by) VALUES " +
            "(1, 'Ann', 'Oslo', 1, 1, 500, NULL), " +
            "(2, 'Bob', 'Oslo', 1, 1, 500, 1), " +
            "(3, 'Cid', 'Bergen', 1, 2, -50, 1), " +
            "(4, 'Dan', NULL, 2, 2, 300, NULL), " +
            "(5, 'Eve', 'Lima', 3, NULL, 250, 2), " +
            "(6, 'Fay', 'Oslo', 1, 1, 800, 2), " +
            "(7, 'Gus', 'lima', 3, 3, 0, 5), " +
            "(8, 'Hal', 'Bergen', 2, 3, -50, NULL), " +
            "(9, 'Ivy', '', 1, 1, 500, 1), " +
            "(10, 'Jon', 'Oslo', NULL, 2, 500, NULL), " +
            "(11, 'Kim', 'Bergen', 2, 2, 150, 7)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customer_id, status, total, year) VALUES " +
            "(100, 1, 'paid', 120, 2020), " +
            "(101, 1, 'open', 80, 2021), " +
            "(102, 2, 'paid', 120, 2021), " +
            "(103, 2, 'void', NULL, 2022), " +
            "(104, 3, 'paid', 300, 2022), " +
            "(105, 4, 'open', 45, 2023), " +
            "(106, NULL, 'paid', 999, 2023), " +
            "(107, 5, 'paid', 120, 2020), " +
            "(108, 5, 'open', 15, 2021), " +
            "(109, 6, 'paid', 700, 2022), " +
            "(110, 6, 'paid', 700, 2022), " +
            "(111, 7, 'open', -30, 2023), " +
            "(112, 8, 'paid', 0, 2020), " +
            "(113, 8, 'paid', 250, NULL), " +
            "(114, 9, 'paid', 120, 2021), " +
            "(115, 10, 'paid', 60, 2022), " +
            "(116, 10, 'open', 60, 2023), " +
            "(117, 3, 'open', 300, 2023), " +
            "(118, NULL, 'open', NULL, NULL), " +
            "(119, 9, 'paid', 120, 2020)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO order_items (id, order_id, sku, qty) VALUES " +
            "(500, 100, 'WIDGET', 2), " +
            "(501, 100, 'GADGET', 1), " +
            "(502, 102, 'WIDGET', 5), " +
            "(503, 104, 'SPROCKET', 3), " +
            "(504, 105, 'WIDGET', 1), " +
            "(505, NULL, 'ORPHAN', 9), " +
            "(506, 107, 'GADGET', 2), " +
            "(507, 109, 'WIDGET', 10), " +
            "(508, 110, 'WIDGET', 10), " +
            "(509, 111, 'SPROCKET', 0), " +
            "(510, 112, 'DOOHICKEY', 4), " +
            "(511, 113, 'WIDGET', 6), " +
            "(512, 114, 'GADGET', 1), " +
            "(513, 116, 'SPROCKET', 2), " +
            "(514, 117, 'DOOHICKEY', 7), " +
            "(515, NULL, 'WIDGET', 3), " +
            "(516, 119, 'GADGET', 2)");

        return new Fixture(dbname, database, executor);
    }

    private static async Task ExecDdl(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>Sorted id list — for queries whose row order is not under test.</summary>
    private static List<long> SortedIds(List<QueryResultRow> rows, string column = "id")
        => rows.Select(r => r.Row[column].LongValue).OrderBy(x => x).ToList();

    // ── Join ordering and predicate pushdown ─────────────────────────────────

    /// <summary>
    /// The selective filter sits on the last declared table of a four-way chain. Reordering the
    /// join (or pushing the region filter down) is the planner's business; correctness requires
    /// the NULL customer_id and NULL order_id rows to drop out of the chain either way.
    /// </summary>
    [Test]
    public async Task JoinOrder_FourWayChain_FilterOnLastDeclaredTable()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT o.id FROM orders o " +
            "JOIN customers c ON o.customer_id = c.id " +
            "JOIN regions r ON c.region_id = r.id " +
            "JOIN order_items i ON i.order_id = o.id " +
            "WHERE r.name = 'west' " +
            "ORDER BY o.id");

        // West region holds customers Eve and Gus; Eve's order 107 has item 506, her order 108
        // has no items, Gus's order 111 has item 509.
        Assert.AreEqual(new List<long> { 107, 111 }, SortedIds(rows));
    }

    /// <summary>
    /// Star join with filters on two dimension tables. The customer with region_id NULL must
    /// vanish (join), the tier NULL customer must vanish (filter), and the customer with no
    /// orders must not produce a group.
    /// </summary>
    [Test]
    public async Task JoinOrder_StarJoin_FiltersOnTwoDimensions()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT r.name, COUNT(*) AS cnt FROM orders o " +
            "JOIN customers c ON o.customer_id = c.id " +
            "JOIN regions r ON c.region_id = r.id " +
            "WHERE c.tier = 2 AND r.name != 'north' " +
            "GROUP BY r.name ORDER BY r.name");

        // Tier-2 customers joinable to a region: Cid (north, excluded by !=) and Dan (south).
        // Dan's single order 105 is the whole result; Kim has no orders, Jon has a NULL region.
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("south", rows[0].Row["name"].StrValue);
        Assert.AreEqual(1, rows[0].Row["cnt"].LongValue);
    }

    /// <summary>
    /// Self-join over the referral chain: the inner side's tier filter must be evaluated against
    /// the referee (m), not the referrer, and Eve's NULL tier must drop the (Gus -> Eve) pair.
    /// </summary>
    [Test]
    public async Task JoinOrder_SelfJoinReferralChain()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT c.name, m.name AS manager FROM customers c " +
            "JOIN customers m ON c.referred_by = m.id " +
            "WHERE m.tier >= 2 " +
            "ORDER BY c.id");

        // Referrers used: Ann/Bob/Eve (tier 1 or NULL) and Gus (tier 3). Only Kim was referred
        // by someone with tier >= 2.
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("Kim", rows[0].Row["name"].StrValue);
        Assert.AreEqual("Gus", rows[0].Row["manager"].StrValue);
    }

    /// <summary>
    /// Comma-separated FROM list mixes equi-join predicates with local filters on three
    /// different tables — the normalizer must turn it into the same join as explicit syntax and
    /// push each local predicate onto its own table.
    /// </summary>
    [Test]
    public async Task JoinOrder_CommaJoinWithMixedWhere()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT c.id FROM customers c, orders o, regions r " +
            "WHERE c.id = o.customer_id AND c.region_id = r.id " +
            "AND r.name = 'south' AND o.total > 50 " +
            "ORDER BY c.id");

        // South holds Dan, Hal, Kim. Dan's order 105 totals 45, Hal's 112 totals 0 and 113
        // totals 250, Kim has no orders.
        Assert.AreEqual(new List<long> { 8 }, SortedIds(rows));
    }

    /// <summary>
    /// A single-table filter in the ON clause and the same filter in the WHERE clause are
    /// logically identical for an inner join; the pushdown must treat them equivalently.
    /// </summary>
    [Test]
    public async Task JoinOrder_OnClauseFilterEqualsWhereFilter()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> onRows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id AND c.tier = 3 " +
            "WHERE o.year = 2023 ORDER BY o.id");

        List<QueryResultRow> whereRows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id " +
            "WHERE c.tier = 3 AND o.year = 2023 ORDER BY o.id");

        // Tier-3 customers are Gus and Hal; only Gus has a 2023 order (111). Hal's 113 has a
        // NULL year and must drop.
        Assert.AreEqual(new List<long> { 111 }, SortedIds(onRows));
        Assert.AreEqual(SortedIds(onRows), SortedIds(whereRows));
    }

    /// <summary>
    /// Duplicate equi-predicates and a trivially-true self-equality in the ON clause must not
    /// duplicate or lose rows — the count is the plain join cardinality.
    /// </summary>
    [Test]
    public async Task JoinOrder_RedundantEquiPredicatesKeepCardinality()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT COUNT(*) AS cnt FROM orders o " +
            "JOIN customers c ON o.customer_id = c.id AND o.customer_id = c.id AND c.id = c.id");

        // 18 of the 20 orders point at a real customer; the two orphans have NULL customer_id.
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(18, rows[0].Row["cnt"].LongValue);
    }

    /// <summary>
    /// A cross-table contradiction in the ON clause must prune to an empty result instead of
    /// scanning forever or returning the unconstrained join.
    /// </summary>
    [Test]
    public async Task JoinOrder_ContradictoryJoinPredicateIsEmpty()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT o.id FROM orders o " +
            "JOIN customers c ON o.customer_id = c.id AND c.id = 5 AND c.tier = 99");

        Assert.AreEqual(0, rows.Count);
    }

    // ── Predicate and index-selection traps ───────────────────────────────────

    /// <summary>
    /// OR across two differently-indexed columns cannot be answered by one index. Jon matches
    /// both branches and must appear exactly once — a plan that concatenates two index scans
    /// without deduplicating returns him twice.
    /// </summary>
    [Test]
    public async Task Predicate_OrAcrossColumnsCountsOverlapOnce()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM customers WHERE city = 'Oslo' OR tier = 2 ORDER BY id");

        Assert.AreEqual(new List<long> { 1, 2, 3, 4, 6, 10, 11 }, SortedIds(rows));
    }

    /// <summary>
    /// Four overlapping range conjuncts on an indexed column: the scan must intersect to the
    /// tightest window (150, 700], keeping the 700 ties and dropping NULL totals.
    /// </summary>
    [Test]
    public async Task Predicate_RedundantBoundsIntersectToTightest()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM orders " +
            "WHERE total > 50 AND total > 150 AND total < 900 AND total <= 700 " +
            "ORDER BY id");

        Assert.AreEqual(new List<long> { 104, 109, 110, 113, 117 }, SortedIds(rows));
    }

    /// <summary>
    /// Contradictory bounds on an indexed column: the derived key range is empty and the query
    /// must return zero rows, not "everything between" or an inverted range.
    /// </summary>
    [Test]
    public async Task Predicate_ContradictoryBoundsIsEmpty()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM orders WHERE year > 2022 AND year < 2020");

        Assert.AreEqual(0, rows.Count);
    }

    /// <summary>
    /// BETWEEN with reversed operands is an empty interval in SQL — a normalizer that swaps the
    /// bounds "to be helpful" would return the 2020..2022 rows instead.
    /// </summary>
    [Test]
    public async Task Predicate_ReversedBetweenIsEmpty()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM orders WHERE year BETWEEN 2022 AND 2020");

        Assert.AreEqual(0, rows.Count);
    }

    /// <summary>
    /// NOT BETWEEN is a three-valued comparison: the orders with a NULL year (113, 118) must be
    /// dropped along with the 2020/2021 rows, keeping only 2022 and 2023. The NOT BETWEEN
    /// spelling and the NOT (x BETWEEN …) form must return the same rows.
    /// </summary>
    [Test]
    public async Task Predicate_NotBetweenDropsNulls()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM orders WHERE year NOT BETWEEN 2020 AND 2021 ORDER BY id");

        List<QueryResultRow> parenthesised = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM orders WHERE NOT (year BETWEEN 2020 AND 2021) ORDER BY id");

        // 2022 (103, 104, 109, 110, 115) and 2023 (105, 106, 111, 116, 117); the NULL-year
        // orders 113 and 118 must drop because NOT over UNKNOWN is still UNKNOWN.
        List<long> expected = new() { 103, 104, 105, 106, 109, 110, 111, 115, 116, 117 };
        Assert.AreEqual(expected, SortedIds(rows));
        Assert.AreEqual(expected, SortedIds(parenthesised));
    }

    /// <summary>
    /// Inequality excludes NULLs: Dan's NULL city matches neither 'Oslo' nor its negation, and
    /// the empty-string city of Ivy counts as a real value that != 'Oslo'.
    /// </summary>
    [Test]
    public async Task Predicate_InequalityDropsNulls()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM customers WHERE city != 'Oslo' ORDER BY id");

        Assert.AreEqual(new List<long> { 3, 5, 7, 8, 9, 11 }, SortedIds(rows));
    }

    /// <summary>
    /// A NULL literal inside an IN list makes membership UNKNOWN for non-matching rows, so the
    /// NULL-tier customer must drop while tiers 1 and 3 match.
    /// </summary>
    [Test]
    public async Task Predicate_InListWithNullLiteral()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM customers WHERE tier IN (1, 3, NULL) ORDER BY id");

        Assert.AreEqual(new List<long> { 1, 2, 6, 7, 8, 9 }, SortedIds(rows));
    }

    /// <summary>
    /// The classic NOT IN trap: the subquery result contains NULLs (customers with no
    /// referee), so NOT IN yields UNKNOWN for every outer row and the result is empty. A
    /// rewrite to an anti-join must preserve that.
    /// </summary>
    [Test]
    public async Task Predicate_NotInSubqueryContainingNullIsEmpty()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM orders " +
            "WHERE customer_id NOT IN (SELECT referred_by FROM customers)");

        Assert.AreEqual(0, rows.Count);
    }

    /// <summary>
    /// Same shape with the NULLs filtered out of the subquery: now NOT IN is a plain anti-join
    /// and must return every customer that never refereed anyone.
    /// </summary>
    [Test]
    public async Task Predicate_NotInSubqueryWithoutNull()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM customers " +
            "WHERE id NOT IN (SELECT referred_by FROM customers WHERE referred_by IS NOT NULL) " +
            "ORDER BY id");

        Assert.AreEqual(new List<long> { 3, 4, 6, 8, 9, 10, 11 }, SortedIds(rows));
    }

    /// <summary>
    /// LIKE is case-sensitive: only names containing a lowercase 'a' match, which rules out
    /// the capitalized 'Ann' and keeps Dan, Fay and Hal.
    /// </summary>
    [Test]
    public async Task Predicate_LikeIsCaseSensitive()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM customers WHERE name LIKE '%a%' ORDER BY id");

        Assert.AreEqual(new List<long> { 4, 6, 8 }, SortedIds(rows));
    }

    /// <summary>
    /// A predicate on the second column of the composite (tier, credit) index has no usable
    /// prefix: the planner must fall back to a wider scan or filter and still find both -50
    /// credits across different tiers.
    /// </summary>
    [Test]
    public async Task Predicate_CompositeIndexSecondColumnOnly()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM customers WHERE credit = -50 ORDER BY id");

        Assert.AreEqual(new List<long> { 3, 8 }, SortedIds(rows));
    }

    /// <summary>
    /// An IN list on the leading column plus an equality on the second gives two point ranges
    /// on the composite index — and must not match Jon, whose NULL tier fails the IN.
    /// </summary>
    [Test]
    public async Task Predicate_CompositeIndexWithInListOnLeadingColumn()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM customers WHERE tier IN (1, 3) AND credit = 500 ORDER BY id");

        Assert.AreEqual(new List<long> { 1, 2, 9 }, SortedIds(rows));
    }

    /// <summary>
    /// Deeply nested AND/OR/NOT tree. The NULL total must only survive through its explicit
    /// IS NULL branch, and NOT over an UNKNOWN status would drop rows if three-valued logic
    /// were collapsed to false.
    /// </summary>
    [Test]
    public async Task Predicate_NestedAndOrNotTree()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM orders " +
            "WHERE ((year = 2020 OR year = 2021) AND (total > 100 OR total IS NULL)) " +
            "AND NOT (status = 'void') " +
            "ORDER BY id");

        Assert.AreEqual(new List<long> { 100, 102, 107, 114, 119 }, SortedIds(rows));
    }

    // ── ORDER BY / LIMIT ──────────────────────────────────────────────────────

    /// <summary>
    /// Top-N over an indexed column with heavy ties: the descending sort must keep the 700/300
    /// tie groups intact and the id tiebreak inside them must stay stable.
    /// </summary>
    [Test]
    public async Task OrderBy_DescendingWithTiesAndLimit()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id, total FROM orders WHERE total IS NOT NULL " +
            "ORDER BY total DESC, id ASC LIMIT 5");

        Assert.AreEqual(
            new List<long> { 106, 109, 110, 104, 117 },
            rows.Select(r => r.Row["id"].LongValue).ToList());
        Assert.AreEqual(
            new List<long> { 999, 700, 700, 300, 300 },
            rows.Select(r => r.Row["total"].LongValue).ToList());
    }

    /// <summary>
    /// LIMIT/OFFSET paging over a non-unique sort key: every page must be disjoint and their
    /// concatenation must reconstruct the full ordered result, including a short last page and
    /// an empty page past the end.
    /// </summary>
    [Test]
    public async Task OrderBy_LimitOffsetPagingReconstructsFullResult()
    {
        Fixture f = await SetupAsync();

        List<long> all = (await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM orders WHERE status = 'paid' ORDER BY id"))
            .Select(r => r.Row["id"].LongValue).ToList();

        Assert.AreEqual(12, all.Count);

        List<long> paged = new();
        for (int offset = 0; offset <= 15; offset += 5)
        {
            List<long> page = (await ExecQuery(f.Database, f.Executor, f.DbName,
                "SELECT id FROM orders WHERE status = 'paid' ORDER BY id LIMIT 5 OFFSET " + offset))
                .Select(r => r.Row["id"].LongValue).ToList();

            Assert.LessOrEqual(page.Count, 5, "page must respect LIMIT");
            paged.AddRange(page);
        }

        Assert.AreEqual(all, paged);

        List<QueryResultRow> pastEnd = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM orders WHERE status = 'paid' ORDER BY id LIMIT 5 OFFSET 20");
        Assert.AreEqual(0, pastEnd.Count);
    }

    // ── Subqueries ────────────────────────────────────────────────────────────

    /// <summary>
    /// Correlated EXISTS with an extra range predicate inside the subquery: the correlation
    /// must bind to the outer customer while the total filter stays inner.
    /// </summary>
    [Test]
    public async Task Subquery_CorrelatedExistsWithInnerRange()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM customers c " +
            "WHERE EXISTS (SELECT id FROM orders o WHERE o.customer_id = c.id AND o.total > 200) " +
            "ORDER BY c.id");

        // Cid (300s), Fay (700s) and Hal (250); every other customer tops out at 120 or less.
        Assert.AreEqual(new List<long> { 3, 6, 8 }, SortedIds(rows));
    }

    /// <summary>
    /// NOT EXISTS as the anti-join dual of the above: Kim is the only customer with no orders.
    /// </summary>
    [Test]
    public async Task Subquery_NotExistsFindsCustomerWithoutOrders()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM customers c " +
            "WHERE NOT EXISTS (SELECT id FROM orders o WHERE o.customer_id = c.id) " +
            "ORDER BY c.id");

        Assert.AreEqual(new List<long> { 11 }, SortedIds(rows));
    }

    /// <summary>
    /// Scalar subquery compared against an outer column: AVG ignores the NULL totals (18
    /// values, mean ~226.6) and the mixed int/float comparison must widen numerically.
    /// </summary>
    [Test]
    public async Task Subquery_ScalarAvgComparison()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM orders WHERE total > (SELECT AVG(total) FROM orders) ORDER BY id");

        Assert.AreEqual(new List<long> { 104, 106, 109, 110, 113, 117 }, SortedIds(rows));
    }

    /// <summary>
    /// Derived table aggregating per customer, joined back and filtered on the aggregate: the
    /// derived schema must expose customer_id/order_count/total_sum for binding, SUM must
    /// ignore the NULL total in Bob's group, and the two orphan orders must not form a group
    /// that survives the join.
    /// </summary>
    [Test]
    public async Task Subquery_DerivedTableAggregateJoinedBack()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT c.id, c.name, d.order_count, d.total_sum " +
            "FROM (SELECT customer_id, COUNT(*) AS order_count, SUM(total) AS total_sum " +
            "      FROM orders GROUP BY customer_id) d " +
            "JOIN customers c ON c.id = d.customer_id " +
            "WHERE d.total_sum >= 200 " +
            "ORDER BY c.id");

        Assert.AreEqual(5, rows.Count);

        Assert.AreEqual(1, rows[0].Row["id"].LongValue);
        Assert.AreEqual("Ann", rows[0].Row["name"].StrValue);
        Assert.AreEqual(2, rows[0].Row["order_count"].LongValue);
        Assert.AreEqual(200, rows[0].Row["total_sum"].LongValue);

        Assert.AreEqual(3, rows[1].Row["id"].LongValue);
        Assert.AreEqual("Cid", rows[1].Row["name"].StrValue);
        Assert.AreEqual(600, rows[1].Row["total_sum"].LongValue);

        Assert.AreEqual(6, rows[2].Row["id"].LongValue);
        Assert.AreEqual("Fay", rows[2].Row["name"].StrValue);
        Assert.AreEqual(1400, rows[2].Row["total_sum"].LongValue);

        Assert.AreEqual(8, rows[3].Row["id"].LongValue);
        Assert.AreEqual("Hal", rows[3].Row["name"].StrValue);
        Assert.AreEqual(250, rows[3].Row["total_sum"].LongValue);

        Assert.AreEqual(9, rows[4].Row["id"].LongValue);
        Assert.AreEqual("Ivy", rows[4].Row["name"].StrValue);
        Assert.AreEqual(240, rows[4].Row["total_sum"].LongValue);
    }

    /// <summary>
    /// IN over a subquery that itself contains a correlated EXISTS — two nested subquery
    /// forms in one statement, plus a NULL order_id item that must not leak into the IN set.
    /// </summary>
    [Test]
    public async Task Subquery_InOverSubqueryWithCorrelatedExists()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT c.id FROM customers c WHERE c.id IN (" +
            "  SELECT o.customer_id FROM orders o " +
            "  WHERE EXISTS (SELECT id FROM order_items i WHERE i.order_id = o.id AND i.qty >= 7)) " +
            "ORDER BY c.id");

        // Items with qty >= 7 hang off orders 109, 110 (Fay) and 117 (Cid); the qty-9 orphan
        // item's NULL order_id can never match.
        Assert.AreEqual(new List<long> { 3, 6 }, SortedIds(rows));
    }

    /// <summary>
    /// The correlated EXISTS reaches back into a nullable outer column inside arithmetic: the
    /// threshold is tier * 100 per outer row. Eve's tier is NULL, so her threshold is NULL and
    /// every comparison against it is UNKNOWN — her orders (120 and 15) must not qualify her, and
    /// the NULL must not raise an error either.
    /// </summary>
    [Test]
    public async Task Subquery_CorrelatedExistsWithOuterArithmetic()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT c.id FROM customers c WHERE EXISTS (" +
            "  SELECT id FROM orders o WHERE o.customer_id = c.id AND o.total > c.tier * 100) " +
            "ORDER BY c.id");

        // Thresholds: tier 1 -> 100 (Ann 120, Bob 120, Fay 700, Ivy 120 clear it), tier 2 -> 200
        // (Cid 300 clears it; Dan 45 and Jon 60 do not; Kim has no orders), tier 3 -> 300 (Gus -30
        // and Hal 250 do not). Eve's NULL tier makes her threshold NULL.
        Assert.AreEqual(new List<long> { 1, 2, 3, 6, 9 }, SortedIds(rows));
    }

    /// <summary>
    /// Pattern predicates over the nullable, indexed city column: Dan's NULL city is UNKNOWN for
    /// LIKE, NOT LIKE and ILIKE alike, so it never appears and never raises a type error. LIKE is
    /// case-sensitive ('lima' only), ILIKE is not ('Lima' and 'lima'), and the empty string is a
    /// real value that does not start with 'O'.
    /// </summary>
    [Test]
    public async Task Predicate_PatternMatchOverNullableIndexedColumn()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> notOslo = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM customers WHERE city NOT LIKE 'O%'");
        Assert.AreEqual(new List<long> { 3, 5, 7, 8, 9, 11 }, SortedIds(notOslo));

        List<QueryResultRow> sensitive = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM customers WHERE city LIKE 'l%'");
        Assert.AreEqual(new List<long> { 7 }, SortedIds(sensitive));

        List<QueryResultRow> insensitive = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT id FROM customers WHERE city ILIKE 'l%'");
        Assert.AreEqual(new List<long> { 5, 7 }, SortedIds(insensitive));
    }

    // ── Aggregation ───────────────────────────────────────────────────────────

    /// <summary>
    /// GROUP BY + HAVING over two aggregate expressions: the void group's NULL SUM must fail
    /// the HAVING comparison (UNKNOWN), and the open group's SUM must skip its NULL member.
    /// </summary>
    [Test]
    public async Task Aggregate_GroupByHavingOnAggregates()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT status, COUNT(*) AS cnt, SUM(total) AS total_sum " +
            "FROM orders GROUP BY status " +
            "HAVING SUM(total) > 100 AND COUNT(*) >= 2 " +
            "ORDER BY status");

        Assert.AreEqual(2, rows.Count);

        Assert.AreEqual("open", rows[0].Row["status"].StrValue);
        Assert.AreEqual(7, rows[0].Row["cnt"].LongValue);
        Assert.AreEqual(470, rows[0].Row["total_sum"].LongValue);

        Assert.AreEqual("paid", rows[1].Row["status"].StrValue);
        Assert.AreEqual(12, rows[1].Row["cnt"].LongValue);
        Assert.AreEqual(3609, rows[1].Row["total_sum"].LongValue);
    }

    /// <summary>
    /// COUNT(*) counts rows, COUNT(col) counts non-NULL values: the 2022 bucket holds five
    /// orders, one of which (the void) has a NULL total. Both counters and their difference run
    /// in one statement, over one scan of the same predicate.
    /// </summary>
    [Test]
    public async Task Aggregate_CountStarVersusCountColumn()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT COUNT(*) AS cnt, COUNT(total) AS non_null, COUNT(*) - COUNT(total) AS nulls " +
            "FROM orders WHERE year = 2022");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(5, rows[0].Row["cnt"].LongValue);
        Assert.AreEqual(4, rows[0].Row["non_null"].LongValue);
        Assert.AreEqual(1, rows[0].Row["nulls"].LongValue);
    }

    /// <summary>
    /// Aggregates with no GROUP BY over an empty input still produce exactly one row: COUNT
    /// is 0 and SUM/MIN/MAX are NULL rather than the statement returning no rows.
    /// </summary>
    [Test]
    public async Task Aggregate_EmptyInputWithoutGroupByYieldsNullRow()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT COUNT(*) AS cnt, SUM(total) AS total_sum, MAX(total) AS max_total, MIN(total) AS min_total " +
            "FROM orders WHERE year = 1999");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(0, rows[0].Row["cnt"].LongValue);
        Assert.AreEqual(ColumnType.Null, rows[0].Row["total_sum"].Type);
        Assert.AreEqual(ColumnType.Null, rows[0].Row["max_total"].Type);
        Assert.AreEqual(ColumnType.Null, rows[0].Row["min_total"].Type);
    }

    /// <summary>
    /// The NULL rule for BETWEEN must hold where HAVING evaluates it, not only in WHERE: a
    /// group whose SUM is NULL (the void status) makes NOT (SUM BETWEEN …) UNKNOWN, so the
    /// group drops instead of surviving the negation.
    /// </summary>
    [Test]
    public async Task Aggregate_HavingNotBetweenOverNullAggregateDropsGroup()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT status FROM orders GROUP BY status " +
            "HAVING NOT (SUM(total) BETWEEN 0 AND 500) " +
            "ORDER BY status");

        // paid sums to 3609 (outside the window, kept); open sums to 470 (inside, dropped);
        // void sums to NULL, and NOT over the resulting UNKNOWN must drop it too.
        Assert.AreEqual(
            new List<string> { "paid" },
            rows.Select(r => r.Row["status"].StrValue).ToList());
    }

    /// <summary>
    /// DISTINCT over a join that multiplies rows: every customer's city appears once per order
    /// before deduplication, the empty string sorts before all letters, and 'lima'/'Lima' are
    /// distinct values under ordinal ordering.
    /// </summary>
    [Test]
    public async Task Aggregate_DistinctOverJoinDuplicates()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT DISTINCT c.city FROM customers c JOIN orders o ON o.customer_id = c.id " +
            "WHERE c.city IS NOT NULL " +
            "ORDER BY c.city");

        Assert.AreEqual(
            new List<string> { "", "Bergen", "Lima", "Oslo", "lima" },
            rows.Select(r => r.Row["city"].StrValue).ToList());
    }

    // ── Projection pushdown ───────────────────────────────────────────────────

    /// <summary>
    /// The sort key (tier) is not in the projection: projection pushdown must still fetch it
    /// for the sorter while the output carries only the name.
    /// </summary>
    [Test]
    public async Task Projection_OrderByColumnOutsideProjection()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT name FROM customers WHERE tier IS NOT NULL ORDER BY tier DESC, name ASC");

        Assert.AreEqual(
            new List<string> { "Gus", "Hal", "Cid", "Dan", "Jon", "Kim", "Ann", "Bob", "Fay", "Ivy" },
            rows.Select(r => r.Row["name"].StrValue).ToList());
    }

    /// <summary>
    /// Arithmetic over columns of two joined tables in the projection: the needed columns of
    /// both tables must survive pushdown, and the expression evaluates per joined row.
    /// </summary>
    [Test]
    public async Task Projection_ArithmeticOverJoinedColumns()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT o.id, i.qty * 2 + 1 AS doubled FROM orders o " +
            "JOIN order_items i ON i.order_id = o.id " +
            "WHERE o.total >= 700 " +
            "ORDER BY o.id");

        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual(109, rows[0].Row["id"].LongValue);
        Assert.AreEqual(21, rows[0].Row["doubled"].LongValue);
        Assert.AreEqual(110, rows[1].Row["id"].LongValue);
        Assert.AreEqual(21, rows[1].Row["doubled"].LongValue);
    }

    // ── Name resolution ───────────────────────────────────────────────────────

    /// <summary>
    /// Both sides of the join expose a "name" column: the unqualified reference is ambiguous
    /// and the binder must reject the statement instead of silently picking one side.
    /// </summary>
    [Test]
    public async Task Binder_AmbiguousUnqualifiedColumnIsRejected()
    {
        Fixture f = await SetupAsync();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            await ExecQuery(f.Database, f.Executor, f.DbName,
                "SELECT name FROM regions r JOIN customers c ON c.region_id = r.id");
        });

        Console.WriteLine("Ambiguous column rejected with: " + ex.Message);
    }

    /// <summary>
    /// Table aliases and their qualified references are case-insensitive identifiers: C.TIER
    /// and c.tier must resolve to the same column, and a mixed-case ORDER BY on a column the
    /// projection only exposes through an alias must still bind.
    /// </summary>
    [Test]
    public async Task Binder_AliasAndColumnCaseInsensitive()
    {
        Fixture f = await SetupAsync();

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT C.ID AS cid FROM customers C WHERE C.TIER = 1 ORDER BY C.ID");

        Assert.AreEqual(new List<long> { 1, 2, 6, 9 }, SortedIds(rows, "cid"));
    }

    // ── Views ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Querying through a view joined to a base table: the view's WHERE (total > 100, NULL
    /// total dropped) must compose with the outer join and tier filter, and the ORDER BY on a
    /// view column plus its id must break the 120 ties deterministically.
    /// </summary>
    [Test]
    public async Task View_JoinThroughViewWithOuterFilter()
    {
        Fixture f = await SetupAsync();

        await ExecDdl(f.Database, f.Executor, f.DbName,
            "CREATE VIEW big_orders AS SELECT id, customer_id, total FROM orders WHERE total > 100");

        List<QueryResultRow> rows = await ExecQuery(f.Database, f.Executor, f.DbName,
            "SELECT c.name, v.total FROM big_orders v " +
            "JOIN customers c ON c.id = v.customer_id " +
            "WHERE c.tier = 1 " +
            "ORDER BY v.total DESC, v.id ASC");

        Assert.AreEqual(
            new List<(string Name, long Total)>
            {
                ("Fay", 700), ("Fay", 700),
                ("Ann", 120), ("Bob", 120), ("Ivy", 120), ("Ivy", 120)
            },
            rows.Select(r => (r.Row["name"].StrValue!, r.Row["total"].LongValue)).ToList());
    }
}
