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
/// An in-memory oracle for the left outer join, run against every operator over generated data.
/// The oracle, not an operator, is the reference: each operator must produce the oracle's multiset
/// of (left id, right id or NULL) pairs, with and without a residual <c>ON</c> conjunct, and for a
/// mixed inner/outer/inner chain whose <c>WHERE</c> touches the preserved side, the null-extended
/// side, and a table joined above the outer join.
/// </summary>
[NonParallelizable]
public sealed class TestLeftOuterJoinParityOracle : SharedNodeBaseTest
{
    public enum Mode { NestedLoop, IndexNestedLoop, Hash, Merge }

    private sealed record Fixture(string DbName, DatabaseDescriptor Database, CommandExecutor Executor);

    private sealed record LeftRow(long Id, long? K, long V);
    private sealed record RightRow(long Id, long? K, long W);

    private static readonly LeftRow[] Lefts = GenerateLefts();
    private static readonly RightRow[] Rights = GenerateRights();

    private static LeftRow[] GenerateLefts()
    {
        Random rng = new(1234);
        LeftRow[] rows = new LeftRow[40];
        for (int i = 0; i < rows.Length; i++)
        {
            long? k = i % 7 == 6 ? null : rng.Next(0, 9);
            rows[i] = new LeftRow(i + 1, k, rng.Next(0, 5));
        }
        return rows;
    }

    private static RightRow[] GenerateRights()
    {
        Random rng = new(4321);
        RightRow[] rows = new RightRow[55];
        for (int i = 0; i < rows.Length; i++)
        {
            long? k = i % 11 == 10 ? null : rng.Next(0, 7);
            rows[i] = new RightRow(100 + i, k, rng.Next(0, 10));
        }
        return rows;
    }

    private async Task<Fixture> SetupAsync(Mode mode)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "lefts",
            columns: [new("id", ColumnType.Integer64), new("k", ColumnType.Integer64), new("v", ColumnType.Integer64, notNull: true)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "rights",
            columns: [new("id", ColumnType.Integer64), new("k", ColumnType.Integer64), new("w", ColumnType.Integer64, notNull: true)],
            constraints: mode == Mode.IndexNestedLoop
                ? [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]), new(ConstraintType.IndexMulti, "rights_k_idx", [new("k", OrderType.Ascending)])]
                : [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.Insert(new InsertTicket(txn, dbname, "lefts",
            values: Lefts.Select(l => new Dictionary<string, ColumnValue>
            {
                { "id", new(ColumnType.Integer64, l.Id) },
                { "k", l.K is { } k ? new(ColumnType.Integer64, k) : ColumnValue.Null },
                { "v", new(ColumnType.Integer64, l.V) },
            }).ToList()));

        await executor.Insert(new InsertTicket(txn, dbname, "rights",
            values: Rights.Select(r => new Dictionary<string, ColumnValue>
            {
                { "id", new(ColumnType.Integer64, r.Id) },
                { "k", r.K is { } k ? new(ColumnType.Integer64, k) : ColumnValue.Null },
                { "w", new(ColumnType.Integer64, r.W) },
            }).ToList()));

        await database.Transactions.CommitAsync(txn);

        switch (mode)
        {
            case Mode.NestedLoop: executor.Statistics.ForceNestedLoopForTesting = true; break;
            case Mode.IndexNestedLoop: executor.Statistics.ForceIndexNestedLoopForTesting = true; break;
            case Mode.Hash: executor.Statistics.ForceHashJoinForTesting = true; break;
            case Mode.Merge: executor.Statistics.ForceMergeJoinForTesting = true; break;
        }

        return new Fixture(dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> Run(Fixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: f.DbName, sql: sql, parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await f.Database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static List<(long, long?)> Oracle(Func<RightRow, bool>? residual)
    {
        List<(long, long?)> expected = new();

        foreach (LeftRow l in Lefts)
        {
            bool matched = false;

            if (l.K is { } k)
            {
                foreach (RightRow r in Rights)
                {
                    if (r.K == k && (residual is null || residual(r)))
                    {
                        matched = true;
                        expected.Add((l.Id, r.Id));
                    }
                }
            }

            if (!matched)
                expected.Add((l.Id, null));
        }

        return expected.OrderBy(p => p.Item1).ThenBy(p => p.Item2 ?? -1).ToList();
    }

    private static List<(long, long?)> Shape(List<QueryResultRow> rows) =>
        rows.Select(r => (
            r.Row["lid"].LongValue,
            r.Row["rid"].Type == ColumnType.Null ? (long?)null : r.Row["rid"].LongValue))
        .OrderBy(p => p.Item1).ThenBy(p => p.Item2 ?? -1).ToList();

    [Test]
    [TestCase(Mode.NestedLoop)]
    [TestCase(Mode.IndexNestedLoop)]
    [TestCase(Mode.Hash)]
    [TestCase(Mode.Merge)]
    public async Task EquiJoinOnly_MatchesTheOracle(Mode mode)
    {
        Fixture f = await SetupAsync(mode);

        List<QueryResultRow> rows = await Run(f,
            "SELECT l.id AS lid, r.id AS rid FROM lefts l LEFT JOIN rights r ON r.k = l.k");

        Assert.AreEqual(Oracle(null), Shape(rows), $"{mode}: rows must equal the oracle's multiset");
    }

    [Test]
    [TestCase(Mode.NestedLoop)]
    [TestCase(Mode.IndexNestedLoop)]
    [TestCase(Mode.Hash)]
    [TestCase(Mode.Merge)]
    public async Task EquiJoinWithResidualOnConjunct_MatchesTheOracle(Mode mode)
    {
        Fixture f = await SetupAsync(mode);

        List<QueryResultRow> rows = await Run(f,
            "SELECT l.id AS lid, r.id AS rid FROM lefts l LEFT JOIN rights r ON r.k = l.k AND r.w >= 5");

        Assert.AreEqual(Oracle(r => r.W >= 5), Shape(rows), $"{mode}: a residual ON conjunct decides matching, not filtering");
    }

    [Test]
    [TestCase(Mode.NestedLoop)]
    [TestCase(Mode.IndexNestedLoop)]
    [TestCase(Mode.Hash)]
    [TestCase(Mode.Merge)]
    public async Task WhereOnTheNullExtendedSide_FiltersAfterPadding(Mode mode)
    {
        Fixture f = await SetupAsync(mode);

        // ON decides matching, WHERE filters the padded output: the unmatched left rows survive
        // an IS NULL test and vanish under an equality test.
        List<QueryResultRow> isNull = await Run(f,
            "SELECT l.id AS lid, r.id AS rid FROM lefts l LEFT JOIN rights r ON r.k = l.k WHERE r.w IS NULL");
        List<(long, long?)> expectedNull = Oracle(null).Where(p => p.Item2 is null).ToList();
        Assert.AreEqual(expectedNull, Shape(isNull), $"{mode}: IS NULL keeps exactly the padded rows (w is NOT NULL on real rows)");

        List<QueryResultRow> equal = await Run(f,
            "SELECT l.id AS lid, r.id AS rid FROM lefts l LEFT JOIN rights r ON r.k = l.k WHERE r.w = 3");
        List<(long, long?)> expectedEqual = Oracle(null)
            .Where(p => p.Item2 is { } rid && Rights.First(r => r.Id == rid).W == 3).ToList();
        Assert.AreEqual(expectedEqual, Shape(equal), $"{mode}: an equality on the null-extended side removes every padded row");
    }

    // ── mixed chain: a JOIN b LEFT JOIN c JOIN d ───────────────────────────────

    private sealed record ChainFixture(string DbName, DatabaseDescriptor Database, CommandExecutor Executor);

    private static readonly (long Id, long X)[] As = Enumerable.Range(1, 12).Select(i => ((long)i, (long)(i % 3))).ToArray();
    private static readonly (long Id, long AId)[] Bs = Enumerable.Range(1, 20).Select(i => ((long)i, (long)((i * 5) % 13) + 1)).ToArray();
    private static readonly (long Id, long BId, long Y)[] Cs = Enumerable.Range(1, 15).Select(i => ((long)i, (long)((i * 7) % 23) + 1, (long)(i % 4))).ToArray();
    private static readonly (long Id, long AId, long Z)[] Ds = Enumerable.Range(1, 18).Select(i => ((long)i, (long)((i * 3) % 14) + 1, (long)(i % 5))).ToArray();

    private async Task<ChainFixture> SetupChainAsync(Mode mode)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        async Task Table(string name, params (string name, ColumnType type)[] cols)
        {
            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname, tableName: name,
                columns: cols.Select(c => new ColumnInfo(c.name, c.type)).ToArray(),
                constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
                ifNotExists: false));
        }

        await Table("ta", ("id", ColumnType.Integer64), ("x", ColumnType.Integer64));
        await Table("tb", ("id", ColumnType.Integer64), ("a_id", ColumnType.Integer64));
        await Table("tc", ("id", ColumnType.Integer64), ("b_id", ColumnType.Integer64), ("y", ColumnType.Integer64));
        await Table("td", ("id", ColumnType.Integer64), ("a_id", ColumnType.Integer64), ("z", ColumnType.Integer64));

        KvTransaction txn = await database.Transactions.BeginAsync();

        static ColumnValue I(long v) => new(ColumnType.Integer64, v);

        await executor.Insert(new InsertTicket(txn, dbname, "ta",
            values: As.Select(a => new Dictionary<string, ColumnValue> { { "id", I(a.Id) }, { "x", I(a.X) } }).ToList()));
        await executor.Insert(new InsertTicket(txn, dbname, "tb",
            values: Bs.Select(b => new Dictionary<string, ColumnValue> { { "id", I(b.Id) }, { "a_id", I(b.AId) } }).ToList()));
        await executor.Insert(new InsertTicket(txn, dbname, "tc",
            values: Cs.Select(c => new Dictionary<string, ColumnValue> { { "id", I(c.Id) }, { "b_id", I(c.BId) }, { "y", I(c.Y) } }).ToList()));
        await executor.Insert(new InsertTicket(txn, dbname, "td",
            values: Ds.Select(d => new Dictionary<string, ColumnValue> { { "id", I(d.Id) }, { "a_id", I(d.AId) }, { "z", I(d.Z) } }).ToList()));

        await database.Transactions.CommitAsync(txn);

        switch (mode)
        {
            case Mode.NestedLoop: executor.Statistics.ForceNestedLoopForTesting = true; break;
            case Mode.Hash: executor.Statistics.ForceHashJoinForTesting = true; break;
            case Mode.Merge: executor.Statistics.ForceMergeJoinForTesting = true; break;
        }

        return new ChainFixture(dbname, database, executor);
    }

    /// <summary>(a.id, b.id, c.id or null, d.id) for a JOIN b LEFT JOIN c JOIN d, before any WHERE.</summary>
    private static IEnumerable<(long A, long B, long? C, long D)> ChainOracle()
    {
        foreach ((long aId, long _) in As)
        {
            foreach ((long bId, long bAId) in Bs)
            {
                if (bAId != aId) continue;

                List<long?> cIds = Cs.Where(c => c.BId == bId).Select(c => (long?)c.Id).ToList();
                if (cIds.Count == 0) cIds.Add(null);

                foreach (long? cId in cIds)
                {
                    foreach ((long dId, long dAId, long _) in Ds)
                    {
                        if (dAId == aId)
                            yield return (aId, bId, cId, dId);
                    }
                }
            }
        }
    }

    private static List<(long, long, long?, long)> Sorted(IEnumerable<(long A, long B, long? C, long D)> rows) =>
        rows.OrderBy(r => r.A).ThenBy(r => r.B).ThenBy(r => r.C ?? -1).ThenBy(r => r.D).ToList();

    private static List<(long, long, long?, long)> ShapeChain(List<QueryResultRow> rows) =>
        Sorted(rows.Select(r => (
            r.Row["aid"].LongValue,
            r.Row["bid"].LongValue,
            r.Row["cid"].Type == ColumnType.Null ? (long?)null : r.Row["cid"].LongValue,
            r.Row["did"].LongValue)));

    private const string ChainSql =
        "SELECT a.id AS aid, b.id AS bid, c.id AS cid, d.id AS did FROM ta a " +
        "JOIN tb b ON b.a_id = a.id LEFT JOIN tc c ON c.b_id = b.id JOIN td d ON d.a_id = a.id";

    [Test]
    [TestCase(Mode.NestedLoop)]
    [TestCase(Mode.Hash)]
    [TestCase(Mode.Merge)]
    public async Task MixedChain_MatchesTheOracle_WithPushedAndPostJoinConjuncts(Mode mode)
    {
        ChainFixture f = await SetupChainAsync(mode);

        static async Task<List<QueryResultRow>> Run(ChainFixture f, string sql)
        {
            KvTransaction txn = await f.Database.Transactions.BeginAsync();
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: txn, database: f.DbName, sql: sql, parameters: null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await f.Database.Transactions.CommitAsync(txn);
            return rows;
        }

        List<QueryResultRow> twoTable = await Run(f, "SELECT b.id AS bid, c.id AS cid FROM tb b LEFT JOIN tc c ON c.b_id = b.id");
        List<(long, long?)> twoExpected = Bs.SelectMany(b =>
        {
            List<long?> cs = Cs.Where(c => c.BId == b.Id).Select(c => (long?)c.Id).ToList();
            if (cs.Count == 0) cs.Add(null);
            return cs.Select(c => (b.Id, c));
        }).OrderBy(p => p.Item1).ThenBy(p => p.Item2 ?? -1).ToList();
        List<(long, long?)> twoActual = twoTable.Select(r => (r.Row["bid"].LongValue, r.Row["cid"].Type == ColumnType.Null ? (long?)null : r.Row["cid"].LongValue))
            .OrderBy(p => p.Item1).ThenBy(p => p.Item2 ?? -1).ToList();
        Assert.AreEqual(twoExpected, twoActual, $"{mode}: two-table b LEFT JOIN c; actual = {string.Join(" ", twoActual)}");

        List<(long, long, long?, long)> plain = ShapeChain(await Run(f, ChainSql));
        Assert.AreEqual(Sorted(ChainOracle()), plain, $"{mode}: plain chain; actual = {string.Join(" ", plain)}");

        // The same chain with an inner join in the middle keeps only the matched rows. A merge join
        // over an interior left input must still sort it by the new key (b.id), even though the
        // upstream ordering is on a column with the same bare name (a.id).
        List<(long, long, long?, long)> innerChain = ShapeChain(await Run(f, ChainSql.Replace("LEFT JOIN", "JOIN")));
        Assert.AreEqual(Sorted(ChainOracle().Where(r => r.C is not null)), innerChain, $"{mode}: inner chain");

        // a.x and d.z are pushed to their scans; c.y runs after the join.
        List<(long, long, long?, long)> expectedFiltered = Sorted(ChainOracle().Where(r =>
            As.First(a => a.Id == r.A).X == 1
            && r.C is { } cId && Cs.First(c => c.Id == cId).Y == 2
            && Ds.First(d => d.Id == r.D).Z == 3));
        Assert.AreEqual(expectedFiltered, ShapeChain(await Run(f, ChainSql + " WHERE a.x = 1 AND c.y = 2 AND d.z = 3")),
            $"{mode}: pushed and post-join conjuncts together");

        // A conjunct that keeps only the padded rows of the null-extended side.
        List<(long, long, long?, long)> expectedPadded = Sorted(ChainOracle().Where(r => r.C is null && As.First(a => a.Id == r.A).X == 1));
        Assert.AreEqual(expectedPadded, ShapeChain(await Run(f, ChainSql + " WHERE a.x = 1 AND c.id IS NULL")),
            $"{mode}: IS NULL on the null-extended side keeps the padded rows");
    }

    // ── composite key with a NULL component, followed by a join on the key prefix ──────────────

    private static readonly (long Id, long X, long? Y)[] Ps = Enumerable.Range(1, 36)
        .Select(i => ((long)i, (long)(i % 4), i % 5 == 4 ? (long?)null : i % 3)).ToArray();
    private static readonly (long Id, long X, long Y)[] Qs = Enumerable.Range(1, 20)
        .Select(i => ((long)i, (long)(i % 4), (long)(i % 3))).ToArray();
    private static readonly (long Id, long X)[] Rs = Enumerable.Range(1, 10)
        .Select(i => ((long)i, (long)(i % 6))).ToArray();

    /// <summary>(p.id, q.id or null, r.id or null) for p LEFT JOIN q ON (x, y) LEFT JOIN r ON x; a NULL p.y pairs with nothing.</summary>
    private static List<(long, long?, long?)> CompositeOracle()
    {
        List<(long, long?, long?)> rows = new();

        foreach ((long pId, long pX, long? pY) in Ps)
        {
            List<long?> qIds = pY is null ? [] : Qs.Where(q => q.X == pX && q.Y == pY).Select(q => (long?)q.Id).ToList();
            if (qIds.Count == 0) qIds.Add(null);

            List<long?> rIds = Rs.Where(r => r.X == pX).Select(r => (long?)r.Id).ToList();
            if (rIds.Count == 0) rIds.Add(null);

            foreach (long? qId in qIds)
                foreach (long? rId in rIds)
                    rows.Add((pId, qId, rId));
        }

        return rows.OrderBy(r => r.Item1).ThenBy(r => r.Item2 ?? -1).ThenBy(r => r.Item3 ?? -1).ToList();
    }

    [Test]
    [TestCase(Mode.NestedLoop)]
    [TestCase(Mode.Hash)]
    [TestCase(Mode.Merge)]
    public async Task CompositeKeyWithNullComponent_ThenJoinOnThePrefix_MatchesTheOracle(Mode mode)
    {
        // The first join's key is (p.x, p.y) and p.y is NULL on some rows. A merge join advertises
        // its output as ordered by that key, and the second join, keyed on p.x, trusts the
        // ordering and skips its sort. A NULL-keyed padded row must therefore stay in key order:
        // (2, NULL) sorts after (1, 1), not before every keyed row. Hoisting the padded rows to
        // the front made the second merge walk p.x = 2, 1, ... and miss the matches for x = 1.
        // Both right-side shapes are covered: a derived table (materialized merge path) and a
        // base table (streaming merge path).
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        async Task Table(string name, params (string name, ColumnType type)[] cols)
        {
            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname, tableName: name,
                columns: cols.Select(c => new ColumnInfo(c.name, c.type)).ToArray(),
                constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
                ifNotExists: false));
        }

        await Table("tp", ("id", ColumnType.Integer64), ("x", ColumnType.Integer64), ("y", ColumnType.Integer64));
        await Table("tq", ("id", ColumnType.Integer64), ("x", ColumnType.Integer64), ("y", ColumnType.Integer64));
        await Table("tr", ("id", ColumnType.Integer64), ("x", ColumnType.Integer64));

        static ColumnValue I(long v) => new(ColumnType.Integer64, v);
        static ColumnValue N(long? v) => v is { } l ? I(l) : ColumnValue.Null;

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(txn, dbname, "tp",
            values: Ps.Select(p => new Dictionary<string, ColumnValue> { { "id", I(p.Id) }, { "x", I(p.X) }, { "y", N(p.Y) } }).ToList()));
        await executor.Insert(new InsertTicket(txn, dbname, "tq",
            values: Qs.Select(q => new Dictionary<string, ColumnValue> { { "id", I(q.Id) }, { "x", I(q.X) }, { "y", I(q.Y) } }).ToList()));
        await executor.Insert(new InsertTicket(txn, dbname, "tr",
            values: Rs.Select(r => new Dictionary<string, ColumnValue> { { "id", I(r.Id) }, { "x", I(r.X) } }).ToList()));
        await database.Transactions.CommitAsync(txn);

        switch (mode)
        {
            case Mode.NestedLoop: executor.Statistics.ForceNestedLoopForTesting = true; break;
            case Mode.Hash: executor.Statistics.ForceHashJoinForTesting = true; break;
            case Mode.Merge: executor.Statistics.ForceMergeJoinForTesting = true; break;
        }

        async Task<List<(long, long?, long?)>> Shape(string sql)
        {
            KvTransaction t = await database.Transactions.BeginAsync();
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: t, database: dbname, sql: sql, parameters: null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(t);

            static long? Opt(ColumnValue v) => v.Type == ColumnType.Null ? null : v.LongValue;
            return rows.Select(r => (r.Row["pid"].LongValue, Opt(r.Row["qid"]), Opt(r.Row["rid"])))
                .OrderBy(r => r.Item1).ThenBy(r => r.Item2 ?? -1).ThenBy(r => r.Item3 ?? -1).ToList();
        }

        List<(long, long?, long?)> expected = CompositeOracle();
        Assert.IsTrue(expected.Any(r => r.Item2 is null && r.Item3 is not null), "the data must hold a padded row that still matches the third table");

        List<(long, long?, long?)> derived = await Shape(
            "SELECT p.id AS pid, q.id AS qid, r.id AS rid FROM tp p " +
            "LEFT JOIN (SELECT id, x, y FROM tq) q ON p.x = q.x AND p.y = q.y LEFT JOIN tr r ON p.x = r.x");
        Assert.AreEqual(expected, derived, $"{mode}: derived right side; actual = {string.Join(" ", derived)}");

        List<(long, long?, long?)> baseTable = await Shape(
            "SELECT p.id AS pid, q.id AS qid, r.id AS rid FROM tp p " +
            "LEFT JOIN tq q ON p.x = q.x AND p.y = q.y LEFT JOIN tr r ON p.x = r.x");
        Assert.AreEqual(expected, baseTable, $"{mode}: base-table right side; actual = {string.Join(" ", baseTable)}");
    }
}
