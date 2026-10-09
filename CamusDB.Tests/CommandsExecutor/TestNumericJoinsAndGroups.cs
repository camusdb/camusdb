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
/// NUMERIC keys in the operators that hash or order values: joins under every strategy, GROUP BY,
/// DISTINCT and foreign keys. Every join runs under the nested-loop, index nested-loop, hash and merge
/// strategies and must give the oracle's pairs, which follow the evaluator's comparison rule: NUMERIC
/// equals NUMERIC by value, and NUMERIC equals INT64 exactly. A strategy that hashes or orders a mixed
/// pair by type would drop matches that the nested loop finds.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestNumericJoinsAndGroups : BaseTest
{
    public enum Mode { NestedLoop, IndexNestedLoop, Hash, Merge }

    private sealed record Row(long Id, string? P, long? N);

    /// <summary>
    /// Left rows. Values sit past 2⁶⁴ unscaled (both halves set), differ only in the high half, cross
    /// zero, and include an INT64 that a double cannot hold (2⁵³ + 1).
    /// </summary>
    private static readonly Row[] Lefts =
    [
        new(1, "1.5", 2),
        new(2, "2", -3),
        new(3, "2.000000001", 9007199254740993),
        new(4, "-3", null),
        new(5, null, 7),
        new(6, "12345678901234567890.123456789", 2),
        new(7, "18446744073.709551616", 0),
        new(8, "9007199254740993", 1),
    ];

    private static readonly Row[] Rights =
    [
        new(101, "1.50", 1),
        new(102, "2", 2),
        new(103, "2", 9007199254740992),
        new(104, "-3", -3),
        new(105, null, null),
        new(106, "12345678901234567890.123456789", 7),
        new(107, "0.709551616", 9007199254740993),
        new(108, "9007199254740992", 2),
        new(109, "0", 0),
    ];

    private static Int128? Unscaled(string? text) => text is null ? null : ColumnValue.FromNumericString(text).NumericUnscaled;

    private static Int128? Scaled(long? n) => n is { } v ? NumericMath.FromInt64(v) : null;

    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync(Mode mode, CamusDBOptions? options = null)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = options is null
            ? await CreateDatabase()
            : await CreateDatabase(options);

        await executor.CreateTable(new CreateTableTicket(
            databaseName: db, tableName: "lefts",
            columns: [new("id", ColumnType.Integer64), new("p", ColumnType.Numeric), new("n", ColumnType.Integer64)],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: db, tableName: "rights",
            columns: [new("id", ColumnType.Integer64), new("p", ColumnType.Numeric), new("n", ColumnType.Integer64)],
            constraints: mode == Mode.IndexNestedLoop
                ?
                [
                    new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)]),
                    new(ConstraintType.IndexMulti, "rights_p_idx", [new("p", OrderType.Ascending)]),
                    new(ConstraintType.IndexMulti, "rights_n_idx", [new("n", OrderType.Ascending)]),
                ]
                : [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(tx, db, "lefts", values: Lefts.Select(ToValues).ToList()));
        await executor.Insert(new InsertTicket(tx, db, "rights", values: Rights.Select(ToValues).ToList()));
        await database.Transactions.CommitAsync(tx);

        Force(executor, mode);

        return (db, database, executor);
    }

    private static Dictionary<string, ColumnValue> ToValues(Row row) => new()
    {
        { "id", new(ColumnType.Integer64, row.Id) },
        { "p", row.P is null ? ColumnValue.Null : ColumnValue.FromNumericString(row.P) },
        { "n", row.N is { } n ? new(ColumnType.Integer64, n) : ColumnValue.Null },
    };

    private static async Task<List<QueryResultRow>> Run(DatabaseDescriptor database, CommandExecutor executor, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));
            return await cursor.ToListAsync();
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static List<(long, long)> Pairs(List<QueryResultRow> rows) => rows
        .Select(r => (r.Row["lid"].LongValue, r.Row["rid"].LongValue))
        .OrderBy(p => p.Item1).ThenBy(p => p.Item2)
        .ToList();

    private static List<(long, long)> Oracle(Func<Row, Row, bool> match) =>
        (from l in Lefts from r in Rights where match(l, r) select (l.Id, r.Id))
        .OrderBy(p => p.Item1).ThenBy(p => p.Item2)
        .ToList();

    [Test]
    public async Task Join_NumericToNumeric_EveryStrategyMatchesTheOracle([Values] Mode mode)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(mode);

        List<(long, long)> expected = Oracle((l, r) => Unscaled(l.P) is { } a && Unscaled(r.P) is { } b && a == b);
        Assert.IsNotEmpty(expected);

        List<QueryResultRow> rows = await Run(database, executor, db,
            "SELECT l.id AS lid, r.id AS rid FROM lefts l JOIN rights r ON l.p = r.p");

        CollectionAssert.AreEqual(expected, Pairs(rows), $"{mode}");
    }

    [Test]
    public async Task Join_Int64ToNumeric_IsExact_UnderEveryStrategy([Values] Mode mode, [Values] bool numericOnTheRight)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(mode);

        // INT64 = NUMERIC compares exactly: 9007199254740993 matches only 9007199254740993, never the
        // neighbouring value a double would round it to.
        List<(long, long)> expected = numericOnTheRight
            ? Oracle((l, r) => Scaled(l.N) is { } a && Unscaled(r.P) is { } b && a == b)
            : Oracle((l, r) => Unscaled(l.P) is { } a && Scaled(r.N) is { } b && a == b);
        Assert.IsNotEmpty(expected);

        string on = numericOnTheRight ? "l.n = r.p" : "l.p = r.n";
        List<QueryResultRow> rows = await Run(database, executor, db,
            $"SELECT l.id AS lid, r.id AS rid FROM lefts l JOIN rights r ON {on}");

        CollectionAssert.AreEqual(expected, Pairs(rows), $"{mode}, ON {on}");
    }

    [Test]
    public async Task LeftJoin_Int64ToNumeric_PadsOnlyTheUnmatchedRows([Values] Mode mode)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(mode);

        List<QueryResultRow> rows = await Run(database, executor, db,
            "SELECT l.id AS lid, r.id AS rid FROM lefts l LEFT JOIN rights r ON l.n = r.p");

        List<(long, long?)> actual = rows
            .Select(r => (r.Row["lid"].LongValue, r.Row["rid"].Type == ColumnType.Null ? (long?)null : r.Row["rid"].LongValue))
            .OrderBy(p => p.Item1).ThenBy(p => p.Item2 ?? -1)
            .ToList();

        List<(long, long?)> expected = [];
        foreach (Row l in Lefts)
        {
            List<long> matches = Rights.Where(r => Scaled(l.N) is { } a && Unscaled(r.P) is { } b && a == b).Select(r => r.Id).ToList();
            if (matches.Count == 0)
                expected.Add((l.Id, null));
            else
                expected.AddRange(matches.Select(m => (l.Id, (long?)m)));
        }

        CollectionAssert.AreEqual(expected.OrderBy(p => p.Item1).ThenBy(p => p.Item2 ?? -1).ToList(), actual, $"{mode}");
    }

    [Test]
    public async Task Join_Int64ToFloat64_MatchesByValue_UnderEveryStrategy([Values] Mode mode)
    {
        // The same rule held before NUMERIC existed: the evaluator compares INT64 with FLOAT64 by value,
        // so 2 = 2.0 matches under every strategy, not only the nested loop.
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, database, "CREATE TABLE a (id int64 PRIMARY KEY, n int64)");
        await ExecDDL(executor, database, "CREATE TABLE b (id int64 PRIMARY KEY, f float64)");
        if (mode == Mode.IndexNestedLoop)
            await ExecDDL(executor, database, "CREATE INDEX b_f ON b (f)");
        await Exec(executor, database, "INSERT INTO a (id, n) VALUES (1, 2), (2, 3), (3, NULL)");
        await Exec(executor, database, "INSERT INTO b (id, f) VALUES (10, 2.0), (11, 2.5), (12, 3.0), (13, NULL)");
        Force(executor, mode);

        List<QueryResultRow> rows = await Run(database, executor, db,
            "SELECT a.id AS lid, b.id AS rid FROM a JOIN b ON a.n = b.f");

        CollectionAssert.AreEqual(new List<(long, long)> { (1, 10), (2, 12) }, Pairs(rows), $"{mode}");
    }

    [Test]
    public async Task CommaJoin_Int64ToNumeric_IsExact([Values] Mode mode)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(mode);

        List<QueryResultRow> rows = await Run(database, executor, db,
            "SELECT l.id AS lid, r.id AS rid FROM lefts l, rights r WHERE l.n = r.p");

        CollectionAssert.AreEqual(Oracle((l, r) => Scaled(l.N) is { } a && Unscaled(r.P) is { } b && a == b), Pairs(rows), $"{mode}");
    }

    [Test]
    public async Task InSubquery_Int64AgainstNumeric_IsExact([Values] Mode mode)
    {
        // The semi-join probes an index on rights.p when there is one (the IndexNestedLoop fixture).
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(mode);

        List<QueryResultRow> inRows = await Run(database, executor, db,
            "SELECT id FROM lefts WHERE n IN (SELECT p FROM rights) ORDER BY id");
        List<long> expectedIn = Lefts.Where(l => Scaled(l.N) is { } a && Rights.Any(r => Unscaled(r.P) == a)).Select(l => l.Id).ToList();
        CollectionAssert.AreEqual(expectedIn, inRows.Select(r => r.Row["id"].LongValue).ToList(), $"{mode}: IN");

        List<QueryResultRow> existsRows = await Run(database, executor, db,
            "SELECT id FROM lefts l WHERE EXISTS (SELECT 1 FROM rights r WHERE r.p = l.n) ORDER BY id");
        CollectionAssert.AreEqual(expectedIn, existsRows.Select(r => r.Row["id"].LongValue).ToList(), $"{mode}: EXISTS");
    }

    [Test]
    public async Task HashJoin_ThatSpills_MatchesTheOracle()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await SetupAsync(Mode.Hash, Options with { SpillEnabled = true, ForceSpillThresholdRows = 2 });

        List<QueryResultRow> rows = await Run(database, executor, db,
            "SELECT l.id AS lid, r.id AS rid FROM lefts l JOIN rights r ON l.p = r.p");

        CollectionAssert.AreEqual(Oracle((l, r) => Unscaled(l.P) is { } a && Unscaled(r.P) is { } b && a == b), Pairs(rows));
    }

    [Test]
    public async Task GroupBy_KeysThatDifferOnlyInTheHighHalf_StayApart([Values] bool spill)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = spill
            ? await CreateDatabase(Options with { SpillEnabled = true, ForceSpillThresholdRows = 2 })
            : await CreateDatabase();
        await ExecDDL(executor, database, "CREATE TABLE g (id int64 PRIMARY KEY, p numeric)");

        // k × 2⁶⁴ unscaled + 7: every key has the same low 64 bits.
        List<string> keys = Enumerable.Range(1, 20)
            .Select(k => NumericMath.Format(((Int128)k << 64) + 7))
            .ToList();

        Dictionary<string, ColumnValue> parameters = [];
        List<string> values = [];
        int id = 0;
        for (int k = 0; k < keys.Count; k++)
        {
            for (int copy = 0; copy <= k % 3; copy++)
            {
                parameters[$"@v{id}"] = ColumnValue.FromNumericString(keys[k]);
                values.Add($"({id}, @v{id})");
                id++;
            }
        }
        await Exec(executor, database, $"INSERT INTO g (id, p) VALUES {string.Join(", ", values)}", parameters);

        List<QueryResultRow> grouped = await Run(database, executor, db, "SELECT p, COUNT(*) AS c FROM g GROUP BY p");
        Assert.AreEqual(keys.Count, grouped.Count, "one group per key");
        foreach (QueryResultRow row in grouped)
        {
            int k = keys.IndexOf(row.Row["p"].NumericValue!);
            Assert.GreaterOrEqual(k, 0);
            Assert.AreEqual(k % 3 + 1, row.Row["c"].LongValue, $"key {keys[k]}");
        }

        List<QueryResultRow> distinct = await Run(database, executor, db, "SELECT DISTINCT p FROM g");
        CollectionAssert.AreEquivalent(keys, distinct.Select(r => r.Row["p"].NumericValue));
    }

    [Test]
    public async Task ForeignKey_BetweenNumericColumns_MatchesByValue_AndNamesTheKey()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, database, "CREATE TABLE rates (code numeric NOT NULL, label string, PRIMARY KEY (code))");
        await ExecDDL(executor, database,
            "CREATE TABLE loans (id int64 NOT NULL, rate numeric REFERENCES rates (code), PRIMARY KEY (id))");

        await Exec(executor, database, "INSERT INTO rates (code, label) VALUES (NUMERIC '1.5', 'low'), (NUMERIC '12345678901234567890.123456789', 'wide')");

        // 1.50 is the same NUMERIC as 1.5, so it has a parent.
        await Exec(executor, database, "INSERT INTO loans (id, rate) VALUES (1, NUMERIC '1.50'), (2, NUMERIC '12345678901234567890.123456789'), (3, NULL)");

        CamusDBException missing = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Exec(executor, database, "INSERT INTO loans (id, rate) VALUES (4, NUMERIC '1.500000001')"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, missing.Code);
        StringAssert.Contains("(rate)=(1.500000001)", missing.Message, "the key renders in canonical form");

        CamusDBException mismatch = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, database, "CREATE TABLE bad (id int64 NOT NULL, rate int64 REFERENCES rates (code), PRIMARY KEY (id))"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidForeignKeyDefinition, mismatch.Code, "an INT64 column cannot reference a NUMERIC key");
    }

    [Test]
    public async Task NumericColumn_ComparedWithAString_HasNoComparisonRule()
    {
        // Spanner has no NUMERIC = STRING signature, and the engine does not parse the string either.
        // The pair follows the engine's rule for a pair with no comparison rule: never equal, and an
        // ordering operator is an error. A typed literal (NUMERIC '1.5') is the way to write the value.
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, database, "CREATE TABLE s (id int64 PRIMARY KEY, p numeric)");
        await Exec(executor, database, "INSERT INTO s (id, p) VALUES (1, 1.5), (2, 3)");

        Assert.AreEqual(1, (await Run(database, executor, db, "SELECT id FROM s WHERE p = NUMERIC '1.5'")).Count);
        Assert.AreEqual(0, (await Run(database, executor, db, "SELECT id FROM s WHERE p = '1.5'")).Count, "never equal");
        Assert.AreEqual(2, (await Run(database, executor, db, "SELECT id FROM s WHERE p <> '1.5'")).Count);

        CamusDBException ordering = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Run(database, executor, db, "SELECT id FROM s WHERE p < '2'"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ordering.Code);

        // CHECK applies the same refusal at write time.
        await ExecDDL(executor, database, "CREATE TABLE c (id int64 PRIMARY KEY, p numeric, CHECK (p > '0'))");
        CamusDBException check = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Exec(executor, database, "INSERT INTO c (id, p) VALUES (1, 1.5)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, check.Code);
        StringAssert.Contains("incompatible types", check.Message);
    }

    // ─── Derived columns whose cells have a narrower type than declared ───────────

    /// <summary>
    /// <c>COALESCE(p, 0)</c> is declared NUMERIC, but for a NULL <c>p</c> the evaluator returns an
    /// INT64 zero. Both join columns are declared NUMERIC, so the join may pick a hash, merge or index
    /// key, and that key must see one type: left row 5 (NULL) matches right row 109 (0) and right row
    /// 105 (NULL, so also 0) under every strategy, inner and left outer.
    /// </summary>
    [Test]
    public async Task Join_OnCoalesceOfANullableNumeric_MatchesTheIntegerFallback([Values] Mode mode, [Values] bool leftOuter)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(mode);

        static Int128 Fallback(string? p) => Unscaled(p) ?? 0;
        List<(long, long)> expected = Oracle((l, r) => Fallback(l.P) == Fallback(r.P));
        Assert.Contains((5L, 109L), expected);
        Assert.Contains((5L, 105L), expected);

        // The outer join pads each unmatched left row with a NULL right id, shown here as -1.
        if (leftOuter)
        {
            expected.AddRange(Lefts.Where(l => expected.All(e => e.Item1 != l.Id)).Select(l => (l.Id, -1L)));
            expected = expected.OrderBy(e => e.Item1).ThenBy(e => e.Item2).ToList();
        }

        string join = leftOuter ? "LEFT JOIN" : "JOIN";
        List<QueryResultRow> rows = await Run(database, executor, db,
            "SELECT a.id AS lid, b.id AS rid " +
            "FROM (SELECT id, COALESCE(p, 0) AS v FROM lefts) a " +
            $"{join} (SELECT id, COALESCE(p, 0) AS v FROM rights) b ON a.v = b.v");

        List<(long, long)> actual = rows
            .Select(r => (r.Row["lid"].LongValue, r.Row["rid"].Type == ColumnType.Null ? -1L : r.Row["rid"].LongValue))
            .OrderBy(e => e.Item1).ThenBy(e => e.Item2)
            .ToList();

        CollectionAssert.AreEqual(expected, actual, $"{mode}, {join}");
    }

    /// <summary>
    /// A CASE whose ELSE is an integer and whose THEN is NUMERIC was declared INT64, from the ELSE
    /// branch alone. Joined with an INT64 column, both keys then looked INT64, and a hash or merge join
    /// compared NUMERIC cells with INT64 cells by type, dropping exact matches such as 2 = 2. The CASE
    /// is now declared NUMERIC, the widest branch type, and the derived table widens its cells to it.
    /// </summary>
    [Test]
    public async Task Join_OnCaseMixingNumericAndInteger_IsExact_UnderEveryStrategy([Values] Mode mode)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(mode);

        List<(long, long)> expected = Oracle((l, r) => (Unscaled(l.P) ?? 0) is var a && Scaled(r.N) is { } b && a == b);
        Assert.Contains((2L, 102L), expected, "NUMERIC 2 = INT64 2");

        List<QueryResultRow> rows = await Run(database, executor, db,
            "SELECT a.id AS lid, r.id AS rid " +
            "FROM (SELECT id, CASE WHEN p IS NOT NULL THEN p ELSE 0 END AS v FROM lefts) a " +
            "JOIN rights r ON a.v = r.n");

        CollectionAssert.AreEqual(expected, Pairs(rows), $"{mode}");
    }

    [Test]
    public async Task DerivedColumns_CarryTheirDeclaredNumericType()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(Mode.NestedLoop);

        List<QueryResultRow> rows = await Run(database, executor, db,
            "SELECT id, c, k FROM (SELECT id, COALESCE(p, 0) AS c, CASE WHEN id = 5 THEN 1 ELSE p END AS k FROM lefts) d WHERE id = 5");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Numeric, rows[0].Row["c"].Type, "COALESCE fallback widened");
        Assert.AreEqual("0", rows[0].Row["c"].NumericValue);
        Assert.AreEqual(ColumnType.Numeric, rows[0].Row["k"].Type, "CASE integer branch widened");
        Assert.AreEqual("1", rows[0].Row["k"].NumericValue);
    }

    // ─── IN lists that mix NUMERIC, INT64 and float items ─────────────────────────

    /// <summary>
    /// Mixed numeric equality is not transitive: NUMERIC 9007199254740992 and 9007199254740993 differ,
    /// but both equal the double 9007199254740992.0. A list past eight items is hashed, and a hash
    /// set built with the mixed rule dropped the float item as a duplicate of the NUMERIC item, so row
    /// 8 (NUMERIC 9007199254740993) lost its float match once the list grew. Padding the list with
    /// items that match nothing, in either order, must not change the result. Rows 2 (2) and 8 match.
    /// </summary>
    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    [TestCase(true, true)]
    public async Task InList_MixingNumericAndFloat_KeepsEveryMatch_AtAnyLength(bool padded, bool indexed)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(Mode.NestedLoop);
        if (indexed)
            await ExecDDL(executor, database, "CREATE INDEX lefts_p ON lefts (p)");

        string padding = padded ? ", 101, 102, 103, 104, 105, 106, 107" : "";
        foreach (string items in new[]
                 {
                     "NUMERIC '9007199254740992', 9007199254740992.0, 2",
                     "9007199254740992.0, NUMERIC '9007199254740992', 2",
                 })
        {
            List<QueryResultRow> rows = await Run(database, executor, db, $"SELECT id FROM lefts WHERE p IN ({items}{padding})");
            CollectionAssert.AreEquivalent(new[] { 2L, 8L }, rows.Select(r => r.Row["id"].LongValue), $"IN ({items}{padding})");
        }
    }

    /// <summary>
    /// The same trap with INT64 and FLOAT64, on a column with no index (a table scan evaluates the
    /// list): INT64 9007199254740992 and the double 9007199254740992.0 are "equal", and the double also
    /// equals row 3's INT64 9007199254740993.
    /// </summary>
    [Test]
    public async Task InList_MixingIntegerAndFloat_KeepsEveryMatch_PastTheHashThreshold()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(Mode.NestedLoop);

        List<QueryResultRow> rows = await Run(database, executor, db,
            "SELECT id FROM lefts WHERE n IN (9007199254740992, 9007199254740992.0, 101, 102, 103, 104, 105, 106, 107)");

        CollectionAssert.AreEquivalent(new[] { 3L }, rows.Select(r => r.Row["id"].LongValue));
    }

    private static void Force(CommandExecutor executor, Mode mode)
    {
        switch (mode)
        {
            case Mode.NestedLoop: executor.Statistics.ForceNestedLoopForTesting = true; break;
            case Mode.IndexNestedLoop: executor.Statistics.ForceIndexNestedLoopForTesting = true; break;
            case Mode.Hash: executor.Statistics.ForceHashJoinForTesting = true; break;
            case Mode.Merge: executor.Statistics.ForceMergeJoinForTesting = true; break;
        }
    }

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
            await db.Transactions.CommitAsync(tx);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }
}
