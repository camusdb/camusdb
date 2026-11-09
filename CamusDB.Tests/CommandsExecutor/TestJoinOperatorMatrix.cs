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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Every join operator against every join kind: each forced operator (nested loop, index nested loop
/// with and without its probe memo, hash, merge) runs INNER, LEFT, RIGHT and CROSS joins over tables
/// whose columns share the names <c>id, k, v</c>. The right side is a table with duplicate and NULL keys,
/// an empty table, a FLOAT64 key and a NUMERIC key against the INT64 key of the left side; the left
/// side is also empty once. Each query is a <c>SELECT *</c>, so each case also checks the six bare names
/// and every value by position. The expected rows were measured on PostgreSQL 16.11; they are compared
/// as a multiset, because the operators emit them in different orders.
///
/// <para>The index scan fetch batch is 2 rows, so the index nested loop reads the five left rows in
/// three pages, and the left key 2 repeats across two of them. Without the memo each page sends one
/// batched probe; with the memo on, the second page answers k 2 from the memo and sends none. EXPLAIN
/// (ANALYZE) shows the batches and the memo hits.</para>
///
/// <para>EXPLAIN confirms that the planner used the forced operator for each join with a same-type
/// equality, so a quiet fallback to another operator cannot pass as coverage. A mixed-type equality is
/// never a join key, so each mixed-type join must show a nested loop in every mode.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestJoinOperatorMatrix : BaseTest
{
    public enum Mode { NestedLoop, IndexNestedLoop, IndexNestedLoopWithoutMemo, Hash, Merge }

    private sealed record Db(string Name, DatabaseDescriptor Database, CommandExecutor Executor);

    /// <summary>
    /// A query, whether its equality has one type on both sides, and its rows as PostgreSQL gives them.
    /// Only a same-type equality can be the key of the forced operator. A CROSS JOIN with no WHERE has
    /// no equality, and a mixed-type equality is never a key (see <c>JoinEquiJoinAnalyzer</c>: the hash
    /// table, the merge order and the index key treat two types as unequal, the evaluator does not), so
    /// each of those runs as a nested loop whatever operator is forced.
    /// </summary>
    private sealed record Case(string Sql, bool SameTypeKey, string[] Rows);

    private static readonly string[] Names = ["id", "k", "v", "id", "k", "v"];

    private static readonly string[] MatchedRows =
        ["1|1|10|5|1|500", "2|2|20|1|2|100", "2|2|20|2|2|200", "3|2|30|1|2|100", "3|2|30|2|2|200"];

    private static readonly string[] MixedTypeMatchedRows =
        ["1|1|10|4|1|400", "2|2|20|1|2|100", "3|2|30|1|2|100"];

    private static readonly string[] UnmatchedLeftRows =
        ["4|NULL|40|NULL|NULL|NULL", "5|9|50|NULL|NULL|NULL"];

    private static readonly string[] MixedTypeUnmatchedRightRows =
        ["NULL|NULL|NULL|2|2.5|200", "NULL|NULL|NULL|3|NULL|300"];

    private static IEnumerable<Case> Cases()
    {
        // Duplicate keys on both sides, a NULL key on both sides, and a key on each side that the other lacks.
        yield return new("SELECT * FROM ml JOIN mr ON ml.k = mr.k", true, MatchedRows);
        yield return new("SELECT * FROM ml LEFT JOIN mr ON ml.k = mr.k", true, [.. MatchedRows, .. UnmatchedLeftRows]);
        yield return new("SELECT * FROM ml RIGHT JOIN mr ON ml.k = mr.k", true,
            [.. MatchedRows, "NULL|NULL|NULL|3|3|300", "NULL|NULL|NULL|4|NULL|400"]);
        yield return new("SELECT * FROM ml CROSS JOIN mr WHERE ml.k = mr.k", true, MatchedRows);

        // An empty right side.
        yield return new("SELECT * FROM ml JOIN mre ON ml.k = mre.k", true, []);
        yield return new("SELECT * FROM ml LEFT JOIN mre ON ml.k = mre.k", true,
            ["1|1|10|NULL|NULL|NULL", "2|2|20|NULL|NULL|NULL", "3|2|30|NULL|NULL|NULL", .. UnmatchedLeftRows]);
        yield return new("SELECT * FROM ml RIGHT JOIN mre ON ml.k = mre.k", true, []);
        yield return new("SELECT * FROM ml CROSS JOIN mre WHERE ml.k = mre.k", true, []);
        yield return new("SELECT * FROM ml CROSS JOIN mre", false, []);

        // An empty left side.
        yield return new("SELECT * FROM mle JOIN mr ON mle.k = mr.k", true, []);
        yield return new("SELECT * FROM mle LEFT JOIN mr ON mle.k = mr.k", true, []);
        yield return new("SELECT * FROM mle RIGHT JOIN mr ON mle.k = mr.k", true,
            ["NULL|NULL|NULL|1|2|100", "NULL|NULL|NULL|2|2|200", "NULL|NULL|NULL|3|3|300",
             "NULL|NULL|NULL|4|NULL|400", "NULL|NULL|NULL|5|1|500"]);
        yield return new("SELECT * FROM mle CROSS JOIN mr", false, []);

        // INT64 = FLOAT64 and INT64 = NUMERIC: equal values match across the types, and 2.5 matches nothing.
        foreach (string right in new[] { "mrf", "mrn" })
        {
            yield return new($"SELECT * FROM ml JOIN {right} ON ml.k = {right}.k", false, MixedTypeMatchedRows);
            yield return new($"SELECT * FROM ml LEFT JOIN {right} ON ml.k = {right}.k", false,
                [.. MixedTypeMatchedRows, .. UnmatchedLeftRows]);
            yield return new($"SELECT * FROM ml RIGHT JOIN {right} ON ml.k = {right}.k", false,
                [.. MixedTypeMatchedRows, .. MixedTypeUnmatchedRightRows]);
            yield return new($"SELECT * FROM ml CROSS JOIN {right} WHERE ml.k = {right}.k", false, MixedTypeMatchedRows);
        }
    }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { CostBasedJoinOrderEnabled = false, AutoAnalyzeEnabled = false, IndexScanFetchBatchSize = 2 };

    [Test]
    [TestCase(Mode.NestedLoop)]
    [TestCase(Mode.IndexNestedLoop)]
    [TestCase(Mode.IndexNestedLoopWithoutMemo)]
    [TestCase(Mode.Hash)]
    [TestCase(Mode.Merge)]
    public async Task EveryJoinKind_GivesThePostgreSqlRows(Mode mode)
    {
        Db db = await SetupAsync();
        Force(db.Executor, mode);

        try
        {
            List<string> failures = [];

            // Each case reports its problems as text, not through an assertion, so one run lists every
            // case that fails instead of stopping at the first one.
            foreach (Case c in Cases())
            {
                if (await Check(db, mode, c) is { } problem)
                    failures.Add($"{c.Sql}\n    {problem}");
            }

            Assert.IsEmpty(failures, $"{mode}:\n" + string.Join("\n", failures));

            if (mode is Mode.IndexNestedLoop or Mode.IndexNestedLoopWithoutMemo)
            {
                List<IReadOnlyDictionary<string, ColumnValue>> plan =
                    await Rows(db, "EXPLAIN (ANALYZE) SELECT * FROM ml LEFT JOIN mr ON ml.k = mr.k");
                string detail = plan.Single(r => r["node"].StrValue == "index-nested-loop-join")["detail"].StrValue ?? "";

                // Five left rows in pages of two: (k 1, k 2), (k 2, NULL), (k 9). With the memo on, the
                // second page finds k 2 in the memo and has no other key to probe, so it sends no batch.
                if (mode == Mode.IndexNestedLoop)
                {
                    StringAssert.Contains("probe-memo-hits=1", detail, "the key 2 repeats in the second page");
                    StringAssert.Contains("probe-batches=2", detail, "the second page has nothing left to probe");
                }
                else
                {
                    StringAssert.DoesNotContain("probe-memo-hits", detail);
                    StringAssert.Contains("probe-batches=3", detail, "each page probes its keys");
                }
            }
        }
        finally
        {
            Force(db.Executor, null);
        }
    }

    /// <summary>The problems of one case as one line, or null when the plan, the names and the rows are right.</summary>
    private static async Task<string?> Check(Db db, Mode mode, Case c)
    {
        List<string> problems = [];

        string node = c.SameTypeKey ? ExpectedNode(mode) : "nested-loop-join";
        List<IReadOnlyDictionary<string, ColumnValue>> plan = await Rows(db, "EXPLAIN " + c.Sql);
        List<string> nodes = plan.Select(r => r["node"].StrValue ?? "").ToList();

        if (!nodes.Any(n => n.StartsWith(node, StringComparison.Ordinal)))
            problems.Add($"the plan is {string.Join(" / ", nodes)}, not a {node}");

        QuerySchemaHolder holder = new();
        KvTransaction tx = await db.Database.Transactions.BeginAsync();
        List<QueryResultRow> rows;

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, db.Name, c.Sql, null), schemaOut: holder);
            rows = await cursor.ToListAsync();
        }
        finally
        {
            await db.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        string[] names = holder.Schema.Select(column => column.Name).ToArray();

        if (!names.SequenceEqual(Names))
            problems.Add($"the names are {string.Join(", ", names)}");

        List<string> actual = rows
            .Select(row => string.Join("|", holder.Schema.Select(column =>
                row.Row.TryGetValue(column.RowKey, out ColumnValue? cell) ? TestJoinOutputNames.Cell(cell) : "<missing>")))
            .ToList();
        List<string> expected = c.Rows.ToList();
        actual.Sort(StringComparer.Ordinal);
        expected.Sort(StringComparer.Ordinal);

        if (!actual.SequenceEqual(expected))
            problems.Add($"the rows are [{string.Join(", ", actual)}], not [{string.Join(", ", expected)}]");

        return problems.Count == 0 ? null : string.Join("; ", problems);
    }

    private static string ExpectedNode(Mode mode) => mode switch
    {
        Mode.NestedLoop => "nested-loop-join",
        Mode.IndexNestedLoop or Mode.IndexNestedLoopWithoutMemo => "index-nested-loop-join",
        Mode.Hash => "hash-join",
        Mode.Merge => "merge-join",
        _ => throw new ArgumentOutOfRangeException(nameof(mode)),
    };

    private static void Force(CommandExecutor executor, Mode? mode)
    {
        executor.Statistics.ForceNestedLoopForTesting = mode == Mode.NestedLoop;
        executor.Statistics.ForceIndexNestedLoopForTesting = mode is Mode.IndexNestedLoop or Mode.IndexNestedLoopWithoutMemo;
        executor.Statistics.ForceHashJoinForTesting = mode == Mode.Hash;
        executor.Statistics.ForceMergeJoinForTesting = mode == Mode.Merge;
        executor.Statistics.IndexProbeMemoCapacityForTesting = mode == Mode.IndexNestedLoopWithoutMemo ? 0 : null;
    }

    private async Task<Db> SetupAsync()
    {
        (string name, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        Db db = new(name, database, executor);

        // Every table has an index on k, so the index nested loop can probe whichever side is inner,
        // including the left table that a RIGHT JOIN swaps to the inner side.
        foreach ((string table, string keyType) in new[]
                 { ("ml", "INT64"), ("mle", "INT64"), ("mr", "INT64"), ("mre", "INT64"), ("mrf", "FLOAT64"), ("mrn", "NUMERIC") })
        {
            await Ddl(db, $"CREATE TABLE {table} (id INT64 PRIMARY KEY, k {keyType}, v INT64)");
            await Ddl(db, $"CREATE INDEX {table}_k ON {table} (k)");
        }

        await Dml(db, "INSERT INTO ml (id, k, v) VALUES (1, 1, 10), (2, 2, 20), (3, 2, 30), (4, NULL, 40), (5, 9, 50)");
        await Dml(db, "INSERT INTO mr (id, k, v) VALUES (1, 2, 100), (2, 2, 200), (3, 3, 300), (4, NULL, 400), (5, 1, 500)");
        await Dml(db, "INSERT INTO mrf (id, k, v) VALUES (1, 2.0, 100), (2, 2.5, 200), (3, NULL, 300), (4, 1.0, 400)");
        await Dml(db, "INSERT INTO mrn (id, k, v) VALUES (1, 2, 100), (2, 2.5, 200), (3, NULL, 300), (4, 1, 400)");

        return db;
    }

    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>> Rows(Db db, string sql)
    {
        KvTransaction tx = await db.Database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await db.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
            List<IReadOnlyDictionary<string, ColumnValue>> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(row.Row);
            return rows;
        }
        finally
        {
            await db.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Ddl(Db db, string sql)
    {
        KvTransaction tx = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Database.Transactions.CommitAsync(tx);
    }

    private static async Task Dml(Db db, string sql)
    {
        KvTransaction tx = await db.Database.Transactions.BeginAsync();
        await db.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Database.Transactions.CommitAsync(tx);
    }
}
