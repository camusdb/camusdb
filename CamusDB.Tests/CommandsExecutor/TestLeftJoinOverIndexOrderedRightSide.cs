/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
/// A left outer join pads a left row with no match from a right key list it computes from metadata
/// before any right row exists, and every real right row must carry exactly that key list. A right
/// side read through an index entry (the index-ordered scan of a merge join, an index range or an
/// IN-list leaf) decodes the index's key columns as well as the columns the query reads, so the entry
/// can be checked against the row. The key list once held only the columns the query reads, so a
/// merge join whose right index had a key column the query did not read failed on its first matched
/// row with the internal code <see cref="CamusDBErrorCodes.InvalidInternalOperation"/>
/// ("Join row shape diverged from join layout: unexpected right key"). The planner picks that shape
/// by itself for a self-join under a LEFT JOIN on a table without statistics, and the test seams
/// force it on demand. These tests drive each join operator through SQL and check the rows.
/// </summary>
public sealed class TestLeftJoinOverIndexOrderedRightSide : SharedNodeBaseTest
{
    /// <summary>The query of the fuzzer finding: a self-join under a LEFT JOIN that reads one column of the inner join.</summary>
    private const string SelfJoinUnderLeftJoin =
        "SELECT a0.c0 AS l, a1.c0 AS m, a2.c0 AS r FROM t0 AS a0 INNER JOIN t0 AS a1 ON (a0.c1 = a1.c1) LEFT JOIN t1 AS a2 ON (a0.c1 = a2.c2)";

    private static readonly List<string> SelfJoinUnderLeftJoinRows = ["1|1|1", "1|1|2", "2|2|NULL"];

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync(bool autoAnalyze)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { AutoAnalyzeEnabled = autoAnalyze });

        // t1_i2 has the key column c1 that no query here reads, after the join column c2.
        await Ddl(database, executor, dbname, "CREATE TABLE t0 (c0 int64 PRIMARY KEY, c1 float64, c2 float64)");
        await Ddl(database, executor, dbname, "CREATE INDEX t0_i3 ON t0 (c1, c2)");
        await Ddl(database, executor, dbname, "CREATE TABLE t1 (c0 int64 PRIMARY KEY, c1 float64, c2 float64)");
        await Ddl(database, executor, dbname, "CREATE INDEX t1_i2 ON t1 (c2, c1)");
        // The shrunk shape of the fuzzer: the index also has an INCLUDE column.
        await Ddl(database, executor, dbname, "CREATE TABLE t2 (c0 int64 PRIMARY KEY, c2 string NOT NULL, c3 string, c6 bool NOT NULL)");
        await Ddl(database, executor, dbname, "CREATE INDEX t2_i0 ON t2 (c3, c6) INCLUDE (c2)");
        await Ddl(database, executor, dbname, "CREATE TABLE t3 (c0 int64 PRIMARY KEY, c1 float64, c2 float64)");
        await Ddl(database, executor, dbname, "CREATE INDEX t3_i ON t3 (c2, c1)");

        await Write(database, executor, dbname, "INSERT INTO t0 (c0, c1, c2) VALUES (1, 100.25, NULL), (2, 7.5, 1.0), (3, NULL, 2.0)");
        await Write(database, executor, dbname, "INSERT INTO t1 (c0, c1, c2) VALUES (1, 1.5, 100.25), (2, 9.0, 100.25)");
        await Write(database, executor, dbname, "INSERT INTO t2 (c0, c2, c3, c6) VALUES (1, 'a', 'u1', true), (2, 'b', 'u2', false), (3, 'c', NULL, true)");

        return (dbname, database, executor);
    }

    private static async Task Ddl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task Write(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>A second engine on the same node, for a planner seam, opened on the test database.</summary>
    private async Task<(DatabaseDescriptor database, CommandExecutor executor)> ArmAsync(string dbname, System.Action<CommandExecutor> prepare)
    {
        CommandExecutor arm = CreateCommandExecutor(Options with { AutoAnalyzeEnabled = false });
        prepare(arm);
        DatabaseDescriptor database = await arm.OpenDatabase(dbname);
        return (database, arm);
    }

    /// <summary>The rows of <paramref name="sql"/> as "a|b|c" text ("NULL" for a NULL), sorted, so a result compares without an ORDER BY.</summary>
    private static async Task<List<string>> Rows(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows.Select(r => string.Join("|", r.Row.Values.Select(Text))).OrderBy(x => x, System.StringComparer.Ordinal).ToList();
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    /// <summary>The plan of <paramref name="sql"/>, one EXPLAIN line per text line.</summary>
    private static async Task<string> Explain(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        List<string> lines = await Rows(database, executor, dbname, "EXPLAIN " + sql);
        return string.Join("\n", lines);
    }

    private static string Text(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Bool => value.BoolValue ? "true" : "false",
        ColumnType.Integer64 => value.LongValue.ToString(),
        ColumnType.String => value.StrValue ?? "",
        _ => value.ToString() ?? "",
    };

    [Test]
    public async Task SelfJoinUnderLeftJoin_DefaultPlanOnLargerTables_IsAMergeJoinOverTheIndexAndReturnsEveryRow()
    {
        // Above the planner's merge-join preference threshold (100 left rows), with an index on the join
        // column of both sides of each join, the planner picks merge joins by itself and reads t5 in
        // the order of t5_i (c2, c1). No test seam is set here.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(autoAnalyze: false);

        await Ddl(database, executor, dbname, "CREATE TABLE t4 (c0 int64 PRIMARY KEY, c1 float64, c2 float64)");
        await Ddl(database, executor, dbname, "CREATE INDEX t4_i ON t4 (c1, c2)");
        await Ddl(database, executor, dbname, "CREATE TABLE t5 (c0 int64 PRIMARY KEY, c1 float64, c2 float64)");
        await Ddl(database, executor, dbname, "CREATE INDEX t5_i ON t5 (c2, c1)");

        // t4: c1 = i % 12, so each of the values 0..5 has 13 rows and each of 6..11 has 12 rows.
        List<string> values = new(150);
        for (int i = 1; i <= 150; i++)
            values.Add($"({i}, {i % 12}.0, NULL)");
        await Write(database, executor, dbname, "INSERT INTO t4 (c0, c1, c2) VALUES " + string.Join(", ", values));
        // t5: one row joins c1 = 10, two rows join c1 = 11, every other c1 value is padded.
        await Write(database, executor, dbname, "INSERT INTO t5 (c0, c1, c2) VALUES (1, 1.0, 10.0), (2, 2.0, 11.0), (3, 3.0, 11.0)");

        const string sql = "SELECT a0.c0 AS l, a1.c0 AS m, a2.c0 AS r FROM t4 AS a0 INNER JOIN t4 AS a1 ON (a0.c1 = a1.c1) LEFT JOIN t5 AS a2 ON (a0.c1 = a2.c2)";

        string plan = await Explain(database, executor, dbname, sql);
        StringAssert.Contains("merge-join", plan);
        StringAssert.Contains("forced-index=t5_i", plan);
        StringAssert.Contains("kind=left-outer", plan);

        // The self-join gives 6 × 13² + 6 × 12² = 1,878 pairs; the 144 pairs with c1 = 10 match one t5
        // row, the 144 pairs with c1 = 11 match two, and the other 1,590 pairs are padded.
        List<string> rows = await Rows(database, executor, dbname, sql);
        Assert.AreEqual(2022, rows.Count);
        Assert.AreEqual(1590, rows.Count(r => r.EndsWith("|NULL")));
        Assert.AreEqual(144, rows.Count(r => r.EndsWith("|1")));
        Assert.AreEqual(144, rows.Count(r => r.EndsWith("|2")));
        Assert.AreEqual(144, rows.Count(r => r.EndsWith("|3")));

        // The hash join reads the right side through its primary rows, so it never had the defect.
        (DatabaseDescriptor hashDatabase, CommandExecutor hash) = await ArmAsync(dbname, e => e.Statistics.ForceHashJoinForTesting = true);
        CollectionAssert.AreEqual(await Rows(hashDatabase, hash, dbname, sql), rows);
    }

    [Test]
    public async Task SelfJoinUnderLeftJoin_EveryForcedJoinOperator_ReturnsTheSameRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(autoAnalyze: true);

        CollectionAssert.AreEqual(SelfJoinUnderLeftJoinRows, await Rows(database, executor, dbname, SelfJoinUnderLeftJoin), "default plan");

        // The index nested loop is not an arm: a composite index gives it no match for this shape, and its
        // probe page reads rows without the entry check, so it never widens the right key set.
        (System.Action<CommandExecutor> prepare, string name, string planNode)[] arms =
        [
            (e => e.Statistics.ForceMergeJoinForTesting = true, "merge", "merge-join"),
            (e => e.Statistics.ForceHashJoinForTesting = true, "hash", "hash-join"),
            (e => e.Statistics.ForceNestedLoopForTesting = true, "nested-loop", "nested-loop-join"),
        ];

        foreach ((System.Action<CommandExecutor> prepare, string name, string planNode) in arms)
        {
            (DatabaseDescriptor armDatabase, CommandExecutor arm) = await ArmAsync(dbname, prepare);
            string plan = await Explain(armDatabase, arm, dbname, SelfJoinUnderLeftJoin);
            StringAssert.Contains(planNode, plan, name);
            CollectionAssert.AreEqual(SelfJoinUnderLeftJoinRows, await Rows(armDatabase, arm, dbname, SelfJoinUnderLeftJoin), name);
        }
    }

    [Test]
    public async Task RightJoinSelfJoin_ForcedMergeJoin_IndexKeyColumnTheQueryDoesNotRead_ReturnsEveryRow()
    {
        // The shrunk fuzzer finding: the RIGHT JOIN runs as a LEFT JOIN with the sides swapped, the
        // right side (a0) is read in the order of t2_i0 (c3, c6), and the query never reads c6.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync(autoAnalyze: true);
        (DatabaseDescriptor armDatabase, CommandExecutor arm) = await ArmAsync(dbname, e => e.Statistics.ForceMergeJoinForTesting = true);

        const string sql = "SELECT a0.c3 AS o2, a1.c0 AS r FROM t2 AS a0 RIGHT JOIN t2 AS a1 ON (a0.c3 = a1.c3)";
        List<string> expected = ["NULL|3", "u1|1", "u2|2"];

        string plan = await Explain(armDatabase, arm, dbname, sql);
        StringAssert.Contains("merge-join", plan);
        StringAssert.Contains("forced-index=t2_i0", plan);

        CollectionAssert.AreEqual(expected, await Rows(armDatabase, arm, dbname, sql));
        CollectionAssert.AreEqual(expected, await Rows(database, executor, dbname, sql), "default plan");
    }

    [Test]
    public async Task LeftJoin_ForcedMergeJoin_PaddedAndMatchedRowsFeedTheNextJoin()
    {
        // The second join reads padded rows and matched rows of the first; both must carry one key set.
        (string dbname, _, _) = await SetupAsync(autoAnalyze: true);
        (DatabaseDescriptor armDatabase, CommandExecutor arm) = await ArmAsync(dbname, e => e.Statistics.ForceMergeJoinForTesting = true);

        const string sql = "SELECT a0.c0 AS l, a2.c0 AS r, a3.c0 AS s FROM t0 AS a0 LEFT JOIN t1 AS a2 ON (a0.c1 = a2.c2) INNER JOIN t2 AS a3 ON (a3.c0 = a0.c0)";

        CollectionAssert.AreEqual(new List<string> { "1|1|1", "1|2|1", "2|NULL|2", "3|NULL|3" }, await Rows(armDatabase, arm, dbname, sql));
    }

    [Test]
    public async Task LeftJoin_ForcedMergeJoin_WhereOnThePaddedSide_KeepsOnlyTheUnmatchedRows()
    {
        (string dbname, _, _) = await SetupAsync(autoAnalyze: true);
        (DatabaseDescriptor armDatabase, CommandExecutor arm) = await ArmAsync(dbname, e => e.Statistics.ForceMergeJoinForTesting = true);

        const string sql = "SELECT a0.c0 AS l FROM t0 AS a0 LEFT JOIN t1 AS a2 ON (a0.c1 = a2.c2) WHERE a2.c0 IS NULL";

        CollectionAssert.AreEqual(new List<string> { "2", "3" }, await Rows(armDatabase, arm, dbname, sql));
    }

    [Test]
    public async Task LeftJoin_EmptyRightTableReadInIndexOrder_ForcedMergeJoin_PadsEveryLeftRow()
    {
        (string dbname, _, _) = await SetupAsync(autoAnalyze: true);
        (DatabaseDescriptor armDatabase, CommandExecutor arm) = await ArmAsync(dbname, e => e.Statistics.ForceMergeJoinForTesting = true);

        const string sql = "SELECT a0.c0 AS l, a3.c0 AS r FROM t0 AS a0 LEFT JOIN t3 AS a3 ON (a0.c1 = a3.c2)";

        string plan = await Explain(armDatabase, arm, dbname, sql);
        StringAssert.Contains("forced-index=t3_i", plan);

        CollectionAssert.AreEqual(new List<string> { "1|NULL", "2|NULL", "3|NULL" }, await Rows(armDatabase, arm, dbname, sql));
    }
}
