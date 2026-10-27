/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An equality on the first column plus a range on the second column must pick the composite
/// index whose second key is the range column, and must use both as scan bounds. The schema is
/// the one an audit-log table had in production: four non-unique indexes that all start with the
/// same uuid column, only one of them with the range column second, and that one is neither the
/// first nor the last by name. The uuid column holds one value, so its distinct count is 1 and
/// it has no histogram (ANALYZE builds none for uuid), which is the statistics shape that made
/// the cost model prefer an index that binds only the equality.
///
/// <para>Each query runs on <c>plain</c> (no secondary index) and on <c>audit</c>, and both must
/// return the same ids. The data puts a row at the range bound and one just below it, so a bound
/// that is too wide or too narrow shows as a wrong row. <c>rows_read</c> of the scan must equal
/// the matching entries: the scan starts at the bound and stops at the end of the prefix.</para>
/// </summary>
[NonParallelizable]
public sealed class TestEqualityPrefixRangeIndexChoice : SharedNodeBaseTest
{
    private const int RowCount = 300;

    private const long BaseTime = 1_700_000_000;

    /// <summary>Rows 200..299 are at or above this bound: 100 rows, one third of the table.</summary>
    private const long From = BaseTime + 200;

    /// <summary>Rows 0..99 are below this bound: 100 rows. The forms with an upper bound use it.</summary>
    private const long Low = BaseTime + 100;

    private const string StudioId = "019fee67-1234-7000-8000-00000000abcd";

    private const string StudioEquality = "studio_id = CAST('" + StudioId + "' AS UUID)";

    private const string RangeIndex = "audit_c_studio_created";

    private const string PrefixOnlyIndex = "audit_a_studio_action_created";

    private sealed record Row(long Id, string Action, long Actor, long Env, long CreatedAt);

    private static List<Row> BuildRows()
    {
        List<Row> rows = new(RowCount);

        for (int i = 0; i < RowCount; i++)
            rows.Add(new Row(i + 1, "action" + (i % 3), i % 7, i % 2, BaseTime + i));

        return rows;
    }

    private static async Task ExecDdl(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        _ = await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>
    /// Creates <c>plain</c> (no secondary index) and <c>audit</c> (the four production indexes)
    /// with the rows of <see cref="BuildRows"/>, and runs ANALYZE on <c>audit</c> when asked.
    /// Index names sort so that the index with the range column is third of four.
    /// </summary>
    private async Task<(string, DatabaseDescriptor, CommandExecutor, List<Row>)> CreateTables(bool analyze)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<Row> rows = BuildRows();

        StringBuilder values = new();
        foreach (Row row in rows)
        {
            if (values.Length > 0)
                values.Append(", ");

            values.Append(string.Create(CultureInfo.InvariantCulture,
                $"({row.Id}, CAST('{StudioId}' AS UUID), '{row.Action}', {row.Actor}, {row.Env}, {row.CreatedAt}, 'deploy {row.Id}')"));
        }

        foreach (string table in new[] { "plain", "audit" })
        {
            await ExecDdl(executor, database, dbname,
                $"CREATE TABLE {table} (id int64 PRIMARY KEY, studio_id uuid, action string, actor int64, env int64, created_at int64, summary string)");
            await ExecNonQuery(executor, database, dbname,
                $"INSERT INTO {table} (id, studio_id, action, actor, env, created_at, summary) VALUES {values}");
        }

        await ExecDdl(executor, database, dbname, $"CREATE INDEX {PrefixOnlyIndex} ON audit (studio_id, action, created_at)");
        await ExecDdl(executor, database, dbname, "CREATE INDEX audit_b_studio_actor_created ON audit (studio_id, actor, created_at)");
        await ExecDdl(executor, database, dbname, $"CREATE INDEX {RangeIndex} ON audit (studio_id, created_at)");
        await ExecDdl(executor, database, dbname, "CREATE INDEX audit_d_studio_env_created ON audit (studio_id, env, created_at)");

        if (analyze)
            _ = await ExecQuery(executor, database, dbname, "ANALYZE audit");

        return (dbname, database, executor, rows);
    }

    private static async Task<long[]> SelectIds(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string source, string where)
    {
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            $"SELECT id FROM {source} WHERE {where} ORDER BY id");

        return rows.Select(r => r.Row["id"].LongValue).ToArray();
    }

    /// <summary>
    /// The <c>index-range-scan</c> row of EXPLAIN ANALYZE for the query, as its detail text and
    /// the entries it read, or a failure message when the plan has no such row.
    /// </summary>
    private static async Task<(string Detail, long RowsRead)> ExplainScan(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string query)
    {
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "EXPLAIN (ANALYZE) " + query);

        int at = rows.FindIndex(r =>
            r.Row.TryGetValue("node", out ColumnValue? node) && node.StrValue == "index-range-scan");

        if (at < 0)
        {
            string plan = string.Join(" | ", rows.Select(r =>
                (r.Row.TryGetValue("node", out ColumnValue? node) ? node.StrValue : "") + " " +
                (r.Row.TryGetValue("detail", out ColumnValue? detail) ? detail.StrValue : "")));
            Assert.Fail($"no index-range-scan in the plan of {query}: {plan}");
        }

        return (rows[at].Row["detail"].StrValue!, rows[at].Row["rows_read"].LongValue);
    }

    private static long[] Ids(IEnumerable<Row> rows) => rows.Select(r => r.Id).OrderBy(id => id).ToArray();

    /// <summary>
    /// The range forms around a bound, each with the rows the data says it matches. Each bound value
    /// is a row and the values next to it are rows, so each form has a neighbor. Every form matches
    /// about a third of the table or less: above half, the cost model is right to read the table
    /// instead of the index, and the plan check would not be about the choice between indexes.
    /// </summary>
    private static IEnumerable<(string Where, Func<Row, bool> Match)> RangeForms()
    {
        yield return ($"created_at >= {From}", r => r.CreatedAt >= From);
        yield return ($"created_at > {From}", r => r.CreatedAt > From);
        yield return ($"created_at < {Low}", r => r.CreatedAt < Low);
        yield return ($"created_at <= {Low}", r => r.CreatedAt <= Low);
        yield return ($"created_at >= {From} AND created_at < {From + 10}", r => r.CreatedAt >= From && r.CreatedAt < From + 10);
    }

    private static async Task CheckRangeForms(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, List<Row> rows, string statisticsNote)
    {
        List<string> failures = new();

        foreach ((string range, Func<Row, bool> match) in RangeForms())
        {
            string where = StudioEquality + " AND " + range;
            long[] expected = Ids(rows.Where(match));
            Assert.That(expected, Is.Not.Empty, $"the data must hold a row for {range}");

            foreach (string source in new[] { "plain", "audit", "audit" })
            {
                long[] ids = await SelectIds(executor, database, dbname, source, where);
                if (!ids.SequenceEqual(expected))
                    failures.Add($"{statisticsNote} {source}: {where} gave {ids.Length} rows, expected {expected.Length}");
            }

            (string detail, long rowsRead) = await ExplainScan(executor, database, dbname, $"SELECT id FROM audit WHERE {where}");
            if (!detail.Contains($"index={RangeIndex}", StringComparison.Ordinal))
                failures.Add($"{statisticsNote} {where} picked another index: {detail}");
            if (rowsRead != expected.Length)
                failures.Add($"{statisticsNote} {where} read {rowsRead} entries, expected {expected.Length}: {detail}");
        }

        Assert.That(failures, Is.Empty, string.Join("\n", failures));
    }

    [Test]
    public async Task EqualityPrefixPlusRange_WithStatistics_PicksTheIndexWithTheRangeColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<Row> rows) = await CreateTables(analyze: true);

        await CheckRangeForms(executor, database, dbname, rows, "with statistics:");
    }

    [Test]
    public async Task EqualityPrefixPlusRange_WithoutStatistics_PicksTheIndexWithTheRangeColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<Row> rows) = await CreateTables(analyze: false);

        await CheckRangeForms(executor, database, dbname, rows, "without statistics:");
    }

    /// <summary>
    /// The production statement: a residual <c>contains</c> filter, ORDER BY the range column
    /// descending and a LIMIT. No index delivers that order, so the sort cost is the same for
    /// every candidate and the bounds alone decide.
    /// </summary>
    [Test]
    public async Task EqualityPrefixPlusRange_WithOrderByAndLimit_PicksTheIndexWithTheRangeColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<Row> rows) = await CreateTables(analyze: true);

        string query = $"SELECT id FROM audit WHERE {StudioEquality} AND created_at >= {From} " +
                       "AND contains(lower(summary), 'deploy') ORDER BY created_at DESC, id DESC LIMIT 50";

        long[] expected = rows.Where(r => r.CreatedAt >= From).OrderByDescending(r => r.CreatedAt).Take(50).Select(r => r.Id).ToArray();
        long[] ids = (await ExecQuery(executor, database, dbname, query)).Select(r => r.Row["id"].LongValue).ToArray();
        Assert.That(ids, Is.EqualTo(expected));

        (string detail, long rowsRead) = await ExplainScan(executor, database, dbname, query);
        Assert.That(detail, Does.Contain($"index={RangeIndex}"), detail);
        Assert.That(rowsRead, Is.EqualTo(rows.Count(r => r.CreatedAt >= From)), detail);
    }

    /// <summary>
    /// The same choice with a <c>bool</c> prefix column, which has no distinct count and no histogram
    /// (ANALYZE skips the type), the presence sweep of a game database: <c>is_online = true AND
    /// updated_at &lt; @stale</c> over <c>(is_online, shard, updated_at)</c> and <c>(is_online,
    /// updated_at)</c>. The prefix cannot be priced from statistics, so it must weigh the same in
    /// both steps, and when the two still tie the step that binds the range column must win over
    /// the one whose name sorts first.
    /// </summary>
    [Test]
    public async Task BoolEqualityPrefixPlusRange_PicksTheIndexWithTheRangeColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        StringBuilder values = new();
        for (int i = 0; i < RowCount; i++)
        {
            if (values.Length > 0)
                values.Append(", ");

            values.Append(string.Create(CultureInfo.InvariantCulture,
                $"({i + 1}, {(i % 4 == 0 ? "false" : "true")}, {i % 5}, {BaseTime + i})"));
        }

        foreach (string table in new[] { "presence_plain", "presence" })
        {
            await ExecDdl(executor, database, dbname,
                $"CREATE TABLE {table} (id int64 PRIMARY KEY, is_online bool, shard int64, updated_at int64)");
            await ExecNonQuery(executor, database, dbname,
                $"INSERT INTO {table} (id, is_online, shard, updated_at) VALUES {values}");
        }

        await ExecDdl(executor, database, dbname, "CREATE INDEX presence_a_online_shard_updated ON presence (is_online, shard, updated_at)");
        await ExecDdl(executor, database, dbname, "CREATE INDEX presence_b_online_updated ON presence (is_online, updated_at)");
        _ = await ExecQuery(executor, database, dbname, "ANALYZE presence");

        // Every row is older than this bound and the bound value is not a row: the range column
        // alone selects everything, so the two steps can only differ by the prefix.
        long stale = BaseTime + RowCount;
        List<string> failures = new();

        foreach (string where in new[] { $"is_online = true AND updated_at < {stale}", $"is_online = true AND updated_at < {Low}" })
        {
            long[] plain = await SelectIds(executor, database, dbname, "presence_plain", where);
            Assert.That(plain, Is.Not.Empty);

            for (int run = 0; run < 2; run++)
            {
                long[] ids = await SelectIds(executor, database, dbname, "presence", where);
                if (!ids.SequenceEqual(plain))
                    failures.Add($"{where} run {run}: {ids.Length} rows, expected {plain.Length}");
            }

            (string detail, long rowsRead) = await ExplainScan(executor, database, dbname, $"SELECT id FROM presence WHERE {where}");
            if (!detail.Contains("index=presence_b_online_updated", StringComparison.Ordinal))
                failures.Add($"{where} picked another index: {detail}");
            if (rowsRead != plain.Length)
                failures.Add($"{where} read {rowsRead} entries, expected {plain.Length}: {detail}");
        }

        Assert.That(failures, Is.Empty, string.Join("\n", failures));
    }

    /// <summary>
    /// <c>FORCE_INDEX</c> names the index the scan must use. When the predicate can bound that
    /// index, the scan uses those bounds; it does not compete with the other indexes.
    /// </summary>
    [Test]
    public async Task ForceIndex_SteersTheScanToTheNamedIndex_WithItsPredicateBounds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<Row> rows) = await CreateTables(analyze: true);

        string where = $"{StudioEquality} AND created_at >= {From}";
        long[] expected = Ids(rows.Where(r => r.CreatedAt >= From));

        foreach ((string index, long entries) in new[] { (PrefixOnlyIndex, (long)RowCount), (RangeIndex, expected.Length), ("audit_d_studio_env_created", (long)RowCount) })
        {
            string source = $"audit@{{FORCE_INDEX={index}}}";

            // Twice: the second run replays the cached plan.
            for (int run = 0; run < 2; run++)
            {
                long[] ids = await SelectIds(executor, database, dbname, source, where);
                Assert.That(ids, Is.EqualTo(expected), $"{index} run {run}");
            }

            (string detail, long rowsRead) = await ExplainScan(executor, database, dbname, $"SELECT id FROM {source} WHERE {where}");
            Assert.That(detail, Does.Contain($"index={index}"), detail);
            Assert.That(rowsRead, Is.EqualTo(entries), $"{index}: {detail}");
        }
    }
}
