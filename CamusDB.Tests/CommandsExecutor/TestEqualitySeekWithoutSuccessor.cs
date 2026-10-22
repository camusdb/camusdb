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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An equality on a non-unique index seeks the index even when the planner cannot compute the
/// value's successor: a type with no successor (date, datetime, float32, bytes) or a value at its
/// type's maximum (<c>true</c>, the largest int64). The seek then uses the inclusive <c>[v, v]</c>
/// range, which ScanIndex widens with the index sentinel and trims back to <c>key == v</c>.
///
/// <para>Every predicate runs on a table with no secondary index and on the same rows with an index,
/// and both must return the rows the test computes from its own data. The data puts a value next to
/// every probed value (one second later, the next day, the next half, a byte string that extends the
/// probed one, the largest int64 minus one), so an end bound that is too wide or too narrow shows as a
/// wrong row.</para>
/// </summary>
[NonParallelizable]
public sealed class TestEqualitySeekWithoutSuccessor : SharedNodeBaseTest
{
    private const int RowCount = 60;

    private static readonly DateTime BaseTime = new(2000, 1, 1, 10, 0, 0, DateTimeKind.Utc);

    private sealed record Row(long Id, DateTime Dt, DateTime Dd, float F, byte[] B, bool Flag, long N, long G);

    private static List<Row> BuildRows()
    {
        List<Row> rows = new(RowCount);

        for (int i = 1; i <= RowCount; i++)
        {
            // Every third row of a day is one second after the others, so dt = v has a neighbor.
            DateTime dt = BaseTime.AddDays(i % 10).AddSeconds(i % 3 == 0 ? 1 : 0);
            DateTime dd = BaseTime.Date.AddDays(i % 7);
            float f = (i % 8) * 0.5f;

            // Every fourth row extends its byte string by one zero byte: X'0003' and X'000300'.
            byte[] b = i % 4 == 0 ? [0x00, (byte)(i % 5), 0x00] : [0x00, (byte)(i % 5)];

            long n = i switch
            {
                1 => long.MaxValue,
                2 => long.MaxValue - 1,
                _ => i,
            };

            rows.Add(new Row(i, dt, dd, f, b, i % 10 == 0, n, i));
        }

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
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>
    /// Creates three tables with the rows of <see cref="BuildRows"/>: <c>plain</c> (no secondary
    /// index), <c>indexed</c> (a non-unique index on each probed column) and <c>composite</c> (only the
    /// non-unique index <c>(dt, g)</c>, so no single-column index competes with it).
    /// </summary>
    private async Task<(string, DatabaseDescriptor, CommandExecutor, List<Row>)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        List<Row> rows = BuildRows();

        StringBuilder values = new();
        foreach (Row row in rows)
        {
            if (values.Length > 0)
                values.Append(", ");

            values.Append(string.Create(CultureInfo.InvariantCulture,
                $"({row.Id}, '{row.Dt:yyyy-MM-ddTHH:mm:ss}Z', '{row.Dd:yyyy-MM-dd}', {row.F:0.0}, " +
                $"X'{Convert.ToHexString(row.B)}', {(row.Flag ? "true" : "false")}, {row.N}, {row.G})"));
        }

        foreach (string table in new[] { "plain", "indexed", "composite" })
        {
            await ExecDdl(executor, database, dbname,
                $"CREATE TABLE {table} (id int64 PRIMARY KEY, dt datetime, dd date, f float32, b bytes, flag bool, n int64, g int64)");
            await ExecNonQuery(executor, database, dbname,
                $"INSERT INTO {table} (id, dt, dd, f, b, flag, n, g) VALUES {values}");
        }

        foreach (string column in new[] { "dt", "dd", "f", "b", "flag", "n" })
            await ExecDdl(executor, database, dbname, $"CREATE INDEX indexed_{column} ON indexed ({column})");

        await ExecDdl(executor, database, dbname, "CREATE INDEX composite_dt_g ON composite (dt, g)");

        return (dbname, database, executor, rows);
    }

    private static async Task<long[]> SelectIds(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string table, string where,
        Dictionary<string, ColumnValue> parameters)
    {
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            $"SELECT id FROM {table} WHERE {where} ORDER BY id", parameters);

        return rows.Select(r => r.Row["id"].LongValue).ToArray();
    }

    private static async Task<string> Explain(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string table, string where,
        Dictionary<string, ColumnValue> parameters)
    {
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            $"EXPLAIN SELECT id FROM {table} WHERE {where}", parameters);

        return string.Join(" | ", rows.Select(r =>
            (r.Row.TryGetValue("node", out ColumnValue? node) ? node.StrValue : "") + " " +
            (r.Row.TryGetValue("detail", out ColumnValue? detail) ? detail.StrValue : "")));
    }

    /// <summary>
    /// Runs <paramref name="where"/> once on <c>plain</c> and twice on <paramref name="table"/>, and
    /// records each result that is not <paramref name="expected"/>. The second run on
    /// <paramref name="table"/> replays the cached plan, which re-binds the cached index through the
    /// forced-index matcher, so that path must seek the index too. Last, records a plan on
    /// <paramref name="table"/> that does not seek <paramref name="index"/>.
    /// </summary>
    private static async Task Check(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string table,
        string where, Dictionary<string, ColumnValue> parameters, string index,
        long[] expected, List<string> failures)
    {
        Assert.That(expected, Is.Not.Empty, $"the data must hold a row for {where}");

        foreach (string from in new[] { "plain", table, table })
        {
            long[] ids = await SelectIds(executor, database, dbname, from, where, parameters);
            if (!ids.SequenceEqual(expected))
                failures.Add($"{from}: {where} gave [{string.Join(", ", ids)}], expected [{string.Join(", ", expected)}]");
        }

        string plan = await Explain(executor, database, dbname, table, where, parameters);
        if (!plan.Contains("index-range-scan") || !plan.Contains($"index={index}"))
            failures.Add($"{table}: {where} does not seek {index}: {plan}");
    }

    private static long[] Ids(IEnumerable<Row> rows) => rows.Select(r => r.Id).OrderBy(id => id).ToArray();

    [Test]
    public async Task Equality_OnATypeWithNoSuccessor_SeeksTheIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<Row> rows) = await CreateTables();

        DateTime dt = BaseTime.AddDays(2);
        DateTime dd = BaseTime.Date.AddDays(3);
        float f = 1.5f;
        byte[] b = [0x00, 0x03];

        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@dt"] = new ColumnValue(ColumnType.DateTime, dt.Ticks),
            ["@dd"] = new ColumnValue(ColumnType.Date, dd.Ticks),
            ["@f"] = new ColumnValue(ColumnType.Float32, (double)f),
            ["@b"] = new ColumnValue(b),
        };

        List<string> failures = [];

        await Check(executor, database, dbname, "indexed", "dt = @dt", parameters, "indexed_dt",
            Ids(rows.Where(r => r.Dt == dt)), failures);
        await Check(executor, database, dbname, "indexed", "dd = @dd", parameters, "indexed_dd",
            Ids(rows.Where(r => r.Dd == dd)), failures);
        await Check(executor, database, dbname, "indexed", "f = @f", parameters, "indexed_f",
            Ids(rows.Where(r => r.F == f)), failures);
        await Check(executor, database, dbname, "indexed", "b = @b", parameters, "indexed_b",
            Ids(rows.Where(r => r.B.AsSpan().SequenceEqual(b))), failures);

        Assert.That(failures, Is.Empty, string.Join("\n", failures));
    }

    [Test]
    public async Task Equality_AtTheTypesMaximum_SeeksTheIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<Row> rows) = await CreateTables();

        Dictionary<string, ColumnValue> parameters = new();
        List<string> failures = [];

        await Check(executor, database, dbname, "indexed", "flag = true", parameters, "indexed_flag",
            Ids(rows.Where(r => r.Flag)), failures);
        await Check(executor, database, dbname, "indexed", $"n = {long.MaxValue}", parameters, "indexed_n",
            Ids(rows.Where(r => r.N == long.MaxValue)), failures);
        await Check(executor, database, dbname, "indexed", $"n = {long.MaxValue - 1}", parameters, "indexed_n",
            Ids(rows.Where(r => r.N == long.MaxValue - 1)), failures);

        Assert.That(failures, Is.Empty, string.Join("\n", failures));
    }

    [Test]
    public async Task CompositeIndex_EqualityPrefixWithNoSuccessor_SeeksTheIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor, List<Row> rows) = await CreateTables();

        DateTime dt = BaseTime.AddDays(4);
        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@dt"] = new ColumnValue(ColumnType.DateTime, dt.Ticks),
        };

        List<string> failures = [];

        // A range on the second column with a lower bound only, an upper bound only, and both.
        await Check(executor, database, dbname, "composite", "dt = @dt AND g > 20", parameters, "composite_dt_g",
            Ids(rows.Where(r => r.Dt == dt && r.G > 20)), failures);
        await Check(executor, database, dbname, "composite", "dt = @dt AND g < 40", parameters, "composite_dt_g",
            Ids(rows.Where(r => r.Dt == dt && r.G < 40)), failures);
        await Check(executor, database, dbname, "composite", "dt = @dt AND g >= 14 AND g <= 44", parameters, "composite_dt_g",
            Ids(rows.Where(r => r.Dt == dt && r.G >= 14 && r.G <= 44)), failures);

        // The equality prefix alone.
        await Check(executor, database, dbname, "composite", "dt = @dt", parameters, "composite_dt_g",
            Ids(rows.Where(r => r.Dt == dt)), failures);

        Assert.That(failures, Is.Empty, string.Join("\n", failures));
    }
}
