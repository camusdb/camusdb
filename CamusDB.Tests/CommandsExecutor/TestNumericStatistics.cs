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

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Statistics.Models;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Statistics over a NUMERIC column: the write-time min/max, ANALYZE (distinct count, histogram,
/// bounds), SHOW STATISTICS rendering, and the estimate math in <see cref="ScalarBound"/> and
/// <see cref="ColumnHistogram"/>. The values include two that differ only in the high 64 bits of
/// the unscaled value, so a comparison or distinct key that reads only the low half fails.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestNumericStatistics : BaseTest
{
    private const string Min = "-99999999999999999999999999999.999999999";

    private const string Max = "99999999999999999999999999999.999999999";

    /// <summary>1 × 2⁶⁴ + 5 and 2 × 2⁶⁴ + 5 unscaled: the same low half.</summary>
    private static readonly string SameLowA = NumericMath.Format(((Int128)1 << 64) + 5);

    private static readonly string SameLowB = NumericMath.Format(((Int128)2 << 64) + 5);

    private static ScalarBound Bound(string text) => ScalarBound.FromColumnValue(ColumnValue.FromNumericString(text));

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, DatabaseDescriptor database, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: database.Name, sql: sql, parameters: null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor database, string sql, bool ddl = false)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(tx, database.Name, sql, null);
            if (ddl)
                await executor.ExecuteDDLSQL(ticket);
            else
                await executor.ExecuteNonSQLQuery(ticket);
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static string? Text(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? v) && v.Type != ColumnType.Null ? v.StrValue : null;

    private static long? Number(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? v) && v.Type != ColumnType.Null ? v.LongValue : null;

    private static QueryResultRow ColumnRow(List<QueryResultRow> rows, string column)
        => rows.Single(r => Text(r, "kind") == "column" && Text(r, "target") == column);

    [Test]
    public async Task ShowStatistics_ReportsNumericBounds_DistinctCount_AndHistogram()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, database, "CREATE TABLE ledger (id int64 PRIMARY KEY, p numeric)", ddl: true);
        await Exec(executor, database, "CREATE INDEX ledger_p ON ledger (p)", ddl: true);

        // Written first, so a min/max that never moves off the first value fails.
        List<string> values = ["0.5", SameLowA, SameLowB, Max, "-1.25", Min, "0.5", "7"];
        string rows = string.Join(", ", values.Select((v, i) => $"({i}, NUMERIC '{v}')"));
        await Exec(executor, database, $"INSERT INTO ledger (id, p) VALUES {rows}, (99, NULL)");

        // Write-time bounds, before any ANALYZE.
        QueryResultRow before = ColumnRow(await Query(executor, database, "SHOW STATISTICS FOR ledger"), "p");
        Assert.AreEqual(Min, Text(before, "min_value"), "write-time min");
        Assert.AreEqual(Max, Text(before, "max_value"), "write-time max");

        await Query(executor, database, "ANALYZE ledger");

        QueryResultRow after = ColumnRow(await Query(executor, database, "SHOW STATISTICS FOR ledger"), "p");
        Assert.AreEqual(Min, Text(after, "min_value"));
        Assert.AreEqual(Max, Text(after, "max_value"));
        Assert.AreEqual(7, Number(after, "distinct_count"), "0.5 twice; the two same-low-half values are distinct");
        Assert.Greater(Number(after, "histogram_buckets") ?? 0, 0, "ANALYZE builds a NUMERIC histogram");

        // The estimate feeds the plan; the rows must stay exact whatever the estimate is.
        Assert.AreEqual(3, (await Query(executor, database, "SELECT id FROM ledger WHERE p > 1000000000")).Count);
        Assert.AreEqual(2, (await Query(executor, database, "SELECT id FROM ledger WHERE p < NUMERIC '0.5'")).Count);
    }

    [Test]
    public void ScalarBound_ComparesNumericExactly_BySignAndByTheHighHalf()
    {
        Assert.Less(Bound(SameLowA).CompareTo(Bound(SameLowB)), 0, "same low half, high half decides");
        Assert.Greater(Bound("0.000000001").CompareTo(Bound("-0.000000001")), 0, "sign");
        Assert.Less(Bound(Min).CompareTo(Bound(Max)), 0);
        Assert.AreEqual(0, Bound("1.50").CompareTo(Bound("1.5")));

        // A bound of another numeric type (a predicate constant) compares by value as a double.
        ScalarBound five = ScalarBound.FromColumnValue(new ColumnValue(ColumnType.Integer64, 5L));
        Assert.Less(five.CompareTo(Bound("5.5")), 0);
        Assert.Greater(Bound("5.5").CompareTo(five), 0);
        Assert.AreEqual(0, five.CompareTo(Bound("5")));
    }

    [Test]
    public void ScalarBound_NumericSurvivesTheStatisticsJson()
    {
        ScalarBound original = Bound(Min);
        string json = System.Text.Json.JsonSerializer.Serialize(original, CamusDB.Core.Catalogs.MetaJsonContext.Default.ScalarBound);
        ScalarBound back = System.Text.Json.JsonSerializer.Deserialize(json, CamusDB.Core.Catalogs.MetaJsonContext.Default.ScalarBound)!;

        Assert.AreEqual(ColumnType.Numeric, back.Type);
        Assert.AreEqual(0, original.CompareTo(back));
        Assert.IsTrue(back.TryToDouble(out double d));
        Assert.AreEqual(-1e29, d, 1e15);
    }

    [Test]
    public void Histogram_InterpolatesInsideANumericBucket_ForNumericAndInt64Constants()
    {
        ColumnHistogram histogram = new()
        {
            TotalRows = 100,
            MinValue = Bound("0"),
            Buckets =
            [
                new ColumnHistogramBucket { UpperBound = Bound("100"), CumulativeRows = 100 },
            ],
        };

        Assert.AreEqual(0.25, histogram.CumulativeFraction(Bound("25")), 1e-9);
        Assert.AreEqual(0.25, histogram.CumulativeFraction(ScalarBound.FromColumnValue(new ColumnValue(ColumnType.Integer64, 25L))), 1e-9,
            "an INT64 constant against a NUMERIC histogram");
        Assert.AreEqual(0.0, histogram.CumulativeFraction(Bound("-1")));
        Assert.AreEqual(1.0, histogram.CumulativeFraction(Bound("101")));
    }
}
