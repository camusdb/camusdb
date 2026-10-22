/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
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
/// Comparisons between a date, a datetime and a string. A date compares as the datetime at its
/// midnight, and a string is parsed into the other operand's type, so <c>day = '2026-03-15'</c>,
/// <c>created &gt;= '2026-01-01'</c> and <c>created = day</c> work without a CAST. A string that does
/// not parse is an error, never a silent non-match.
///
/// <para>Every predicate runs twice: on a table with no secondary index, where the evaluator decides
/// each row, and on the same rows with an index on each temporal column, where the planner converts
/// the constant before it builds the index key. Both must return the same rows. Filler rows in 2050
/// make the 2000 ranges selective, so the planner seeks the index for them.</para>
/// </summary>
[NonParallelizable]
public sealed class TestTemporalComparison : SharedNodeBaseTest
{
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
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>
    /// Creates <c>plain</c> (no secondary index) and <c>indexed</c> (an index on each of <c>dt</c> and
    /// <c>dd</c>), with the same rows:
    /// 1 = 2000-01-01 at midnight, 2 = 2000-01-02 at 10:00, 3 = 2000-01-03 at midnight, and 40 filler
    /// rows at noon from 2050-01-01. <c>dd</c> is the UTC day of <c>dt</c>.
    /// </summary>
    private async Task<(string, DatabaseDescriptor, CommandExecutor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        StringBuilder values = new(
            "(1, '2000-01-01T00:00:00Z', '2000-01-01'), " +
            "(2, '2000-01-02T10:00:00Z', '2000-01-02'), " +
            "(3, '2000-01-03T00:00:00Z', '2000-01-03')");

        DateTime filler = new(2050, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        for (int i = 0; i < 40; i++)
        {
            DateTime at = filler.AddDays(i);
            values.Append($", ({100 + i}, '{at:yyyy-MM-ddTHH:mm:ss}Z', '{at:yyyy-MM-dd}')");
        }

        foreach (string table in new[] { "plain", "indexed" })
        {
            await ExecDdl(executor, database, dbname, $"CREATE TABLE {table} (id int64 PRIMARY KEY, dt datetime, dd date)");
            await ExecNonQuery(executor, database, dbname, $"INSERT INTO {table} (id, dt, dd) VALUES {values}");
        }

        await ExecDdl(executor, database, dbname, "CREATE INDEX indexed_dt ON indexed (dt)");
        await ExecDdl(executor, database, dbname, "CREATE INDEX indexed_dd ON indexed (dd)");


        return (dbname, database, executor);
    }

    private static async Task AssertIds(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string where, params long[] expected)
    {
        foreach (string table in new[] { "plain", "indexed" })
        {
            List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
                $"SELECT id FROM {table} WHERE {where} ORDER BY id");

            Assert.That(rows.Select(r => r.Row["id"].LongValue).ToArray(), Is.EqualTo(expected), $"{table}: {where}");
        }
    }

    private static async Task<string> ExplainIndexed(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string where,
        Dictionary<string, ColumnValue> parameters)
    {
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, $"EXPLAIN SELECT id FROM indexed WHERE {where}", parameters);

        return string.Join(" | ", rows.Select(r =>
            (r.Row.TryGetValue("node", out ColumnValue? node) ? node.StrValue : "") + " " +
            (r.Row.TryGetValue("detail", out ColumnValue? detail) ? detail.StrValue : "")));
    }

    /// <summary>Runs <paramref name="where"/> on both tables and records each result that differs.</summary>
    private static async Task CollectMismatch(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string where,
        Dictionary<string, ColumnValue> parameters, long[] expected, List<string> failures)
    {
        foreach (string table in new[] { "plain", "indexed" })
        {
            List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
                $"SELECT id FROM {table} WHERE {where} ORDER BY id", parameters);
            long[] ids = rows.Select(r => r.Row["id"].LongValue).ToArray();
            if (!ids.SequenceEqual(expected))
                failures.Add($"{table}: {where} gave [{string.Join(", ", ids)}]");
        }
    }

    private static async Task AssertInvalidInput(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql, string messagePart)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        }, sql);

        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code, sql);
        Assert.That(ex.Message, Does.Contain(messagePart), sql);
    }

    [Test]
    public async Task DateTimeColumn_ComparesWithADate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await AssertIds(executor, database, dbname, "dt = make_date(2000, 1, 1)", 1);
        await AssertIds(executor, database, dbname, "make_date(2000, 1, 3) = dt", 3);
        await AssertIds(executor, database, dbname, "dt <> make_date(2000, 1, 1) AND dt < make_date(2001, 1, 1)", 2, 3);
        await AssertIds(executor, database, dbname, "dt < make_date(2000, 1, 2)", 1);
        await AssertIds(executor, database, dbname, "dt > make_date(2000, 1, 2) AND dt <= make_date(2000, 1, 3)", 2, 3);
        await AssertIds(executor, database, dbname, "dt BETWEEN make_date(2000, 1, 2) AND make_date(2000, 1, 3)", 2, 3);

        // The row column of another type compares the same way.
        await AssertIds(executor, database, dbname, "dt = dd", 1, 3);
        await AssertIds(executor, database, dbname, "dd < dt", 2, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
            110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
            130, 131, 132, 133, 134, 135, 136, 137, 138, 139);
    }

    [Test]
    public async Task DateColumn_ComparesWithADateTime()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        // A datetime at midnight is that date.
        await AssertIds(executor, database, dbname, "dd = make_timestamp(2000, 1, 1, 0, 0, 0)", 1);
        await AssertIds(executor, database, dbname, "dd >= make_timestamp(2000, 1, 2, 0, 0, 0) AND dd <= make_date(2000, 12, 31)", 2, 3);

        // Any other datetime equals no date, and a range bound compares with the date's midnight.
        await AssertIds(executor, database, dbname, "dd = make_timestamp(2000, 1, 2, 10, 0, 0)");
        await AssertIds(executor, database, dbname, "dd < make_timestamp(2000, 1, 2, 10, 0, 0)", 1, 2);
        await AssertIds(executor, database, dbname, "dd > make_timestamp(2000, 1, 2, 10, 0, 0) AND dd < make_date(2001, 1, 1)", 3);
    }

    [Test]
    public async Task TemporalColumns_CompareWithAStringLiteral()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await AssertIds(executor, database, dbname, "dd = '2000-01-01'", 1);
        await AssertIds(executor, database, dbname, "'2000-01-03' = dd", 3);
        await AssertIds(executor, database, dbname, "dd >= '2000-01-02' AND dd < '2001-01-01'", 2, 3);

        await AssertIds(executor, database, dbname, "dt = '2000-01-02T10:00:00Z'", 2);
        await AssertIds(executor, database, dbname, "dt = '2000-01-02T12:00:00+02:00'", 2);

        // A bare date is midnight UTC of that day.
        await AssertIds(executor, database, dbname, "dt = '2000-01-01'", 1);
        await AssertIds(executor, database, dbname, "dt >= '2000-01-02' AND dt < '2001-01-01'", 2, 3);
        await AssertIds(executor, database, dbname, "dt BETWEEN '2000-01-01' AND '2000-01-02'", 1);
    }

    [Test]
    public async Task TemporalColumns_InLists()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await AssertIds(executor, database, dbname, "dd IN ('2000-01-01', '2000-01-03')", 1, 3);
        await AssertIds(executor, database, dbname, "dt IN ('2000-01-01', '2000-01-02T10:00:00Z')", 1, 2);
        await AssertIds(executor, database, dbname, "dd NOT IN ('2000-01-01') AND dd < '2001-01-01'", 2, 3);

        // More than eight items: the list is a hash set, and a string item must still find a date.
        string many = string.Join(", ", Enumerable.Range(1, 12).Select(i => $"'1990-01-{i:D2}'"));
        await AssertIds(executor, database, dbname, $"dd IN ({many}, '2000-01-03')", 3);
        await AssertIds(executor, database, dbname, $"dt IN ({many}, '2000-01-02T10:00:00Z')", 2);
    }

    [Test]
    public async Task IndexSeek_UsesTheConvertedConstant()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        // A typed parameter is a constant to the planner, as a literal is; a function call is not.
        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@day"] = new ColumnValue(ColumnType.Date, new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks),
            ["@midnight"] = new ColumnValue(ColumnType.DateTime, new DateTime(2000, 1, 3, 0, 0, 0, DateTimeKind.Utc).Ticks),
            ["@morning"] = new ColumnValue(ColumnType.DateTime, new DateTime(2000, 1, 2, 10, 0, 0, DateTimeKind.Utc).Ticks),
            ["@text"] = new ColumnValue(ColumnType.String, "2000-01-02T10:00:00Z"),
        };

        // Each predicate must seek the index with the converted constant, and the seek must return
        // the rows the scan returns. A temporal type has no successor value, so an equality seeks the
        // inclusive [v, v] range.
        List<string> failures = [];
        foreach ((string where, string index, long[] expected) in new[]
        {
            ("dt = @day", "indexed_dt", new long[] { 1 }),
            ("dt = @text", "indexed_dt", new long[] { 2 }),
            ("dd = @midnight", "indexed_dd", new long[] { 3 }),
            ("dt >= @day AND dt <= @day", "indexed_dt", new long[] { 1 }),
            ("dt >= @text AND dt <= @text", "indexed_dt", new long[] { 2 }),
            ("dt >= '2000-01-02' AND dt < '2001-01-01'", "indexed_dt", new long[] { 2, 3 }),
            ("dt > @day AND dt < '2001-01-01'", "indexed_dt", new long[] { 2, 3 }),
            ("dt IN ('2000-01-01', '2000-01-02T10:00:00Z')", "indexed_dt", new long[] { 1, 2 }),
            ("dd >= '2000-01-01' AND dd <= '2000-01-01'", "indexed_dd", new long[] { 1 }),
            ("dd >= @midnight AND dd < '2001-01-01'", "indexed_dd", new long[] { 3 }),
            ("dd IN ('2000-01-01', '2000-01-03')", "indexed_dd", new long[] { 1, 3 }),
        })
        {
            string plan = await ExplainIndexed(executor, database, dbname, where, parameters);
            if (!plan.Contains(index))
                failures.Add($"{where} does not use {index}: {plan}");

            await CollectMismatch(executor, database, dbname, where, parameters, expected, failures);
        }

        // A datetime that is not a midnight against a date column: equality matches nothing, and a
        // range bound has no date key, so the filter compares with each midnight.
        foreach ((string where, long[] expected) in new[]
        {
            ("dd = @morning", Array.Empty<long>()),
            ("dd < @morning", new long[] { 1, 2 }),
            ("dd IN ('2000-01-01') AND dd > @morning", Array.Empty<long>()),
        })
            await CollectMismatch(executor, database, dbname, where, parameters, expected, failures);

        Assert.That(failures, Is.Empty, string.Join("\n", failures));
    }

    [Test]
    public async Task MalformedString_IsAnError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        foreach (string table in new[] { "plain", "indexed" })
        {
            await AssertInvalidInput(executor, database, dbname,
                $"SELECT id FROM {table} WHERE dt >= '2000-13-01'", "Cannot compare a datetime with '2000-13-01'");
            await AssertInvalidInput(executor, database, dbname,
                $"SELECT id FROM {table} WHERE dd = 'yesterday'", "Cannot compare a date with 'yesterday'");
            await AssertInvalidInput(executor, database, dbname,
                $"SELECT id FROM {table} WHERE dd IN ('2000-01-01', 'yesterday')", "Cannot compare a date with 'yesterday'");
        }

        // A date column takes only the date form; a datetime string is not truncated.
        await AssertInvalidInput(executor, database, dbname,
            "SELECT id FROM plain WHERE dd = '2000-01-01T00:00:00Z'", "expected yyyy-MM-dd");
    }

    [Test]
    public async Task OtherSurfaces_UseTheSameRule()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        // A projection and a simple CASE.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname,
            "SELECT dt = dd AS same, dt > '2000-01-01' AS later, " +
            "CASE dt WHEN make_date(2000, 1, 1) THEN 'first' ELSE 'other' END AS label FROM plain WHERE id = 1");
        Assert.IsTrue(rows[0].Row["same"].BoolValue);
        Assert.IsFalse(rows[0].Row["later"].BoolValue);
        Assert.AreEqual("first", rows[0].Row["label"].StrValue);

        // A string parameter, as a driver sends one.
        rows = await ExecQuery(executor, database, dbname, "SELECT id FROM indexed WHERE dt = @at",
            new() { ["@at"] = new ColumnValue(ColumnType.String, "2000-01-02T10:00:00Z") });
        Assert.That(rows.Select(r => r.Row["id"].LongValue).ToArray(), Is.EqualTo(new long[] { 2 }));

        // A join between a datetime column and a date column.
        rows = await ExecQuery(executor, database, dbname,
            "SELECT p.id AS id FROM plain p JOIN indexed i ON p.dt = i.dd ORDER BY p.id");
        Assert.That(rows.Select(r => r.Row["id"].LongValue).ToArray(), Is.EqualTo(new long[] { 1, 3 }));

        // UPDATE and DELETE find their rows by the converted constant.
        await ExecNonQuery(executor, database, dbname, "UPDATE indexed SET dd = '2000-02-01' WHERE dt = make_date(2000, 1, 1)");
        await ExecNonQuery(executor, database, dbname, "DELETE FROM indexed WHERE dd = '2000-01-03'");
        rows = await ExecQuery(executor, database, dbname, "SELECT id, dd FROM indexed WHERE dt < '2001-01-01' ORDER BY id");
        Assert.That(rows.Select(r => r.Row["id"].LongValue).ToArray(), Is.EqualTo(new long[] { 1, 2 }));
        Assert.AreEqual("2000-02-01", rows[0].Row["dd"].IsoValue);
    }

    [Test]
    public async Task CheckConstraint_ComparesDateAndDateTime()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(executor, database, dbname,
            "CREATE TABLE bookings (id int64 PRIMARY KEY, starts datetime, day date, " +
            "CHECK (starts >= day), CHECK (day >= '2000-01-01'))");

        await ExecNonQuery(executor, database, dbname,
            "INSERT INTO bookings (id, starts, day) VALUES (1, '2000-01-01T09:00:00Z', '2000-01-01')");

        foreach (string row in new[]
        {
            "(2, '1999-12-31T23:00:00Z', '2000-01-01')", // starts before its day
            "(3, '1999-12-31T09:00:00Z', '1999-12-31')", // day before 2000
        })
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            ExecuteSQLTicket ticket = new(txnState: tx, database: dbname,
                sql: $"INSERT INTO bookings (id, starts, day) VALUES {row}", parameters: null);
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteNonSQLQuery(ticket), row);
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex!.Code, row);
        }
    }
}
