/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Shared driver for the index-selection differential tests. A differential case runs one query
/// against a fresh database, creates an index, and runs the same query again. Both results must
/// equal the expected rows: comparing "before" to "after" alone would pass when both sides are
/// wrong, so the expectation is always spelled out. Standalone and cluster fixtures share this
/// driver because the planner is the same on both; only the index backfill differs.
/// </summary>
internal static class IndexDifferentialProbe
{
    /// <summary>
    /// <c>probe(id, a INT NOT NULL, b INT, f FLOAT NOT NULL)</c> and
    /// <c>other(id, a INT NOT NULL, b INT)</c>. The first row of each table carries a NULL in
    /// <c>b</c>, so it has no entry in any unique index that includes <c>b</c>.
    /// </summary>
    public static async Task CreateFixture(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        bool extraRowInGroupOne = false)
    {
        await ExecDDL(executor, database, dbname,
            "CREATE TABLE probe (id OID PRIMARY KEY NOT NULL, a INT NOT NULL, b INT, f FLOAT NOT NULL)");
        await ExecDML(executor, database, dbname,
            "INSERT INTO probe (id, a, b, f) VALUES (gen_id(), 1, NULL, 1.0)");
        await ExecDML(executor, database, dbname,
            "INSERT INTO probe (id, a, b, f) VALUES (gen_id(), 2, 5, 2.5)");

        if (extraRowInGroupOne)
            await ExecDML(executor, database, dbname,
                "INSERT INTO probe (id, a, b, f) VALUES (gen_id(), 1, 3, 1.0)");

        await ExecDDL(executor, database, dbname,
            "CREATE TABLE other (id OID PRIMARY KEY NOT NULL, a INT NOT NULL, b INT)");
        await ExecDML(executor, database, dbname,
            "INSERT INTO other (id, a, b) VALUES (gen_id(), 1, NULL)");
        await ExecDML(executor, database, dbname,
            "INSERT INTO other (id, a, b) VALUES (gen_id(), 2, 7)");
    }

    /// <summary>
    /// Runs <paramref name="query"/> before and after <paramref name="indexDdl"/> and asserts both
    /// results equal <paramref name="expected"/>, rendered by <see cref="Render"/>.
    /// </summary>
    public static async Task AssertSameRowsBeforeAndAfterIndex(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string query,
        string indexDdl,
        string expected)
    {
        string before = await QueryRendered(executor, database, dbname, query);
        Assert.AreEqual(expected, before, $"Table scan (no index) returned the wrong rows for: {query}");

        await ExecDDL(executor, database, dbname, indexDdl);

        string after = await QueryRendered(executor, database, dbname, query);
        Assert.AreEqual(expected, after, $"After `{indexDdl}` the index path returned different rows for: {query}");
    }

    public static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    public static async Task ExecDML(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>
    /// Runs a query and renders the rows as a canonical sorted string, e.g. <c>a=1;a=2</c>.
    /// Any exception propagates: a failing index scan must fail the test, not hide as a difference.
    /// </summary>
    public static async Task<string> QueryRendered(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return Render(rows);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// Returns every EXPLAIN row as <c>node(detail)</c>, or <c>node</c> when the detail is empty,
    /// so a test can match on the operator name by prefix and on its detail by substring.
    /// </summary>
    public static async Task<string[]> ExplainNodes(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: "EXPLAIN " + sql, parameters: null);
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows.Select(r =>
            {
                string node = r.Row["node"].StrValue ?? "";
                string detail = r.Row.TryGetValue("detail", out ColumnValue? dv) ? dv?.StrValue ?? "" : "";
                return detail.Length == 0 ? node : $"{node}({detail})";
            }).ToArray();
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    public static string Render(IEnumerable<QueryResultRow> rows)
    {
        List<string> rendered = rows
            .Select(r => string.Join(",", r.Row.OrderBy(kv => kv.Key, System.StringComparer.Ordinal).Select(kv => kv.Key + "=" + Render(kv.Value))))
            .ToList();
        rendered.Sort(System.StringComparer.Ordinal);
        return string.Join(";", rendered);
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Float64 or ColumnType.Float32 => value.FloatValue.ToString("R", CultureInfo.InvariantCulture),
        ColumnType.Bool => value.BoolValue ? "true" : "false",
        _ => value.StrValue ?? value.ToString() ?? "",
    };
}
