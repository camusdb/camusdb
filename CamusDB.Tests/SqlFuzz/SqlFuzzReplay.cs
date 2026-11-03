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
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.SqlFuzz;

/// <summary>
/// The rows of one read: the output column names and the cells of each row, in column order.
/// <see cref="Types"/> holds the declared type of each output column, which the rows alone cannot show
/// for an empty or all-NULL result. <see cref="WireError"/> is set when a caller asked for the HTTP JSON
/// check and a row did not serialize.
/// </summary>
internal sealed record SqlFuzzRows(IReadOnlyList<string> Columns, IReadOnlyList<IReadOnlyList<ColumnValue>> Rows)
{
    public IReadOnlyList<ColumnType> Types { get; init; } = [];

    public string? WireError { get; init; }

    /// <summary>The output shape for <see cref="SqlFuzzValues.ShapeDifference"/>.</summary>
    public IReadOnlyList<(string Name, ColumnType Type)> Shape =>
        Columns.Select((c, i) => (c, i < Types.Count ? Types[i] : ColumnType.Null)).ToList();
}

/// <summary>
/// What a write must do to its table, for <see cref="WriteEffect"/>. <see cref="Kind"/> is
/// <c>insert</c>, <c>insert-select</c>, <c>update</c>, <c>delete</c> or <c>upsert</c>.
/// <list type="bullet">
/// <item><see cref="PredictionSql"/> runs before the write. For an UPDATE it returns the key and the new
/// value of each assigned column (in <see cref="AssignedColumns"/> order) for every row the WHERE
/// selects; for a DELETE, the key of each such row; for an UPSERT with DO UPDATE, the key and the new
/// assigned values of each proposed row whose key exists.</item>
/// <item><see cref="ProposedRowsSql"/> also runs before the write. Its queries return the full rows an
/// INSERT, an INSERT … SELECT or an UPSERT proposes, with the columns of <see cref="ProposedColumns"/>.</item>
/// <item><see cref="InsertedKeys"/> holds the rendered keys of the rows an INSERT or UPSERT names. The
/// rule falls back to it when a proposed-rows query cannot run.</item>
/// </list>
/// </summary>
internal sealed record WriteSpec(
    string Table,
    string Kind,
    string WriteSql,
    string? PredictionSql,
    IReadOnlyList<string> AssignedColumns,
    IReadOnlyList<string> InsertedKeys,
    IReadOnlyList<string>? ProposedRowsSql = null,
    IReadOnlyList<string>? ProposedColumns = null);

/// <summary>
/// The result of one checked write. <see cref="Problem"/> is null when the write had the effect the rule
/// requires. <see cref="Error"/> is the error code of a write that failed. <see cref="Checked"/> is false
/// when the reads the rule needs could not run, so nothing was compared.
/// </summary>
internal readonly record struct WriteResult(string? Problem, string? Error, int Affected, bool Checked);

/// <summary>
/// The rule for the effect of one write on its table. The rule computes the full expected table after
/// the write from the table before it and the predictions, and compares it with the table after the
/// write, row by row and cell by cell:
/// <list type="bullet">
/// <item>A write that fails leaves the table exactly as it was: no changed row, no lost row, and no new
/// row (a multi-row INSERT that keeps its first row and then fails is a finding).</item>
/// <item>DELETE removes the rows its WHERE selected before it ran, and no other row.</item>
/// <item>UPDATE gives each selected row the values the assignments had on the old row, and leaves the
/// other columns and the other rows as they were.</item>
/// <item>INSERT and INSERT … SELECT add exactly the proposed rows, with the proposed value in every
/// column.</item>
/// <item>UPSERT inserts each proposed row whose key is absent. A proposed row whose key exists changes
/// nothing with DO NOTHING, and gets the predicted assigned values with DO UPDATE.</item>
/// <item>The affected-row count is the count of deleted, updated or inserted rows; for an UPSERT it is
/// the inserted rows plus the updated rows, and a skipped conflict counts nothing.</item>
/// </list>
/// The first column of <c>SELECT *</c> is the primary key. The SQL fuzzer and the regression tests it
/// prints share this one copy of the rule.
/// </summary>
internal static class WriteEffect
{
    /// <summary>
    /// Runs <see cref="WriteSpec.WriteSql"/> with the reads it needs, and checks the effect.
    /// <paramref name="query"/> returns null when a read failed; the rule then checks what it still can.
    /// The write always runs, also when a read before it failed, so the history of the table stays the
    /// one the caller records. With <paramref name="checkAfterFailure"/> false (a write inside an
    /// explicit transaction, where a failure makes every later read fail), a failed write is returned
    /// without the after-read; the transaction rule then checks the state after the rollback.
    /// </summary>
    public static async Task<WriteResult> CheckAsync(
        WriteSpec spec,
        Func<string, Task<SqlFuzzRows?>> query,
        Func<string, Task<(int Affected, string? Error)>> write,
        bool checkAfterFailure = true)
    {
        string snapshotSql = "SELECT * FROM " + spec.Table;
        SqlFuzzRows? before = await query(snapshotSql).ConfigureAwait(false);
        SqlFuzzRows? prediction = before is null || spec.PredictionSql is null ? null : await query(spec.PredictionSql).ConfigureAwait(false);
        List<IReadOnlyList<ColumnValue>>? proposed = before is null ? null : await ProposedAsync(spec, query).ConfigureAwait(false);

        (int affected, string? error) = await write(spec.WriteSql).ConfigureAwait(false);

        if (before is null || (error is not null && !checkAfterFailure))
            return new WriteResult(null, error, affected, Checked: false);

        SqlFuzzRows? after = await query(snapshotSql).ConfigureAwait(false);
        if (after is null)
            return new WriteResult("the table cannot be read after the write", error, affected, true);

        string? twins = Twins(before, "before the write") ?? Twins(after, "after the write");
        if (twins is not null)
            return new WriteResult(twins, error, affected, true);

        Dictionary<string, IReadOnlyList<ColumnValue>> old = ByKey(before);
        Dictionary<string, IReadOnlyList<ColumnValue>> now = ByKey(after);

        string? problem = error is not null
            ? Compare(old, now) is { } changed ? $"the write failed ({error}) but changed the table: {changed}" : null
            : Expected(spec, after.Columns, old, now, prediction, proposed, affected);
        return new WriteResult(problem, error, affected, true);
    }

    private static async Task<List<IReadOnlyList<ColumnValue>>?> ProposedAsync(WriteSpec spec, Func<string, Task<SqlFuzzRows?>> query)
    {
        if (spec.ProposedRowsSql is null)
            return null;
        List<IReadOnlyList<ColumnValue>> rows = new();
        foreach (string sql in spec.ProposedRowsSql)
        {
            SqlFuzzRows? result = await query(sql).ConfigureAwait(false);
            if (result is null)
                return null;
            rows.AddRange(result.Rows);
        }
        return rows;
    }

    private static string? Expected(
        WriteSpec spec,
        IReadOnlyList<string> columns,
        Dictionary<string, IReadOnlyList<ColumnValue>> old,
        Dictionary<string, IReadOnlyList<ColumnValue>> now,
        SqlFuzzRows? prediction,
        List<IReadOnlyList<ColumnValue>>? proposed,
        int affected)
    {
        string what = spec.Kind.ToUpperInvariant();
        Dictionary<string, IReadOnlyList<ColumnValue>> expected = new(old, StringComparer.Ordinal);

        switch (spec.Kind)
        {
            case "delete":
            {
                if (prediction is null)
                {
                    // Without the prediction: the write may only remove rows.
                    foreach (string key in now.Keys)
                        if (!old.ContainsKey(key))
                            return $"DELETE added row {key}";
                    if (affected != old.Count - now.Count)
                        return $"DELETE reported {affected} row(s) and took the table from {old.Count} to {now.Count} rows";
                    return Compare(old.Where(kv => now.ContainsKey(kv.Key)).ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal), now);
                }
                HashSet<string> deleted = prediction.Rows.Select(r => SqlFuzzValues.Render(r[0])).ToHashSet(StringComparer.Ordinal);
                if (affected != deleted.Count)
                    return $"DELETE reported {affected} row(s); its WHERE selected {deleted.Count} before it ran";
                foreach (string key in deleted)
                    expected.Remove(key);
                return Compare(expected, now, what);
            }

            case "update":
            {
                if (prediction is null)
                    return CompareKeys(old, now);
                Dictionary<string, IReadOnlyList<ColumnValue>> updated = ByKey(prediction);
                if (affected != updated.Count)
                    return $"UPDATE reported {affected} row(s); its WHERE selected {updated.Count} before it ran";
                foreach ((string key, IReadOnlyList<ColumnValue> predicted) in updated)
                {
                    if (!old.TryGetValue(key, out IReadOnlyList<ColumnValue>? row))
                        return $"the UPDATE prediction names row {key}, which the table does not hold";
                    string? assigned = Assign(row, columns, spec.AssignedColumns, predicted, out IReadOnlyList<ColumnValue> changed);
                    if (assigned is not null)
                        return assigned;
                    expected[key] = changed;
                }
                return Compare(expected, now, what);
            }

            case "insert":
            case "insert-select":
            {
                if (proposed is null)
                {
                    // Without the proposed rows: the named keys arrive, and no old row changes.
                    if (spec.Kind == "insert")
                    {
                        if (affected != spec.InsertedKeys.Count)
                            return $"INSERT reported {affected} row(s); it names {spec.InsertedKeys.Count}";
                        foreach (string key in spec.InsertedKeys)
                            if (!now.ContainsKey(key))
                                return $"INSERT reported success but row {key} is missing";
                    }
                    if (affected != now.Count - old.Count)
                        return $"{what} reported {affected} row(s) and took the table from {old.Count} to {now.Count} rows";
                    return Compare(old, now.Where(kv => old.ContainsKey(kv.Key)).ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal));
                }

                if (affected != proposed.Count)
                    return $"{what} reported {affected} row(s); it proposes {proposed.Count}";
                foreach (IReadOnlyList<ColumnValue> row in proposed)
                {
                    IReadOnlyList<ColumnValue> stored = ToTableRow(row, spec.ProposedColumns!, columns);
                    string key = SqlFuzzValues.Render(stored[0]);
                    if (!expected.TryAdd(key, stored))
                        return $"{what} succeeded with key {key}, which the table already held or the statement proposed twice";
                }
                return Compare(expected, now, what);
            }

            case "upsert":
            {
                bool doUpdate = spec.AssignedColumns.Count > 0;
                Dictionary<string, IReadOnlyList<ColumnValue>>? updates = prediction is null ? null : ByKey(prediction);
                if (proposed is null || (doUpdate && updates is null))
                {
                    // Without the predictions: no row goes away, no row the statement does not name
                    // appears, and the rows it does not name stay as they were.
                    HashSet<string> named = spec.InsertedKeys.ToHashSet(StringComparer.Ordinal);
                    foreach (string key in old.Keys)
                        if (!now.ContainsKey(key))
                            return $"UPSERT removed row {key}";
                    foreach (string key in now.Keys)
                        if (!old.ContainsKey(key) && !named.Contains(key))
                            return $"UPSERT added row {key}, which it does not name";
                    return Compare(
                        old.Where(kv => !named.Contains(kv.Key)).ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal),
                        now.Where(kv => !named.Contains(kv.Key)).ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal));
                }

                int inserted = 0;
                int updatedCount = 0;
                HashSet<string> seen = new(StringComparer.Ordinal);
                foreach (IReadOnlyList<ColumnValue> row in proposed)
                {
                    IReadOnlyList<ColumnValue> stored = ToTableRow(row, spec.ProposedColumns!, columns);
                    string key = SqlFuzzValues.Render(stored[0]);
                    if (!seen.Add(key))
                        return $"the UPSERT proposes key {key} twice; the rule cannot predict it";
                    if (!old.TryGetValue(key, out IReadOnlyList<ColumnValue>? existing))
                    {
                        expected[key] = stored;
                        inserted++;
                        continue;
                    }
                    if (!doUpdate)
                        continue;
                    if (!updates!.TryGetValue(key, out IReadOnlyList<ColumnValue>? predicted))
                        return $"the UPSERT prediction has no new values for the existing row {key}";
                    string? assigned = Assign(existing, columns, spec.AssignedColumns, predicted, out IReadOnlyList<ColumnValue> changed);
                    if (assigned is not null)
                        return assigned;
                    expected[key] = changed;
                    updatedCount++;
                }
                if (affected != inserted + updatedCount)
                    return $"UPSERT reported {affected} row(s); it inserts {inserted} and updates {updatedCount}";
                return Compare(expected, now, what);
            }

            default:
                return null;
        }
    }

    /// <summary>The old row with each assigned column replaced by its predicted value (the prediction holds the key first).</summary>
    private static string? Assign(IReadOnlyList<ColumnValue> row, IReadOnlyList<string> columns, IReadOnlyList<string> assigned, IReadOnlyList<ColumnValue> predicted, out IReadOnlyList<ColumnValue> changed)
    {
        ColumnValue[] copy = row.ToArray();
        changed = copy;
        for (int i = 0; i < assigned.Count; i++)
        {
            int column = IndexOf(columns, assigned[i]);
            if (column < 0)
                return $"column {assigned[i]} is not in the table";
            copy[column] = predicted[i + 1];
        }
        return null;
    }

    /// <summary>A proposed row in the column order of <c>SELECT *</c>. A column the statement does not name stays NULL.</summary>
    private static IReadOnlyList<ColumnValue> ToTableRow(IReadOnlyList<ColumnValue> row, IReadOnlyList<string> proposedColumns, IReadOnlyList<string> columns)
    {
        ColumnValue[] stored = new ColumnValue[columns.Count];
        for (int c = 0; c < columns.Count; c++)
        {
            int at = IndexOf(proposedColumns, columns[c]);
            stored[c] = at >= 0 && at < row.Count ? row[at] : ColumnValue.Null;
        }
        return stored;
    }

    private static string? Twins(SqlFuzzRows rows, string when)
    {
        HashSet<string> keys = new(StringComparer.Ordinal);
        foreach (IReadOnlyList<ColumnValue> row in rows.Rows)
            if (!keys.Add(SqlFuzzValues.Render(row[0])))
                return $"the table holds key {SqlFuzzValues.Render(row[0])} twice {when}";
        return null;
    }

    private static Dictionary<string, IReadOnlyList<ColumnValue>> ByKey(SqlFuzzRows rows)
    {
        Dictionary<string, IReadOnlyList<ColumnValue>> map = new(StringComparer.Ordinal);
        foreach (IReadOnlyList<ColumnValue> row in rows.Rows)
            map[SqlFuzzValues.Render(row[0])] = row;
        return map;
    }

    private static int IndexOf(IReadOnlyList<string> columns, string name)
    {
        for (int i = 0; i < columns.Count; i++)
            if (string.Equals(columns[i], name, StringComparison.OrdinalIgnoreCase))
                return i;
        return -1;
    }

    private static string? CompareKeys(Dictionary<string, IReadOnlyList<ColumnValue>> old, Dictionary<string, IReadOnlyList<ColumnValue>> now)
    {
        foreach (string key in old.Keys)
            if (!now.ContainsKey(key))
                return $"row {key} is gone";
        foreach (string key in now.Keys)
            if (!old.ContainsKey(key))
                return $"row {key} appeared";
        return null;
    }

    /// <summary>Null when <paramref name="actual"/> holds exactly the rows of <paramref name="expected"/>, cell by cell.</summary>
    private static string? Compare(Dictionary<string, IReadOnlyList<ColumnValue>> expected, Dictionary<string, IReadOnlyList<ColumnValue>> actual, string? what = null)
    {
        string prefix = what is null ? "" : what + ": ";
        foreach ((string key, IReadOnlyList<ColumnValue> row) in expected)
        {
            if (!actual.TryGetValue(key, out IReadOnlyList<ColumnValue>? found))
                return $"{prefix}row {key} is missing";
            string a = SqlFuzzValues.RenderRow(row);
            string b = SqlFuzzValues.RenderRow(found);
            if (a != b)
                return $"{prefix}row {key} is {b}; the rule expects {a}";
        }
        foreach (string key in actual.Keys)
            if (!expected.ContainsKey(key))
                return $"{prefix}row {key} appeared";
        return null;
    }
}

/// <summary>
/// What the shared rules need from a session: reads, writes and transaction control, each with its
/// error code instead of an exception. A caller decides what an outcome with an internal code means
/// (the fuzzer records a finding, a printed test fails), so the rules see only refusals.
/// </summary>
internal interface ISqlFuzzSession
{
    /// <summary>A read, in <paramref name="transaction"/> or, when it is null, in a read-only snapshot.</summary>
    Task<(SqlFuzzRows? Rows, string? Error)> QueryAsync(string sql, KvTransaction? transaction);

    /// <summary>A write, in <paramref name="transaction"/> or, when it is null, on its own.</summary>
    Task<(int Affected, string? Error)> WriteAsync(string sql, KvTransaction? transaction);

    Task<KvTransaction> BeginAsync();

    /// <summary>Null when the commit succeeded, else its error code.</summary>
    Task<string?> CommitAsync(KvTransaction transaction);

    Task RollbackAsync(KvTransaction transaction);
}

internal enum SqlFuzzVerdictKind
{
    Pass,
    Skip,
    Fail,
}

/// <summary>The verdict of a shared rule: pass, skip with a reason, or fail with a signature and the detail.</summary>
internal sealed record SqlFuzzVerdict(SqlFuzzVerdictKind Kind, string Signature, string Detail)
{
    public static SqlFuzzVerdict Pass { get; } = new(SqlFuzzVerdictKind.Pass, "", "");

    public static SqlFuzzVerdict Skip(string reason) => new(SqlFuzzVerdictKind.Skip, "", reason);

    public static SqlFuzzVerdict Fail(string signature, string detail) => new(SqlFuzzVerdictKind.Fail, signature, detail);
}

/// <summary>
/// The rule for one explicit transaction of writes, shared by the SQL fuzzer and its printed tests.
/// <list type="number">
/// <item>Each write must have its effect (<see cref="WriteEffect"/>), read inside the transaction, so
/// a later write sees the earlier ones.</item>
/// <item>When a statement fails (a generated write, or the statement that fails on purpose), the
/// PostgreSQL rule applies: the next statement, a read included, is refused with <c>CADB0824</c>,
/// COMMIT rolls back and reports <c>CADB0824</c>, and the committed rows equal the rows before the
/// transaction.</item>
/// <item>When no statement fails, COMMIT succeeds, and the committed rows equal the rows the
/// transaction saw at its end.</item>
/// </list>
/// The integrity audit of the tables is the caller's next step.
/// </summary>
internal static class TransactionEffect
{
    public static async Task<(SqlFuzzVerdict Verdict, bool Committed)> CheckAsync(
        IReadOnlyList<WriteSpec> writes,
        string? failingSql,
        bool failureIsDuplicateKey,
        string afterSql,
        bool afterIsQuery,
        IReadOnlyList<string> tables,
        ISqlFuzzSession session)
    {
        Dictionary<string, List<string>> before = new(StringComparer.Ordinal);
        foreach (string table in tables)
        {
            (SqlFuzzRows? rows, string? error) = await session.QueryAsync("SELECT * FROM " + table, null).ConfigureAwait(false);
            if (rows is null)
                return (SqlFuzzVerdict.Skip($"the snapshot of {table} failed: {error}"), false);
            before[table] = SqlFuzzValues.Multiset(rows.Rows);
        }

        KvTransaction tx = await session.BeginAsync().ConfigureAwait(false);
        try
        {
            string? failed = null;
            foreach (WriteSpec spec in writes)
            {
                WriteResult result = await WriteEffect.CheckAsync(spec,
                    async sql => (await session.QueryAsync(sql, tx).ConfigureAwait(false)).Rows,
                    sql => session.WriteAsync(sql, tx),
                    checkAfterFailure: false).ConfigureAwait(false);
                if (result.Problem is not null)
                    return (SqlFuzzVerdict.Fail("dml:effect-in-transaction:" + spec.Kind, $"{result.Problem}\n  in: {spec.WriteSql}"), false);
                if (result.Error is not null)
                {
                    failed = result.Error;
                    break;
                }
            }

            if (failed is null && failingSql is not null)
            {
                (_, string? error) = await session.WriteAsync(failingSql, tx).ConfigureAwait(false);
                if (error is null)
                {
                    return failureIsDuplicateKey
                        ? (SqlFuzzVerdict.Fail("dml:duplicate-key-accepted", $"the second INSERT of one key in one transaction succeeded: {failingSql}"), false)
                        : (SqlFuzzVerdict.Skip("the deliberate failure did not fail: " + failingSql), false);
                }
                failed = error;
            }

            if (failed is not null)
            {
                string? refused = afterIsQuery
                    ? (await session.QueryAsync(afterSql, tx).ConfigureAwait(false)).Error
                    : (await session.WriteAsync(afterSql, tx).ConfigureAwait(false)).Error;
                if (refused != CamusDBErrorCodes.TransactionRollbackOnly)
                    return (SqlFuzzVerdict.Fail("dml:after-failure-not-refused", $"after {failed}, the next statement gave {refused ?? "success"} instead of {CamusDBErrorCodes.TransactionRollbackOnly}: {afterSql}"), false);

                string? commit = await session.CommitAsync(tx).ConfigureAwait(false);
                if (commit != CamusDBErrorCodes.TransactionRollbackOnly)
                    return (SqlFuzzVerdict.Fail("dml:commit-after-failure", $"after {failed}, COMMIT gave {commit ?? "success"} instead of {CamusDBErrorCodes.TransactionRollbackOnly}"), false);

                foreach (string table in tables)
                {
                    (SqlFuzzRows? rows, string? error) = await session.QueryAsync("SELECT * FROM " + table, null).ConfigureAwait(false);
                    if (rows is null)
                        return (SqlFuzzVerdict.Skip($"the snapshot of {table} after the rollback failed: {error}"), false);
                    if (SqlFuzzValues.Difference(before[table], SqlFuzzValues.Multiset(rows.Rows), "before", "after") is { } changed)
                        return (SqlFuzzVerdict.Fail("dml:aborted-transaction-changed-state", $"{table}: {changed}"), false);
                }
                return (SqlFuzzVerdict.Pass, false);
            }

            Dictionary<string, List<string>> final = new(StringComparer.Ordinal);
            foreach (string table in tables)
            {
                (SqlFuzzRows? rows, string? error) = await session.QueryAsync("SELECT * FROM " + table, tx).ConfigureAwait(false);
                if (rows is null)
                    return (SqlFuzzVerdict.Fail("dml:read-in-transaction-failed", $"a read of {table} in a transaction with no failed statement gave {error}"), false);
                final[table] = SqlFuzzValues.Multiset(rows.Rows);
            }

            string? commitError = await session.CommitAsync(tx).ConfigureAwait(false);
            if (commitError is not null)
                return (SqlFuzzVerdict.Fail("dml:commit-failed", $"COMMIT of a transaction with no failed statement gave {commitError}"), false);

            foreach (string table in tables)
            {
                (SqlFuzzRows? rows, string? error) = await session.QueryAsync("SELECT * FROM " + table, null).ConfigureAwait(false);
                if (rows is null)
                    return (SqlFuzzVerdict.Skip($"the snapshot of {table} after the commit failed: {error}"), true);
                if (SqlFuzzValues.Difference(final[table], SqlFuzzValues.Multiset(rows.Rows), "end of the transaction", "committed") is { } changed)
                    return (SqlFuzzVerdict.Fail("dml:commit-changed-state", $"{table}: {changed}"), true);
            }
            return (SqlFuzzVerdict.Pass, true);
        }
        finally
        {
            await session.RollbackAsync(tx).ConfigureAwait(false);
        }
    }
}

/// <summary>
/// Replays the statements of a SQL fuzzer finding in a regression test of this project, and asserts
/// the relation the fuzzer's oracle checked. The fuzzer prints tests that call it; see
/// <c>docs/sql-fuzzer.md</c>.
///
/// <para><b>Setup history</b> replays as the campaign ran it: <see cref="Write"/>, <see cref="Read"/>
/// and <see cref="Transaction"/> record a refusal (a <see cref="CamusDBException"/>) and go on, because
/// the campaign recorded the same refusal the first time. An internal-error code or any other exception
/// fails the test at once. The assertions use the rules the campaign used: <see cref="WriteEffect"/>,
/// <see cref="TransactionEffect"/>, <see cref="GroupPartition"/>, <see cref="SqlFuzzErrorRules"/> and
/// <see cref="SqlFuzzWire"/>.</para>
/// </summary>
internal sealed class SqlFuzzReplay
{
    private readonly CommandExecutor executor;
    private readonly DatabaseDescriptor database;
    private readonly string dbname;

    public SqlFuzzReplay(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        this.executor = executor;
        this.database = database;
        this.dbname = dbname;
    }

    /// <summary>The same database through another executor, for a configuration arm built with <c>CreateCommandExecutor(Options with { … })</c>.</summary>
    public async Task<SqlFuzzReplay> On(CommandExecutor other)
    {
        DatabaseDescriptor descriptor = await other.OpenDatabase(dbname);
        return new SqlFuzzReplay(other, descriptor, dbname);
    }

    /// <summary>The executor of this replay, for a test seam a printed arm sets.</summary>
    public CommandExecutor Executor => executor;

    public async Task Ddl(string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>A setup write on its own. A refusal is returned as the error code; an internal code fails the test.</summary>
    public async Task<(int Affected, string? Error)> Write(string sql)
    {
        try
        {
            return (await WriteStrict(sql, null), null);
        }
        catch (CamusDBException ex)
        {
            FailOnInternal(ex, sql);
            return (0, ex.Code);
        }
    }

    /// <summary>A setup read, for its effect on the caches. A refusal is ignored; an internal code fails the test.</summary>
    public async Task Read(string sql)
    {
        try
        {
            await Query(sql);
        }
        catch (CamusDBException ex)
        {
            FailOnInternal(ex, sql);
        }
    }

    /// <summary>
    /// A setup transaction as the campaign ran it: each statement joins the transaction, a refusal is
    /// recorded and the next statement runs (it is then refused with <c>CADB0824</c>), and COMMIT runs
    /// when <paramref name="commit"/> is true. A refused COMMIT is part of the history. A statement that
    /// starts with SELECT runs as a read in the transaction.
    /// </summary>
    public async Task Transaction(bool commit, params string[] writes)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            foreach (string sql in writes)
            {
                try
                {
                    if (sql.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                        await Query(sql, transaction: tx);
                    else
                        await WriteStrict(sql, tx);
                }
                catch (CamusDBException ex) { FailOnInternal(ex, sql); }
            }
            if (commit)
            {
                try { await database.Transactions.CommitAsync(tx); }
                catch (CamusDBException ex) { FailOnInternal(ex, "COMMIT"); }
            }
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    public async Task Analyze(string table) => await Query("ANALYZE " + table, readWrite: true);

    /// <summary>
    /// Runs a read and returns its rows. Exceptions propagate. With <paramref name="readWrite"/> the read
    /// runs in its own read-write transaction (ANALYZE writes statistics); with
    /// <paramref name="transaction"/> it joins that one; else it runs in a read-only snapshot. With
    /// <paramref name="wireCheck"/> every row is also written in the HTTP JSON row format.
    /// </summary>
    public async Task<SqlFuzzRows> Query(string sql, bool readWrite = false, KvTransaction? transaction = null, bool wireCheck = false)
    {
        bool own = transaction is null && readWrite;
        KvTransaction tx = transaction ?? (readWrite ? await database.Transactions.BeginAsync() : KvTransaction.CreateReadOnly());
        try
        {
            QuerySchemaHolder schema = new();
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null), schemaOut: schema);
            List<IReadOnlyList<ColumnValue>> rows = new();
            List<string>? keys = null;
            SqlFuzzWire? wire = wireCheck ? new SqlFuzzWire() : null;
            string? wireError = null;
            await foreach (QueryResultRow row in cursor)
            {
                keys ??= schema.Schema.Count > 0 ? schema.Schema.Where(c => !c.Hidden).Select(c => c.RowKey).ToList() : row.Row.Keys.ToList();
                if (wire is not null && wireError is null)
                    wireError = wire.TryWrite(row, schema.Schema);
                rows.Add(keys.Select(k => row.Row.TryGetValue(k, out ColumnValue? v) ? v : ColumnValue.Null).ToList());
            }
            if (own)
                await database.Transactions.CommitAsync(tx);
            List<DerivedColumnSchema> visible = schema.Schema.Where(c => !c.Hidden).ToList();
            return new SqlFuzzRows(visible.Select(c => c.Name).ToList(), rows)
            {
                Types = visible.Select(c => c.Type).ToList(),
                WireError = wireError,
            };
        }
        finally
        {
            if (own)
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>The rows of a read as a sorted multiset of row texts.</summary>
    public async Task<List<string>> Rows(string sql) => SqlFuzzValues.Multiset((await Query(sql)).Rows);

    /// <summary>The first cell of the first row; NULL for an empty result.</summary>
    public async Task<ColumnValue> Scalar(string sql)
    {
        SqlFuzzRows rows = await Query(sql);
        return rows.Rows.Count == 0 ? ColumnValue.Null : rows.Rows[0][0];
    }

    /// <summary>The parts of a partition hold, together, exactly the rows of the whole (ternary logic partitioning), with the same output shape.</summary>
    public async Task AssertPartitionRows(string whole, params string[] parts)
    {
        SqlFuzzRows wholeRows = await Query(whole);
        List<List<string>> partRows = new();
        foreach (string part in parts)
        {
            SqlFuzzRows rows = await Query(part);
            Assert.That(SqlFuzzValues.ShapeDifference(wholeRows.Shape, rows.Shape, "whole", "part"), Is.Null, part);
            partRows.Add(SqlFuzzValues.Multiset(rows.Rows));
        }
        Assert.That(SqlFuzzValues.Difference(SqlFuzzValues.Multiset(wholeRows.Rows), SqlFuzzValues.Union(partRows.ToArray()), "whole", "parts"), Is.Null);
    }

    /// <summary>
    /// Each DISTINCT read returns no row twice, and the parts hold, together, exactly the rows of the
    /// whole. The whole is compared as it came, so a duplicate in it is not hidden.
    /// </summary>
    public async Task AssertPartitionDistinct(string whole, params string[] parts)
    {
        List<string> wholeRows = await Rows(whole);
        Assert.That(SqlFuzzValues.Duplicate(wholeRows), Is.Null, "the whole DISTINCT read returned a row twice");
        List<List<string>> partRows = new();
        foreach (string part in parts)
        {
            List<string> rows = await Rows(part);
            Assert.That(SqlFuzzValues.Duplicate(rows), Is.Null, "a DISTINCT part returned a row twice: " + part);
            partRows.Add(rows);
        }
        Assert.That(SqlFuzzValues.Difference(wholeRows, SqlFuzzValues.SetUnion(partRows.ToArray()), "whole", "parts"), Is.Null);
    }

    /// <summary>The aggregate of the whole equals the aggregate combined from the parts.</summary>
    public async Task AssertPartitionAggregate(string function, string whole, params string[] parts)
    {
        SqlFuzzRows wholeRows = await Query(whole);
        ColumnValue expected = wholeRows.Rows.Count == 0 ? ColumnValue.Null : wholeRows.Rows[0][0];
        double? absSum = wholeRows.Rows.Count == 0 ? null : SqlFuzzValues.AbsSum(wholeRows.Columns, wholeRows.Rows[0], 0);
        List<ColumnValue> values = new();
        foreach (string part in parts)
            values.Add(await Scalar(part));
        ColumnValue? combined = SqlFuzzValues.Combine(function, values);
        Assert.That(combined, Is.Not.Null, "the parts cannot be combined exactly");
        Assert.That(SqlFuzzValues.SameAggregate(function, expected, combined!, absSum), Is.True,
            $"{function}: whole {SqlFuzzValues.Render(expected)}, parts combine to {SqlFuzzValues.Render(combined!)}");
    }

    /// <summary>
    /// For a grouped read whose first <paramref name="keyColumns"/> columns are the group key and whose
    /// other columns are the aggregates <paramref name="functions"/>: each group of the whole equals the
    /// same group combined from the parts, and the parts hold no other group.
    /// </summary>
    public async Task AssertPartitionGroups(int keyColumns, string[] functions, string whole, params string[] parts)
    {
        List<SqlFuzzRows> partRows = new();
        foreach (string part in parts)
            partRows.Add(await Query(part));
        string? problem = GroupPartition.Compare(keyColumns, functions, await Query(whole), partRows);
        Assert.That(problem, Is.Null);
    }

    /// <summary>The optimized count equals the number of rows where the predicate is TRUE in the reference projection.</summary>
    public async Task AssertNoRecCount(string optimized, string reference)
    {
        ColumnValue a = await Scalar(optimized);
        ColumnValue b = await Scalar(reference);
        long countA = a.Type == ColumnType.Null ? 0 : a.LongValue;
        long countB = b.Type == ColumnType.Null ? 0 : b.LongValue;
        Assert.That(countA, Is.EqualTo(countB), "optimized WHERE count vs SUM(CASE WHEN p THEN 1 ELSE 0 END)");
    }

    /// <summary>The optimized rows equal the reference rows whose last column (the predicate flag) is 1, with the same output shape.</summary>
    public async Task AssertNoRecRows(string optimized, string reference)
    {
        SqlFuzzRows optimizedRows = await Query(optimized);
        SqlFuzzRows flagged = await Query(reference);
        Assert.That(SqlFuzzValues.ShapeDifference(optimizedRows.Shape, flagged.Shape.Take(flagged.Shape.Count - 1).ToList(), "optimized", "reference"), Is.Null);
        List<string> actual = SqlFuzzValues.Multiset(flagged.Rows
            .Where(r => r[^1].Type == ColumnType.Integer64 && r[^1].LongValue == 1)
            .Select(r => (IReadOnlyList<ColumnValue>)r.Take(r.Count - 1).ToList()));
        Assert.That(SqlFuzzValues.Difference(SqlFuzzValues.Multiset(optimizedRows.Rows), actual, "optimized", "reference"), Is.Null);
    }

    /// <summary>Two plans of one query give the same rows with the same output shape.</summary>
    public static async Task AssertSameRows(SqlFuzzReplay a, string sqlA, SqlFuzzReplay b, string sqlB)
    {
        SqlFuzzRows rowsA = await a.Query(sqlA);
        SqlFuzzRows rowsB = await b.Query(sqlB);
        Assert.That(SqlFuzzValues.ShapeDifference(rowsA.Shape, rowsB.Shape, "first plan", "second plan"), Is.Null);
        Assert.That(SqlFuzzValues.Difference(SqlFuzzValues.Multiset(rowsA.Rows), SqlFuzzValues.Multiset(rowsB.Rows), "first plan", "second plan"), Is.Null);
    }

    /// <summary>
    /// The error-classification rule for one statement, on the entry point the campaign used:
    /// <c>query</c> (a read in a read-only snapshot, with every row also written in the HTTP JSON row
    /// format), <c>analyze</c> (a read in a read-write transaction) or <c>write</c>. The statement must
    /// finish within <paramref name="seconds"/>, a read's rows must serialize, and a failure must be a
    /// <see cref="CamusDBException"/> whose code is not an internal code. Returns null when the rule
    /// holds, else the problem; an exception that is not a <see cref="CamusDBException"/> propagates.
    /// </summary>
    public async Task<string?> ClassifyAsync(string sql, string route, int seconds = 10)
    {
        Task<string?> run = route switch
        {
            "query" => Task.Run(async () => (await Query(sql, wireCheck: true)).WireError),
            "analyze" => Task.Run(async () => { await Query(sql, readWrite: true); return (string?)null; }),
            _ => Task.Run(async () => { await WriteStrict(sql, null); return (string?)null; }),
        };
        if (await Task.WhenAny(run, Task.Delay(TimeSpan.FromSeconds(seconds))) != run)
            return $"the statement did not finish within {seconds} s";
        try
        {
            string? wireError = await run;
            return wireError is null ? null : "a row did not serialize to the HTTP JSON row format: " + wireError;
        }
        catch (CamusDBException ex) when (SqlFuzzErrorRules.InternalCodes.Contains(ex.Code))
        {
            return $"{ex.Code}: {ex.Message}";
        }
        catch (CamusDBException)
        {
            return null;
        }
    }

    /// <summary>The test form of <see cref="ClassifyAsync"/>: the rule must hold.</summary>
    public async Task AssertClassified(string sql, string route, int seconds = 10)
    {
        string? problem = await ClassifyAsync(sql, route, seconds);
        Assert.That(problem, Is.Null, sql);
    }

    /// <summary>Every listed table passes <see cref="TableIntegrityAudit"/>.</summary>
    public async Task AssertIntegrity(params string[] tables)
    {
        foreach (string table in tables)
        {
            IReadOnlyList<string> problems = await TableIntegrityAudit.CheckAsync(executor, database, dbname, table);
            Assert.That(problems, Is.Empty, string.Join("\n", problems));
        }
    }

    /// <summary>A write has the effect <see cref="WriteEffect"/> requires.</summary>
    public async Task AssertWriteEffect(WriteSpec spec)
    {
        ReplaySession session = new(this);
        WriteResult result = await WriteEffect.CheckAsync(spec,
            async sql => (await session.QueryAsync(sql, null)).Rows,
            sql => session.WriteAsync(sql, null));
        Assert.That(result.Problem, Is.Null);
    }

    /// <summary>One explicit transaction has the effect <see cref="TransactionEffect"/> requires.</summary>
    public async Task AssertTransaction(WriteSpec[] writes, string? failing, bool failureIsDuplicateKey, string after, bool afterIsQuery, params string[] tables)
    {
        (SqlFuzzVerdict verdict, _) = await TransactionEffect.CheckAsync(writes, failing, failureIsDuplicateKey, after, afterIsQuery, tables, new ReplaySession(this));
        Assert.That(verdict.Kind, Is.Not.EqualTo(SqlFuzzVerdictKind.Fail), verdict.Signature + ": " + verdict.Detail);
    }

    private async Task<int> WriteStrict(string sql, KvTransaction? transaction)
    {
        KvTransaction tx = transaction ?? await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            if (transaction is null)
                await database.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        finally
        {
            if (transaction is null)
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static void FailOnInternal(CamusDBException ex, string sql)
    {
        if (SqlFuzzErrorRules.InternalCodes.Contains(ex.Code))
            Assert.Fail($"{ex.Code}: {ex.Message}\n  in: {sql}");
    }

    /// <summary>The session the shared rules use in a printed test: a refusal is a code, an internal code fails the test.</summary>
    private sealed class ReplaySession(SqlFuzzReplay replay) : ISqlFuzzSession
    {
        public async Task<(SqlFuzzRows? Rows, string? Error)> QueryAsync(string sql, KvTransaction? transaction)
        {
            try { return (await replay.Query(sql, transaction: transaction), null); }
            catch (CamusDBException ex) { FailOnInternal(ex, sql); return (null, ex.Code); }
        }

        public async Task<(int Affected, string? Error)> WriteAsync(string sql, KvTransaction? transaction)
        {
            try { return (await replay.WriteStrict(sql, transaction), null); }
            catch (CamusDBException ex) { FailOnInternal(ex, sql); return (0, ex.Code); }
        }

        public Task<KvTransaction> BeginAsync() => replay.database.Transactions.BeginAsync();

        public async Task<string?> CommitAsync(KvTransaction transaction)
        {
            try { await replay.database.Transactions.CommitAsync(transaction); return null; }
            catch (CamusDBException ex) { FailOnInternal(ex, "COMMIT"); return ex.Code; }
        }

        public Task RollbackAsync(KvTransaction transaction) => replay.database.Transactions.RollbackIfNotCompletedAsync(transaction);
    }
}

/// <summary>The group-by rule of ternary logic partitioning, shared by the fuzzer and the printed tests.</summary>
internal static class GroupPartition
{
    /// <summary>
    /// Returns null when each group of <paramref name="whole"/> equals the same group combined from
    /// <paramref name="parts"/> and the parts hold no other group, else the problem.
    /// </summary>
    public static string? Compare(int keyColumns, IReadOnlyList<string> functions, SqlFuzzRows whole, IReadOnlyList<SqlFuzzRows> parts)
    {
        Dictionary<string, IReadOnlyList<ColumnValue>> wholeGroups = new(StringComparer.Ordinal);
        foreach (IReadOnlyList<ColumnValue> row in whole.Rows)
        {
            string key = SqlFuzzValues.RenderRow(row.Take(keyColumns).ToList());
            if (!wholeGroups.TryAdd(key, row))
                return $"the whole has group {key} twice";
        }

        Dictionary<string, List<IReadOnlyList<ColumnValue>>> partGroups = new(StringComparer.Ordinal);
        foreach (SqlFuzzRows part in parts)
        {
            HashSet<string> seen = new(StringComparer.Ordinal);
            foreach (IReadOnlyList<ColumnValue> row in part.Rows)
            {
                string key = SqlFuzzValues.RenderRow(row.Take(keyColumns).ToList());
                if (!seen.Add(key))
                    return $"a part has group {key} twice";
                if (!partGroups.TryGetValue(key, out List<IReadOnlyList<ColumnValue>>? list))
                    partGroups[key] = list = new();
                list.Add(row);
            }
        }

        foreach (string key in partGroups.Keys)
            if (!wholeGroups.ContainsKey(key))
                return $"group {key} is in a part but not in the whole";

        foreach ((string key, IReadOnlyList<ColumnValue> row) in wholeGroups)
        {
            if (!partGroups.TryGetValue(key, out List<IReadOnlyList<ColumnValue>>? rows))
                return $"group {key} is in the whole but in no part";

            for (int f = 0; f < functions.Count; f++)
            {
                int column = keyColumns + f;
                ColumnValue? combined = SqlFuzzValues.Combine(functions[f], rows.Select(r => r[column]).ToList());
                if (combined is null)
                    continue;
                if (!SqlFuzzValues.SameAggregate(functions[f], row[column], combined, SqlFuzzValues.AbsSum(whole.Columns, row, f)))
                    return $"group {key} {functions[f]}: whole {SqlFuzzValues.Render(row[column])}, parts combine to {SqlFuzzValues.Render(combined)}";
            }
        }

        return null;
    }
}
