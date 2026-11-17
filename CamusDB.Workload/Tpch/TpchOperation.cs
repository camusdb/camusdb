/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using CamusDB.Client;
using CamusDB.Workload.Client;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Tpch;

public enum TpchOutcome
{
    /// <summary>The query answered and its rows equal the reference.</summary>
    Verified,

    /// <summary>The query answered with rows that differ from the reference.</summary>
    Mismatch,

    /// <summary>The query did not answer.</summary>
    Error,
}

/// <summary>One execution of one query, kept so the per-query figures can be cut to the measured window.</summary>
public readonly record struct TpchExecution(
    int Query, DateTime StartedUtc, double Milliseconds, TpchOutcome Outcome, int Rows, string? ErrorCode);

public sealed record TpchMismatch(int Query, string StartedUtc, string Difference, string Digest, string ReferenceDigest);

/// <summary>The reference answer of one query and how long the reference execution took.</summary>
public sealed record TpchReference(TpchQuery Query, TpchResultSet Result, double Milliseconds);

/// <summary>
/// The read operation of the TPC-H shape: one whole query per operation, on a read-only connection.
///
/// <para>The data never changes after the load, so each query has one right answer. It is recorded once
/// before the measured window (<see cref="CaptureReferencesAsync"/>) and every execution after that is
/// compared with it. A different answer is a wrong answer whatever was happening to the cluster at the
/// time, so a mismatch is reported as an internal error and is never waived.</para>
///
/// <para>Queries are taken in turn from one shared cursor rather than drawn at random: the queries differ
/// in cost by orders of magnitude, and a run must reach every one of them to have tested it.</para>
/// </summary>
public sealed class TpchOperation : IReadOperation
{
    /// <summary>A reference execution that fails after running this long is not retried.</summary>
    private static readonly TimeSpan SlowFailure = TimeSpan.FromSeconds(30);

    /// <summary>Mismatches kept in full for the artifact; the rest are only counted.</summary>
    private const int MaxMismatchesKept = 200;

    private readonly ConnectionSet _connections;
    private readonly TpchReference[] _references;
    private readonly ConcurrentQueue<TpchExecution> _executions = new();
    private readonly ConcurrentQueue<TpchMismatch> _mismatches = new();
    private int _cursor = -1;
    private int _mismatchCount;

    public TpchOperation(ConnectionSet connections, IReadOnlyList<TpchReference> references)
    {
        if (references.Count == 0)
            throw new ArgumentException("at least one query is needed", nameof(references));
        _connections = connections;
        _references = references.ToArray();
    }

    public IReadOnlyCollection<TpchExecution> Executions => _executions;

    public IReadOnlyCollection<TpchMismatch> Mismatches => _mismatches;

    public int MismatchCount => Volatile.Read(ref _mismatchCount);

    public async Task<OperationResult> ExecuteAsync(long rowIndex, CancellationToken ct)
    {
        TpchReference reference = _references[(uint)Interlocked.Increment(ref _cursor) % (uint)_references.Length];
        int number = reference.Query.Number;
        DateTime startedUtc = DateTime.UtcNow;
        long startedAt = Stopwatch.GetTimestamp();

        try
        {
            TpchResultSet result = await RunQueryAsync(_connections.NextRead(), reference.Query, ct).ConfigureAwait(false);
            double ms = Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds;

            string? difference = TpchResultComparer.Difference(reference.Result, result);
            if (difference is null)
            {
                _executions.Enqueue(new TpchExecution(number, startedUtc, ms, TpchOutcome.Verified, result.Rows.Count, null));
                return OperationResult.ReadOk();
            }

            string code = $"TPCH_MISMATCH_{reference.Query.Name}";
            _executions.Enqueue(new TpchExecution(number, startedUtc, ms, TpchOutcome.Mismatch, result.Rows.Count, code));
            if (Interlocked.Increment(ref _mismatchCount) <= MaxMismatchesKept)
                _mismatches.Enqueue(new TpchMismatch(
                    number, startedUtc.ToString("O", CultureInfo.InvariantCulture), difference, result.Digest, reference.Result.Digest));
            return OperationResult.Failure(OperationKind.Read, OperationStatus.InternalError, code, difference);
        }
        catch (Exception ex)
        {
            double ms = Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds;
            (OperationStatus status, string code) = ErrorClassifier.Classify(ex);
            _executions.Enqueue(new TpchExecution(number, startedUtc, ms, TpchOutcome.Error, 0, code));
            return OperationResult.Failure(OperationKind.Read, status, code, ErrorClassifier.MessageOf(ex));
        }
    }

    /// <summary>Runs one query and returns its rows in canonical form.</summary>
    public static async Task<TpchResultSet> RunQueryAsync(CamusConnection conn, TpchQuery query, CancellationToken ct)
    {
        using CamusCommand cmd = conn.CreateSelectCommand(query.Sql);
        using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        List<TpchCell[]> rows = new();
        int columns = reader.FieldCount;
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            TpchCell[] row = new TpchCell[columns];
            for (int i = 0; i < columns; i++)
                row[i] = CellOf(reader, i);
            rows.Add(row);
        }
        return TpchResultComparer.Canonical(rows);
    }

    private static TpchCell CellOf(CamusDataReader reader, int ordinal)
    {
        ColumnValue value = reader.GetColumnValue(ordinal);
        switch (value.Type)
        {
            case ColumnType.Null:
                return TpchCell.Null;
            case ColumnType.Integer64:
                return TpchCell.Integer(reader.GetInt64(ordinal));
            case ColumnType.Float64:
            case ColumnType.Float32:
                return TpchCell.Float(reader.GetDouble(ordinal));
            case ColumnType.Numeric:
                return TpchCell.Exact(value.StrValue ?? reader.GetDecimal(ordinal).ToString(CultureInfo.InvariantCulture));
            case ColumnType.String:
            case ColumnType.Id:
                return TpchCell.String(reader.GetString(ordinal));
            default:
                return TpchCell.String(Convert.ToString(reader.GetValue(ordinal), CultureInfo.InvariantCulture) ?? "");
        }
    }

    /// <summary>
    /// Runs every query once on a quiet cluster and keeps the answers. A transient failure is retried a
    /// few times; a query the server refuses, or cannot finish, fails the capture by name so it can be
    /// left out with <c>--tpch-queries</c>.
    /// </summary>
    public static async Task<List<TpchReference>> CaptureReferencesAsync(
        CamusConnection conn, IReadOnlyList<TpchQuery> queries, Action<string> log, CancellationToken ct)
    {
        const int attempts = 3;
        List<TpchReference> references = new(queries.Count);

        foreach (TpchQuery query in queries)
        {
            for (int attempt = 1; ; attempt++)
            {
                long startedAt = Stopwatch.GetTimestamp();
                try
                {
                    TpchResultSet result = await RunQueryAsync(conn, query, ct).ConfigureAwait(false);
                    double ms = Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds;
                    references.Add(new TpchReference(query, result, ms));
                    log($"  {query.Name}: {result.Rows.Count:N0} row(s) in {ms:N0} ms, digest {result.Digest}");
                    break;
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    // A transport transient fails fast. An attempt that ran this long before failing met the
                    // request deadline, and the query would only meet it again.
                    bool retryable = ErrorClassifier.IsRetryableForIdempotentRead(ex)
                        && Stopwatch.GetElapsedTime(startedAt) < SlowFailure;
                    if (!retryable || attempt >= attempts)
                        throw new InvalidOperationException(
                            $"{query.Name} failed its reference execution after {Stopwatch.GetElapsedTime(startedAt).TotalSeconds:F1}s " +
                            $"(attempt {attempt}): {ex.GetType().Name}: {ErrorClassifier.MessageOf(ex)}", ex);
                    await Task.Delay(1000 * attempt, ct).ConfigureAwait(false);
                }
            }
        }
        return references;
    }

    /// <summary>
    /// Compares the loaded database with what the generator recorded about it: the row count of every
    /// table, and the answers of Q1 and Q6 when the reference pass ran them. Returns one sentence per
    /// disagreement. This is the only check whose expected values do not come from the database, so it is
    /// what keeps a reference taken from damaged data from becoming the truth.
    /// </summary>
    public static async Task<List<string>> VerifyLoadAsync(
        CamusConnection conn, TpchFacts facts, IReadOnlyList<TpchReference> references, CancellationToken ct)
    {
        List<string> failures = new();

        foreach (string table in TpchSchema.Tables)
        {
            long expected = facts.Counts.GetValueOrDefault(table);
            long found = await TpchLoader.CountAsync(conn, table, ct).ConfigureAwait(false);
            if (found != expected)
                failures.Add($"table {table} holds {found:N0} row(s), the load recorded {expected:N0}");
        }

        if (references.FirstOrDefault(r => r.Query.Number == 1) is TpchReference q1)
            failures.AddRange(CheckQ1(facts, q1.Result));
        if (references.FirstOrDefault(r => r.Query.Number == 6) is TpchReference q6)
            failures.AddRange(CheckQ6(facts, q6.Result));

        return failures;
    }

    public static List<string> CheckQ1(TpchFacts facts, TpchResultSet result)
    {
        List<string> failures = new();
        if (result.Rows.Count != facts.Q1.Count)
            failures.Add($"Q01 returned {result.Rows.Count} group(s), the load recorded {facts.Q1.Count}");

        foreach (TpchCell[] row in result.Rows)
        {
            if (row.Length != 10)
            {
                failures.Add($"Q01 returned a row of {row.Length} column(s), expected 10");
                break;
            }

            string key = $"{row[0].Text}|{row[1].Text}";
            if (!facts.Q1.TryGetValue(key, out TpchQ1Group? group))
            {
                failures.Add($"Q01 returned group {key}, which the load never produced");
                continue;
            }

            (string Name, decimal Expected, int Column, decimal Tolerance)[] checks =
            [
                ("sum_qty", group.SumQty, 2, 0m),
                ("sum_base_price", group.SumBasePrice, 3, 0m),
                ("sum_disc_price", group.SumDiscPrice, 4, 0m),
                ("sum_charge", group.SumCharge, 5, 0m),
                ("avg_qty", group.SumQty / group.Count, 6, 0.000001m),
                ("avg_price", group.SumBasePrice / group.Count, 7, 0.000001m),
                ("avg_disc", group.SumDiscount / group.Count, 8, 0.000001m),
                ("count_order", group.Count, 9, 0m),
            ];
            foreach ((string name, decimal expected, int column, decimal tolerance) in checks)
            {
                if (!Matches(row[column], expected, tolerance))
                    failures.Add($"Q01 group {key}: {name} is {row[column]}, the load recorded {expected.ToString(CultureInfo.InvariantCulture)}");
            }
        }
        return failures;
    }

    public static List<string> CheckQ6(TpchFacts facts, TpchResultSet result)
    {
        if (result.Rows.Count != 1 || result.Rows[0].Length != 1)
            return [$"Q06 returned {result.Rows.Count} row(s), expected one row of one column"];

        TpchCell revenue = result.Rows[0][0];
        if (facts.Q6Rows == 0)
            return revenue.Kind == TpchCellKind.Null ? [] : [$"Q06 revenue is {revenue}, the load recorded no qualifying line"];
        return Matches(revenue, facts.Q6Revenue, 0m)
            ? []
            : [$"Q06 revenue is {revenue}, the load recorded {facts.Q6Revenue.ToString(CultureInfo.InvariantCulture)}"];
    }

    /// <summary>An exact cell must equal the expected value (within <paramref name="tolerance"/>); a float
    /// cell is given the comparer's relative tolerance on top.</summary>
    private static bool Matches(TpchCell cell, decimal expected, decimal tolerance)
    {
        switch (cell.Kind)
        {
            case TpchCellKind.Integer:
            case TpchCellKind.Exact:
                return decimal.TryParse(cell.Text, NumberStyles.Number, CultureInfo.InvariantCulture, out decimal value)
                       && Math.Abs(value - expected) <= tolerance;
            case TpchCellKind.Float:
                return TpchResultComparer.FloatsClose(cell.Number, (double)expected)
                       || Math.Abs(cell.Number - (double)expected) <= (double)tolerance;
            default:
                return false;
        }
    }
}
