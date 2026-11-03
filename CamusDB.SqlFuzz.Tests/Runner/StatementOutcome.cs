/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.SqlFuzz.Tests.Runner;

/// <summary>What kind of result one statement gave.</summary>
internal enum OutcomeKind
{
    /// <summary>A read that returned a result set, possibly empty. <see cref="StatementOutcome.Columns"/> and <see cref="StatementOutcome.Rows"/> are set.</summary>
    Rows,

    /// <summary>A write or a DDL statement that finished. <see cref="StatementOutcome.AffectedRows"/> holds the count of a write.</summary>
    Affected,

    /// <summary>The engine refused the statement with a <see cref="CamusDBException"/>. <see cref="StatementOutcome.ErrorCode"/> is set.</summary>
    Error,

    /// <summary>
    /// The statement failed with an exception that is not a <see cref="CamusDBException"/>, for example
    /// an <see cref="OverflowException"/>. This is always a finding: every refusal the engine means to
    /// give has a specific error code.
    /// </summary>
    Unclassified,

    /// <summary>The statement did not finish within the statement time limit, also after its cancellation.</summary>
    Hang,
}

/// <summary>One output column of a result set: its name and the type the engine reported for it.</summary>
internal readonly record struct OutcomeColumn(string Name, ColumnType Type);

/// <summary>
/// The full outcome of one statement, kept the same for every oracle. An expected error is still an
/// outcome: the error-classification oracle accepts it, but a DML-integrity or differential oracle
/// still inspects the state it left. The column list stays set for an empty result, so two empty
/// results with different shapes do not compare as equal.
/// </summary>
internal sealed class StatementOutcome
{
    private StatementOutcome(OutcomeKind kind, string sql)
    {
        Kind = kind;
        Sql = sql;
    }

    public OutcomeKind Kind { get; }

    /// <summary>The exact SQL text the engine received.</summary>
    public string Sql { get; }

    public IReadOnlyList<OutcomeColumn> Columns { get; private init; } = [];

    /// <summary>The cells of each row, in the order of <see cref="Columns"/>.</summary>
    public IReadOnlyList<ColumnValue[]> Rows { get; private init; } = [];

    public int AffectedRows { get; private init; }

    public string? ErrorCode { get; private init; }

    public string? ErrorMessage { get; private init; }

    /// <summary>The exception of an <see cref="OutcomeKind.Error"/> or <see cref="OutcomeKind.Unclassified"/> outcome.</summary>
    public Exception? Exception { get; private init; }

    public TimeSpan Elapsed { get; private init; }

    /// <summary>
    /// Set when the rows of a successful read did not serialize to the HTTP JSON row format
    /// (<see cref="CamusDB.Core.CommandsExecutor.Controllers.Queries.CompactRowJsonWriter"/>): the
    /// exception type and message. A statement that succeeds in the engine but cannot reach a client is
    /// a finding of the error-classification oracle.
    /// </summary>
    public string? WireError { get; private init; }

    /// <summary>A copy of this outcome with the time the statement took.</summary>
    public StatementOutcome WithElapsed(TimeSpan elapsed) => new(Kind, Sql)
    {
        Columns = Columns,
        Rows = Rows,
        AffectedRows = AffectedRows,
        ErrorCode = ErrorCode,
        ErrorMessage = ErrorMessage,
        Exception = Exception,
        WireError = WireError,
        Elapsed = elapsed,
    };

    public bool IsSuccess => Kind is OutcomeKind.Rows or OutcomeKind.Affected;

    public static StatementOutcome FromRows(string sql, IReadOnlyList<OutcomeColumn> columns, IReadOnlyList<ColumnValue[]> rows, string? wireError = null) =>
        new(OutcomeKind.Rows, sql) { Columns = columns, Rows = rows, WireError = wireError };

    public static StatementOutcome FromAffected(string sql, int affected) =>
        new(OutcomeKind.Affected, sql) { AffectedRows = affected };

    public static StatementOutcome FromException(string sql, Exception exception)
    {
        Exception root = Unwrap(exception);
        if (root is CamusDBException camus)
            return new StatementOutcome(OutcomeKind.Error, sql) { ErrorCode = camus.Code, ErrorMessage = camus.Message, Exception = camus };

        return new StatementOutcome(OutcomeKind.Unclassified, sql)
        {
            ErrorCode = root.GetType().Name,
            ErrorMessage = root.Message,
            Exception = root,
        };
    }

    public static StatementOutcome FromHang(string sql, TimeSpan limit) =>
        new(OutcomeKind.Hang, sql) { ErrorMessage = $"did not finish within {limit.TotalSeconds:0.#} s" };

    /// <summary>
    /// An <see cref="AggregateException"/> or a <see cref="System.Reflection.TargetInvocationException"/>
    /// only carries the real failure. The classification looks at the failure itself.
    /// </summary>
    private static Exception Unwrap(Exception exception)
    {
        Exception current = exception;
        for (int i = 0; i < 8; i++)
        {
            if (current is AggregateException { InnerExceptions.Count: 1 } aggregate)
                current = aggregate.InnerExceptions[0];
            else if (current is System.Reflection.TargetInvocationException { InnerException: { } inner })
                current = inner;
            else
                break;
        }
        return current;
    }

    public override string ToString() => Kind switch
    {
        OutcomeKind.Rows => $"{Rows.Count} row(s) [{string.Join(", ", Columns.Select(c => c.Name + ":" + c.Type))}]" + (WireError is null ? "" : $" wire error {WireError}"),
        OutcomeKind.Affected => $"{AffectedRows} row(s) affected",
        OutcomeKind.Error => $"{ErrorCode}: {ErrorMessage}",
        OutcomeKind.Unclassified => $"unclassified {ErrorCode}: {ErrorMessage}",
        OutcomeKind.Hang => $"hang: {ErrorMessage}",
        _ => Kind.ToString(),
    };
}
