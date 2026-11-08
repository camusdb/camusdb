/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Tests.SqlFuzz;

namespace CamusDB.SqlFuzz.Tests.Runner;

/// <summary>
/// One error the generator can cause on purpose: its code and the generator rule that causes it. A
/// <see cref="MessageContains"/> narrows a broad code, such as <c>CADB0400</c>, to the one meaning the
/// entry expects.
/// </summary>
internal sealed record ExpectedError(string Code, string Rule, string? MessageContains = null);

/// <summary>
/// A form CamusDB does not support that a generator could write. The generator does not write these
/// forms; the list records them, so a person who extends the generator knows why a form is missing.
/// They are compatibility gaps that this fuzzer records but does not decide.
/// </summary>
internal sealed record CompatibilityGap(string Form, string Outcome);

/// <summary>
/// A defect the fuzzer found that is not fixed yet. An outcome that matches one is counted and named
/// in the report, but it is not a new finding, so a campaign stays useful while the fix waits. Remove
/// the entry in the commit that fixes the defect; the regression test of the fix then guards it.
/// </summary>
internal sealed record KnownIssue(string Code, string MessageContains, string Description);

/// <summary>How the error-classification oracle sees one outcome.</summary>
internal enum ErrorVerdict
{
    /// <summary>A success, or an error on the expected-error list.</summary>
    Accepted,

    /// <summary>A <see cref="CamusDBException"/> with a specific code that is not on the list. Counted, not a finding: the code is specific, so the engine meant it.</summary>
    UnlistedCode,

    /// <summary>A finding: an exception that is not a <see cref="CamusDBException"/>, an internal-error code, or a hang.</summary>
    Finding,

    /// <summary>A finding that matches <see cref="ErrorClassifier.KnownIssues"/>: counted, not reported again.</summary>
    KnownIssue,
}

/// <summary>
/// The expected-error list and the rule of the error-classification oracle.
///
/// <para><b>Rule.</b> Every error must be a <see cref="CamusDBException"/> with a specific code. These
/// outcomes are findings:</para>
/// <list type="bullet">
/// <item>An exception that is not a <see cref="CamusDBException"/>, for example an
/// <see cref="OverflowException"/>, an <see cref="InvalidCastException"/>, a
/// <see cref="NullReferenceException"/>, an <see cref="IndexOutOfRangeException"/> or a
/// <see cref="KeyNotFoundException"/>.</item>
/// <item>A code that means "this is a defect, not a refusal" (<see cref="InternalCodes"/>):
/// <c>CADB0099</c> (an internal operation that must not happen), and the codes for corrupt storage.</item>
/// <item>A statement that does not finish within the time limit.</item>
/// </list>
/// <para>An expected error is not a finding of this oracle, but it is still an outcome. A DML-integrity
/// oracle still checks the state it left, and a differential oracle still compares it with the other
/// engine's outcome.</para>
/// </summary>
internal static class ErrorClassifier
{
    /// <summary>The errors that the generator can cause on purpose, each with the rule that causes it.</summary>
    public static IReadOnlyList<ExpectedError> Expected { get; } =
    [
        new(CamusDBErrorCodes.IntegerValueOutOfRange, "INT64 arithmetic, negation, SUM or CAST on a boundary value"),
        new(CamusDBErrorCodes.FloatValueOutOfRange, "FLOAT64 arithmetic that overflows or underflows on 1e300 or 5e-324"),
        new(CamusDBErrorCodes.NumericValueOutOfRange, "NUMERIC arithmetic or CAST past 38 digits"),
        new(CamusDBErrorCodes.InvalidInput, "division or modulo by a zero value", "zero"),
        new(CamusDBErrorCodes.InvalidInput, "CAST of a STRING, NaN or infinity that has no value of the target type", "convert"),
        new(CamusDBErrorCodes.InvalidInput, "CAST of NaN or an infinity to INT64 or NUMERIC", "cast"),
        new(CamusDBErrorCodes.InvalidInput, "a function argument out of its domain"),
        new(CamusDBErrorCodes.DuplicatePrimaryKey, "INSERT or INSERT … SELECT with a key that exists"),
        new(CamusDBErrorCodes.DuplicateUniqueKeyValue, "INSERT, UPDATE or UPSERT with a key of a unique index that exists"),
        new(CamusDBErrorCodes.NotNullViolation, "a NULL into a NOT NULL column (an UPDATE with a NULL literal, a deliberate failure)"),
        new(CamusDBErrorCodes.TransactionRollbackOnly, "a statement or COMMIT after a failed statement in an explicit transaction"),
        new(CamusDBErrorCodes.InvalidJsonText, "a JSONB value from a STRING that is not JSON"),
        new(CamusDBErrorCodes.ResultSetTooLarge, "a CROSS JOIN of the largest tables past the result limit"),
    ];

    /// <summary>Forms the generator does not write because CamusDB does not support them.</summary>
    public static IReadOnlyList<CompatibilityGap> Gaps { get; } =
    [
        new("CREATE TABLE without a primary key", "CADB0400: A primary key column is mandatory in the table"),
        new("COUNT(DISTINCT x), SUM(DISTINCT x)", "CADB0406: syntax error"),
        new("date + integer", "CADB0400: no matching signature"),
        new("t@{FORCE_INDEX=i} AS a (the hint before the alias)", "CADB0406: syntax error; write t AS a@{FORCE_INDEX=i}"),
        new("SELECT a.* (a qualified star)", "CADB0406: syntax error"),
        new("FULL [OUTER] JOIN, NATURAL JOIN, JOIN … USING", "CADB0533 or a syntax error"),
        new("a RIGHT JOIN after another join", "CADB0533"),
        new("an IN-list item other than a literal, a parameter or a name (a CAST, an expression)", "CADB0406: syntax error; the item is a simple_expr"),
        new("an integer literal in ORDER BY", "a column position, not a value"),
    ];

    /// <summary>Defects the fuzzer found that wait for a fix. See <see cref="KnownIssue"/>.</summary>
    public static IReadOnlyList<KnownIssue> KnownIssues { get; } = [];

    /// <summary>Codes that report a defect of the engine, not a refusal of the statement. The printed regression tests use the same list (<see cref="SqlFuzzErrorRules.InternalCodes"/>).</summary>
    public static IReadOnlySet<string> InternalCodes => SqlFuzzErrorRules.InternalCodes;

    public static ErrorVerdict Classify(StatementOutcome outcome)
    {
        switch (outcome.Kind)
        {
            case OutcomeKind.Rows:
            case OutcomeKind.Affected:
                return outcome.WireError is null ? ErrorVerdict.Accepted : ErrorVerdict.Finding;
            case OutcomeKind.Unclassified:
            case OutcomeKind.Hang:
                return ErrorVerdict.Finding;
        }

        if (InternalCodes.Contains(outcome.ErrorCode!))
            return IsKnown(outcome) ? ErrorVerdict.KnownIssue : ErrorVerdict.Finding;

        return IsExpected(outcome) ? ErrorVerdict.Accepted : ErrorVerdict.UnlistedCode;
    }

    public static bool IsKnown(StatementOutcome outcome) =>
        KnownIssues.Any(k => k.Code == outcome.ErrorCode && (outcome.ErrorMessage ?? "").Contains(k.MessageContains, StringComparison.OrdinalIgnoreCase));

    public static bool IsExpected(StatementOutcome outcome)
    {
        if (outcome.Kind != OutcomeKind.Error)
            return false;
        foreach (ExpectedError entry in Expected)
        {
            if (entry.Code != outcome.ErrorCode)
                continue;
            if (entry.MessageContains is null || (outcome.ErrorMessage ?? "").Contains(entry.MessageContains, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    /// <summary>
    /// A short signature for a finding of this oracle: the same signature means the same kind of failure,
    /// which the shrinker keeps. An internal code and an unclassified exception carry the shape of their
    /// message (<see cref="SqlFuzzErrorRules.MessageShape"/>), because one code or one exception type
    /// covers unrelated causes: two causes of <c>CADB0099</c> are two findings, each with its own report
    /// and regression test.
    /// </summary>
    public static string Signature(StatementOutcome outcome) => outcome.Kind switch
    {
        OutcomeKind.Hang => "hang",
        OutcomeKind.Unclassified => "unclassified:" + outcome.ErrorCode + ":" + SqlFuzzErrorRules.MessageShape(outcome.ErrorMessage, 60),
        OutcomeKind.Error when InternalCodes.Contains(outcome.ErrorCode!) => "code:" + outcome.ErrorCode + ":" + SqlFuzzErrorRules.MessageShape(outcome.ErrorMessage, 60),
        OutcomeKind.Error => "code:" + outcome.ErrorCode,
        _ => outcome.WireError is not null ? "wire:" + outcome.WireError.Split(':')[0] : "ok",
    };
}
