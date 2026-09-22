/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The sequence functions: <c>nextval</c>, <c>currval</c>, <c>lastval</c> and <c>setval</c>.
///
/// <para><b>None of them calls storage from here.</b> Expression evaluation is synchronous and
/// row-oriented, while drawing from a sequence is an asynchronous, routed call — so a
/// <c>nextval</c> evaluated per row would be one network round trip per row, on the hottest path
/// in the engine. Instead the statement reserves the values it needs in <b>one</b> call before it
/// starts, and each evaluation takes the next value out of that reserved run with no I/O. The
/// reservation is put in the statement's parameter dictionary by
/// <see cref="SequenceStatementBinder"/>, which is the same channel
/// <see cref="SessionScalarFunctions"/> uses to carry the session, and for the same reason: it
/// travels with the ticket everywhere the expression goes and needs no ambient state.</para>
///
/// <para><b>The cursor is mutated in place, under a lock on the dictionary.</b> Handing out the
/// next value is a read-then-write, and a projection may be evaluated from more than one thread.
/// The dictionary is a per-statement copy the binder made, so nothing outside the statement can be
/// holding that lock and contention is between the statement's own rows.</para>
///
/// <para><b>An advance is not part of the transaction that triggered it.</b> A rolled-back
/// statement consumes the values it drew and they are never reissued. That is the only workable
/// answer: rolling one back would mean either holding the sequence's partition under lock for the
/// life of every writing transaction, or reissuing values — which would break the one guarantee
/// the whole feature rests on.</para>
/// </summary>
internal static class SequenceScalarFunctions
{
    /// <summary>Maps a sequence's lower-cased name to its immutable id, for the statement's calls.</summary>
    internal const string IdKeyPrefix = "@@seqid:";

    /// <summary>The next value of the reserved run, by sequence id. Advanced on every draw.</summary>
    internal const string NextKeyPrefix = "@@seqnext:";

    /// <summary>
    /// How many values of the reserved run are still to be handed out, by sequence id. Reaching
    /// zero is an engine defect: the reservation is sized before the statement runs.
    /// </summary>
    internal const string RemainingKeyPrefix = "@@seqremaining:";

    /// <summary>
    /// The step between the values of the reserved run, by sequence id — derived from the run's own
    /// endpoints, never read from the catalog. A counter's increment and the record that describes
    /// it are changed by two separate writes, so the record can disagree with the run in hand.
    /// </summary>
    internal const string StepKeyPrefix = "@@seqstep:";

    /// <summary>The value to report for one sequence's <c>currval</c>, by sequence id.</summary>
    internal const string CurrentKeyPrefix = "@@seqcurrent:";

    /// <summary>
    /// Marks a sequence this statement really drew from, by sequence id. A value merely seeded so
    /// <c>currval</c> could read it carries no marker, so it is not recorded back as a draw.
    /// </summary>
    internal const string DrawnKeyPrefix = "@@seqdrawn:";

    /// <summary>The id of the sequence this statement drew from most recently.</summary>
    internal const string LastDrawnIdKey = "@@seqlastid";

    /// <summary>The value to report for <c>lastval</c>.</summary>
    internal const string LastValueKey = "@@seqlast";

    public static void Register(ScalarFunctionRegistry registry)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "nextval",
            MinArity = 1,
            MaxArity = 1,
            IsVolatile = true,
            StatementEvaluator = NextVal,
            Evaluator = static (calledName, _) => throw OutsideStatement(calledName),
            InferReturnType = static _ => ColumnType.Integer64,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "currval",
            MinArity = 1,
            MaxArity = 1,
            // Volatile so a query naming it never reaches the shared result cache: its answer
            // depends on what the caller's own transaction drew, and a cached row would serve one
            // caller's number to the next.
            IsVolatile = true,
            StatementEvaluator = CurrVal,
            Evaluator = static (calledName, _) => throw OutsideStatement(calledName),
            InferReturnType = static _ => ColumnType.Integer64,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "lastval",
            MinArity = 0,
            MaxArity = 0,
            IsVolatile = true,
            StatementEvaluator = LastVal,
            Evaluator = static (calledName, _) => throw OutsideStatement(calledName),
            InferReturnType = static _ => ColumnType.Integer64,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "setval",
            MinArity = 2,
            MaxArity = 3,
            IsVolatile = true,
            StatementEvaluator = SetVal,
            Evaluator = static (calledName, _) => throw OutsideStatement(calledName),
            InferReturnType = static _ => ColumnType.Integer64,
        });
    }

    /// <summary>
    /// Takes the next value of the run the binder reserved for this call's sequence.
    /// </summary>
    /// <remarks>
    /// An exhausted run is an engine defect, not a user error: the binder counted the values the
    /// statement would need and reserved exactly that many, so reaching the end means the count and
    /// the evaluation disagree. It is raised loudly rather than quietly reserving more, because
    /// quietly reserving more from a synchronous evaluator is the per-row round trip this whole
    /// design exists to avoid.
    /// </remarks>
    private static ColumnValue NextVal(
        string calledName, IReadOnlyList<ColumnValue> arguments, Dictionary<string, ColumnValue>? parameters)
    {
        string sequenceName = RequireSequenceName(calledName, arguments);

        if (parameters is null)
            throw OutsideStatement(calledName);

        string sequenceId = RequireSequenceId(sequenceName, parameters);

        return TakeFromRun(sequenceId, sequenceName, parameters, () => OutsideStatement(calledName));
    }

    /// <summary>
    /// Hands out the next value of a reserved run and advances the cursor, under a lock on the
    /// statement's parameter dictionary.
    /// </summary>
    /// <remarks>
    /// <para>The step comes from the run, not from the sequence's recorded increment. The counter
    /// and the record are changed by two separate writes, so a reservation can be allocated with
    /// one increment while the record still describes another; stepping by the record's number
    /// would then either overshoot the run or emit values Kahuna never allocated.</para>
    ///
    /// <para>An exhausted run is an engine defect, not a user error: the binder counted the values
    /// the statement would need and reserved exactly that many. It is raised loudly rather than
    /// quietly reserving more, because reserving more from a synchronous evaluator is the per-row
    /// round trip this whole design exists to avoid.</para>
    ///
    /// <para>Shared with <see cref="SequenceDefaults"/> so a column default and an explicit call
    /// draw from the one cursor in the one way.</para>
    /// </remarks>
    internal static ColumnValue TakeFromRun(
        string sequenceId,
        string sequenceName,
        Dictionary<string, ColumnValue> parameters,
        Func<CamusDBException> unreserved)
    {
        lock (parameters)
        {
            if (!parameters.TryGetValue(NextKeyPrefix + sequenceId, out ColumnValue? next)
                || !parameters.TryGetValue(RemainingKeyPrefix + sequenceId, out ColumnValue? remaining)
                || !parameters.TryGetValue(StepKeyPrefix + sequenceId, out ColumnValue? step))
                throw unreserved();

            if (remaining.LongValue <= 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"The statement drew more values from sequence '{sequenceName}' than it reserved. " +
                    "This is an engine defect: the reservation is sized before the statement runs.");

            long value = next.LongValue;

            parameters[NextKeyPrefix + sequenceId] = new ColumnValue(ColumnType.Integer64, value + step.LongValue);
            parameters[RemainingKeyPrefix + sequenceId] = new ColumnValue(ColumnType.Integer64, remaining.LongValue - 1);

            // Recorded so currval and lastval answer from what was handed out. The marker is what
            // separates a real draw from a value the binder merely seeded for currval to read.
            ColumnValue drawn = new(ColumnType.Integer64, value);
            parameters[CurrentKeyPrefix + sequenceId] = drawn;
            parameters[DrawnKeyPrefix + sequenceId] = ColumnValue.True;
            parameters[LastDrawnIdKey] = new ColumnValue(ColumnType.String, sequenceId);
            parameters[LastValueKey] = drawn;

            return drawn;
        }
    }

    /// <summary>
    /// The value this transaction last drew from the named sequence. Reads local state only: it
    /// never calls the sequencer, because what the sequencer reports is the reserved ceiling and
    /// not a value anyone was issued.
    /// </summary>
    private static ColumnValue CurrVal(
        string calledName, IReadOnlyList<ColumnValue> arguments, Dictionary<string, ColumnValue>? parameters)
    {
        string sequenceName = RequireSequenceName(calledName, arguments);

        if (parameters is null)
            throw OutsideStatement(calledName);

        string sequenceId = RequireSequenceId(sequenceName, parameters);

        lock (parameters)
        {
            if (parameters.TryGetValue(CurrentKeyPrefix + sequenceId, out ColumnValue? current))
                return current;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.SequenceValueNotYetDefined,
            $"currval('{sequenceName}') is not defined: this transaction has not drawn a value from that " +
            "sequence yet. A value another caller drew is not an answer, and the sequence's reported " +
            "number is a reserved ceiling rather than a value that was issued.");
    }

    private static ColumnValue LastVal(
        string calledName, IReadOnlyList<ColumnValue> arguments, Dictionary<string, ColumnValue>? parameters)
    {
        if (parameters is not null)
        {
            lock (parameters)
            {
                if (parameters.TryGetValue(LastValueKey, out ColumnValue? last))
                    return last;
            }
        }

        throw new CamusDBException(
            CamusDBErrorCodes.SequenceValueNotYetDefined,
            "lastval() is not defined: this transaction has not drawn a value from any sequence yet.");
    }

    /// <summary>
    /// Reports the value <c>setval</c> installed. The counter itself was moved by the binder before
    /// the statement ran, because moving it is an asynchronous call that takes about one block
    /// lease to answer — far too slow, and far too consequential, to run from a row evaluator.
    /// </summary>
    private static ColumnValue SetVal(
        string calledName, IReadOnlyList<ColumnValue> arguments, Dictionary<string, ColumnValue>? parameters)
    {
        string sequenceName = RequireSequenceName(calledName, arguments);

        if (parameters is null)
            throw OutsideStatement(calledName);

        string sequenceId = RequireSequenceId(sequenceName, parameters);

        lock (parameters)
        {
            if (parameters.TryGetValue(CurrentKeyPrefix + sequenceId, out ColumnValue? installed))
                return installed;
        }

        throw OutsideStatement(calledName);
    }

    /// <summary>
    /// The first argument of every sequence function but <c>lastval</c> is the sequence's name, as
    /// a string literal. A non-literal is refused rather than evaluated, because the binder has to
    /// know which sequence a statement touches before the statement runs.
    /// </summary>
    private static string RequireSequenceName(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        if (arguments.Count > 0 && arguments[0].Type == ColumnType.String && arguments[0].StrValue is { Length: > 0 } name)
            return name;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"{calledName}() takes the sequence's name as a string literal, for example {calledName}('order_no')");
    }

    private static string RequireSequenceId(string sequenceName, Dictionary<string, ColumnValue> parameters)
    {
        if (parameters.TryGetValue(IdKeyPrefix + sequenceName.ToLowerInvariant(), out ColumnValue? id)
            && id.StrValue is { Length: > 0 } sequenceId)
            return sequenceId;

        throw new CamusDBException(
            CamusDBErrorCodes.SequenceDoesntExist,
            $"Sequence '{sequenceName}' does not exist");
    }

    /// <summary>
    /// Raised where a sequence function is evaluated outside a statement the binder prepared — a
    /// stored CHECK condition, or a column default replayed by a path that never reserved a run.
    /// Those uses are refused when the schema is written, so reaching this means a new evaluation
    /// path was added without binding the statement first.
    /// </summary>
    private static CamusDBException OutsideStatement(string calledName)
        => new(
            CamusDBErrorCodes.SequenceCallNotAllowedHere,
            $"'{calledName}' cannot be used here. It is only available where the engine can bound how many " +
            "values the statement will draw before it runs: an INSERT ... VALUES list, a column DEFAULT, " +
            "or a SELECT with no FROM clause.");
}
