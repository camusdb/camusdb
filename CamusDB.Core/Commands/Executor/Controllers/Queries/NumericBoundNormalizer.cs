/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Rewrites a numeric comparison constant into the domain of the indexed column so an index key
/// or range bound addresses the same rows the row-by-row evaluator would accept.
///
/// <para>The evaluator (<c>SQLExecutorBaseCreator.CompareValues</c>) widens a mixed
/// Integer64/Float64/Float32 comparison to <c>double</c>. An index key does not widen: the
/// <see cref="Storage.Kv.KeyEncoder"/> transform differs per type and
/// <see cref="ColumnValue.CompareTo"/> throws across types. So a Float64 literal on an Integer64
/// column, left untouched, becomes a lookup key that addresses no entry, or a range bound the
/// scan cannot compare. This class closes that gap before any selector sees the constant.</para>
///
/// <para>Every rewrite must be exact. A fractional bound on an integer column moves to the nearest
/// integer that keeps the same row set (<c>a &lt; 1.5</c> becomes <c>a &lt;= 1</c>); a fractional
/// equality can match no integer and is handed back to the evaluator, never rounded. Anything the
/// integer domain cannot express (NaN, infinities, values outside the <c>long</c> range) is also
/// handed back: the evaluator's answer is always right, only the index shortcut is lost.</para>
/// </summary>
internal static class NumericBoundNormalizer
{
    /// <summary>What <see cref="Normalize"/> decided for one comparison.</summary>
    internal enum Outcome
    {
        /// <summary>The constant already has the column's type, or the pair is not numeric.</summary>
        Unchanged,

        /// <summary>The operator and constant were rewritten into the column's domain, exactly.</summary>
        Converted,

        /// <summary>
        /// No exact rewrite exists. The comparison must stop driving index selection and the
        /// original conjunct must stay in the residual filter, where the evaluator decides.
        /// </summary>
        LeaveToEvaluator,
    }

    /// <summary>What <see cref="NormalizeInListItem"/> decided for one list item.</summary>
    internal enum ItemOutcome
    {
        /// <summary>The item already has the column's type, or the pair is not numeric.</summary>
        Unchanged,

        /// <summary>The item was rewritten into the column's domain, exactly.</summary>
        Converted,

        /// <summary>The item can equal no value of the column and must be dropped from the list.</summary>
        Drop,
    }

    // 2^63 as a double: the smallest double the long range cannot hold. (double)long.MaxValue rounds
    // up to exactly this value, so the upper check must be exclusive.
    private const double LongRangeExclusiveUpper = 9223372036854775808.0;
    private const double LongRangeInclusiveLower = -9223372036854775808.0;

    internal static bool IsNumeric(ColumnType type) => MixedNumericComparison.IsNumeric(type);

    /// <summary>
    /// Rewrites <paramref name="op"/> <paramref name="constant"/> for a column of
    /// <paramref name="columnType"/>. Only the operators the index selector understands
    /// (<c>=</c>, <c>!=</c>, <c>&lt;</c>, <c>&lt;=</c>, <c>&gt;</c>, <c>&gt;=</c>) are rewritten;
    /// any other operator is left to the evaluator when the types differ.
    /// </summary>
    internal static Outcome Normalize(
        string op,
        ColumnValue constant,
        ColumnType columnType,
        out string newOp,
        out ColumnValue newConstant)
    {
        newOp = op;
        newConstant = constant;

        if (constant.Type == columnType || !IsNumeric(columnType) || !IsNumeric(constant.Type))
            return Outcome.Unchanged;

        switch (columnType)
        {
            case ColumnType.Integer64:
            {
                Outcome outcome = NormalizeToInteger(op, ToDouble(constant), out newOp, out newConstant);
                if (outcome != Outcome.Converted)
                    newConstant = constant;
                return outcome;
            }

            case ColumnType.Float64:
                // The evaluator widens the long to double before it compares, so the widened
                // value is exactly what a stored Float64 is compared against — even beyond 2^53,
                // where the widening rounds, because the evaluator rounds the same way.
                newConstant = new ColumnValue(ColumnType.Float64, ToDouble(constant));
                return Outcome.Converted;

            case ColumnType.Float32:
                return NormalizeToFloat32(constant, out newConstant) ? Outcome.Converted : Outcome.LeaveToEvaluator;

            default:
                return Outcome.Unchanged;
        }
    }

    /// <summary>
    /// Rewrites one <c>IN</c> list item for a column of <paramref name="columnType"/>. Only
    /// equality applies to a list item, so an item the column's domain cannot hold exactly matches
    /// nothing and is dropped rather than left to the evaluator.
    /// </summary>
    internal static ItemOutcome NormalizeInListItem(
        ColumnValue item,
        ColumnType columnType,
        out ColumnValue newItem)
    {
        newItem = item;

        if (item.Type == columnType || !IsNumeric(columnType) || !IsNumeric(item.Type))
            return ItemOutcome.Unchanged;

        switch (columnType)
        {
            case ColumnType.Integer64:
            {
                Outcome outcome = NormalizeToInteger("=", ToDouble(item), out _, out newItem);
                return outcome == Outcome.Converted ? ItemOutcome.Converted : ItemOutcome.Drop;
            }

            case ColumnType.Float64:
                newItem = new ColumnValue(ColumnType.Float64, ToDouble(item));
                return ItemOutcome.Converted;

            case ColumnType.Float32:
                return NormalizeToFloat32(item, out newItem) ? ItemOutcome.Converted : ItemOutcome.Drop;

            default:
                return ItemOutcome.Unchanged;
        }
    }

    private static double ToDouble(ColumnValue value) => MixedNumericComparison.ToDouble(value);

    /// <summary>
    /// A Float32 column stores a value that survives a round trip through <c>float</c>, and the
    /// evaluator compares that stored value as a double. A literal that does not survive the same
    /// round trip can therefore equal no stored value, and a range bound on it has no exact
    /// single-precision equivalent, so only exactly representable literals are converted.
    /// </summary>
    private static bool NormalizeToFloat32(ColumnValue constant, out ColumnValue newConstant)
    {
        double value = ToDouble(constant);
        float narrowed = (float)value;

        if (double.IsNaN(value) || (double)narrowed != value)
        {
            newConstant = constant;
            return false;
        }

        newConstant = new ColumnValue(ColumnType.Float32, (double)narrowed);
        return true;
    }

    private static Outcome NormalizeToInteger(string op, double value, out string newOp, out ColumnValue newConstant)
    {
        newOp = op;
        newConstant = null!;

        if (double.IsNaN(value) || double.IsInfinity(value)
            || value < LongRangeInclusiveLower || value >= LongRangeExclusiveUpper)
            return Outcome.LeaveToEvaluator;

        double floor = Math.Floor(value);
        bool integral = floor == value;

        switch (op)
        {
            case "=":
            case "!=":
                if (!integral)
                    return Outcome.LeaveToEvaluator;

                newConstant = new ColumnValue(ColumnType.Integer64, (long)value);
                return Outcome.Converted;

            case "<":
                // a < 1.5  ⇔  a <= 1 ; a < 2.0  ⇔  a < 2
                newOp = integral ? "<" : "<=";
                newConstant = new ColumnValue(ColumnType.Integer64, (long)floor);
                return Outcome.Converted;

            case "<=":
                // a <= 1.5  ⇔  a <= 1
                newConstant = new ColumnValue(ColumnType.Integer64, (long)floor);
                return Outcome.Converted;

            case ">":
                // a > 1.5  ⇔  a >= 2 ; a > 2.0  ⇔  a > 2
                newOp = integral ? ">" : ">=";
                newConstant = new ColumnValue(ColumnType.Integer64, integral ? (long)value : (long)Math.Ceiling(value));
                return Outcome.Converted;

            case ">=":
                // a >= 1.5  ⇔  a >= 2
                newConstant = new ColumnValue(ColumnType.Integer64, integral ? (long)value : (long)Math.Ceiling(value));
                return Outcome.Converted;

            default:
                return Outcome.LeaveToEvaluator;
        }
    }
}
