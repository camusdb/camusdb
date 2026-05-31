
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Predicates;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Determines whether an index scan's key bounds make a column-vs-constant predicate redundant.
/// </summary>
internal static class IndexScanBoundAnalysis
{
    private readonly struct ColumnScanBounds
    {
        public ColumnValue? Lower { get; init; }

        public bool LowerInclusive { get; init; }

        public ColumnValue? Upper { get; init; }

        public bool UpperInclusive { get; init; }
    }

    internal static bool IsComparisonAbsorbedByScan(
        AnalyzedComparison comparison,
        QueryPlanStep? scanStep,
        TableDescriptor table)
    {
        if (scanStep is null || scanStep.Value.Index is null)
            return false;

        ColumnScanBounds? bounds = GetColumnScanBounds(scanStep.Value, table, comparison.ColumnName);
        if (bounds is null)
            return false;

        return IsComparisonAbsorbed(comparison, bounds.Value);
    }

    private static ColumnScanBounds? GetColumnScanBounds(
        QueryPlanStep scanStep,
        TableDescriptor table,
        string columnName)
    {
        TableIndexSchema index = scanStep.Index!;
        int columnIndex = Array.IndexOf(index.Columns, columnName);
        if (columnIndex < 0)
            return null;

        switch (scanStep.Type)
        {
            case QueryPlanStepType.QueryFromIndex:
            {
                CompositeColumnValue? lookupKey = scanStep.LookupKey;
                if (lookupKey is null && scanStep.ColumnValue is ColumnValue singleValue)
                    lookupKey = new CompositeColumnValue(new[] { singleValue });

                if (lookupKey is null || columnIndex >= lookupKey.Values.Length)
                    return null;

                ColumnValue point = lookupKey.Values[columnIndex];
                return new ColumnScanBounds
                {
                    Lower = point,
                    LowerInclusive = true,
                    Upper = point,
                    UpperInclusive = true,
                };
            }

            case QueryPlanStepType.RangeScanFromIndex:
            {
                ColumnValue? lower = GetBoundComponent(scanStep.FromBound, columnIndex);
                ColumnValue? upper = GetBoundComponent(scanStep.ToBound, columnIndex);

                if (lower is null && upper is null)
                    return null;

                bool lowerInclusive = scanStep.FromInclusive;
                bool upperInclusive = scanStep.ToInclusive;

                int toBoundLength = scanStep.ToBound?.Values.Length ?? 0;

                // Prefix-cap bounds pin earlier index columns with inclusive equality.
                if (toBoundLength > 0
                    && columnIndex < toBoundLength
                    && lower is not null
                    && upper is not null)
                {
                    lowerInclusive = true;
                }

                return new ColumnScanBounds
                {
                    Lower = lower,
                    LowerInclusive = lowerInclusive,
                    Upper = upper,
                    UpperInclusive = upperInclusive,
                };
            }

            default:
                return null;
        }
    }

    private static ColumnValue? GetBoundComponent(CompositeColumnValue? bound, int columnIndex)
    {
        if (bound is null || bound.Values.Length <= columnIndex)
            return null;

        return bound.Values[columnIndex];
    }

    private static bool IsComparisonAbsorbed(AnalyzedComparison comparison, ColumnScanBounds bounds)
    {
        return comparison.Constant.Type switch
        {
            ColumnType.Integer64 => IsIntegerComparisonAbsorbed(comparison, bounds),
            ColumnType.Bool => IsBoolComparisonAbsorbed(comparison, bounds),
            ColumnType.Float64 => IsFloatComparisonAbsorbed(comparison, bounds),
            ColumnType.String or ColumnType.Id => IsPointComparisonAbsorbed(comparison, bounds),
            _ => false,
        };
    }

    private static bool IsIntegerComparisonAbsorbed(AnalyzedComparison comparison, ColumnScanBounds bounds)
    {
        if (!TryGetIntegerRange(bounds, out long min, out long max))
            return false;

        long constant = comparison.Constant.LongValue;

        return comparison.Operator switch
        {
            "=" => min == max && min == constant,
            ">=" => min >= constant,
            ">" => min > constant,
            "<=" => max <= constant,
            "<" => max < constant,
            "!=" => constant < min || constant > max,
            _ => false,
        };
    }

    private static bool IsBoolComparisonAbsorbed(AnalyzedComparison comparison, ColumnScanBounds bounds)
    {
        if (!TryGetBoolRange(bounds, out int min, out int max))
            return false;

        int constant = comparison.Constant.BoolValue ? 1 : 0;

        return comparison.Operator switch
        {
            "=" => min == max && min == constant,
            ">=" => min >= constant,
            ">" => min > constant,
            "<=" => max <= constant,
            "<" => max < constant,
            "!=" => constant < min || constant > max,
            _ => false,
        };
    }

    private static bool IsFloatComparisonAbsorbed(AnalyzedComparison comparison, ColumnScanBounds bounds)
    {
        if (!TryGetFloatRange(bounds, out double min, out double max))
            return false;

        double constant = comparison.Constant.FloatValue;

        return comparison.Operator switch
        {
            "=" => min == max && min == constant,
            ">=" => min >= constant,
            ">" => min > constant,
            "<=" => max <= constant,
            "<" => max < constant,
            "!=" => constant < min || constant > max,
            _ => false,
        };
    }

    private static bool IsPointComparisonAbsorbed(AnalyzedComparison comparison, ColumnScanBounds bounds)
    {
        if (comparison.Operator != "=")
            return false;

        return bounds.Lower is not null
            && bounds.Upper is not null
            && bounds.LowerInclusive
            && bounds.UpperInclusive
            && bounds.Lower.CompareTo(bounds.Upper) == 0
            && bounds.Lower.CompareTo(comparison.Constant) == 0;
    }

    private static bool TryGetIntegerRange(ColumnScanBounds bounds, out long min, out long max)
    {
        min = long.MinValue;
        max = long.MaxValue;

        if (bounds.Lower is { Type: ColumnType.Integer64 } lower)
        {
            min = lower.LongValue;
            if (!bounds.LowerInclusive)
            {
                if (min == long.MaxValue)
                    return false;

                min++;
            }
        }

        if (bounds.Upper is { Type: ColumnType.Integer64 } upper)
        {
            max = upper.LongValue;
            if (!bounds.UpperInclusive)
            {
                if (max == long.MinValue)
                    return false;

                max--;
            }
        }

        return bounds.Lower is not null || bounds.Upper is not null;
    }

    private static bool TryGetFloatRange(ColumnScanBounds bounds, out double min, out double max)
    {
        min = double.NegativeInfinity;
        max = double.PositiveInfinity;

        if (bounds.Lower is { Type: ColumnType.Float64 } lower)
        {
            min = lower.FloatValue;
            if (!bounds.LowerInclusive)
                min = Math.BitIncrement(min);
        }

        if (bounds.Upper is { Type: ColumnType.Float64 } upper)
        {
            max = upper.FloatValue;
            if (!bounds.UpperInclusive)
                max = Math.BitDecrement(max);
        }

        return bounds.Lower is not null || bounds.Upper is not null;
    }

    private static bool TryGetBoolRange(ColumnScanBounds bounds, out int min, out int max)
    {
        min = 0;
        max = 1;

        if (bounds.Lower is { Type: ColumnType.Bool } lower)
        {
            min = lower.BoolValue ? 1 : 0;
            if (!bounds.LowerInclusive)
            {
                if (min == 1)
                    return false;

                min = 1;
            }
        }

        if (bounds.Upper is { Type: ColumnType.Bool } upper)
        {
            max = upper.BoolValue ? 1 : 0;
            if (!bounds.UpperInclusive)
            {
                if (max == 0)
                    return false;

                max = 0;
            }
        }

        return bounds.Lower is not null || bounds.Upper is not null;
    }
}
