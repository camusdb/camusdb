
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

public record struct QueryPlanStep
{
    public QueryPlanStepType Type { get; }

    public TableIndexSchema? Index { get; } = null;

    /// <summary>Single equality value used by <see cref="QueryPlanStepType.QueryFromIndex"/>.</summary>
    public ColumnValue? ColumnValue { get; } = null;

    /// <summary>Composite lookup key used by <see cref="QueryPlanStepType.QueryFromIndex"/>.</summary>
    public CompositeColumnValue? LookupKey { get; } = null;

    /// <summary>Inclusive lower bound for <see cref="QueryPlanStepType.RangeScanFromIndex"/>. Null means unbounded.</summary>
    public CompositeColumnValue? FromBound { get; } = null;

    /// <summary>Whether the lower bound is inclusive (≥) or exclusive (>).</summary>
    public bool FromInclusive { get; } = true;

    /// <summary>Inclusive upper bound for <see cref="QueryPlanStepType.RangeScanFromIndex"/>. Null means unbounded.</summary>
    public CompositeColumnValue? ToBound { get; } = null;

    /// <summary>Whether the upper bound is inclusive (≤) or exclusive (&lt;).</summary>
    public bool ToInclusive { get; } = true;

    /// <summary>
    /// How many leading key columns of a <see cref="QueryPlanStepType.RangeScanFromIndex"/> step the
    /// predicate fixes by equality. The bounds alone do not say: an equality on an ascending column
    /// with a successor becomes the half-open <c>[v, next(v))</c>, which has the shape of a range on
    /// that column, and a prefix plus a range puts the range value only in one bound. The cost model
    /// prices the first <c>n</c> columns by distinct count and the column after them, when a bound
    /// reaches it, by its histogram; the residual filter drops an equality on one of these columns.
    /// Zero on a step that did not come from the predicate matcher (an ORDER BY scan, a step rebuilt
    /// from a plan node), and the readers then infer the prefix from the bound shape.
    /// </summary>
    public int EqualityPrefixLength { get; init; }

    public QueryPlanStep(QueryPlanStepType type, TableIndexSchema? index = null, ColumnValue? columnValue = null)
    {
        Type = type;
        Index = index;
        ColumnValue = columnValue;
        if (columnValue is not null)
            LookupKey = new CompositeColumnValue(new[] { columnValue });
    }

    public QueryPlanStep(QueryPlanStepType type, TableIndexSchema index, CompositeColumnValue lookupKey)
    {
        Type = type;
        Index = index;
        LookupKey = lookupKey;
        ColumnValue = lookupKey.Values.Length == 1 ? lookupKey.Values[0] : null;
    }

    public QueryPlanStep(
        QueryPlanStepType type,
        TableIndexSchema index,
        CompositeColumnValue? fromBound,
        bool fromInclusive,
        CompositeColumnValue? toBound,
        bool toInclusive)
    {
        Type = type;
        Index = index;
        FromBound = fromBound;
        FromInclusive = fromInclusive;
        ToBound = toBound;
        ToInclusive = toInclusive;
    }
}
