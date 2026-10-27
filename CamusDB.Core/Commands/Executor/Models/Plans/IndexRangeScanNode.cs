
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>Bounded or half-bounded scan on an index key range.</summary>
public sealed class IndexRangeScanNode : PhysicalPlanNode
{
    public TableIndexSchema Index { get; }

    public CompositeColumnValue? FromBound { get; }

    public bool FromInclusive { get; }

    public CompositeColumnValue? ToBound { get; }

    public bool ToInclusive { get; }

    /// <summary>
    /// How many leading key columns the bounds fix by equality, carried from
    /// <see cref="QueryPlanStep.EqualityPrefixLength"/>. The cost model prices those columns by
    /// distinct count instead of reading an equality's half-open <c>[v, next(v))</c> as a range on the
    /// column. Zero when unknown; the estimate then infers the prefix from the bound shape.
    /// </summary>
    public int EqualityPrefixLength { get; init; }

    /// <summary>Bound table source when used as a join leaf; null for single-table plans.</summary>
    public BoundTableSource? BoundSource { get; init; }

    /// <summary>Residual filter applied to fetched rows after the index range scan; null means no additional filter.</summary>
    public NodeAst? ExecutionFilter { get; init; }

    public override bool CanDecomposeToLocalPlusMerge => true;

    public IndexRangeScanNode(
        TableIndexSchema index,
        CompositeColumnValue? fromBound,
        bool fromInclusive,
        CompositeColumnValue? toBound,
        bool toInclusive)
    {
        Index = index;
        FromBound = fromBound;
        FromInclusive = fromInclusive;
        ToBound = toBound;
        ToInclusive = toInclusive;
    }
}
