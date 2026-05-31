
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>Bounded or half-bounded scan on an index key range.</summary>
public sealed class IndexRangeScanNode : PhysicalPlanNode
{
    public TableIndexSchema Index { get; }

    public CompositeColumnValue? FromBound { get; }

    public bool FromInclusive { get; }

    public CompositeColumnValue? ToBound { get; }

    public bool ToInclusive { get; }

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
