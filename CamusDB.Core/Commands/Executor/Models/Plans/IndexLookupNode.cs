
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>Point lookup on a unique or multi-value index.</summary>
public sealed class IndexLookupNode : PhysicalPlanNode
{
    public TableIndexSchema Index { get; }

    public CompositeColumnValue LookupKey { get; }

    public IndexLookupNode(TableIndexSchema index, CompositeColumnValue lookupKey)
    {
        Index = index;
        LookupKey = lookupKey;
    }

    public IndexLookupNode(TableIndexSchema index, ColumnValue lookupValue)
        : this(index, new CompositeColumnValue(new[] { lookupValue }))
    {
    }
}
