
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A logical <see cref="SelectQuery"/> after name resolution and table open.
/// </summary>
public sealed class BoundSelectQuery
{
    public SelectQuery Query { get; }

    /// <summary>
    /// Ordered bound sources for nested-loop join execution. Left-deep join trees preserve
    /// left-to-right source order in this list.
    /// </summary>
    public IReadOnlyList<BoundTableSource> Sources { get; }

    public QueryRowNameResolver RowNames { get; }

    public BoundSelectQuery(
        SelectQuery query,
        IReadOnlyList<BoundTableSource> sources,
        QueryRowNameResolver rowNames)
    {
        Query = query;
        Sources = sources;
        RowNames = rowNames;
    }

    /// <summary>
    /// Primary table for today's single-table execution path.
    /// </summary>
    public TableDescriptor PrimaryTable
    {
        get
        {
            if (Sources.Count != 1)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Multi-source SELECT execution is not supported yet");
            }

            return Sources[0].Table;
        }
    }
}
