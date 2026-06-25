
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
    /// Ordered bound table sources for nested-loop join execution. Left-deep join trees preserve
    /// left-to-right source order in this list.
    /// </summary>
    public IReadOnlyList<BoundTableSource> Sources { get; }

    /// <summary>
    /// Bound derived-table sources referenced by the query.
    /// </summary>
    public IReadOnlyList<BoundDerivedTableSource> DerivedSources { get; }

    public QueryRowNameResolver RowNames { get; }

    public bool IsMultiSource =>
        Query.Source is JoinSource
        || Sources.Count > 1
        || DerivedSources.Count > 0;

    public BoundSelectQuery(
        SelectQuery query,
        IReadOnlyList<BoundTableSource> sources,
        QueryRowNameResolver rowNames,
        IReadOnlyList<BoundDerivedTableSource>? derivedSources = null)
    {
        Query = query;
        Sources = sources;
        DerivedSources = derivedSources ?? Array.Empty<BoundDerivedTableSource>();
        RowNames = rowNames;
    }

    /// <summary>
    /// Primary table for today's single-table execution path.
    /// </summary>
    public TableDescriptor PrimaryTable
    {
        get
        {
            if (IsMultiSource)
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Multi-source SELECT execution is not supported yet");
            }

            return Sources[0].Table;
        }
    }
}
