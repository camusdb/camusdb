
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Plans;

/// <summary>
/// Semi/anti-join plan node produced by the IN/NOT IN rewrite.
///
/// Wraps the outer scan (<see cref="PhysicalPlanNode.Input"/>) and, for each outer row,
/// probes <see cref="InnerTable"/> to decide whether to emit or discard the row:
/// <list type="bullet">
///   <item><see cref="SemiJoinMode.Semi"/> — emit when a probe match is found (IN).</item>
///   <item><see cref="SemiJoinMode.Anti"/> — emit when no probe match is found, inner column NOT NULL.</item>
///   <item><see cref="SemiJoinMode.NullAwareAnti"/> — same as Anti but pre-checks for inner NULLs;
///     if any exist, discards all outer rows (SQL three-valued UNKNOWN semantics).</item>
/// </list>
/// The inner WHERE (<see cref="InnerFilter"/>) is evaluated per probe with <see cref="InnerResolver"/>,
/// the name scope of the inner SELECT, so a reference the subquery qualifies with its own alias
/// reads the decoded inner row, which is keyed by bare column names.
/// </summary>
public sealed class SemiJoinNode : PhysicalPlanNode
{
    public SemiJoinMode Mode { get; }

    public TableDescriptor InnerTable { get; }

    public string OuterColumn { get; }

    public string InnerColumn { get; }

    public TableIndexSchema? InnerIndex { get; }

    public NodeAst? InnerFilter { get; }

    /// <summary>The name scope of the inner SELECT; see <see cref="SemiJoinSpec.InnerResolver"/>.</summary>
    public QueryRowNameResolver InnerResolver { get; }

    /// <summary>The inner columns a probe decodes; see <see cref="SemiJoinSpec.InnerRequiredColumns"/>.</summary>
    public IReadOnlySet<string> InnerRequiredColumns { get; }

    public SemiJoinNode(
        PhysicalPlanNode outerInput,
        SemiJoinMode mode,
        TableDescriptor innerTable,
        string outerColumn,
        string innerColumn,
        TableIndexSchema? innerIndex,
        NodeAst? innerFilter,
        QueryRowNameResolver innerResolver,
        IReadOnlySet<string> innerRequiredColumns)
    {
        Input = outerInput;
        Mode = mode;
        InnerTable = innerTable;
        OuterColumn = outerColumn;
        InnerColumn = innerColumn;
        InnerIndex = innerIndex;
        InnerFilter = innerFilter;
        InnerResolver = innerResolver;
        InnerRequiredColumns = innerRequiredColumns;
    }
}
