
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Describes a semi/anti-join extracted from an IN / NOT IN subquery predicate (R11).
/// Produced by <see cref="SemiJoinAnalyzer"/> and consumed by <see cref="QueryPlanner"/>
/// to build a <see cref="CamusDB.Core.CommandsExecutor.Models.Plans.SemiJoinNode"/> into the plan.
/// </summary>
public sealed record SemiJoinSpec(
    /// <summary>Column name in the outer table row used as the probe key.</summary>
    string OuterColumn,

    /// <summary>The inner table to probe.</summary>
    TableDescriptor InnerTable,

    /// <summary>Column in the inner table to match against <see cref="OuterColumn"/>.</summary>
    string InnerColumn,

    /// <summary>Index on <see cref="InnerColumn"/>, if any — enables fast index probe.</summary>
    TableIndexSchema? InnerIndex,

    /// <summary>Additional predicate from the inner SELECT's WHERE clause (may be null).</summary>
    NodeAst? InnerFilter,

    /// <summary>Join flavour: semi (IN), anti (NOT IN + NOT NULL), or null-aware anti (NOT IN + nullable).</summary>
    SemiJoinMode Mode
);
