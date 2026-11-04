
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Describes a semi/anti-join extracted from an IN / NOT IN subquery predicate.
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

    /// <summary>
    /// The name scope of the inner SELECT: its one table under its alias, or under the table name
    /// when the query gives no alias. <see cref="InnerFilter"/> is evaluated with it, so a reference
    /// the subquery qualifies with its own alias reads the decoded inner row, which is keyed by bare
    /// column names. The analyzer resolves every reference of the filter through it and then freezes
    /// it, so an execution that shares the spec reads the memo without a write.
    /// </summary>
    QueryRowNameResolver InnerResolver,

    /// <summary>
    /// The inner-table columns a probe decodes: <see cref="InnerColumn"/> plus every column that
    /// <see cref="InnerFilter"/> reads, as bare names. Built once by the analyzer, so a probe does not
    /// rebuild it for every outer row.
    /// </summary>
    IReadOnlySet<string> InnerRequiredColumns,

    /// <summary>Join flavour: semi (IN), anti (NOT IN + NOT NULL), or null-aware anti (NOT IN + nullable).</summary>
    SemiJoinMode Mode
);
