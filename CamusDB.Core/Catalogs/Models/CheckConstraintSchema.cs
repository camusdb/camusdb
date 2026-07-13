
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Persisted definition of a <c>CHECK</c> constraint. The expression is stored as a
/// human-readable SQL text string (produced by <c>PlanRenderer.RenderExpr</c>) so it can be
/// shown in <c>SHOW CREATE TABLE</c> output and re-parsed on table-open. The parsed AST is
/// a transient, per-load cache — never persisted — rebuilt lazily by <c>CatalogsManager</c>
/// after the JSON checkpoint is loaded.
/// <para>
/// <b>NULL semantics:</b> enforcement uses a three-valued evaluator (see
/// <c>CheckEvaluator</c>), not the WHERE-path evaluator, so a NULL referenced column causes
/// the check to pass (unknown → satisfied), matching SQL standard semantics.
/// </para>
/// </summary>
public sealed class CheckConstraintSchema
{
    /// <summary>
    /// Constraint name. Auto-generated as <c>{table}_{column}_check</c> for column-level checks
    /// and <c>{table}_check{N}</c> for unnamed table-level checks.
    /// </summary>
    public string Name { get; set; } = "";

    /// <summary>
    /// SQL text of the condition, produced by rendering the parsed AST via
    /// <c>PlanRenderer.RenderExpr</c>. This is the persisted form; re-parsed to
    /// <see cref="ParsedCondition"/> at table-open time.
    /// </summary>
    public string Expression { get; set; } = "";

    /// <summary>
    /// Identifiers referenced by the condition. Used at DDL time to verify all referenced
    /// columns exist, and at UPDATE time to skip re-evaluation when none of the check's
    /// columns are included in the SET list.
    /// </summary>
    public string[] ReferencedColumns { get; set; } = [];

    /// <summary>
    /// Parsed AST of <see cref="Expression"/>. Not persisted; rebuilt lazily by
    /// <c>CatalogsManager.LoadCheckConstraintAsts</c> after schema is loaded from JSON.
    /// </summary>
    [JsonIgnore]
    public NodeAst? ParsedCondition { get; set; }
}
