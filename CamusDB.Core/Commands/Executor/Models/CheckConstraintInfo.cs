
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Carries one CHECK constraint through the ticket → CatalogsManager boundary. Populated by
/// <c>SQLExecutorCreateTableCreator</c> after desugaring and rendering column-level and
/// table-level CHECK expressions, and consumed by <c>CatalogsManager.CreateTableEntry</c>
/// to build <c>TableSchema.CheckConstraints</c>.
/// </summary>
public sealed class CheckConstraintInfo
{
    /// <summary>Constraint name (auto-generated when not explicitly provided).</summary>
    public string Name { get; }

    /// <summary>
    /// Rendered SQL text of the condition (from <c>CheckConditionRenderer</c>).
    /// Stored verbatim in <c>CheckConstraintSchema.Expression</c>.
    /// </summary>
    public string Expression { get; }

    /// <summary>Column names referenced by the condition.</summary>
    public string[] ReferencedColumns { get; }

    public CheckConstraintInfo(string name, string expression, string[] referencedColumns)
    {
        Name = name;
        Expression = expression;
        ReferencedColumns = referencedColumns;
    }
}
