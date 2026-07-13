
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Identifies the ALTER TABLE constraint operation to perform.
/// </summary>
public enum AlterConstraintOperation
{
    /// <summary>Adds a named CHECK constraint and validates it against all existing rows.</summary>
    AddCheck,

    /// <summary>
    /// Drops a named constraint by name. The name is resolved against CHECK constraints and
    /// named NOT NULL constraints on each column.
    /// </summary>
    DropConstraint,

    /// <summary>
    /// Sets NOT NULL on a column (<c>ALTER TABLE t ALTER COLUMN c SET NOT NULL</c>).
    /// Scans all existing rows and rejects the ALTER if any row has NULL in the target column.
    /// <see cref="AlterConstraintTicket.ColumnName"/> identifies the target column.
    /// </summary>
    SetNotNull,

    /// <summary>
    /// Removes the NOT NULL constraint from a column (<c>ALTER TABLE t ALTER COLUMN c DROP NOT NULL</c>).
    /// Unconditional — no existing-row scan required.
    /// <see cref="AlterConstraintTicket.ColumnName"/> identifies the target column.
    /// </summary>
    DropNotNull,
}

/// <summary>
/// Ticket for <c>ALTER TABLE … ADD CONSTRAINT … CHECK</c>, <c>ALTER TABLE … DROP CONSTRAINT …</c>,
/// <c>ALTER TABLE … ALTER COLUMN … SET NOT NULL</c>, and
/// <c>ALTER TABLE … ALTER COLUMN … DROP NOT NULL</c> operations.
/// </summary>
public readonly struct AlterConstraintTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    /// <summary>
    /// Name of the constraint to add or drop. Empty/unused for <see cref="AlterConstraintOperation.SetNotNull"/>
    /// and <see cref="AlterConstraintOperation.DropNotNull"/>.
    /// </summary>
    public string ConstraintName { get; }

    /// <summary>
    /// The SQL expression text for the new CHECK constraint.
    /// Null when <see cref="Operation"/> is not <see cref="AlterConstraintOperation.AddCheck"/>.
    /// </summary>
    public string? Expression { get; }

    /// <summary>
    /// Column names referenced in the CHECK expression.
    /// Null when <see cref="Operation"/> is not <see cref="AlterConstraintOperation.AddCheck"/>.
    /// </summary>
    public string[]? ReferencedColumns { get; }

    public AlterConstraintOperation Operation { get; }

    /// <summary>
    /// Target column name for <see cref="AlterConstraintOperation.SetNotNull"/> and
    /// <see cref="AlterConstraintOperation.DropNotNull"/>. Null for CHECK-based operations.
    /// </summary>
    public string? ColumnName { get; }

    public AlterConstraintTicket(
        string databaseName,
        string tableName,
        string constraintName,
        string? expression,
        string[]? referencedColumns,
        AlterConstraintOperation operation,
        string? columnName = null)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        ConstraintName = constraintName;
        Expression = expression;
        ReferencedColumns = referencedColumns;
        Operation = operation;
        ColumnName = columnName;
    }
}
