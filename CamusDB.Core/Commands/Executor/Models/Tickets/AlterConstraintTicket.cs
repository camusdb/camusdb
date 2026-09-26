
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

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

    /// <summary>
    /// Sets the storage strategy of a <c>string</c>, <c>bytes</c> or array column
    /// (<c>ALTER TABLE t ALTER COLUMN c SET STORAGE PLAIN | MAIN | EXTERNAL | EXTENDED</c>).
    /// Changes the form of future writes only: no stored row is read or rewritten, so the statement
    /// is instant whatever the table size. <see cref="AlterConstraintTicket.ColumnName"/> and
    /// <see cref="AlterConstraintTicket.Storage"/> identify the column and the strategy.
    /// </summary>
    SetStorage,

    /// <summary>
    /// Adds a foreign key (<c>ALTER TABLE t ADD [CONSTRAINT name] FOREIGN KEY …</c>). The constraint
    /// is in <see cref="AlterConstraintTicket.ForeignKey"/>. Appended last: the operation travels to the
    /// leader as an integer when DDL is forwarded.
    /// </summary>
    AddForeignKey,
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

    /// <summary>The new strategy for <see cref="AlterConstraintOperation.SetStorage"/>; null for every other operation.</summary>
    public ColumnStorageStrategy? Storage { get; }

    /// <summary>The constraint to add, for <see cref="AlterConstraintOperation.AddForeignKey"/>; null otherwise.</summary>
    public ForeignKeyInfo? ForeignKey { get; }

    public AlterConstraintTicket(
        string databaseName,
        string tableName,
        string constraintName,
        string? expression,
        string[]? referencedColumns,
        AlterConstraintOperation operation,
        string? columnName = null,
        ColumnStorageStrategy? storage = null,
        ForeignKeyInfo? foreignKey = null)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        ConstraintName = constraintName;
        Expression = expression;
        ReferencedColumns = referencedColumns;
        Operation = operation;
        ColumnName = columnName;
        Storage = storage;
        ForeignKey = foreignKey;
    }
}
