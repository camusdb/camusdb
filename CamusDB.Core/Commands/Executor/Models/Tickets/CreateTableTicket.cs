
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct CreateTableTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public ColumnInfo[] Columns { get; }

    public ConstraintInfo[] Constraints { get; }

    /// <summary>
    /// CHECK constraints collected from both column-level inline declarations (desugared to
    /// named constraints) and explicit table-level <c>CONSTRAINT name CHECK (cond)</c> clauses.
    /// Empty when no CHECK constraints were declared.
    /// </summary>
    public CheckConstraintInfo[] CheckConstraints { get; }

    public bool IfNotExists { get; }

    /// <summary>
    /// Table-level comment from a trailing <c>) COMMENT '…'</c>. Null when none was declared. There
    /// is no removal form here — clearing a table comment is <c>COMMENT ON TABLE … IS NULL</c>.
    /// </summary>
    public string? Comment { get; }

    public CreateTableTicket(
        string databaseName,
        string tableName,
        ColumnInfo[] columns,
        ConstraintInfo[] constraints,
        bool ifNotExists,
        CheckConstraintInfo[]? checkConstraints = null,
        string? comment = null
    )
    {
        Comment = comment;
        DatabaseName = databaseName;
        TableName = tableName;
        Columns = columns;
        Constraints = constraints;
        CheckConstraints = checkConstraints ?? [];
        IfNotExists = ifNotExists;
    }
}

