
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

public readonly struct AlterIndexTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    public string IndexName { get; }

    public ColumnIndexInfo[] Columns { get; }

    /// <summary>
    /// Names of the stored/payload (INCLUDE) columns, in declared order. Empty when the statement
    /// has no INCLUDE clause. These are materialized into every index entry's value so covering
    /// scans can return them without a primary-row fetch; they never participate in the key,
    /// ordering, or uniqueness (unlike <see cref="Columns"/>), hence a bare name list with no
    /// direction.
    /// </summary>
    public string[] IncludeColumns { get; }

    public AlterIndexOperation Operation { get; }

    public bool IfNotExists { get; }

    public string? NewName { get; }

    public AlterIndexTicket(
        string databaseName,
        string tableName,
        string indexName,
        ColumnIndexInfo[] columns,
        AlterIndexOperation operation,
        bool ifNotExists = false,
        string? newName = null,
        string[]? includeColumns = null
    )
    {
        DatabaseName = databaseName;
        TableName = tableName;
        IndexName = indexName;
        Columns = columns;
        Operation = operation;
        IfNotExists = ifNotExists;
        NewName = newName;
        IncludeColumns = includeColumns ?? [];
    }
}
