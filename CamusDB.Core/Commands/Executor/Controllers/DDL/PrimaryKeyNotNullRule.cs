/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// SQL requires every primary-key column to be NOT NULL, and the engine depends on it: a primary
/// key is stored as an ordinary unique index, and a unique index carries no entry for a row with a
/// NULL in any key column. A nullable primary-key column would therefore accept a row the key
/// cannot see — no uniqueness enforced, no prefix scan or seek complete — so the planner's
/// completeness rule would refuse every prefix use of the key. Declaring the columns NOT NULL at
/// creation, whether or not the user wrote it, keeps the key complete and keeps those access paths.
/// Tables created before this rule keep their declared nullability; nothing rewrites an existing
/// schema, because a stored NULL row cannot be ruled out at load time.
/// </summary>
internal static class PrimaryKeyNotNullRule
{
    /// <summary>
    /// Marks every column named by a PRIMARY KEY constraint of <paramref name="ticket"/> NOT NULL.
    /// Runs once at the table-creation choke point (<see cref="TableCreator"/>), so the SQL path,
    /// the ticket path, CREATE TABLE AS SELECT and a forwarded cluster request all persist the same
    /// schema.
    /// </summary>
    public static void ApplyToCreateTable(CreateTableTicket ticket)
    {
        foreach (ConstraintInfo constraint in ticket.Constraints)
        {
            if (constraint.Type != ConstraintType.PrimaryKey)
                continue;

            foreach (ColumnIndexInfo keyColumn in constraint.Columns)
            {
                ColumnInfo[] columns = ticket.Columns;
                for (int i = 0; i < columns.Length; i++)
                {
                    ColumnInfo column = columns[i];
                    if (column.NotNull || !string.Equals(column.Name, keyColumn.Name, StringComparison.OrdinalIgnoreCase))
                        continue;

                    // ColumnInfo is immutable; replace the entry with a NOT NULL copy of it.
                    columns[i] = new ColumnInfo(
                        column.Name,
                        column.Type,
                        notNull: true,
                        column.Default,
                        column.MaxLength,
                        column.ArrayElementType,
                        column.DefaultFunction,
                        column.NotNullConstraintName,
                        column.Comment);
                }
            }
        }
    }

    /// <summary>
    /// Rejects <c>DROP NOT NULL</c> on a primary-key column of <paramref name="table"/>: the key
    /// would stop holding every row the moment a NULL was inserted.
    /// </summary>
    public static void RejectDropNotNullOnPrimaryKey(TableDescriptor table, string columnName)
    {
        if (!table.Indexes.TryGetValue(CamusDBConstants.PrimaryKeyInternalName, out TableIndexSchema? primaryKey))
            return;

        foreach (string keyColumn in primaryKey.Columns)
        {
            if (string.Equals(keyColumn, columnName, StringComparison.OrdinalIgnoreCase))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{columnName}' is part of the primary key of table '{table.Name}'; primary-key columns are always NOT NULL");
        }
    }
}
