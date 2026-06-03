/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

public static class SchemaElementStateRules
{
    public static bool IsReadable(TableColumnSchema column) => column.State == SchemaElementState.Public;

    public static bool IsWritable(TableColumnSchema column) =>
        column.State is SchemaElementState.WriteOnly or SchemaElementState.Public;

    public static bool IsReadable(TableIndexSchema index) => index.State == SchemaElementState.Public;

    public static bool IsWritable(TableIndexSchema index) =>
        index.State is SchemaElementState.WriteOnly or SchemaElementState.Public;

    public static bool IsReadableIndex(TableSchema table, TableIndexSchema index)
    {
        if (!IsReadable(index))
            return false;

        return index.Columns.All(columnName =>
            table.Columns?.Any(column => column.Name == columnName && IsReadable(column)) == true
        );
    }

    public static bool IsWritableIndex(TableSchema table, TableIndexSchema index)
    {
        if (!IsWritable(index))
            return false;

        return index.Columns.All(columnName =>
            table.Columns?.Any(column => column.Name == columnName && IsWritable(column)) == true
        );
    }
}
