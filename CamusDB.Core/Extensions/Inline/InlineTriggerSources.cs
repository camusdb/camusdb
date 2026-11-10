/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// Builds the compile input of an inline trigger from the live schema: the columns of its table and
/// the inline functions of its database, in a fixed order. The DDL builds one to check the body on the
/// proposing node, and the trigger runtime builds one at each bind, so a unit always matches the
/// schema it fires under.
/// </summary>
internal static class InlineTriggerSources
{
    public static InlineTriggerSource For(DatabaseDescriptor database, TableDescriptor table, string triggerName, string body) =>
        For(database.Schema, table.Schema, triggerName, body);

    public static InlineTriggerSource For(Schema schema, TableSchema table, string triggerName, string body)
    {
        IReadOnlyList<TableColumnSchema> columns = table.Columns ?? [];
        InlineTriggerColumn[] shape = new InlineTriggerColumn[columns.Count];

        for (int i = 0; i < shape.Length; i++)
            shape[i] = new InlineTriggerColumn(columns[i].Name, columns[i].Type, columns[i].ArrayElementType);

        // The functions in name order, so that two nodes build the same unit from the same catalog.
        List<InlineFunctionDefinition> functions = [.. schema.Functions.Values];
        functions.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        return new InlineTriggerSource
        {
            Name = triggerName,
            TableName = table.Name ?? "",
            Source = body,
            Columns = shape,
            Functions = functions,
        };
    }
}
