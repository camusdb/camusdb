
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal static class BoundSourceCatalog
{
    public static bool TableHasColumn(BoundTableSource source, string columnName)
    {
        foreach (TableColumnSchema column in source.Table.Schema.Columns ?? [])
        {
            if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase) && SchemaElementStateRules.IsReadable(column))
                return true;
        }

        return false;
    }

    public static bool TryResolveUnqualifiedOwner(BoundSelectQuery bound, string columnName, out string alias)
    {
        List<string> owners = new();

        foreach (BoundTableSource source in bound.Sources)
        {
            if (TableHasColumn(source, columnName))
                owners.Add(source.Alias);
        }

        foreach (BoundDerivedTableSource source in bound.DerivedSources)
        {
            if (source.HasColumn(columnName))
                owners.Add(source.Alias);
        }

        if (owners.Count == 1)
        {
            alias = owners[0];
            return true;
        }

        alias = "";
        return false;
    }

    public static BoundTableSource FindTableSource(BoundSelectQuery bound, TableSource tableSource)
    {
        string alias = tableSource.Alias ?? tableSource.TableName;

        foreach (BoundTableSource source in bound.Sources)
        {
            if (source.Source.TableName == tableSource.TableName && source.Alias == alias)
                return source;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Bound source not found for table '{tableSource.TableName}' alias '{alias}'");
    }

    public static BoundDerivedTableSource FindDerivedSource(BoundSelectQuery bound, DerivedTableSource derivedSource)
    {
        foreach (BoundDerivedTableSource source in bound.DerivedSources)
        {
            if (source.Source == derivedSource || source.Alias == derivedSource.Alias)
                return source;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Bound derived source not found for alias '{derivedSource.Alias}'");
    }

    public static BoundJoinRightSource FindJoinRightSource(BoundSelectQuery bound, QuerySource source)
    {
        return source switch
        {
            TableSource tableSource => BoundJoinRightSource.FromTable(FindTableSource(bound, tableSource)),
            DerivedTableSource derivedSource => BoundJoinRightSource.FromDerived(FindDerivedSource(bound, derivedSource)),
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Unsupported join right source: {source.GetType().Name}"),
        };
    }
}
