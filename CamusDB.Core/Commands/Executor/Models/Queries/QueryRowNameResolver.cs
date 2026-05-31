
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Resolves column references against bound query sources for validation and row lookup.
/// </summary>
public sealed class QueryRowNameResolver
{
    private readonly IReadOnlyList<BoundTableSource> sources;

    private readonly Dictionary<string, BoundTableSource> aliasToSource;

    private readonly Dictionary<string, List<string>> columnNameToAliases;

    public QueryRowNameResolver(IReadOnlyList<BoundTableSource> sources)
    {
        this.sources = sources;
        aliasToSource = new Dictionary<string, BoundTableSource>(StringComparer.Ordinal);
        columnNameToAliases = new Dictionary<string, List<string>>(StringComparer.Ordinal);

        foreach (BoundTableSource source in sources)
        {
            if (!aliasToSource.TryAdd(source.Alias, source))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Duplicate alias '{source.Alias}'");
            }

            foreach (TableColumnSchema column in source.Table.Schema.Columns ?? [])
            {
                if (!columnNameToAliases.TryGetValue(column.Name, out List<string>? aliases))
                    columnNameToAliases[column.Name] = aliases = new();

                aliases.Add(source.Alias);
            }
        }
    }

    /// <summary>
    /// Validates that a column reference exists and is not ambiguous. Used during binding.
    /// </summary>
    public void ValidateColumnReference(string identifier)
    {
        _ = ResolveRowLookupKey(identifier);
    }

    /// <summary>
    /// Maps a parsed column reference to the dictionary key used when reading a row.
    /// Single-table scans still store unqualified keys; joined rows use qualified keys per
    /// <see cref="BoundRow"/>.
    /// </summary>
    public string ResolveRowLookupKey(string identifier)
    {
        if (TrySplitQualified(identifier, out string alias, out string columnName))
            return ResolveQualifiedColumn(alias, columnName, identifier);

        return ResolveUnqualifiedColumn(columnName: identifier);
    }

    public static string FormatQualifiedKey(string alias, string columnName) =>
        $"{alias}.{columnName}";

    private string ResolveQualifiedColumn(string alias, string columnName, string originalIdentifier)
    {
        if (!aliasToSource.TryGetValue(alias, out BoundTableSource? source))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Unknown alias '{alias}'");
        }

        if (!SourceHasColumn(source, columnName))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownColumn,
                $"Unknown column: {originalIdentifier}");
        }

        if (sources.Count == 1)
            return columnName;

        return FormatQualifiedKey(alias, columnName);
    }

    private string ResolveUnqualifiedColumn(string columnName)
    {
        if (!columnNameToAliases.TryGetValue(columnName, out List<string>? aliases) || aliases.Count == 0)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownColumn,
                $"Unknown column: {columnName}");
        }

        if (aliases.Count > 1)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Ambiguous column: {columnName}");
        }

        if (sources.Count == 1)
            return columnName;

        return FormatQualifiedKey(aliases[0], columnName);
    }

    private static bool SourceHasColumn(BoundTableSource source, string columnName)
    {
        foreach (TableColumnSchema column in source.Table.Schema.Columns ?? [])
        {
            if (column.Name == columnName)
                return true;
        }

        return false;
    }

    private static bool TrySplitQualified(string identifier, out string alias, out string columnName)
    {
        int dotIndex = identifier.IndexOf('.');

        if (dotIndex <= 0 || dotIndex >= identifier.Length - 1)
        {
            alias = "";
            columnName = identifier;
            return false;
        }

        alias = identifier[..dotIndex];
        columnName = identifier[(dotIndex + 1)..];
        return true;
    }
}
