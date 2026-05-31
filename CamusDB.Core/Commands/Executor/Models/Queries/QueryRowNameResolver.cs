
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

    private readonly IReadOnlyList<BoundDerivedTableSource> derivedSources;

    private readonly Dictionary<string, BoundTableSource> aliasToSource;

    private readonly Dictionary<string, BoundDerivedTableSource> aliasToDerived;

    private readonly Dictionary<string, List<string>> columnNameToAliases;

    public QueryRowNameResolver(
        IReadOnlyList<BoundTableSource> sources,
        IReadOnlyList<BoundDerivedTableSource>? derivedSources = null)
    {
        this.sources = sources;
        this.derivedSources = derivedSources ?? Array.Empty<BoundDerivedTableSource>();
        aliasToSource = new Dictionary<string, BoundTableSource>(StringComparer.Ordinal);
        aliasToDerived = new Dictionary<string, BoundDerivedTableSource>(StringComparer.Ordinal);
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
                RegisterColumnAlias(column.Name, source.Alias);
        }

        foreach (BoundDerivedTableSource source in this.derivedSources)
        {
            if (!aliasToDerived.TryAdd(source.Alias, source))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Duplicate alias '{source.Alias}'");
            }

            foreach (DerivedColumnSchema column in source.Columns)
                RegisterColumnAlias(column.Name, source.Alias);
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
    /// Single base-table scans store unqualified keys; joins and derived-table execution use
    /// qualified keys per <see cref="BoundRow"/>.
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
        if (aliasToSource.TryGetValue(alias, out BoundTableSource? source))
        {
            if (!SourceHasColumn(source, columnName))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownColumn,
                    $"Unknown column: {originalIdentifier}");
            }

            return FormatLookupKey(alias, columnName);
        }

        if (aliasToDerived.TryGetValue(alias, out BoundDerivedTableSource? derived))
        {
            if (!derived.HasColumn(columnName))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownColumn,
                    $"Unknown column: {originalIdentifier}");
            }

            return FormatLookupKey(alias, columnName);
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Unknown alias '{alias}'");
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

        return FormatLookupKey(aliases[0], columnName);
    }

    private string FormatLookupKey(string alias, string columnName)
    {
        if (UsesQualifiedRowKeys())
            return FormatQualifiedKey(alias, columnName);

        return columnName;
    }

    /// <summary>
    /// Whether row dictionaries use qualified alias.column keys for this query shape.
    /// </summary>
    internal bool UsesQualifiedRowKeys()
    {
        if (sources.Count == 1 && derivedSources.Count == 0)
            return false;

        if (sources.Count == 0 && derivedSources.Count == 1)
            return false;

        return true;
    }

    private void RegisterColumnAlias(string columnName, string alias)
    {
        if (!columnNameToAliases.TryGetValue(columnName, out List<string>? aliases))
            columnNameToAliases[columnName] = aliases = new();

        aliases.Add(alias);
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
