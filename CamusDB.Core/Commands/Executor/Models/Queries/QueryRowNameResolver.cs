
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

    /// <summary>
    /// Memoizes the pure mapping from an identifier to its resolved row-lookup key. The result is a
    /// function only of this resolver's fixed sources and the identifier text, both constant for the
    /// life of the query, yet <see cref="ResolveRowLookupKey"/> is invoked once per scanned row per
    /// identifier occurrence on the hot predicate path — recomputing it re-runs the qualified split,
    /// the linear column scan, and key formatting on every row. Caching successful resolutions
    /// collapses all of that to one dictionary probe. Only successes are stored: the three failure
    /// modes throw, so a repeated bad identifier must re-throw the same exception rather than read a
    /// cached result. Safe as a plain <see cref="Dictionary{TKey,TValue}"/> because a resolver
    /// instance is consumed by a single strictly-sequential scan; it is never touched by two threads
    /// at once within one execution.
    /// </summary>
    private readonly Dictionary<string, string> lookupKeyMemo = new(StringComparer.Ordinal);

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
            {
                if (!SchemaElementStateRules.IsReadable(column))
                    continue;

                RegisterColumnAlias(column.Name, source.Alias);
            }
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
        if (lookupKeyMemo.TryGetValue(identifier, out string? cachedKey))
            return cachedKey;

        string lookupKey = TrySplitQualified(identifier, out string alias, out string columnName)
            ? ResolveQualifiedColumn(alias, columnName, identifier)
            : ResolveUnqualifiedColumn(columnName: identifier);

        // Cache successes only. The failure modes above throw before reaching here, so a repeated
        // bad identifier keeps re-throwing the same exception instead of returning a stale result.
        lookupKeyMemo[identifier] = lookupKey;
        return lookupKey;
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

            return QualifiedLookupKey(columnName, originalIdentifier);
        }

        if (aliasToDerived.TryGetValue(alias, out BoundDerivedTableSource? derived))
        {
            if (!derived.HasColumn(columnName))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownColumn,
                    $"Unknown column: {originalIdentifier}");
            }

            return QualifiedLookupKey(columnName, originalIdentifier);
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Unknown alias '{alias}'");
    }

    /// <summary>
    /// Lookup key for an already-qualified <c>alias.column</c> reference. When the query uses
    /// qualified row keys, that key is character-identical to the incoming identifier — parse-time
    /// normalization lowercases the whole identifier as one string, so <c>alias + "." + column</c>
    /// reconstructs it exactly — so the original identifier is returned instead of rebuilding an
    /// equal string via <see cref="FormatQualifiedKey"/>. A single base table stores bare column
    /// keys, so it returns the already-split column name.
    /// </summary>
    private string QualifiedLookupKey(string columnName, string originalIdentifier) =>
        UsesQualifiedRowKeys() ? originalIdentifier : columnName;

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
            if (column.Name == columnName && SchemaElementStateRules.IsReadable(column))
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
