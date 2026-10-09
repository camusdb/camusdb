
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Detects equi-join patterns in join ON clauses for index-nested-loop and
/// hash/merge join. <see cref="TryMatch"/> requires an index; <see cref="TryExtractEquiKeys"/>
/// does not.
///
/// <para>Both sides of a key must have the same declared type. The hash table, the merge order and
/// the index key all treat two values of different types as unequal, but the evaluator does not:
/// INT64 equals NUMERIC exactly, INT64 equals FLOAT64 by value, and a STRING operand converts to a
/// UUID or an object id. A mixed pair taken as a key would drop the matches the evaluator finds, so
/// such a conjunct is not a key; the join keeps it in the ON predicate and the nested loop evaluates
/// it. When a type cannot be resolved (a derived column typed NULL), the pair is kept as a key, as
/// before.</para>
/// </summary>
internal static class JoinEquiJoinAnalyzer
{
    /// <summary>
    /// Extracts every <c>left.col = right.col</c> equi-key pair from <paramref name="onPredicate"/>
    /// without requiring an index on the right side. Used by hash join and merge join.
    /// </summary>
    public static bool TryExtractEquiKeys(
        BoundJoinRightSource rightSource,
        NodeAst onPredicate,
        BoundSelectQuery bound,
        out IReadOnlyList<JoinEquiKeyPair> keyPairs)
    {
        List<JoinEquiKeyPair> pairs = [];
        List<NodeAst> conjuncts = [];
        PredicateAnalyzer.CollectAndConjuncts(onPredicate, conjuncts);

        foreach (NodeAst conjunct in conjuncts)
        {
            if (TryMatchHashJoinConjunct(conjunct, rightSource, bound, out JoinEquiKeyPair? pair))
                pairs.Add(pair);
        }

        keyPairs = pairs;
        return pairs.Count > 0;
    }

    private static bool TryMatchHashJoinConjunct(
        NodeAst conjunct,
        BoundJoinRightSource rightSource,
        BoundSelectQuery bound,
        out JoinEquiKeyPair pair)
    {
        pair = null!;

        if (conjunct.nodeType != NodeType.ExprEquals) return false;
        if (conjunct.leftAst?.nodeType != NodeType.Identifier || conjunct.leftAst.yytext is null) return false;
        if (conjunct.rightAst?.nodeType != NodeType.Identifier || conjunct.rightAst.yytext is null) return false;

        // Try both orientations: either conjunct side might be the build (right) key.
        if (TryMatchHashJoinOrientation(conjunct.leftAst.yytext, conjunct.rightAst.yytext, rightSource, bound, out pair))
            return true;

        return TryMatchHashJoinOrientation(conjunct.rightAst.yytext, conjunct.leftAst.yytext, rightSource, bound, out pair);
    }

    // rightId: should resolve to the build (right) source; leftId: should resolve to a probe (left) source.
    private static bool TryMatchHashJoinOrientation(
        string rightId,
        string leftId,
        BoundJoinRightSource rightSource,
        BoundSelectQuery bound,
        out JoinEquiKeyPair pair)
    {
        pair = null!;

        if (!TrySplitQualified(rightId, out string rightAlias, out string rightColumn)) return false;
        if (!string.Equals(rightAlias, rightSource.Alias, StringComparison.OrdinalIgnoreCase)) return false;
        if (!rightSource.HasColumn(rightColumn)) return false;

        // Arrays are not hashable by value (GetHashCode has no per-element hashing for arrays).
        // Exclude array-typed columns from hash-join equi-keys so the join falls back to
        // nested-loop rather than building a single-bucket hash table and degrading to O(n·m).
        if (rightSource.Table is not null && IsArrayColumn(rightSource.Table, rightColumn))
            return false;

        if (!TryResolveLeftLookupColumnByAlias(leftId, rightSource.Alias, bound, out string leftLookup))
            return false;

        if (!KeyTypesAgree(leftLookup, RightColumnType(rightSource, rightColumn), bound))
            return false;

        pair = new JoinEquiKeyPair(leftLookup, rightColumn);
        return true;
    }

    // Resolves the qualified lookup key for the probe (left) side given the right alias to exclude.
    private static bool TryResolveLeftLookupColumnByAlias(
        string identifier,
        string rightAlias,
        BoundSelectQuery bound,
        out string leftLookupColumn)
    {
        leftLookupColumn = "";

        if (TrySplitQualified(identifier, out string alias, out string columnName))
        {
            if (string.Equals(alias, rightAlias, StringComparison.OrdinalIgnoreCase)) return false;

            foreach (BoundTableSource source in bound.Sources)
            {
                if (source.Alias != alias) continue;
                if (!SourceHasColumn(source, columnName)) return false;
                leftLookupColumn = QueryRowNameResolver.FormatQualifiedKey(alias, columnName);
                return true;
            }

            foreach (BoundDerivedTableSource source in bound.DerivedSources)
            {
                if (source.Alias != alias) continue;
                if (!source.HasColumn(columnName)) return false;
                leftLookupColumn = QueryRowNameResolver.FormatQualifiedKey(alias, columnName);
                return true;
            }

            return false;
        }

        leftLookupColumn = bound.RowNames.ResolveRowLookupKey(identifier);
        if (leftLookupColumn.StartsWith($"{rightAlias}.", StringComparison.OrdinalIgnoreCase)) return false;
        return true;
    }

    public static bool TryMatch(
        BoundTableSource rightSource,
        NodeAst onPredicate,
        BoundSelectQuery bound,
        out JoinEquiJoinIndexMatch match)
    {
        match = null!;

        List<NodeAst> conjuncts = new();
        PredicateAnalyzer.CollectAndConjuncts(onPredicate, conjuncts);

        foreach (NodeAst conjunct in conjuncts)
        {
            if (TryMatchConjunct(conjunct, rightSource, bound, out JoinEquiJoinIndexMatch? conjunctMatch))
            {
                match = conjunctMatch;
                return true;
            }
        }

        return false;
    }

    private static bool TryMatchConjunct(
        NodeAst conjunct,
        BoundTableSource rightSource,
        BoundSelectQuery bound,
        out JoinEquiJoinIndexMatch match)
    {
        match = null!;

        if (conjunct.nodeType != NodeType.ExprEquals)
            return false;

        if (conjunct.leftAst?.nodeType != NodeType.Identifier || conjunct.leftAst.yytext is null)
            return false;

        if (conjunct.rightAst?.nodeType != NodeType.Identifier || conjunct.rightAst.yytext is null)
            return false;

        if (TryMatchOrientation(
                conjunct.leftAst.yytext,
                conjunct.rightAst.yytext,
                rightSource,
                bound,
                out match))
            return true;

        return TryMatchOrientation(
            conjunct.rightAst.yytext,
            conjunct.leftAst.yytext,
            rightSource,
            bound,
            out match);
    }

    private static bool TryMatchOrientation(
        string rightIdentifier,
        string leftIdentifier,
        BoundTableSource rightSource,
        BoundSelectQuery bound,
        out JoinEquiJoinIndexMatch match)
    {
        match = null!;

        if (!TryResolveRightColumn(rightIdentifier, rightSource, out string rightColumnName))
            return false;

        if (!TryResolveLeftLookupColumn(leftIdentifier, rightSource, bound, out string leftLookupColumn))
            return false;

        if (!KeyTypesAgree(leftLookupColumn, TableColumnType(rightSource, rightColumnName), bound))
            return false;

        if (!TryFindLeadingIndex(rightSource.Table, rightColumnName, out TableIndexSchema index))
            return false;

        match = new JoinEquiJoinIndexMatch(index, leftLookupColumn, rightColumnName);
        return true;
    }

    private static bool TryResolveRightColumn(
        string identifier,
        BoundTableSource rightSource,
        out string columnName)
    {
        columnName = "";

        if (!TrySplitQualified(identifier, out string alias, out string column))
            return false;

        if (!string.Equals(alias, rightSource.Alias, StringComparison.OrdinalIgnoreCase))
            return false;

        columnName = column;
        return SourceHasColumn(rightSource, columnName);
    }

    private static bool TryResolveLeftLookupColumn(
        string identifier,
        BoundTableSource rightSource,
        BoundSelectQuery bound,
        out string leftLookupColumn)
    {
        leftLookupColumn = "";

        if (TrySplitQualified(identifier, out string alias, out string columnName))
        {
            if (string.Equals(alias, rightSource.Alias, StringComparison.OrdinalIgnoreCase))
                return false;

            foreach (BoundTableSource source in bound.Sources)
            {
                if (!string.Equals(source.Alias, alias, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (!SourceHasColumn(source, columnName))
                    return false;

                leftLookupColumn = QueryRowNameResolver.FormatQualifiedKey(alias, columnName);
                return true;
            }

            foreach (BoundDerivedTableSource source in bound.DerivedSources)
            {
                if (!string.Equals(source.Alias, alias, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (!source.HasColumn(columnName))
                    return false;

                leftLookupColumn = QueryRowNameResolver.FormatQualifiedKey(alias, columnName);
                return true;
            }

            return false;
        }

        leftLookupColumn = bound.RowNames.ResolveRowLookupKey(identifier);

        if (leftLookupColumn.StartsWith($"{rightSource.Alias}.", StringComparison.OrdinalIgnoreCase))
            return false;

        return true;
    }

    private static bool TryFindLeadingIndex(
        TableDescriptor table,
        string columnName,
        out TableIndexSchema index)
    {
        foreach (TableIndexSchema candidate in table.Indexes.Values)
        {
            // Skip indexes still being built online (Absent/DeleteOnly/WriteOnly or with
            // non-Public key columns): probing a half-backfilled index silently misses rows.
            if (!SchemaElementStateRules.IsReadableIndex(table.Schema, candidate))
                continue;

            if (candidate.Columns.Length == 1 && candidate.Columns[0] == columnName)
            {
                index = candidate;
                return true;
            }
        }

        index = null!;
        return false;
    }

    private static bool SourceHasColumn(BoundTableSource source, string columnName)
    {
        foreach (TableColumnSchema column in source.Table.Schema.Columns ?? [])
        {
            if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase) && SchemaElementStateRules.IsReadable(column))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Returns true when <paramref name="columnName"/> in <paramref name="table"/> has type
    /// <see cref="ColumnType.Array"/>.
    ///
    /// Arrays are excluded from equi-keys: an array column cannot be indexed, so it has no index
    /// strategy, and an array join key is rare enough that the nested loop is the simple, safe
    /// choice. The join falls back to the nested loop through the normal non-equi path.
    /// </summary>
    private static bool IsArrayColumn(BoundTableSource tableSource, string columnName)
    {
        foreach (TableColumnSchema col in tableSource.Table.Schema.Columns ?? [])
        {
            if (string.Equals(col.Name, columnName, StringComparison.OrdinalIgnoreCase) && SchemaElementStateRules.IsReadable(col))
                return col.Type == ColumnType.Array;
        }

        return false;
    }

    /// <summary>
    /// True unless both key columns have a known declared type and the two types differ. See the
    /// class summary for why a mixed pair must not become a hash, merge or index key.
    /// </summary>
    private static bool KeyTypesAgree(string leftLookupColumn, ColumnType? rightType, BoundSelectQuery bound)
    {
        if (rightType is not { } right || right == ColumnType.Null)
            return true;

        if (LeftColumnType(leftLookupColumn, bound) is not { } left || left == ColumnType.Null)
            return true;

        return left == right;
    }

    /// <summary>The declared type of a left lookup key (<c>alias.column</c>), or null when it is unknown.</summary>
    private static ColumnType? LeftColumnType(string leftLookupColumn, BoundSelectQuery bound)
    {
        if (!TrySplitQualified(leftLookupColumn, out string alias, out string columnName))
            return null;

        foreach (BoundTableSource source in bound.Sources)
        {
            if (string.Equals(source.Alias, alias, StringComparison.OrdinalIgnoreCase))
                return TableColumnType(source, columnName);
        }

        foreach (BoundDerivedTableSource source in bound.DerivedSources)
        {
            if (string.Equals(source.Alias, alias, StringComparison.OrdinalIgnoreCase))
                return source.HasColumn(columnName) ? source.GetColumnType(columnName) : null;
        }

        return null;
    }

    private static ColumnType? RightColumnType(BoundJoinRightSource rightSource, string columnName)
    {
        if (rightSource.Table is not null)
            return TableColumnType(rightSource.Table, columnName);

        return rightSource.Derived!.HasColumn(columnName) ? rightSource.Derived.GetColumnType(columnName) : null;
    }

    private static ColumnType? TableColumnType(BoundTableSource source, string columnName)
    {
        foreach (TableColumnSchema column in source.Table.Schema.Columns ?? [])
        {
            if (string.Equals(column.Name, columnName, StringComparison.OrdinalIgnoreCase) && SchemaElementStateRules.IsReadable(column))
                return column.Type;
        }

        return null;
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
