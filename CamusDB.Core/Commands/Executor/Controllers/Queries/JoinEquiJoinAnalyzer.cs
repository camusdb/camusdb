
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
/// Detects <c>right.indexed_column = left.column</c> patterns in join ON clauses (QP4.5).
/// </summary>
internal static class JoinEquiJoinAnalyzer
{
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

        if (!string.Equals(alias, rightSource.Alias, StringComparison.Ordinal))
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
            if (string.Equals(alias, rightSource.Alias, StringComparison.Ordinal))
                return false;

            foreach (BoundTableSource source in bound.Sources)
            {
                if (source.Alias != alias)
                    continue;

                if (!SourceHasColumn(source, columnName))
                    return false;

                leftLookupColumn = QueryRowNameResolver.FormatQualifiedKey(alias, columnName);
                return true;
            }

            return false;
        }

        leftLookupColumn = bound.RowNames.ResolveRowLookupKey(identifier);

        if (leftLookupColumn.StartsWith($"{rightSource.Alias}.", StringComparison.Ordinal))
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
