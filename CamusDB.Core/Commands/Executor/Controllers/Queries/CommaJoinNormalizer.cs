
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Normalizes SQL-89 comma joins into left-deep inner join trees and hoists equi-join
/// predicates from WHERE into ON clauses (QP4.6).
/// </summary>
internal static class CommaJoinNormalizer
{
    public static (QuerySource Source, BoundPredicate? Where) Normalize(
        NodeAst commaJoinAst,
        BoundPredicate? where,
        Func<NodeAst, QuerySource> createQuerySource)
    {
        List<QuerySource> sources = CollectCommaJoinSources(commaJoinAst, createQuerySource);

        if (sources.Count < 2)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Comma join requires at least two table sources");
        }

        return HoistEquiJoinPredicates(sources, where);
    }

    private static List<QuerySource> CollectCommaJoinSources(
        NodeAst commaJoinAst,
        Func<NodeAst, QuerySource> createQuerySource)
    {
        if (commaJoinAst.nodeType != NodeType.CommaJoin || commaJoinAst.leftAst is null || commaJoinAst.rightAst is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Invalid comma join clause");
        }

        List<QuerySource> sources = new() { createQuerySource(commaJoinAst.leftAst) };
        CollectCommaTableList(commaJoinAst.rightAst, sources, createQuerySource);
        return sources;
    }

    private static void CollectCommaTableList(
        NodeAst ast,
        List<QuerySource> sources,
        Func<NodeAst, QuerySource> createQuerySource)
    {
        if (ast.nodeType == NodeType.CommaJoinTableList)
        {
            if (ast.leftAst is not null)
                CollectCommaTableList(ast.leftAst, sources, createQuerySource);

            if (ast.rightAst is not null)
                sources.Add(createQuerySource(ast.rightAst));

            return;
        }

        sources.Add(createQuerySource(ast));
    }

    private static (QuerySource Source, BoundPredicate? Where) HoistEquiJoinPredicates(
        IReadOnlyList<QuerySource> sources,
        BoundPredicate? where)
    {
        List<NodeAst> conjuncts = new();

        if (where?.Expression is not null)
            PredicateAnalyzer.CollectAndConjuncts(where.Expression, conjuncts);

        HashSet<NodeAst> remaining = new(conjuncts);
        QuerySource left = sources[0];
        HashSet<string> leftAliases = new(StringComparer.Ordinal) { GetAlias(sources[0]) };

        for (int i = 1; i < sources.Count; i++)
        {
            QuerySource right = sources[i];
            string rightAlias = GetAlias(right);
            List<NodeAst> joinConjuncts = new();

            foreach (NodeAst conjunct in conjuncts)
            {
                if (TryMatchEquiJoinConjunct(conjunct, rightAlias, leftAliases))
                    joinConjuncts.Add(conjunct);
            }

            foreach (NodeAst conjunct in joinConjuncts)
                remaining.Remove(conjunct);

            NodeAst onPredicate = joinConjuncts.Count == 0
                ? CreateCrossJoinOnPredicate()
                : PredicateAnalyzer.CombineConjuncts(joinConjuncts)!;

            left = new JoinSource(left, right, JoinKind.Inner, onPredicate);
            leftAliases.Add(rightAlias);
        }

        BoundPredicate? newWhere = remaining.Count == 0
            ? null
            : new BoundPredicate(PredicateAnalyzer.CombineConjuncts(remaining)!);

        return (left, newWhere);
    }

    private static bool TryMatchEquiJoinConjunct(
        NodeAst conjunct,
        string rightAlias,
        IReadOnlySet<string> leftAliases)
    {
        if (conjunct.nodeType != NodeType.ExprEquals)
            return false;

        if (conjunct.leftAst?.nodeType != NodeType.Identifier || conjunct.leftAst.yytext is null)
            return false;

        if (conjunct.rightAst?.nodeType != NodeType.Identifier || conjunct.rightAst.yytext is null)
            return false;

        return MatchesEquiJoinOrientation(conjunct.leftAst.yytext, conjunct.rightAst.yytext, rightAlias, leftAliases)
               || MatchesEquiJoinOrientation(conjunct.rightAst.yytext, conjunct.leftAst.yytext, rightAlias, leftAliases);
    }

    private static bool MatchesEquiJoinOrientation(
        string leftIdentifier,
        string rightIdentifier,
        string rightAlias,
        IReadOnlySet<string> leftAliases)
    {
        if (!TrySplitQualified(rightIdentifier, out string resolvedRightAlias, out _))
            return false;

        if (!string.Equals(resolvedRightAlias, rightAlias, StringComparison.Ordinal))
            return false;

        if (!TrySplitQualified(leftIdentifier, out string leftAlias, out _))
            return false;

        return leftAliases.Contains(leftAlias)
               && !string.Equals(leftAlias, rightAlias, StringComparison.Ordinal);
    }

    internal static string GetAlias(QuerySource source) =>
        source switch
        {
            TableSource tableSource => tableSource.Alias ?? tableSource.TableName,
            DerivedTableSource derivedSource => derivedSource.Alias,
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Unsupported comma join source: {source.GetType().Name}"),
        };

    internal static NodeAst CreateCrossJoinOnPredicate() =>
        ColumnValueAstBuilder.FromColumnValue(new ColumnValue(ColumnType.Bool, true));

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
