
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
/// predicates from WHERE into ON clauses.
///
/// <para>The tree follows the FROM order. A table with no equi-join conjunct to the tables
/// before it gets <c>ON TRUE</c>, and all the conjuncts that connect a table go into one
/// <c>ON</c>. That shape is not a plan: the join planner reads the conjuncts one at a time
/// (<see cref="JoinGraph"/>) and rebuilds the tree in a connected order, so an <c>ON TRUE</c>
/// here becomes a cross product only when the predicates need one.</para>
///
/// <para><b>The hoist needs the source of each column.</b> It reads the source from the qualifier,
/// and query creation has no catalog, so it cannot give a source to a bare name. When the
/// <c>WHERE</c> compares two columns and one of them is bare, creation hoists nothing and keeps
/// every join at <c>ON TRUE</c>. The binder then writes the source of each bare name into the
/// <c>WHERE</c> (<see cref="JoinPredicateQualifier"/>) and calls <see cref="HoistResolvedNames"/>.
/// The hoist runs once over all the conjuncts in <c>WHERE</c> order in both cases, so a statement
/// with bare names gets the tree of the same statement with qualified names.</para>
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

        return HoistEquiJoinPredicates(sources, where, namesResolved: false);
    }

    /// <summary>
    /// The hoist for a source list that was collected elsewhere, in declared order. A FROM made of
    /// cross joins only takes this entry so that it gets exactly the comma form's tree and plan.
    /// </summary>
    public static (QuerySource Source, BoundPredicate? Where) NormalizeSources(
        IReadOnlyList<QuerySource> sources,
        BoundPredicate? where)
    {
        if (sources.Count < 2)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Cross join requires at least two table sources");
        }

        return HoistEquiJoinPredicates(sources, where, namesResolved: false);
    }

    /// <summary>
    /// The hoist that query creation left for the binder (see the class summary). It runs only for a
    /// FROM list with nothing hoisted: a left-deep tree of inner joins that all hold the cross-join
    /// literal of <see cref="CreateCrossJoinOnPredicate"/>. Any other tree, and a list with no
    /// conjunct to hoist, returns the two arguments unchanged.
    /// </summary>
    /// <param name="source">The source tree of the query, as query creation built it.</param>
    /// <param name="where">The <c>WHERE</c> after <see cref="JoinPredicateQualifier"/> qualified its names.</param>
    public static (QuerySource Source, BoundPredicate? Where) HoistResolvedNames(QuerySource source, BoundPredicate? where)
    {
        if (where is null || !TryCollectUnhoistedList(source, out List<QuerySource> sources))
            return (source, where);

        (QuerySource hoisted, BoundPredicate? remaining) = HoistEquiJoinPredicates(sources, where, namesResolved: true);

        return IsUnhoistedList(hoisted) ? (source, where) : (hoisted, remaining);
    }

    /// <summary>
    /// The leaves of a FROM list with nothing hoisted, in declared order. False for any other tree.
    /// The cross-join literal is one shared node, so a user-written <c>ON TRUE</c> does not match.
    /// </summary>
    private static bool TryCollectUnhoistedList(QuerySource source, out List<QuerySource> sources)
    {
        sources = [];
        QuerySource current = source;

        while (current is JoinSource join)
        {
            if (join.Kind != JoinKind.Inner
                || join.OperandsSwapped
                || !ReferenceEquals(join.OnPredicate, NodeAst.True)
                || join.Right is not (TableSource or DerivedTableSource))
                return false;

            sources.Add(join.Right);
            current = join.Left;
        }

        if (sources.Count == 0 || current is not (TableSource or DerivedTableSource))
            return false;

        sources.Add(current);
        sources.Reverse();
        return true;
    }

    private static bool IsUnhoistedList(QuerySource source) => TryCollectUnhoistedList(source, out _);

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

    /// <summary>
    /// Builds the left-deep tree of <paramref name="sources"/> and moves each conjunct
    /// <c>left.column = right.column</c> of <paramref name="where"/> into the <c>ON</c> of the join
    /// where the later of its two tables enters.
    /// </summary>
    /// <param name="namesResolved">
    /// False at query creation, where a bare name has no source. A column comparison with a bare
    /// side then stops the hoist: the tree keeps <c>ON TRUE</c> at every join and
    /// <paramref name="where"/> comes back as it is, for <see cref="HoistResolvedNames"/>.
    /// </param>
    private static (QuerySource Source, BoundPredicate? Where) HoistEquiJoinPredicates(
        IReadOnlyList<QuerySource> sources,
        BoundPredicate? where,
        bool namesResolved)
    {
        List<NodeAst> conjuncts = new();

        if (where?.Expression is not null)
            PredicateAnalyzer.CollectAndConjuncts(where.Expression, conjuncts);

        bool hoist = namesResolved || !HasBareColumnComparison(conjuncts);

        HashSet<NodeAst> hoisted = new(ReferenceEqualityComparer.Instance);
        QuerySource left = sources[0];
        // Case-insensitive to match alias binding everywhere else: "FROM orders O, customers c
        // WHERE o.id = c.oid" must still hoist the equi-join into ON — an Ordinal mismatch
        // silently degrades the join to an unindexed cross product with a post-join filter.
        HashSet<string> leftAliases = new(StringComparer.OrdinalIgnoreCase) { GetAlias(sources[0]) };

        for (int i = 1; i < sources.Count; i++)
        {
            QuerySource right = sources[i];
            string rightAlias = GetAlias(right);
            List<NodeAst> joinConjuncts = new();

            if (hoist)
            {
                foreach (NodeAst conjunct in conjuncts)
                {
                    if (TryMatchEquiJoinConjunct(conjunct, rightAlias, leftAliases))
                        joinConjuncts.Add(conjunct);
                }
            }

            foreach (NodeAst conjunct in joinConjuncts)
                hoisted.Add(conjunct);

            NodeAst onPredicate = joinConjuncts.Count == 0
                ? CreateCrossJoinOnPredicate()
                : PredicateAnalyzer.CombineConjuncts(joinConjuncts)!;

            left = new JoinSource(left, right, JoinKind.Inner, onPredicate);
            leftAliases.Add(rightAlias);
        }

        if (hoisted.Count == 0)
            return (left, where);

        // The conjuncts that stay keep their WHERE order.
        List<NodeAst> remaining = new(conjuncts.Count - hoisted.Count);

        foreach (NodeAst conjunct in conjuncts)
        {
            if (!hoisted.Contains(conjunct))
                remaining.Add(conjunct);
        }

        BoundPredicate? newWhere = remaining.Count == 0
            ? null
            : new BoundPredicate(PredicateAnalyzer.CombineConjuncts(remaining)!);

        return (left, newWhere);
    }

    /// <summary>True when a conjunct compares two columns for equality and one of them has no qualifier.</summary>
    private static bool HasBareColumnComparison(List<NodeAst> conjuncts)
    {
        foreach (NodeAst conjunct in conjuncts)
        {
            if (conjunct.nodeType != NodeType.ExprEquals)
                continue;

            if (conjunct.leftAst is not { nodeType: NodeType.Identifier, yytext: { } leftName })
                continue;

            if (conjunct.rightAst is not { nodeType: NodeType.Identifier, yytext: { } rightName })
                continue;

            if (!TrySplitQualified(leftName, out _, out _) || !TrySplitQualified(rightName, out _, out _))
                return true;
        }

        return false;
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

        if (!string.Equals(resolvedRightAlias, rightAlias, StringComparison.OrdinalIgnoreCase))
            return false;

        if (!TrySplitQualified(leftIdentifier, out string leftAlias, out _))
            return false;

        return leftAliases.Contains(leftAlias)
               && !string.Equals(leftAlias, rightAlias, StringComparison.OrdinalIgnoreCase);
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

    internal static NodeAst CreateCrossJoinOnPredicate() => NodeAst.True;

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
