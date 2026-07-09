
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Extracts aggregate sub-expressions from a compound projection and rewrites the tree so that
/// each aggregate call is replaced by a synthetic identifier placeholder. The caller accumulates
/// one <see cref="AggregateMetricState"/> per entry in the returned list, resolves it to a
/// <see cref="CamusDB.Core.CommandsExecutor.Models.ColumnValue"/> after draining the cursor, then
/// evaluates the rewritten expression against a row built from those placeholder→value pairs to
/// produce the final projected value.
///
/// Deduplication: structurally identical aggregate nodes share one placeholder and therefore one
/// accumulator. Identity is determined by <see cref="QueryAstComparer.AreEquivalent"/>, which
/// compares all fields including <c>extendedOne</c>–<c>extendedSix</c>. For example,
/// <c>SUM(x) / SUM(x)</c> extracts a single entry, but
/// <c>MAX(x BETWEEN 10 AND 20) = MAX(x BETWEEN 30 AND 40)</c> extracts two because the
/// <c>BETWEEN</c> bounds differ in <c>extendedOne</c>/<c>extendedTwo</c>.
///
/// Placeholder names use the null character ('\x00') as a prefix, which the SQL lexer cannot
/// produce because identifiers must match <c>[a-zA-Z_][a-zA-Z0-9_]*</c>. This guarantees that a
/// user column named like the synthetic key can never shadow or be shadowed by a placeholder.
///
/// This extractor is called only for compound aggregate projections (those where
/// <see cref="QueryExpressionClassifier.IsCompoundAggregateProjection"/> returns true).
/// </summary>
internal static class QueryAggregateExtractor
{
    /// <summary>
    /// The prefix prepended to each synthetic placeholder name. The null character is not a valid
    /// SQL identifier start character, so it cannot collide with any real column name.
    /// </summary>
    private const char PlaceholderPrefix = '\x00';

    /// <summary>
    /// Extracts all aggregate sub-expressions from <paramref name="expression"/>, deduplicates
    /// structurally identical aggregates, and returns a rewritten tree in which every aggregate
    /// node has been replaced by a synthetic <see cref="NodeType.Identifier"/> node.
    /// </summary>
    /// <param name="expression">
    /// Normally a compound aggregate projection (one where <see cref="QueryExpressionClassifier.IsCompoundAggregateProjection"/>
    /// returns true). A plain scalar expression (no aggregate anywhere) produces zero entries and
    /// the rewritten tree is identical to the input. A bare top-level aggregate (e.g. <c>SUM(x)</c>)
    /// produces one entry; in practice bare aggregates are routed through the fast-path accumulators
    /// by the caller before <c>Extract</c> is reached, so that case is degenerate but handled correctly.
    /// </param>
    /// <returns>
    /// <list type="bullet">
    ///   <item><c>Aggregates</c> — ordered list of <c>(AggregateNode, Placeholder)</c> pairs, one
    ///     per distinct aggregate; the placeholder name is also the key used in the synthetic row.</item>
    ///   <item><c>Rewritten</c> — a new tree identical to <paramref name="expression"/> except
    ///     that each aggregate node is replaced by an <see cref="NodeType.Identifier"/> whose
    ///     <c>yytext</c> is the corresponding placeholder name.</item>
    /// </list>
    /// </returns>
    public static (IReadOnlyList<(NodeAst AggregateNode, string Placeholder)> Aggregates, NodeAst Rewritten)
        Extract(NodeAst expression)
    {
        List<(NodeAst AggregateNode, string Placeholder)> aggregates = [];
        NodeAst rewritten = Rewrite(expression, aggregates);
        return (aggregates, rewritten);
    }

    /// <summary>
    /// Builds a placeholder name for the <paramref name="n"/>-th distinct aggregate.
    /// The null-char prefix makes the name un-lexable as a SQL identifier.
    /// </summary>
    private static string MakePlaceholder(int n) => $"{PlaceholderPrefix}agg{n}";

    /// <summary>
    /// Recursively rewrites <paramref name="node"/>, replacing aggregate calls with placeholders.
    /// Deduplication walks <paramref name="aggregates"/> with
    /// <see cref="QueryAstComparer.AreEquivalent"/> so all semantic fields — including
    /// <c>extendedOne</c>–<c>extendedSix</c> — are compared.
    /// </summary>
    private static NodeAst Rewrite(
        NodeAst node,
        List<(NodeAst, string)> aggregates)
    {
        // If this node is itself an aggregate call, replace it with a placeholder.
        if (node.nodeType == NodeType.ExprFuncCall
            && node.leftAst?.yytext is not null
            && QueryExpressionClassifier.IsAggregateName(node.leftAst.yytext))
        {
            // Walk the already-accumulated list to find a structurally identical aggregate.
            string? placeholder = null;
            foreach ((NodeAst existing, string ph) in aggregates)
            {
                if (QueryAstComparer.AreEquivalent(existing, node))
                {
                    placeholder = ph;
                    break;
                }
            }

            if (placeholder is null)
            {
                placeholder = MakePlaceholder(aggregates.Count);
                aggregates.Add((node, placeholder));
            }

            return new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, placeholder);
        }

        return node.nodeType switch
        {
            NodeType.ExprAlias => node.leftAst is null
                ? node
                : With(node, Rewrite(node.leftAst, aggregates), node.rightAst),

            NodeType.ExprFuncCall => With(node,
                node.leftAst,
                node.rightAst is null ? null : Rewrite(node.rightAst, aggregates)),

            NodeType.ExprArgumentList => new NodeAst(
                NodeType.ExprArgumentList,
                node.leftAst is null ? null : Rewrite(node.leftAst, aggregates),
                node.rightAst is null ? null : Rewrite(node.rightAst, aggregates),
                null, null, null, null, null, null),

            NodeType.ExprCast => With(node,
                node.leftAst is null ? null : Rewrite(node.leftAst, aggregates),
                node.rightAst),

            NodeType.ExprAdd or NodeType.ExprSub or NodeType.ExprMult or NodeType.ExprDiv
            or NodeType.ExprEquals or NodeType.ExprNotEquals
            or NodeType.ExprLessThan or NodeType.ExprGreaterThan
            or NodeType.ExprLessEqualsThan or NodeType.ExprGreaterEqualsThan
            or NodeType.ExprOr or NodeType.ExprAnd
            or NodeType.ExprLike or NodeType.ExprILike
            or NodeType.ExprList => new NodeAst(
                node.nodeType,
                node.leftAst is null ? null : Rewrite(node.leftAst, aggregates),
                node.rightAst is null ? null : Rewrite(node.rightAst, aggregates),
                null, null, null, null, null, null),

            NodeType.ExprNot or NodeType.ExprIsNull or NodeType.ExprIsNotNull => With(node,
                node.leftAst is null ? null : Rewrite(node.leftAst, aggregates),
                null),

            NodeType.ExprBetween => new NodeAst(
                NodeType.ExprBetween,
                node.leftAst is null ? null : Rewrite(node.leftAst, aggregates),
                null,
                node.extendedOne is null ? null : Rewrite(node.extendedOne, aggregates),
                node.extendedTwo is null ? null : Rewrite(node.extendedTwo, aggregates),
                null, null, null, null),

            // Leaf nodes and subqueries are returned unchanged.
            _ => node,
        };
    }

    /// <summary>
    /// Constructs a new <see cref="NodeAst"/> with the same type and extended fields as
    /// <paramref name="source"/> but with replaced <c>leftAst</c>/<c>rightAst</c>.
    /// The <c>yytext</c> and extended fields beyond <c>rightAst</c> are carried over unchanged.
    /// </summary>
    private static NodeAst With(NodeAst source, NodeAst? left, NodeAst? right) =>
        new(source.nodeType, left, right,
            source.extendedOne, source.extendedTwo, source.extendedThree,
            source.extendedFour, source.extendedFive, source.yytext, source.extendedSix);
}
