
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Writes the binder's name resolution into the predicates of a join: each bare column name of an
/// <c>ON</c> or a <c>WHERE</c> that one source has becomes <c>alias.column</c>.
///
/// <para>The join planner reads the source of a column from its qualifier in many places: the
/// equi-key match of the hash, merge and index lookup joins (<see cref="JoinEquiJoinAnalyzer"/>), the
/// hoist of a FROM list (<see cref="CommaJoinNormalizer"/>), the side test of a range seek or an
/// expression key (<see cref="JoinSideExpressions"/>), and the row estimates. A bare name gave those
/// readers no source, so <c>c_custkey = o_custkey</c> was not a join key and the join ran as a nested
/// loop. After this pass a column of a bound join predicate is always qualified, and a reader needs
/// no rule of its own for a bare name.</para>
///
/// <para>The owner of a name comes from <see cref="QueryRowNameResolver.TryGetBareColumnOwner"/>,
/// the rule that evaluation follows. A name that no source or more than one source has stays as
/// written, and the validation of the binder refuses it with the same error as before. A name of an
/// outer query stays as written too: the resolver given here holds the sources of this query only.</para>
///
/// <para>The parser cache shares a parsed tree between executions, so the pass builds new nodes
/// (<see cref="NodeAstWalk.Replace"/>) and never writes to the tree it reads. A nested SELECT is its
/// own scope and is not walked.</para>
/// </summary>
internal static class JoinPredicateQualifier
{
    /// <summary>
    /// Returns <paramref name="expression"/> with each bare column name that <paramref name="scope"/>
    /// gives to one source replaced by <c>alias.column</c>, or the same instance when it has none.
    /// </summary>
    /// <param name="scope">
    /// The sources that the expression can read: every source of the query for a <c>WHERE</c>, and
    /// the sources of the two inputs of the join for an <c>ON</c>.
    /// </param>
    /// <param name="rowNames">
    /// The resolver that evaluation uses. Each new name is resolved through it here, as validation
    /// did for the names as written, so its lookup memo holds every name of the bound predicates
    /// before the statement runs.
    /// </param>
    public static NodeAst Qualify(NodeAst expression, QueryRowNameResolver scope, QueryRowNameResolver rowNames)
    {
        List<NodeAst> references = [];
        QueryExpressionWalker.CollectColumnReferenceNodes(expression, references);

        Dictionary<NodeAst, NodeAst>? replacements = null;

        foreach (NodeAst reference in references)
        {
            string name = reference.yytext!;

            if (JoinSideExpressions.TrySplitQualified(name, out _, out _))
                continue;

            if (!scope.TryGetBareColumnOwner(name, out string alias))
                continue;

            string qualified = QueryRowNameResolver.FormatQualifiedKey(alias, name);
            rowNames.ValidateColumnReference(qualified);

            replacements ??= new Dictionary<NodeAst, NodeAst>(ReferenceEqualityComparer.Instance);
            replacements[reference] = new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, qualified);
        }

        return replacements is null ? expression : NodeAstWalk.Replace(expression, replacements);
    }
}
