
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Maps each correlated subquery node of a prepared statement to the state that evaluates it for one outer
/// row. The name is older than the scalar, <c>IN</c> and quantified forms, which it holds too: a correlated
/// <c>EXISTS</c> marker maps to a <see cref="PreparedExistsSubquery"/>, and every other correlated node maps
/// to a <see cref="PreparedValueSubquery"/>.
///
/// <para>Each node is a new node that the preparer built for this statement, so one execution owns it: the
/// parsed tree, which the parser cache shares, never enters the registry. Each node also carries a hidden
/// name (<see cref="NextHiddenName"/>), under which <see cref="CorrelatedSubqueryValues"/> gives its value
/// for one row to the evaluator. One registry holds every clause of one query level, so two nodes never
/// share a name.</para>
/// </summary>
public sealed class ExistsSubqueryRegistry
{
    private readonly Dictionary<NodeAst, PreparedExistsSubquery> byNode = new(ReferenceEqualityComparer.Instance);

    private readonly Dictionary<NodeAst, PreparedValueSubquery> values = new(ReferenceEqualityComparer.Instance);

    private int nextName;

    internal void Register(NodeAst node, PreparedExistsSubquery prepared) => byNode[node] = prepared;

    internal bool TryGet(NodeAst node, out PreparedExistsSubquery prepared) => byNode.TryGetValue(node, out prepared!);

    internal void RegisterValue(NodeAst node, PreparedValueSubquery prepared) => values[node] = prepared;

    internal bool TryGetValue(NodeAst node, out PreparedValueSubquery prepared) => values.TryGetValue(node, out prepared!);

    /// <summary>True when the registry holds a scalar, <c>IN</c> or quantified subquery.</summary>
    internal bool HasValueSubqueries => values.Count > 0;

    /// <summary>A hidden name that no other node of this registry has, see <see cref="CorrelatedSubqueryValues"/>.</summary>
    internal string NextHiddenName() =>
        CorrelatedSubqueryValues.ParameterPrefix + (nextName++).ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>The names of the subquery of <paramref name="node"/> that resolve to the outer query.</summary>
    internal bool TryGetOuterReferences(NodeAst node, out IReadOnlyList<string> outerReferences)
    {
        if (byNode.TryGetValue(node, out PreparedExistsSubquery? exists))
        {
            outerReferences = exists.OuterReferences;
            return true;
        }

        if (values.TryGetValue(node, out PreparedValueSubquery? value))
        {
            outerReferences = value.OuterReferences;
            return true;
        }

        outerReferences = [];
        return false;
    }
}
