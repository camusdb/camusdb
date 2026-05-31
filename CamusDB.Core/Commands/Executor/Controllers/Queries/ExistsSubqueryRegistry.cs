
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Maps rewritten correlated EXISTS nodes to their prepared subquery state (QP5.4).
/// </summary>
public sealed class ExistsSubqueryRegistry
{
    private readonly Dictionary<NodeAst, PreparedExistsSubquery> byNode = new(ReferenceEqualityComparer.Instance);

    internal void Register(NodeAst node, PreparedExistsSubquery prepared) => byNode[node] = prepared;

    internal bool TryGet(NodeAst node, out PreparedExistsSubquery prepared) => byNode.TryGetValue(node, out prepared!);
}
