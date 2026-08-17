
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Decides whether a residual filter can be evaluated on a peer node executing a query
/// fragment. The remote evaluator has the row, the table's schema, and nothing else — so a
/// shippable filter must be a pure function of the row:
///
/// <list type="bullet">
///   <item>No subqueries or EXISTS (evaluation would re-enter the coordinator's executor).</item>
///   <item>No parameter placeholders (bound values live in the coordinator's ticket; shipping
///   them needs a value wire-codec that partial aggregation will introduce later).</item>
///   <item>No volatile functions (NOW() etc. must be evaluated exactly once, in one place —
///   two nodes evaluating it would disagree).</item>
/// </list>
///
/// A non-shippable filter is not an error: the coordinator simply scans that span locally,
/// exactly as it would without a fragment transport.
/// </summary>
internal static class FragmentFilterShippability
{
    public static bool IsShippable(NodeAst filter)
    {
        if (ScalarFunctionEvaluator.ContainsVolatileFunction(filter))
            return false;

        return WalkIsPure(filter);
    }

    private static bool WalkIsPure(NodeAst? node)
    {
        if (node is null)
            return true;

        switch (node.nodeType)
        {
            case NodeType.Placeholder:
            case NodeType.ExprScalarSubquery:
            case NodeType.ExprInSubquery:
            case NodeType.ExprNotInSubquery:
            case NodeType.ExprExistsCorrelated:
            case NodeType.ExprExistsSubquery:
            case NodeType.Select:
                return false;
        }

        return WalkIsPure(node.leftAst)
            && WalkIsPure(node.rightAst)
            && WalkIsPure(node.extendedOne)
            && WalkIsPure(node.extendedTwo)
            && WalkIsPure(node.extendedThree)
            && WalkIsPure(node.extendedFour)
            && WalkIsPure(node.extendedFive)
            && WalkIsPure(node.extendedSix)
            && WalkIsPure(node.extendedSeven);
    }
}
