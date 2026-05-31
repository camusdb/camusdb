
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal static class InSubqueryAnalyzer
{
    public static void EnsureUncorrelated(NodeAst selectAst, SelectQueryCreator selectQueryCreator)
    {
        if (selectAst.nodeType != NodeType.Select)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "IN subquery must be a SELECT statement");
        }

        SelectQuery innerQuery = selectQueryCreator.CreateSelectQuery(selectAst);

        if (ExistsSubqueryAnalyzer.IsCorrelated(innerQuery))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Correlated IN/NOT IN subqueries are not supported");
        }
    }
}
