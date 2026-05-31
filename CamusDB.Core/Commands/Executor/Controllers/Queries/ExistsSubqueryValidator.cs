
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Validates outer-scope column references inside EXISTS subqueries during binding.
/// </summary>
internal static class ExistsSubqueryValidator
{
    public static void ValidateOuterReferences(NodeAst existsExpression, QueryRowNameResolver outerRowNames)
    {
        if (existsExpression.nodeType != NodeType.ExprExistsSubquery || existsExpression.leftAst is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Invalid EXISTS subquery expression");
        }

        SelectQuery innerQuery = new SelectQueryCreator().CreateSelectQuery(existsExpression.leftAst);
        HashSet<string> innerAliases = ExistsSubqueryAnalyzer.CollectSourceAliases(innerQuery.Source);

        if (innerQuery.Where is null)
            return;

        HashSet<string> references = new(StringComparer.Ordinal);
        QueryExpressionWalker.CollectColumnReferences(innerQuery.Where.Expression, references);

        foreach (string reference in references)
        {
            if (!ExistsSubqueryAnalyzer.ReferencesOuterScope(reference, innerAliases))
                continue;

            outerRowNames.ValidateColumnReference(reference);
        }
    }
}
