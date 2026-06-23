
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Executes uncorrelated scalar subqueries once under the caller's transaction (QP5.2).
/// </summary>
internal sealed class ScalarSubqueryExecutor
{
    private readonly SubqueryQueryExecutor queryExecutor;
    private readonly SelectQueryCreator selectQueryCreator = new();

    public ScalarSubqueryExecutor(SubqueryQueryExecutor queryExecutor)
    {
        this.queryExecutor = queryExecutor;
    }

    public async Task<ColumnValue> ExecuteAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters)
    {
        SelectQuery subquery = selectAst.nodeType == NodeType.Select
            ? selectQueryCreator.CreateSelectQuery(selectAst)
            : throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Scalar subquery must be a SELECT statement");

        List<QueryResultRow> rows = await queryExecutor.ExecuteSelectAsync(
            database,
            selectAst,
            txnState,
            parameters).ConfigureAwait(false);

        if (rows.Count == 0)
            return ColumnValue.Null;

        if (rows.Count > 1 && !HasLimitOne(subquery, parameters))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Scalar subquery returned more than one row");
        }

        return SubqueryQueryExecutor.ExtractSingleColumnValue(rows[0]);
    }

    private static bool HasLimitOne(SelectQuery subquery, Dictionary<string, ColumnValue>? parameters)
    {
        if (subquery.Limit is null)
            return false;

        ColumnValue limit = SqlExecutor.EvalExpr(subquery.Limit, new(), parameters);

        return limit.Type == ColumnType.Integer64 && limit.LongValue == 1;
    }
}
