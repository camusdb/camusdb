
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Materializes uncorrelated IN-subquery results into a value list (QP5.3).
/// </summary>
internal sealed class InSubqueryExecutor
{
    private readonly SubqueryQueryExecutor queryExecutor;

    public InSubqueryExecutor(SubqueryQueryExecutor queryExecutor)
    {
        this.queryExecutor = queryExecutor;
    }

    public async Task<IReadOnlyList<ColumnValue>> ExecuteAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters)
    {
        List<QueryResultRow> rows = await queryExecutor.ExecuteSelectAsync(
            database,
            selectAst,
            txnState,
            parameters).ConfigureAwait(false);

        List<ColumnValue> values = new(rows.Count);

        foreach (QueryResultRow row in rows)
            values.Add(SubqueryQueryExecutor.ExtractSingleColumnValue(row));

        return values;
    }
}
