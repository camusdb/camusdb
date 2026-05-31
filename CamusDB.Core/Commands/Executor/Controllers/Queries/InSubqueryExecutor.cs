
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
/// Materializes uncorrelated IN/NOT IN subquery results (QP5.3 / QP5.3a).
/// </summary>
internal sealed class InSubqueryExecutor
{
    private readonly SubqueryQueryExecutor queryExecutor;

    public InSubqueryExecutor(SubqueryQueryExecutor queryExecutor)
    {
        this.queryExecutor = queryExecutor;
    }

    public async Task<InSubqueryMaterialization> MaterializeAsync(
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

        if (rows.Count == 0)
            return new InSubqueryMaterialization([], ContainsNull: false, IsEmpty: true);

        List<ColumnValue> values = new(rows.Count);
        bool containsNull = false;

        foreach (QueryResultRow row in rows)
        {
            ColumnValue value = SubqueryQueryExecutor.ExtractSingleColumnValue(row);

            if (value.Type == ColumnType.Null)
            {
                containsNull = true;
                continue;
            }

            values.Add(value);
        }

        return new InSubqueryMaterialization(values, containsNull, IsEmpty: false);
    }

    public async Task<IReadOnlyList<ColumnValue>> ExecuteAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters)
    {
        InSubqueryMaterialization materialization = await MaterializeAsync(
            database,
            selectAst,
            txnState,
            parameters).ConfigureAwait(false);

        return materialization.Values;
    }
}
