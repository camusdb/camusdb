
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
/// Materializes uncorrelated IN/NOT IN subquery results.
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
        List<ColumnValue> values = new();
        bool containsNull = false;
        bool anyRow = false;

        await foreach (QueryResultRow row in queryExecutor.ExecuteSelectAsync(
            database, selectAst, txnState, parameters).ConfigureAwait(false))
        {
            anyRow = true;
            ColumnValue value = SubqueryQueryExecutor.ExtractSingleColumnValue(row);

            if (value.Type == ColumnType.Null)
            {
                containsNull = true;
                continue;
            }

            values.Add(value);
        }

        if (!anyRow)
            return new InSubqueryMaterialization([], ContainsNull: false, IsEmpty: true);

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
