
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Executes uncorrelated and correlated EXISTS subqueries (QP5.4).
/// </summary>
internal sealed class ExistsSubqueryExecutor
{
    private readonly SubqueryQueryExecutor? queryExecutor;

    public ExistsSubqueryExecutor(SubqueryQueryExecutor? queryExecutor = null)
    {
        this.queryExecutor = queryExecutor;
    }

    public async Task<bool> ExecuteUncorrelatedAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters)
    {
        if (queryExecutor is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Uncorrelated EXISTS execution requires a subquery executor");
        }

        List<QueryResultRow> rows = await queryExecutor.ExecuteExistsSelectAsync(
            database,
            selectAst,
            txnState,
            parameters).ConfigureAwait(false);

        return rows.Count > 0;
    }

    public async Task<bool> ExecuteCorrelatedAsync(
        DatabaseDescriptor database,
        PreparedExistsSubquery prepared,
        Dictionary<string, ColumnValue> outerRow,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters)
    {
        if (prepared.InnerBound.Sources.Count != 1)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Correlated EXISTS subqueries with multi-source inner queries are not supported yet");
        }

        BoundTableSource innerSource = prepared.InnerBound.Sources[0];
        TableDescriptor innerTable = innerSource.Table;
        HLCTimestamp txId = txnState.TransactionId;

        List<BoundTableSource> combinedTableSources = new(prepared.InnerBound.Sources.Count + prepared.OuterSources.Count);
        combinedTableSources.AddRange(prepared.InnerBound.Sources);
        combinedTableSources.AddRange(prepared.OuterSources);

        List<BoundDerivedTableSource> combinedDerivedSources =
            new(prepared.InnerBound.DerivedSources.Count + prepared.OuterDerivedSources.Count);
        combinedDerivedSources.AddRange(prepared.InnerBound.DerivedSources);
        combinedDerivedSources.AddRange(prepared.OuterDerivedSources);

        QueryRowNameResolver combinedResolver = new(combinedTableSources, combinedDerivedSources);

        await foreach ((ObjectIdValue rowId, byte[] data) in innerTable.Store.ScanRows(txId).ConfigureAwait(false))
        {
            if (data.Length == 0)
                continue;

            Dictionary<string, ColumnValue> innerRow = RowEncoder.Decode(innerTable.Schema, rowId, data);
            Dictionary<string, ColumnValue> evalRow = CorrelatedRowMerger.MergeForEvaluation(
                innerRow,
                innerSource,
                outerRow,
                prepared.OuterSources,
                prepared.OuterDerivedSources);

            if (prepared.InnerWhere is null)
                return true;

            ColumnValue result = SqlExecutor.EvalExpr(
                prepared.InnerWhere,
                evalRow,
                parameters,
                combinedResolver);

            if (IsTrue(result))
                return true;
        }

        return false;
    }

    private static bool IsTrue(ColumnValue value) =>
        value.Type switch
        {
            ColumnType.Bool => value.BoolValue,
            ColumnType.Integer64 => value.LongValue != 0,
            ColumnType.Float64 => value.FloatValue != 0,
            ColumnType.Null => false,
            _ => false,
        };
}
