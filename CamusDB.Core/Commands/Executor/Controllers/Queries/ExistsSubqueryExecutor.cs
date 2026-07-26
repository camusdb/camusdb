
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Executes uncorrelated and correlated EXISTS subqueries.
/// </summary>
internal sealed class ExistsSubqueryExecutor(SubqueryQueryExecutor? queryExecutor = null)
{
    private readonly SubqueryQueryExecutor? queryExecutor = queryExecutor;

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

        await foreach (QueryResultRow _ in queryExecutor.ExecuteExistsSelectAsync(database, selectAst, txnState, parameters).ConfigureAwait(false))
        {
            return true;
        }

        return false;
    }

    public async Task<bool> ExecuteCorrelatedAsync(
        DatabaseDescriptor database,
        PreparedExistsSubquery prepared,
        IReadOnlyDictionary<string, ColumnValue> outerRow,
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

        List<BoundTableSource> combinedTableSources = [.. prepared.InnerBound.Sources, .. prepared.OuterSources];

        List<BoundDerivedTableSource> combinedDerivedSources =
        [
            .. prepared.InnerBound.DerivedSources,
            .. prepared.OuterDerivedSources,
        ];

        QueryRowNameResolver combinedResolver = new(combinedTableSources, combinedDerivedSources);

        if (prepared.SeekPlan is not null)
        {
            (bool seeked, bool exists) = await TrySeekAsync(
                prepared.SeekPlan,
                prepared,
                innerTable,
                innerSource,
                outerRow,
                txnState,
                parameters,
                combinedResolver,
                txId).ConfigureAwait(false);

            if (seeked)
                return exists;
        }

        // Fallback: no index pins the correlation key (or its value could not be encoded into one),
        // so every inner row is decoded and tested. Cost is O(total inner rows) per outer row.
        //
        // The inner scan does not fold its reads (no query ticket reaches here, so an
        // UPDATE/DELETE that contains this subquery cannot mark it as write-driving), and takes no
        // range lock, so a Serializable transaction is not fenced against inner-table phantoms on
        // this path — unlike the seek path above, which locks the range it reads.
        await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data) in innerTable.Store.ScanRows(txnState).ConfigureAwait(false))
        {
            if (data.Length == 0)
                continue;

            // With no inner predicate, any surviving row proves existence — decoding and merging it
            // would only produce a value nothing reads.
            if (prepared.InnerWhere is null)
                return true;

            Dictionary<string, ColumnValue> innerRow = await RowEncoder.DecodeAsync(
                innerTable.Schema,
                txId,
                rowId,
                data,
                visibilitySchemaVersion: innerTable.Schema.Version).ConfigureAwait(false);

            Dictionary<string, ColumnValue> evalRow = CorrelatedRowMerger.MergeForEvaluation(
                innerRow,
                innerSource,
                outerRow,
                prepared.OuterSources,
                prepared.OuterDerivedSources);

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

    /// <summary>
    /// Tests existence by seeking the inner index over just the correlated key instead of scanning
    /// the whole inner table. Returns <c>(seeked: false, _)</c> when the seek key cannot be built
    /// for this outer row, which makes the caller fall back to the scan — the seek is an
    /// optimization and never the only way to get a correct answer.
    ///
    /// The full inner predicate is still evaluated on every row the seek returns, so a row is
    /// accepted on exactly the same terms as on the scan path; the seek only narrows which rows are
    /// considered. A shared range lock over the seeked bounds is taken first so a Serializable
    /// transaction is fenced against a concurrent insert into that key range — the phantom the
    /// existence test would otherwise miss.
    /// </summary>
    private static async Task<(bool Seeked, bool Exists)> TrySeekAsync(
        CorrelatedExistsSeekPlan seekPlan,
        PreparedExistsSubquery prepared,
        TableDescriptor innerTable,
        BoundTableSource innerSource,
        IReadOnlyDictionary<string, ColumnValue> outerRow,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver combinedResolver,
        HLCTimestamp txId)
    {
        // The bound expressions may reference outer columns by bare or qualified name, exactly as
        // the inner predicate does. Merging an empty inner row yields the outer half of that view.
        Dictionary<string, ColumnValue> outerOnlyRow = CorrelatedRowMerger.MergeForEvaluation(
            new Dictionary<string, ColumnValue>(),
            innerSource,
            outerRow,
            prepared.OuterSources,
            prepared.OuterDerivedSources);

        ColumnValue[] prefix = new ColumnValue[seekPlan.PrefixBindings.Count];

        for (int i = 0; i < seekPlan.PrefixBindings.Count; i++)
        {
            CorrelatedExistsSeekBinding binding = seekPlan.PrefixBindings[i];

            ColumnValue value;
            try
            {
                value = SqlExecutor.EvalExpr(binding.Expression, outerOnlyRow, parameters, combinedResolver);
            }
            catch (CamusDBException)
            {
                // The bound expression did not resolve against the outer row alone (for example an
                // outer column absent from a projected intermediate row). Scan instead of guessing.
                return (false, false);
            }

            // An equality against NULL is never true, so no inner row can satisfy the conjunct this
            // binding came from — and it is a conjunct, so the whole inner predicate is false for
            // every row. The existence test is therefore false without reading anything.
            if (value.Type == ColumnType.Null)
                return (true, false);

            // A value of a different type than the column encodes to a different key than the index
            // stores, so seeking with it would silently miss rows. Fall back to the scan.
            if (value.Type != binding.ExpectedType)
                return (false, false);

            prefix[i] = value;
        }

        CompositeColumnValue seekKey = new(prefix);

        await innerTable.Store.AcquireBoundedIndexRangeLockAsync(
            txnState,
            seekPlan.Index.KvId,
            seekKey, true, seekKey, true,
            seekPlan.Unique,
            exclusive: false,
            keyColumnCount: seekPlan.Index.Columns.Length).ConfigureAwait(false);

        await foreach ((CompositeColumnValue _, ObjectIdValue rowId, ReadOnlyMemory<byte> _) in innerTable.Store.ScanIndex(
            txnState,
            seekPlan.Index.KvId,
            seekPlan.KeyTypes,
            seekKey,
            seekKey,
            seekPlan.Unique).ConfigureAwait(false))
        {
            ReadOnlyMemory<byte>? data = await innerTable.Store.GetRow(txnState, rowId).ConfigureAwait(false);

            if (data is null || data.Value.Length == 0)
                continue;

            if (prepared.InnerWhere is null)
                return (true, true);

            Dictionary<string, ColumnValue> innerRow = await RowEncoder.DecodeAsync(
                innerTable.Schema,
                txId,
                rowId,
                data.Value,
                visibilitySchemaVersion: innerTable.Schema.Version).ConfigureAwait(false);

            Dictionary<string, ColumnValue> evalRow = CorrelatedRowMerger.MergeForEvaluation(
                innerRow,
                innerSource,
                outerRow,
                prepared.OuterSources,
                prepared.OuterDerivedSources);

            if (IsTrue(SqlExecutor.EvalExpr(prepared.InnerWhere, evalRow, parameters, combinedResolver)))
                return (true, true);
        }

        return (true, false);
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
