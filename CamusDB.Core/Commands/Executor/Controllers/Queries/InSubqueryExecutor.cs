
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Materializes uncorrelated IN/NOT IN subquery results into a <see cref="SpillableValueList"/>
/// so that the IN/NOT IN value set does not pin <c>O(N)</c> values in a plain
/// <c>List&lt;ColumnValue&gt;</c> when the subquery returns a large result set.
///
/// <para>
/// Callers are responsible for disposing the returned <see cref="InSubqueryMaterialization"/>
/// (which in turn disposes the <see cref="SpillableValueList"/> and any associated spill file)
/// once the membership-test AST has been built from the enumeration.
/// </para>
/// </summary>
internal sealed class InSubqueryExecutor
{
    private readonly SubqueryQueryExecutor queryExecutor;
    private readonly StatisticsManager? stats;

    public InSubqueryExecutor(SubqueryQueryExecutor queryExecutor, StatisticsManager? stats = null)
    {
        this.queryExecutor = queryExecutor;
        this.stats = stats;
    }

    public async Task<InSubqueryMaterialization> MaterializeAsync(
        DatabaseDescriptor database,
        NodeAst selectAst,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? parameters,
        CancellationToken cancellationToken = default)
    {
        QueryExecutionContext context = QueryExecutionContext.For(database, cancellationToken);
        SubqueryOutputType outputType = new();

        InSubqueryMaterialization materialization = await MaterializeRowsAsync(
            queryExecutor.ExecuteSelectAsync(database, selectAst, txnState, parameters, cancellationToken, outputType),
            context).ConfigureAwait(false);

        if (materialization.Values.IsSpilled && stats is not null)
            stats.InSubqueryValueListSpillCount++;

        return materialization with { OutputType = outputType.Type };
    }

    /// <summary>
    /// Collects the one column of <paramref name="rows"/> as a value set. A NULL value is not kept: the set
    /// records that it saw one, and that it saw no row at all, because <c>IN</c> and <c>NOT IN</c> answer
    /// UNKNOWN or a constant from those facts. A correlated subquery runs this once for each outer row,
    /// see <see cref="CorrelatedSubqueryValues"/>.
    /// </summary>
    internal static async Task<InSubqueryMaterialization> MaterializeRowsAsync(
        IAsyncEnumerable<QueryResultRow> rows,
        QueryExecutionContext context)
    {
        SpillableValueList valueList = new(context);
        bool containsNull = false;
        bool anyRow = false;

        try
        {
            await foreach (QueryResultRow row in rows.ConfigureAwait(false))
            {
                anyRow = true;
                ColumnValue value = SubqueryQueryExecutor.ExtractSingleColumnValue(row);

                if (value.Type == ColumnType.Null)
                {
                    containsNull = true;
                    continue;
                }

                await valueList.AddAsync(value).ConfigureAwait(false);
            }

            await valueList.SealAsync().ConfigureAwait(false);
        }
        catch
        {
            // The caller gets no list to dispose, so the spill files it wrote are deleted here.
            await valueList.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        if (!anyRow)
        {
            await valueList.DisposeAsync().ConfigureAwait(false);
            return new InSubqueryMaterialization(new SpillableValueList(context), ContainsNull: false, IsEmpty: true);
        }

        return new InSubqueryMaterialization(valueList, containsNull, IsEmpty: false);
    }

    public async Task<IAsyncEnumerable<ColumnValue>> ExecuteAsync(
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

        return materialization.Values.EnumerateAsync();
    }
}
