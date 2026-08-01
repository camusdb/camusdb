
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
        Dictionary<string, ColumnValue>? parameters)
    {
        QueryExecutionContext context = QueryExecutionContext.For(database);

        SpillableValueList valueList = new(context);
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

            await valueList.AddAsync(value).ConfigureAwait(false);
        }

        await valueList.SealAsync().ConfigureAwait(false);

        if (valueList.IsSpilled && stats is not null)
            stats.InSubqueryValueListSpillCount++;

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
