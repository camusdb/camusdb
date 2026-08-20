
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Proves a cancelled sort raises promptly on every sorter path, at the operator level.
///
/// <para>The SQL layer cannot verify this without a race: a query over a handful of rows completes
/// before a token can be cancelled, so a statement-level test passes for timing reasons. Here the
/// input source itself decides when the operator is mid-flight — it yields one row, signals, and
/// then blocks on the cancellation token — so the assertions do not depend on any timeout.</para>
/// </summary>
public class TestQuerySorterCancellation
{
    /// <summary>
    /// Yields one row, signals <see cref="FirstRowConsumed"/>, and then blocks until the
    /// enumeration's cancellation token fires. The token is the only way out of the block, so a
    /// sorter path that fails to thread the token would hang the test instead of passing it.
    /// <see cref="FinallyRan"/> records that the enumerator was disposed, which is the evidence
    /// that nothing is left running after the cancellation.
    /// </summary>
    private sealed class GatedSource
    {
        public TaskCompletionSource FirstRowConsumed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public volatile bool FinallyRan;

        public async IAsyncEnumerable<QueryResultRow> Rows([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            try
            {
                yield return MakeRow(1L);

                FirstRowConsumed.TrySetResult();

                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            finally
            {
                FinallyRan = true;
            }
        }
    }

    private static QueryResultRow MakeRow(long n) =>
        new(default(ObjectIdValue), new Dictionary<string, ColumnValue>
        {
            { "n", new ColumnValue(ColumnType.Integer64, n) },
        });

    private static NodeAst Identifier(string name) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    private static QueryTicket MakeTicket(params QueryOrderBy[] orderBy)
    {
        KvTransaction txn = new(Kommander.Time.HLCTimestamp.Zero, "query-sorter-cancel-test");

        return new QueryTicket(
            txnState: txn,
            databaseName: "db",
            tableName: "robots",
            index: null,
            projection: null,
            filters: null,
            where: null,
            orderBy: orderBy.ToList(),
            limit: null,
            offset: null,
            parameters: null);
    }

    private static async Task AssertCancelsMidStream(QueryTicket ticket, long? boundedLimit, CamusDBOptions options)
    {
        GatedSource source = new();
        using CancellationTokenSource cancellation = new();
        QuerySorter sorter = new();

        Task<List<QueryResultRow>> consuming = sorter
            .SortResultset(ticket, source.Rows(), new QueryExecutionContext(options), boundedLimit, cancellation.Token)
            .ToListAsync()
            .AsTask();

        // The source has handed over its first row and is now blocked, so the operator is
        // provably mid-enumeration when the token fires.
        await source.FirstRowConsumed.Task;

        cancellation.Cancel();

        Assert.CatchAsync<OperationCanceledException>(async () => await consuming);
        Assert.IsTrue(source.FinallyRan, "the input enumerator must be disposed after cancellation");
    }

    // ── Bounded retention ────────────────────────────────────────────────────

    [Test]
    public Task BoundedColumnSort_CancelledMidStream_Raises() =>
        AssertCancelsMidStream(
            MakeTicket(new QueryOrderBy("n", OrderType.Ascending)),
            boundedLimit: 5,
            CamusDBOptions.Default);

    [Test]
    public Task BoundedExpressionSort_CancelledMidStream_Raises() =>
        AssertCancelsMidStream(
            MakeTicket(new QueryOrderBy("k", OrderType.Ascending, Identifier("n"))),
            boundedLimit: 5,
            CamusDBOptions.Default);

    // ── Full sort, with and without the spill path ───────────────────────────

    [Test]
    public Task FullColumnSort_CancelledMidStream_Raises() =>
        AssertCancelsMidStream(
            MakeTicket(new QueryOrderBy("n", OrderType.Ascending)),
            boundedLimit: null,
            CamusDBOptions.Default);

    [Test]
    public Task FullExpressionSort_CancelledMidStream_Raises() =>
        AssertCancelsMidStream(
            MakeTicket(new QueryOrderBy("k", OrderType.Ascending, Identifier("n"))),
            boundedLimit: null,
            CamusDBOptions.Default);

    [Test]
    public Task FullExpressionSort_WithSpillDisabled_CancelledMidStream_Raises() =>
        AssertCancelsMidStream(
            MakeTicket(new QueryOrderBy("k", OrderType.Ascending, Identifier("n"))),
            boundedLimit: null,
            CamusDBOptions.Default with { SpillEnabled = false });
}
