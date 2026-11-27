/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using CamusDB.Core.Auth;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Answers the activity statements: <c>SHOW [CLUSTER] QUERIES</c>, <c>SHOW [CLUSTER]
/// CONNECTIONS</c>, <c>SHOW [CLUSTER] TRANSACTIONS</c>, <c>CANCEL QUERY</c> and <c>KILL
/// TRANSACTION</c>. The node-local forms read <see cref="QueryActivityRegistry"/> and the host's
/// <see cref="ITransactionActivitySource"/> directly; the cluster forms add the rows of every peer,
/// gathered through <see cref="IClusterActivityTransport"/>.
///
/// <para><b>A peer that fails does not fail the statement.</b> Each peer call has its own timeout
/// (<see cref="CamusDBOptions.ClusterActivityPeerTimeoutMs"/>), the calls run in parallel, and a peer
/// that fails or does not answer in time gives one row that names it and carries the error. An
/// operator runs these statements most often while something is wrong, and a list that fails
/// because one node is down fails exactly then.</para>
///
/// <para><b>Membership is Raft's view at the moment of the statement.</b> A node that joined a
/// moment ago may be missing and a node that left may still be asked; the second case gives an error
/// row, not a wrong answer.</para>
/// </summary>
public sealed class QueryActivityService
{
    private readonly QueryActivityRegistry registry;

    private readonly IClusterActivityTransport? transport;

    private readonly Func<IReadOnlyList<string>> peers;

    private CamusDBOptions options;

    /// <summary>Cluster fan-outs running now, for <see cref="CamusDBOptions.ClusterActivityMaxConcurrentFanOuts"/>.</summary>
    private int fanOutsInFlight;

    /// <summary>The host's open explicit transactions, or null on an engine with no host.</summary>
    private ITransactionActivitySource? transactions;

    /// <param name="registry">This node's running statements and connections.</param>
    /// <param name="transport">The channel to peers, or null on an engine that has none.</param>
    /// <param name="peers">
    /// Returns the Raft endpoints of the other cluster members, not including this node. Returns an
    /// empty list in standalone mode, where Raft's node list holds only quorum witnesses.
    /// </param>
    /// <param name="options">The configuration snapshot in force when the engine is built.</param>
    public QueryActivityService(
        QueryActivityRegistry registry,
        IClusterActivityTransport? transport,
        Func<IReadOnlyList<string>> peers,
        CamusDBOptions options)
    {
        this.registry = registry;
        this.transport = transport;
        this.peers = peers;
        this.options = options;
    }

    /// <summary>This node's registry.</summary>
    public QueryActivityRegistry Registry => registry;

    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Connects the host's registry of open explicit transactions, which <c>SHOW TRANSACTIONS</c> lists
    /// and <c>KILL TRANSACTION</c> acts on. The host builds its registry after the engine, because the
    /// registry needs the engine, so it attaches itself here instead of being passed to the constructor.
    /// One source per engine: a second call replaces the first.
    /// </summary>
    public void AttachTransactions(ITransactionActivitySource source) => Volatile.Write(ref transactions, source);

    /// <summary>
    /// The open explicit transactions <paramref name="viewer"/> may see, on this node or on every
    /// member, oldest first.
    /// </summary>
    public async Task<List<TransactionActivityRow>> ListTransactionsAsync(bool cluster, ActivityViewer viewer, CancellationToken cancellationToken)
    {
        List<TransactionActivityRow> rows = SnapshotTransactions(viewer);

        if (!cluster)
            return rows;

        IReadOnlyList<string> targets = peers();
        if (targets.Count == 0)
            return rows;

        List<TransactionActivityRow>[] fromPeers;

        EnterFanOut();
        try
        {
            fromPeers = await Task.WhenAll(
                targets.Select(target => FetchTransactionsAsync(target, viewer, cancellationToken))).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref fanOutsInFlight);
        }

        foreach (List<TransactionActivityRow> peerRows in fromPeers)
            rows.AddRange(peerRows);

        rows.Sort(static (a, b) => a.StartedAt.CompareTo(b.StartedAt));
        return rows;
    }

    /// <summary>
    /// This node's open explicit transactions that <paramref name="viewer"/> may see, oldest first.
    ///
    /// <para>The SQL text is redacted before it is cut, as for <c>SHOW QUERIES</c>.</para>
    /// </summary>
    private List<TransactionActivityRow> SnapshotTransactions(ActivityViewer viewer)
    {
        List<TransactionActivityRow> rows = [];

        ITransactionActivitySource? source = Volatile.Read(ref transactions);
        if (source is null)
            return rows;

        List<TrackedTransaction> tracked = [];
        source.Snapshot(tracked);
        if (tracked.Count == 0)
            return rows;

        int maxSql = Math.Max(1, options.QueryActivityMaxSqlLength);
        string node = registry.NodeLabel;
        string prefix = registry.IdPrefix;
        Dictionary<Kommander.Time.HLCTimestamp, QueryActivityEntry> running = registry.OldestStatementByTransaction();

        foreach (TrackedTransaction item in tracked)
        {
            Transactions.KvTransaction tx = item.Transaction;
            if (tx.Activity is not { } activity || !activity.VisibleTo(viewer))
                continue;

            string? sql = activity.LastSql;
            if (sql is not null)
            {
                sql = SqlCredentialRedactor.Redact(sql);
                if (sql.Length > maxSql)
                    sql = sql[..maxSql];
            }

            rows.Add(new TransactionActivityRow(
                TransactionIdOf(prefix, tx.ClientId),
                node,
                activity.Database,
                activity.OwnerName,
                activity.ClientAddress,
                activity.Transport,
                tx.IsolationLevel.ToString(),
                tx.Locking.ToString(),
                tx.TransactionMode.ToString(),
                tx.StartTime,
                item.AgeMs,
                item.IdleMs,
                item.State,
                running.GetValueOrDefault(tx.ClientId)?.Id,
                sql,
                tx.ModifiedKeyCount,
                tx.SchemaChanges is not null,
                Error: null));
        }

        rows.Sort(static (a, b) => a.StartedAt.CompareTo(b.StartedAt));
        return rows;
    }

    /// <summary>
    /// The id <c>SHOW TRANSACTIONS</c> reports and <c>KILL TRANSACTION</c> takes:
    /// <c>{nodeTag}{instanceTag}-t{L}.{C}</c>. The prefix names the owner node, so a kill sent to any
    /// node finds it, and the process start, so an id copied before a restart cannot kill a transaction
    /// of the new process whose handle happens to match.
    /// </summary>
    internal static string TransactionIdOf(string prefix, Kommander.Time.HLCTimestamp clientId)
        => string.Create(CultureInfo.InvariantCulture, $"{prefix}-t{clientId.L}.{clientId.C}");

    /// <summary>The client handle inside a transaction id, or false when the text is not shaped like one.</summary>
    internal static bool TryParseTransactionId(string transactionId, out long physical, out uint counter)
    {
        physical = 0;
        counter = 0;

        int start = ActivityIdMinter.PrefixLength + 2;
        if (transactionId.Length <= start || transactionId[ActivityIdMinter.PrefixLength] != '-' ||
            transactionId[ActivityIdMinter.PrefixLength + 1] != 't')
            return false;

        ReadOnlySpan<char> handle = transactionId.AsSpan(start);
        int dot = handle.IndexOf('.');
        if (dot <= 0)
            return false;

        return long.TryParse(handle[..dot], NumberStyles.None, CultureInfo.InvariantCulture, out physical) &&
               uint.TryParse(handle[(dot + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out counter);
    }

    /// <summary>
    /// Ends the open explicit transaction <paramref name="transactionId"/> on whichever member owns it,
    /// or raises. Returns normally when the transaction is rolled back, or was rolled back by another
    /// finalize before the kill.
    ///
    /// <para>The id names its owner as a query id does, and the routing is the same as
    /// <see cref="CancelAsync"/>. The owner waits for the rollback up to half of
    /// <see cref="CamusDBOptions.ClusterActivityPeerTimeoutMs"/>, so a forwarded kill gets its answer
    /// before the peer call times out.</para>
    /// </summary>
    /// <exception cref="CamusDBException">
    /// <see cref="CamusDBErrorCodes.TransactionNotFound"/>, <see cref="CamusDBErrorCodes.TransactionKillPending"/>,
    /// <see cref="CamusDBErrorCodes.TransactionAlreadyCompleted"/> when a commit won or a finalize owns
    /// the transaction, <see cref="CamusDBErrorCodes.TransactionOwnerUnreachable"/> when a peer that may
    /// own it did not answer, or the error of a rollback that failed.
    /// </exception>
    public async Task KillTransactionAsync(string transactionId, ActivityViewer viewer, TransactionKiller killedBy)
    {
        TransactionKillOutcome outcome = TransactionKillOutcome.NotFound;
        string? unreachable = null;

        if (registry.IsLocalId(transactionId))
        {
            outcome = await KillLocalTransactionAsync(transactionId, viewer, killedBy).ConfigureAwait(false);
        }
        else if (transport is not null && ActivityIdMinter.TryGetNodeTag(transactionId, out string tag))
        {
            foreach (string target in peers())
            {
                if (!string.Equals(ActivityIdMinter.NodeTagOf(target), tag, StringComparison.Ordinal))
                    continue;

                // Not linked to the caller: a kill that reached the owner must not be cut short there.
                using CancellationTokenSource timeout = new(options.ClusterActivityPeerTimeoutMs);

                try
                {
                    outcome = await transport.KillTransactionAsync(target, transactionId, viewer, killedBy, timeout.Token).ConfigureAwait(false);
                }
                catch (CamusDBException e) when (e.Code != CamusDBErrorCodes.InvalidInternalOperation)
                {
                    // The owner answered with the error of a rollback that failed. It is the answer.
                    throw;
                }
                catch (Exception)
                {
                    unreachable = target;
                    continue;
                }

                if (outcome != TransactionKillOutcome.NotFound)
                    break;
            }
        }

        switch (outcome)
        {
            case TransactionKillOutcome.Killed:
            case TransactionKillOutcome.RolledBack:
                return;

            case TransactionKillOutcome.Pending:
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionKillPending,
                    $"Transaction '{transactionId}' is killed: it runs no further statement and cannot commit. A statement of it " +
                    "still runs, and the rollback releases its locks when that statement ends; run the same KILL TRANSACTION to check");

            case TransactionKillOutcome.Committed:
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionAlreadyCompleted,
                    $"Transaction '{transactionId}' committed before the kill could end it");

            case TransactionKillOutcome.Finalizing:
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionAlreadyCompleted,
                    $"A commit or rollback owns transaction '{transactionId}', and its outcome is not known yet; a kill cannot end it");

            default:
                if (unreachable is not null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.TransactionOwnerUnreachable,
                        $"Node '{unreachable}', which may own transaction '{transactionId}', did not answer; the transaction may still be open");

                throw new CamusDBException(CamusDBErrorCodes.TransactionNotFound, $"No open transaction has id '{transactionId}'");
        }
    }

    /// <summary>
    /// The node-local form of <see cref="KillTransactionAsync"/>, for an id this process minted. Called
    /// on the owner node, by the statement there or by a peer that forwarded the kill. It never asks
    /// another node.
    /// </summary>
    public async Task<TransactionKillOutcome> KillLocalTransactionAsync(string transactionId, ActivityViewer viewer, TransactionKiller killedBy)
    {
        ITransactionActivitySource? source = Volatile.Read(ref transactions);

        if (source is null || !registry.IsLocalId(transactionId) ||
            !TryParseTransactionId(transactionId, out long physical, out uint counter))
            return TransactionKillOutcome.NotFound;

        TimeSpan wait = TimeSpan.FromMilliseconds(Math.Max(1, options.ClusterActivityPeerTimeoutMs / 2));
        return await source.KillAsync(physical, counter, viewer, killedBy, wait).ConfigureAwait(false);
    }

    /// <summary>
    /// The running statements <paramref name="viewer"/> may see, on this node or on every member,
    /// oldest first.
    /// </summary>
    public async Task<List<QueryActivityRow>> ListQueriesAsync(bool cluster, ActivityViewer viewer, CancellationToken cancellationToken)
    {
        List<QueryActivityRow> rows = registry.SnapshotQueries(viewer);

        if (!cluster)
            return rows;

        IReadOnlyList<string> targets = peers();
        if (targets.Count == 0)
            return rows;

        List<QueryActivityRow>[] fromPeers;

        EnterFanOut();
        try
        {
            fromPeers = await Task.WhenAll(
                targets.Select(target => FetchQueriesAsync(target, viewer, cancellationToken))).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref fanOutsInFlight);
        }

        foreach (List<QueryActivityRow> peerRows in fromPeers)
            rows.AddRange(peerRows);

        // Error rows have no start time and sort first, where an operator sees them before the list.
        rows.Sort(static (a, b) => a.StartedAt.CompareTo(b.StartedAt));
        return rows;
    }

    /// <summary>
    /// The open connections <paramref name="viewer"/> may see, on this node or on every member,
    /// oldest first.
    /// </summary>
    public async Task<List<ConnectionActivityRow>> ListConnectionsAsync(bool cluster, ActivityViewer viewer, CancellationToken cancellationToken)
    {
        List<ConnectionActivityRow> rows = registry.SnapshotConnections(viewer);

        if (!cluster)
            return rows;

        IReadOnlyList<string> targets = peers();
        if (targets.Count == 0)
            return rows;

        List<ConnectionActivityRow>[] fromPeers;

        EnterFanOut();
        try
        {
            fromPeers = await Task.WhenAll(
                targets.Select(target => FetchConnectionsAsync(target, viewer, cancellationToken))).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref fanOutsInFlight);
        }

        foreach (List<ConnectionActivityRow> peerRows in fromPeers)
            rows.AddRange(peerRows);

        rows.Sort(static (a, b) => a.OpenedAt.CompareTo(b.OpenedAt));
        return rows;
    }

    /// <summary>
    /// Takes one slot of <see cref="CamusDBOptions.ClusterActivityMaxConcurrentFanOuts"/>, or refuses
    /// with <see cref="CamusDBErrorCodes.ConcurrencyLimitExceeded"/>. One cluster statement asks every
    /// peer, so without a cap a flood of them multiplies internal traffic by the cluster size. Refusal,
    /// not a queue: a queue would hold the flood in memory and still send it. The caller must release
    /// the slot in a <c>finally</c> once the peers answered. The node-local forms and
    /// <see cref="CancelAsync"/> take no slot: they ask at most the owner of one id.
    /// </summary>
    private void EnterFanOut()
    {
        int limit = options.ClusterActivityMaxConcurrentFanOuts;
        int running = Interlocked.Increment(ref fanOutsInFlight);

        if (limit > 0 && running > limit)
        {
            Interlocked.Decrement(ref fanOutsInFlight);
            throw new CamusDBException(
                CamusDBErrorCodes.ConcurrencyLimitExceeded,
                $"This node already runs {limit} cluster activity statements, the most it allows at one time " +
                "(cluster_activity_max_concurrent_fan_outs). Retry later, or use the node-local form.");
        }
    }

    /// <summary>
    /// Cancels the statement <paramref name="queryId"/> on whichever member owns it, or raises.
    ///
    /// <para>The id names its owner: its first six characters are the tag of the owner's Raft
    /// endpoint. An id this process minted is cancelled here; any other id goes to the peers whose
    /// tag matches. Two members can share a tag, so each match is asked until one owns the id.</para>
    /// </summary>
    /// <exception cref="CamusDBException">
    /// <see cref="CamusDBErrorCodes.QueryNotFound"/>, <see cref="CamusDBErrorCodes.QueryNotCancellable"/>,
    /// or <see cref="CamusDBErrorCodes.QueryOwnerUnreachable"/> when a peer that may own the id did
    /// not answer.
    /// </exception>
    public async Task CancelAsync(string queryId, ActivityViewer viewer)
    {
        QueryCancelOutcome outcome = QueryCancelOutcome.NotFound;
        string? unreachable = null;

        if (registry.IsLocalId(queryId))
        {
            outcome = registry.TryCancel(queryId, viewer);
        }
        else if (transport is not null && ActivityIdMinter.TryGetNodeTag(queryId, out string tag))
        {
            foreach (string target in peers())
            {
                if (!string.Equals(ActivityIdMinter.NodeTagOf(target), tag, StringComparison.Ordinal))
                    continue;

                // The cancel itself must not be abandoned because the caller went away: the timeout
                // is its only bound.
                using CancellationTokenSource timeout = new(options.ClusterActivityPeerTimeoutMs);

                try
                {
                    outcome = await transport.CancelAsync(target, queryId, viewer, timeout.Token).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    unreachable = target;
                    continue;
                }

                if (outcome != QueryCancelOutcome.NotFound)
                    break;
            }
        }

        switch (outcome)
        {
            case QueryCancelOutcome.Cancelled:
                return;

            case QueryCancelOutcome.NotCancellable:
                throw new CamusDBException(
                    CamusDBErrorCodes.QueryNotCancellable,
                    $"Query '{queryId}' cannot be cancelled: it is a write or schema change, which a cancel cannot stop once it runs, or its parse has not ended yet");

            default:
                if (unreachable is not null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.QueryOwnerUnreachable,
                        $"Node '{unreachable}', which may own query '{queryId}', did not answer; the query may still run");

                throw new CamusDBException(CamusDBErrorCodes.QueryNotFound, $"No running query has id '{queryId}'");
        }
    }

    private async Task<List<QueryActivityRow>> FetchQueriesAsync(string target, ActivityViewer viewer, CancellationToken cancellationToken)
    {
        if (transport is null)
            return [QueryErrorRow(target, "This node has no channel to its peers")];

        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.ClusterActivityPeerTimeoutMs);

        try
        {
            return await transport.ListQueriesAsync(target, viewer, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            return [QueryErrorRow(target, Describe(exception, timeout.IsCancellationRequested))];
        }
    }

    private async Task<List<ConnectionActivityRow>> FetchConnectionsAsync(string target, ActivityViewer viewer, CancellationToken cancellationToken)
    {
        if (transport is null)
            return [ConnectionErrorRow(target, "This node has no channel to its peers")];

        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.ClusterActivityPeerTimeoutMs);

        try
        {
            return await transport.ListConnectionsAsync(target, viewer, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            return [ConnectionErrorRow(target, Describe(exception, timeout.IsCancellationRequested))];
        }
    }

    private async Task<List<TransactionActivityRow>> FetchTransactionsAsync(string target, ActivityViewer viewer, CancellationToken cancellationToken)
    {
        if (transport is null)
            return [TransactionErrorRow(target, "This node has no channel to its peers")];

        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.ClusterActivityPeerTimeoutMs);

        try
        {
            return await transport.ListTransactionsAsync(target, viewer, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            return [TransactionErrorRow(target, Describe(exception, timeout.IsCancellationRequested))];
        }
    }

    private static TransactionActivityRow TransactionErrorRow(string node, string error)
        => new("", node, "", null, null, "", "", "", "", default, 0, 0, "", null, null, 0, false, error);

    private string Describe(Exception exception, bool timedOut)
        => timedOut
            ? $"No answer within {options.ClusterActivityPeerTimeoutMs} ms"
            : exception.Message;

    private static QueryActivityRow QueryErrorRow(string node, string error)
        => new("", node, null, null, "", null, "", null, null, "", "", default, 0, 0, false, false, "", error);

    private static ConnectionActivityRow ConnectionErrorRow(string node, string error)
        => new("", node, null, null, "", null, default, 0, 0, 0, 0, 0, error);
}
