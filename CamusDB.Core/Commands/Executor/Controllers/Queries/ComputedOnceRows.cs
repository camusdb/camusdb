/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The rows of the CTEs that a statement computes once (see <see cref="NodeType.CteMaterialization"/>),
/// kept for the statement in a <see cref="SpillableRowList"/>. The first reader of such a CTE runs its
/// query; every other reader of the statement reads the kept rows, so a CTE that is read two times
/// runs one time, and a volatile function in it gives every reader the same values.
///
/// <para><b>Where the rows live.</b> A reader can be any derived-table scan of the statement: in the
/// FROM of the statement, in a subquery, in an operand of a set operation, in the source of an INSERT.
/// Each of those runs with its own ticket, and the one object they all share is the transaction. So the
/// store hangs off the <see cref="KvTransaction"/> (a weak table, so it never keeps the transaction
/// alive), keyed by the reference of the marker node. The marker comes from the parsed tree, and the
/// parser cache gives one tree to every execution of the same text, so the marker alone does not name
/// one execution: an entry is opened by <see cref="Begin"/> when the statement starts and removed by
/// <see cref="ComputedOnceScope.DisposeAsync"/> when it ends, and a reader finds rows only between the
/// two.</para>
///
/// <para><b>A statement inside a statement.</b> A trigger can run SQL on the same transaction while the
/// statement that fired it is still running. When that inner statement has the same text, it has the
/// same markers: its entries are pushed over the outer ones and the outer ones come back when it ends,
/// so neither statement disposes rows that the other still reads. A reader of the outer statement that
/// runs while the inner one is open reads the inner statement's rows. That needs a trigger that runs
/// the text of the statement that fired it, and the rows are still rows of the same transaction.</para>
///
/// <para><b>A reader that finds no entry</b> runs the CTE's query itself, as an inlined CTE does. That is
/// always correct, because every reader binds the same query in the same statement; only the single run
/// is lost. It happens for an entry point that does not open a store, such as the body check of
/// <c>CREATE VIEW</c>.</para>
///
/// <para><b>The rows are kept whole.</b> The first reader computes all of the CTE's rows before any
/// reader gets one, also under a <c>LIMIT</c>. The filter of each reader runs after, on the kept rows,
/// so no reader's filter changes the rows of another.</para>
/// </summary>
internal sealed class ComputedOnceRows
{
    private static readonly ConditionalWeakTable<KvTransaction, ComputedOnceRows> byTransaction = new();

    private readonly object sync = new();

    private readonly Dictionary<NodeAst, Entry> entries = new(ReferenceEqualityComparer.Instance);

    /// <summary>
    /// The kept rows of one CTE for one statement, over the entry of the statement it hid. Read and
    /// changed only under <see cref="sync"/>.
    /// </summary>
    private sealed class Entry(ComputedOnceScope owner, Entry? hidden)
    {
        public ComputedOnceScope Owner { get; } = owner;

        public Entry? Hidden { get; set; } = hidden;

        /// <summary>The run of the first reader; null until a reader starts it.</summary>
        public Task<SpillableRowList>? Rows { get; set; }
    }

    /// <summary>
    /// Opens the entries of the CTEs that the statement parsed to <paramref name="root"/> computes once,
    /// or returns null when it has none or runs without a transaction. The caller disposes the scope when
    /// the statement ends: for a read, when its cursor ends.
    /// </summary>
    public static ComputedOnceScope? Begin(KvTransaction? tx, NodeAst root)
    {
        if (tx is null || CommonTableExpressions.ComputedOnceMarkers(root) is not { } markers)
            return null;

        ComputedOnceRows store = byTransaction.GetValue(tx, static _ => new ComputedOnceRows());
        ComputedOnceScope scope = new(store, markers);

        lock (store.sync)
        {
            foreach (NodeAst marker in markers)
                store.entries[marker] = new Entry(scope, store.entries.GetValueOrDefault(marker));
        }

        return scope;
    }

    /// <summary>
    /// The kept rows of the CTE that <paramref name="marker"/> names, computed by <paramref name="compute"/>
    /// when this is the first reader; null when the statement opened no entry for it, and the caller then
    /// runs the CTE itself. A run that fails fails every reader with the same error.
    /// </summary>
    public static async Task<SpillableRowList?> GetOrComputeAsync(
        KvTransaction? tx, NodeAst marker, Func<Task<SpillableRowList>> compute)
    {
        if (tx is null || !byTransaction.TryGetValue(tx, out ComputedOnceRows? store))
            return null;

        TaskCompletionSource<SpillableRowList>? mine = null;
        Task<SpillableRowList> rows;

        lock (store.sync)
        {
            if (!store.entries.TryGetValue(marker, out Entry? entry))
                return null;

            if (entry.Rows is null)
            {
                mine = new TaskCompletionSource<SpillableRowList>(TaskCreationOptions.RunContinuationsAsynchronously);
                entry.Rows = mine.Task;
            }

            rows = entry.Rows;
        }

        // The run is outside the lock: it reads storage, and it can read another CTE of the statement.
        if (mine is not null)
        {
            try
            {
                mine.SetResult(await compute().ConfigureAwait(false));
            }
            catch (Exception ex)
            {
                mine.SetException(ex);
            }
        }

        return await rows.ConfigureAwait(false);
    }

    /// <summary>Removes the entries of <paramref name="scope"/>, shows the entries they hid, and disposes the kept rows.</summary>
    internal async ValueTask EndAsync(ComputedOnceScope scope, NodeAst[] markers)
    {
        List<Task<SpillableRowList>>? kept = null;

        lock (sync)
        {
            foreach (NodeAst marker in markers)
            {
                if (!entries.TryGetValue(marker, out Entry? entry))
                    continue;

                // The entry of this scope is the top one, or it sits under an inner statement's entry
                // that did not end first; both are removed from the chain.
                Entry? above = null;
                while (entry is not null && !ReferenceEquals(entry.Owner, scope))
                {
                    above = entry;
                    entry = entry.Hidden;
                }

                if (entry is null)
                    continue;

                if (above is not null)
                    above.Hidden = entry.Hidden;
                else if (entry.Hidden is null)
                    entries.Remove(marker);
                else
                    entries[marker] = entry.Hidden;

                if (entry.Rows is not null)
                    (kept ??= []).Add(entry.Rows);
            }
        }

        if (kept is null)
            return;

        foreach (Task<SpillableRowList> rows in kept)
        {
            // A run that failed kept nothing. A run that has not ended belongs to a reader of a statement
            // that already ended; its rows are disposed when it ends.
            if (rows.IsCompletedSuccessfully)
                await rows.Result.DisposeAsync().ConfigureAwait(false);
            else if (!rows.IsCompleted)
                _ = rows.ContinueWith(
                    static done => { if (done.IsCompletedSuccessfully) _ = done.Result.DisposeAsync(); },
                    CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }
    }
}

/// <summary>
/// The entries that one statement opened in <see cref="ComputedOnceRows"/>. Disposing it ends them and
/// disposes their kept rows; a second dispose does nothing.
/// </summary>
internal sealed class ComputedOnceScope : IAsyncDisposable
{
    private readonly ComputedOnceRows store;
    private readonly NodeAst[] markers;
    private int ended;

    internal ComputedOnceScope(ComputedOnceRows store, NodeAst[] markers)
    {
        this.store = store;
        this.markers = markers;
    }

    public ValueTask DisposeAsync() =>
        Interlocked.Exchange(ref ended, 1) == 0 ? store.EndAsync(this, markers) : ValueTask.CompletedTask;

    /// <summary>
    /// <paramref name="cursor"/>, ending the scope when the cursor ends: when the reader reads the last
    /// row, stops early, or fails. The rows of a read are produced while the cursor runs, after the
    /// statement's entry point returned.
    /// </summary>
    public async IAsyncEnumerable<T> EndWhenDone<T>(IAsyncEnumerable<T> cursor, [EnumeratorCancellation] CancellationToken ct = default)
    {
        try
        {
            await foreach (T item in cursor.WithCancellation(ct).ConfigureAwait(false))
                yield return item;
        }
        finally
        {
            await DisposeAsync().ConfigureAwait(false);
        }
    }
}
