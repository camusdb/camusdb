
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.Core.Transactions;

/// <summary>
/// Holds a statement snapshot of a <see cref="KvTransaction"/> open for one read phase of a statement,
/// and closes it on dispose. A write statement (<c>UPDATE</c>, <c>DELETE</c>, <c>INSERT … SELECT</c>)
/// opens one scope for each read step: the subquery rewrite, then the locate scan or the source drain.
/// All of them use the one timestamp the statement minted at its start, so the statement reads one
/// consistent view, and the write phase that follows reads the latest value
/// (<see cref="KvTransaction.QueryReadView"/>).
///
/// <para>The default value opens nothing and closes nothing: it is what <see cref="Open"/> returns when
/// the statement has no timestamp (the transaction does not take statement snapshots).</para>
/// </summary>
internal readonly struct StatementSnapshotScope : IDisposable
{
    private readonly KvTransaction? tx;

    private readonly object? token;

    private StatementSnapshotScope(KvTransaction tx, object token)
    {
        this.tx = tx;
        this.token = token;
    }

    /// <summary>
    /// Opens the statement snapshot of <paramref name="tx"/> at <paramref name="timestamp"/>, or nothing
    /// when <paramref name="timestamp"/> is null.
    /// </summary>
    public static StatementSnapshotScope Open(KvTransaction tx, HLCTimestamp? timestamp) =>
        timestamp is { } t ? new(tx, tx.BeginStatementSnapshot(t)) : default;

    /// <summary>
    /// Closes the snapshot this scope opened. A no-op when a later statement already replaced or
    /// cleared it (<see cref="KvTransaction.EndStatementSnapshot"/>).
    /// </summary>
    public void Dispose()
    {
        if (tx is not null)
            tx.EndStatementSnapshot(token!);
    }
}
