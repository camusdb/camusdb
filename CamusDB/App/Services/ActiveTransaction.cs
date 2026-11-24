/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core;
using CamusDB.Core.Transactions;

namespace CamusDB.App.Services;

/// <summary>
/// One transaction that <see cref="HttpTransactionCoordinator"/> tracks, with the state that decides
/// whether the idle reaper may roll it back.
///
/// <para><b>Idle rule.</b> A transaction is idle only while the server waits for its client: no
/// statement runs on it, no commit or rollback runs on it, and no request owns it. Its idle time starts
/// when it begins, when its last running statement ends, or when a finalize fails, whichever is latest.
/// A statement that runs longer than <see cref="CamusDBOptions.TransactionIdleTimeoutMs"/> is therefore
/// never cut by the reaper. The limit of a statement is
/// <see cref="CamusDBOptions.StatementTimeoutMs"/>.</para>
///
/// <para><b>Owner.</b> The client owns an explicit transaction, so it is idle from its begin. The
/// request that began an autocommit transaction owns that one, so it is busy from its begin. A failed
/// commit or rollback ends that ownership: the client never received a handle, so the reaper is then
/// the only thing that can end the transaction.</para>
///
/// <para><b>One lock.</b> The running-statement count, the finalize claim, the ownership and the idle
/// clock are read and written under one lock, so <see cref="TryClaimForReap"/> is one step with respect
/// to <see cref="TryEnterStatement"/>. A statement that enters first stops the reap. A reap that claims
/// first refuses the statement. No await happens under the lock.</para>
///
/// <para>The idle clock is a <see cref="Stopwatch"/> tick, so wall-clock jumps do not change it.</para>
/// </summary>
internal sealed class ActiveTransaction(KvTransactionsManager manager, KvTransaction transaction, bool isExplicit)
{
    private readonly Lock gate = new();

    /// <summary>Statements that run on the transaction now. A client can send several at once.</summary>
    private int runningStatements;

    /// <summary>True while the request that began an autocommit transaction has not failed to finalize it.</summary>
    private bool ownedByRequest = !isExplicit;

    /// <summary>True while a commit, a rollback or a reap owns the finalize claim.</summary>
    private bool finalizing;

    private long lastActivityTicks = Stopwatch.GetTimestamp();

    public KvTransactionsManager Manager { get; } = manager;

    public KvTransaction Transaction { get; } = transaction;

    /// <summary>
    /// True for a transaction the client began itself. It holds a slot of
    /// <see cref="CamusDBOptions.MaxOpenExplicitTransactions"/>.
    /// </summary>
    public bool IsExplicit { get; } = isExplicit;

    /// <summary>How many statements run on the transaction now.</summary>
    public int RunningStatements
    {
        get
        {
            lock (gate)
                return runningStatements;
        }
    }

    /// <summary>
    /// Counts one more running statement, or returns <see langword="false"/> while a finalize owns the
    /// transaction. A statement must not run beside a commit or rollback of the same session. The caller
    /// must call <see cref="LeaveStatement"/> once for each <see langword="true"/>; see
    /// <see cref="RunningStatement"/>.
    /// </summary>
    public bool TryEnterStatement()
    {
        lock (gate)
        {
            if (finalizing)
                return false;

            runningStatements++;
            return true;
        }
    }

    /// <summary>Ends one running statement and starts the idle time from now.</summary>
    public void LeaveStatement()
    {
        lock (gate)
        {
            runningStatements--;
            lastActivityTicks = Stopwatch.GetTimestamp();
        }
    }

    /// <summary>
    /// Starts the idle time from now without a running statement, or returns <see langword="false"/>
    /// while a finalize owns the transaction.
    /// </summary>
    public bool TryTouch()
    {
        lock (gate)
        {
            if (finalizing)
                return false;

            lastActivityTicks = Stopwatch.GetTimestamp();
            return true;
        }
    }

    /// <summary>
    /// Claims the finalize (commit or rollback) of this transaction. One caller wins with
    /// <see langword="true"/>, and a concurrent finalize gets <see langword="false"/>. The manager's own
    /// status check is a plain read, so without this claim two requests for the same id — a client that
    /// timed out on a slow commit and sent it again — could both drive the commit protocol.
    ///
    /// <para>A running statement does not stop the claim: a client may roll back a transaction whose
    /// statement it no longer waits for.</para>
    /// </summary>
    public bool TryBeginFinalize()
    {
        lock (gate)
        {
            if (finalizing)
                return false;

            finalizing = true;
            return true;
        }
    }

    /// <summary>
    /// Claims the finalize of this transaction for the reaper when it was idle for at least
    /// <paramref name="idleTimeout"/>. The check and the claim are one step, so a statement cannot start
    /// between them.
    /// </summary>
    public bool TryClaimForReap(TimeSpan idleTimeout)
    {
        lock (gate)
        {
            if (finalizing || ownedByRequest || runningStatements > 0)
                return false;

            if (Stopwatch.GetElapsedTime(lastActivityTicks) < idleTimeout)
                return false;

            finalizing = true;
            return true;
        }
    }

    /// <summary>
    /// Releases the finalize claim after a <em>failed</em> attempt, so that a later attempt can run. A
    /// finalize that succeeded removes the entry from the tracking map instead, so its claim is never
    /// used again. From here the transaction is idle again, and the reaper can take it when nobody
    /// retries; that includes an autocommit transaction, whose request now gives up its ownership.
    /// </summary>
    public void EndFinalize()
    {
        lock (gate)
        {
            finalizing = false;
            ownedByRequest = false;
            lastActivityTicks = Stopwatch.GetTimestamp();
        }
    }
}
