/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using CamusDB.Core;
using CamusDB.Core.Diagnostics;
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
/// <para><b>Kill.</b> <c>KILL TRANSACTION</c> fences the transaction under the same lock
/// (<see cref="TryBeginKill"/>): from then on no statement enters, no commit or rollback claims it, and
/// the reaper leaves it alone. The kill takes the finalize claim itself, at once when no statement runs,
/// or when the last running statement leaves (<see cref="LeaveStatement"/>), so its rollback never runs
/// beside a statement of the same transaction.</para>
///
/// <para>The idle clock is a <see cref="Stopwatch"/> tick, so wall-clock jumps do not change it.</para>
/// </summary>
internal sealed class ActiveTransaction(KvTransactionsManager manager, KvTransaction transaction, bool isExplicit)
{
    private readonly Lock gate = new();

    private readonly long registeredTicks = Stopwatch.GetTimestamp();

    /// <summary>Set by the kill that fenced the transaction. Guarded by <see cref="gate"/>.</summary>
    private bool killed;

    /// <summary>
    /// Completed when the last running statement of a fenced transaction leaves and the kill holds the
    /// finalize claim. Created by a kill that met a running statement. Guarded by <see cref="gate"/>.
    /// </summary>
    private TaskCompletionSource? statementsEnded;

    /// <summary>
    /// The outcome of the kill, completed when its rollback ends. Created by the kill that fenced the
    /// transaction, so a second kill can wait for the same rollback. Guarded by <see cref="gate"/>.
    /// </summary>
    private TaskCompletionSource<TransactionKillOutcome>? killDone;

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
            if (finalizing || killed)
                return false;

            runningStatements++;
            return true;
        }
    }

    /// <summary>
    /// Ends one running statement and starts the idle time from now. When a kill fenced the transaction
    /// and this was its last running statement, the kill takes the finalize claim here, in the same step,
    /// and its rollback starts on another thread.
    /// </summary>
    public void LeaveStatement()
    {
        TaskCompletionSource? claimedByKill = null;

        lock (gate)
        {
            runningStatements--;
            lastActivityTicks = Stopwatch.GetTimestamp();

            if (killed && runningStatements == 0 && !finalizing)
            {
                finalizing = true;
                claimedByKill = statementsEnded;
            }
        }

        // Outside the lock. The source runs its continuations asynchronously, so the rollback does not
        // run on the thread that ends the statement.
        claimedByKill?.TrySetResult();
    }

    /// <summary>True once <c>KILL TRANSACTION</c> fenced the transaction.</summary>
    public bool IsKilled
    {
        get
        {
            lock (gate)
                return killed;
        }
    }

    /// <summary>
    /// The state, age and idle time that <c>SHOW TRANSACTIONS</c> reports, read in one step.
    /// </summary>
    public (string State, double AgeMs, double IdleMs) Describe()
    {
        lock (gate)
        {
            string state = finalizing ? TransactionActivityStates.Finalizing
                : killed ? TransactionActivityStates.Killing
                : runningStatements > 0 || ownedByRequest ? TransactionActivityStates.Running
                : TransactionActivityStates.Idle;

            double idleMs = state == TransactionActivityStates.Idle
                ? Stopwatch.GetElapsedTime(lastActivityTicks).TotalMilliseconds
                : 0;

            return (state, Stopwatch.GetElapsedTime(registeredTicks).TotalMilliseconds, idleMs);
        }
    }

    /// <summary>
    /// Fences the transaction for <c>KILL TRANSACTION</c> and marks it killed with
    /// <paramref name="reason"/>, in one step with respect to every other claim.
    ///
    /// <para>A commit, rollback or reap that holds the finalize claim wins: the kill is refused and
    /// fences nothing. So is a transaction whose last commit got no answer: its outcome belongs to the
    /// storage layer, and the client can still learn it by sending its COMMIT again. Otherwise the kill
    /// takes the claim at once when no statement runs (<see cref="KillClaim.Claimed"/>), or at the end
    /// of the last running statement (<see cref="KillClaim.Pending"/>, and
    /// <paramref name="whenStatementsEnd"/> completes then).</para>
    /// </summary>
    public KillClaim TryBeginKill(string reason, out Task? whenStatementsEnd, out Task<TransactionKillOutcome>? outcome)
    {
        whenStatementsEnd = null;

        lock (gate)
        {
            outcome = killDone?.Task;

            if (killed)
                return KillClaim.AlreadyKilled;

            if (finalizing)
                return KillClaim.Finalizing;

            if (Transaction.Status != KvTransactionStatus.Active)
                return KillClaim.OutcomeUnknown;

            killed = true;
            killDone = new TaskCompletionSource<TransactionKillOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
            outcome = killDone.Task;

            // No await under the lock: this sets two fields of the transaction.
            Transaction.MarkKilled(reason);

            if (runningStatements == 0)
            {
                finalizing = true;
                return KillClaim.Claimed;
            }

            statementsEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            whenStatementsEnd = statementsEnded.Task;
            return KillClaim.Pending;
        }
    }

    /// <summary>Reports the end of the kill's rollback to every kill that waits for it.</summary>
    public void CompleteKill(TransactionKillOutcome outcome)
    {
        TaskCompletionSource<TransactionKillOutcome>? done;
        lock (gate)
            done = killDone;

        done?.TrySetResult(outcome);
    }

    /// <summary>Reports a kill whose rollback failed to every kill that waits for it.</summary>
    public void FailKill(Exception failure)
    {
        TaskCompletionSource<TransactionKillOutcome>? done;
        lock (gate)
            done = killDone;

        if (done is null)
            return;

        done.TrySetException(failure);

        // Observed here: no kill may be waiting for it any more, and a faulted task nobody reads is
        // reported as unobserved.
        _ = done.Task.Exception;
    }

    /// <summary>
    /// Starts the idle time from now without a running statement, or returns <see langword="false"/>
    /// while a finalize owns the transaction.
    /// </summary>
    public bool TryTouch()
    {
        lock (gate)
        {
            if (finalizing || killed)
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
    /// statement it no longer waits for. A kill does: the kill owns the finalize of a transaction it
    /// fenced.</para>
    /// </summary>
    public bool TryBeginFinalize()
    {
        lock (gate)
        {
            if (finalizing || killed)
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
            if (finalizing || killed || ownedByRequest || runningStatements > 0)
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

/// <summary>What <see cref="ActiveTransaction.TryBeginKill"/> did.</summary>
internal enum KillClaim
{
    /// <summary>No statement ran: the kill fenced the transaction and holds the finalize claim. Roll back now.</summary>
    Claimed,

    /// <summary>The kill fenced the transaction; it takes the finalize claim when the last running statement ends.</summary>
    Pending,

    /// <summary>An earlier kill fenced the transaction. Wait for its outcome.</summary>
    AlreadyKilled,

    /// <summary>A commit, rollback or reap holds the finalize claim. Nothing was fenced.</summary>
    Finalizing,

    /// <summary>A commit of the transaction got no answer, so its outcome belongs to the storage layer. Nothing was fenced.</summary>
    OutcomeUnknown,
}
