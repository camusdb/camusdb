
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Catalogs.Transactional;

/// <summary>
/// The share that the schema statements of one node have in the DDL lock of one database, when they run
/// outside a DDL transaction. The first statement takes <see cref="TransactionalDdlLock"/>, a statement that
/// starts while others run joins them, and the last one frees it.
///
/// <para><b>Why the statements share it.</b> The lock keeps a schema statement from moving the base that an
/// open DDL transaction commits against. Two statements outside a transaction do not need it between
/// them: <see cref="SchemaDdlGate"/> serializes their proposals and makes a statement wait for a staged
/// change of the same table only. A lock that each of them took alone would make every schema statement of
/// the database wait for an index build, and fail with
/// <see cref="CamusDBErrorCodes.TransactionMustRetry"/> when the build takes longer than the wait.</para>
///
/// <para><b>A DDL transaction waits for the last of them.</b> Statements that overlap without a gap keep it
/// waiting until it gives up with the same retryable code. The transaction changed nothing by then.</para>
/// </summary>
internal sealed class StatementDdlLock
{
    /// <summary>How often a statement looks whether the statements of its node got a lost lock back.</summary>
    private const int LostPollMs = 100;

    /// <summary>Held while a statement joins or leaves, and while the first one waits for the DDL lock.</summary>
    private readonly SemaphoreSlim turn = new(1, 1);

    private TransactionalDdlLock? held;

    private int statements;

    /// <summary>
    /// Joins the statements that hold the DDL lock of <paramref name="database"/> on this node, or takes the
    /// lock for the first of them, and waits up to <paramref name="waitTimeout"/> for a DDL transaction.
    /// Leave with <see cref="Share.DisposeAsync"/>.
    /// </summary>
    internal async Task<Share> EnterAsync(DatabaseDescriptor database, TimeSpan waitTimeout, ILogger? logger)
    {
        long deadline = Environment.TickCount64 + (long)waitTimeout.TotalMilliseconds;

        while (true)
        {
            TimeSpan left = TimeSpan.FromMilliseconds(Math.Max(0, deadline - Environment.TickCount64));

            if (!await turn.WaitAsync(left).ConfigureAwait(false))
                throw TransactionalDdlLock.Busy(database, waitTimeout);

            try
            {
                if (held is null)
                {
                    left = TimeSpan.FromMilliseconds(Math.Max(0, deadline - Environment.TickCount64));
                    held = await TransactionalDdlLock.AcquireForStatementsAsync(database, left, logger).ConfigureAwait(false);
                    statements = 1;
                    return new Share(this);
                }

                if (!held.IsLost)
                {
                    statements++;
                    return new Share(this);
                }
            }
            finally
            {
                turn.Release();
            }

            // A DDL transaction took the lock while statements of this node run. Their renewer takes it again
            // when the transaction ends. The turn is free meanwhile, so a statement that ends can leave.
            if (Environment.TickCount64 >= deadline)
                throw TransactionalDdlLock.Busy(database, waitTimeout);

            await Task.Delay(LostPollMs).ConfigureAwait(false);
        }
    }

    private async Task LeaveAsync()
    {
        await turn.WaitAsync().ConfigureAwait(false);
        try
        {
            if (--statements > 0 || held is null)
                return;

            TransactionalDdlLock last = held;
            held = null;

            await last.ReleaseAsync().ConfigureAwait(false);
        }
        finally
        {
            turn.Release();
        }
    }

    /// <summary>The share of one statement. Disposing it twice is harmless.</summary>
    internal sealed class Share : IAsyncDisposable
    {
        private readonly StatementDdlLock owner;

        private int left;

        internal Share(StatementDdlLock owner) => this.owner = owner;

        public ValueTask DisposeAsync() =>
            Interlocked.Exchange(ref left, 1) == 0 ? new(owner.LeaveAsync()) : ValueTask.CompletedTask;
    }
}
