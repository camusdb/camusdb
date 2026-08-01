
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Frozen;

namespace CamusDB.Core.Transactions;

/// <summary>
/// Utility for executing single-statement (autocommit) serializable operations with bounded
/// automatic retry on transient serialization failures.
///
/// <para><b>The replay-from-BEGIN rule.</b> Strict two-phase locking (Serializable+ReadWrite)
/// aborts a conflicting transaction immediately — the whole transaction is dead, not just the
/// failing statement. The only safe recovery is to replay the entire transaction from a fresh
/// <c>BEGIN</c>: re-read all data, re-apply all writes, commit. Retrying the failing statement
/// inside the same transaction is incorrect because the transaction's earlier reads may now be
/// stale.</para>
///
/// <para><b>Retryable codes.</b> Only three error codes indicate a transient serialization
/// failure that may resolve on retry:
/// <list type="bullet">
///   <item><see cref="CamusDBErrorCodes.TransactionConflict"/> (CADB0502) — an exclusive or
///   shared range lock conflicted with a concurrent transaction. Kahuna returns
///   <c>AlreadyLocked</c> immediately (no 2PC was attempted), so the transaction is safely
///   restartable.</item>
///   <item><see cref="CamusDBErrorCodes.TransactionMustRetry"/> (CADB0504) — a routing-level
///   transient failure (leader election, partition move) returned <c>MustRetry</c> after
///   exhausting the server-side retry budget. No data was written.</item>
///   <item><see cref="CamusDBErrorCodes.TransactionLifetimeExceeded"/> (CADB0505) — the
///   transaction was open longer than <see cref="CamusDBOptions.MaxSerializableTransactionLifetimeMs"/>.
///   Its range locks were released cleanly before this error was raised.</item>
/// </list>
/// All other codes are permanent failures (constraint violations, schema errors, invalid input,
/// etc.) that retry cannot fix and must propagate to the caller.</para>
///
/// <para><b>Autocommit vs. explicit transactions.</b>
/// <see cref="ExecuteAutocommitAsync{T}"/> is intended for single-statement autocommit
/// operations where the entire transaction is the single operation passed in. The
/// <paramref name="operation"/> delegate owns its own <c>BEGIN</c> / <c>COMMIT</c> /
/// <c>ROLLBACK</c> lifecycle; the helper retries the whole delegate (a fresh begin each time).
/// For explicit multi-statement transactions, callers must implement their own retry loop using
/// <see cref="IsRetryable"/> to detect the retryable subset.</para>
/// </summary>
public static class SerializableRetryHelper
{
    /// <summary>Default maximum number of attempts (initial attempt + retries).</summary>
    public const int DefaultMaxAttempts = 5;

    private const int BaseDelayMs = 20;
    private const int MaxDelayMs  = 400;

    /// <summary>
    /// The set of <see cref="CamusDBException.Code"/> values that represent transient
    /// serialization failures — the only codes for which retrying from <c>BEGIN</c> is safe.
    /// </summary>
    public static readonly IReadOnlySet<string> RetryableCodes =
        new HashSet<string>(StringComparer.Ordinal)
        {
            CamusDBErrorCodes.TransactionConflict,
            CamusDBErrorCodes.TransactionMustRetry,
            CamusDBErrorCodes.TransactionLifetimeExceeded,
        }.ToFrozenSet(StringComparer.Ordinal);

    /// <summary>
    /// Returns <see langword="true"/> if <paramref name="ex"/> represents a transient
    /// serialization failure that can be resolved by replaying the transaction from
    /// <c>BEGIN</c>.
    /// </summary>
    public static bool IsRetryable(CamusDBException ex) =>
        RetryableCodes.Contains(ex.Code);

    /// <summary>
    /// Executes <paramref name="operation"/> (a full autocommit transaction) with bounded
    /// automatic retry on transient serialization failures.
    ///
    /// <para>On each attempt the delegate is called with a fresh <paramref name="cancellationToken"/>.
    /// The delegate is responsible for its own <c>BEGIN</c> / <c>COMMIT</c> / <c>ROLLBACK</c>
    /// lifecycle — the helper retries the whole delegate, satisfying the replay-from-BEGIN
    /// rule.</para>
    ///
    /// <para>Between attempts the helper sleeps for <c>min(BaseDelay × 2^attempt, MaxDelay)</c>
    /// ms with ±25 % random jitter to avoid thundering-herd re-collisions.</para>
    /// </summary>
    /// <param name="operation">
    /// A fully self-contained autocommit transaction. Must begin, execute, and commit (or
    /// roll back on error) a fresh transaction on each invocation.
    /// </param>
    /// <param name="maxAttempts">
    /// Total number of attempts (initial + retries). Must be ≥ 1.
    /// </param>
    /// <param name="cancellationToken">Propagated to each attempt.</param>
    /// <exception cref="CamusDBException">
    /// The retryable error from the last attempt once <paramref name="maxAttempts"/> is
    /// exhausted, or any non-retryable error immediately.
    /// </exception>
    public static async Task ExecuteAutocommitAsync(
        Func<CancellationToken, Task> operation,
        int maxAttempts = DefaultMaxAttempts,
        CancellationToken cancellationToken = default)
    {
        await ExecuteAutocommitAsync<object?>(
            async ct => { await operation(ct).ConfigureAwait(false); return null; },
            maxAttempts,
            cancellationToken
        ).ConfigureAwait(false);
    }

    /// <inheritdoc cref="ExecuteAutocommitAsync(Func{CancellationToken,Task},int,CancellationToken)"/>
    public static Task<T> ExecuteAutocommitAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        int maxAttempts = DefaultMaxAttempts,
        CancellationToken cancellationToken = default)
        => ExecuteAutocommitAsync(operation, static () => true, maxAttempts, cancellationToken);

    /// <summary>
    /// Variant of <see cref="ExecuteAutocommitAsync{T}(Func{CancellationToken,Task{T}},int,CancellationToken)"/>
    /// that consults <paramref name="canRetry"/> before every replay and, when it returns
    /// <see langword="false"/>, propagates the retryable error immediately instead of replaying.
    ///
    /// <para>Intended for <b>streaming</b> callers: once an attempt has emitted output on the wire
    /// (a schema or row message), replaying from <c>BEGIN</c> would re-emit it and corrupt the
    /// stream. Such a caller passes <c>canRetry: () =&gt; !alreadyWrote</c> so retry stays available
    /// for early failures (before the first byte) but a conflict surfacing after output has begun is
    /// reported as the terminal error. The predicate is checked only on the retry decision; the
    /// first attempt always runs.</para>
    /// </summary>
    public static async Task<T> ExecuteAutocommitAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        Func<bool> canRetry,
        int maxAttempts = DefaultMaxAttempts,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);

        CamusDBException? lastRetryable = null;

        for (int attempt = 0; attempt < maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await operation(cancellationToken).ConfigureAwait(false);
            }
            catch (CamusDBException ex) when (IsRetryable(ex))
            {
                lastRetryable = ex;

                // A retryable failure whose side effects can no longer be undone (e.g. a streaming
                // caller already wrote output on this attempt) must not replay — a re-run would
                // double-emit. Report it as terminal instead.
                if (!canRetry())
                    throw;

                if (attempt + 1 < maxAttempts)
                {
                    int delayMs = (int)Math.Min(BaseDelayMs * Math.Pow(2, attempt), MaxDelayMs);
                    int jitter  = (int)(delayMs * 0.25 * (Random.Shared.NextDouble() * 2 - 1));
                    await Task.Delay(Math.Max(1, delayMs + jitter), cancellationToken).ConfigureAwait(false);
                }
            }
        }

        throw lastRetryable!;
    }
}
