/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Config;

/// <summary>
/// Client-side mirror of the bound that decides how long a Kahuna coordinator session can still be
/// alive. Two callers need the same number and must never disagree about it:
///
/// <list type="bullet">
///   <item><description><c>EmbeddedKahunaOptionsBuilder</c> lifts the node's session-timeout clamp so
///   the configured serializable lifetime is reachable instead of being silently truncated.</description></item>
///   <item><description><see cref="Transactions.KvTransactionsManager"/> decides when a transaction is
///   old enough that no live session can still own its holdings, which is the safety condition for
///   releasing them from the client-side key mirror.</description></item>
/// </list>
///
/// <para>The engine cannot read the embedded node's effective configuration back through
/// <c>IKahuna</c>, so the values here are a mirror, not a query. Keep them in step with Kahuna:
/// <see cref="NodeDefaultMaxTransactionTimeoutMs"/> mirrors <c>KahunaConfiguration.MaxTransactionTimeout</c>
/// and <see cref="CoordinatorReclaimGraceMs"/> mirrors the sum of the coordinator's reap grace and its
/// maximum participant-effect lease. A mirror that drifts high only makes the release wait longer,
/// which is the safe direction; a mirror that drifts low could release a live transaction's holdings.</para>
/// </summary>
internal static class KahunaSessionLifetime
{
    /// <summary>
    /// The node's own default hard ceiling on an admitted session timeout, in milliseconds. Applies
    /// when the <c>kahuna.max_transaction_timeout_ms</c> key is left unset and the engine's
    /// serializable lifetime does not raise it.
    /// </summary>
    internal const int NodeDefaultMaxTransactionTimeoutMs = 300_000;

    /// <summary>
    /// Milliseconds a session's reach extends past its own deadline, in the worst case. The
    /// coordinator waits a grace period before it reaps a session that passed its deadline, and then
    /// waits again for the longest time a dispatched participant effect can still land. Past the sum,
    /// nothing the session started can still arrive.
    /// </summary>
    internal const int CoordinatorReclaimGraceMs = 30_000;

    /// <summary>
    /// The effective hard ceiling on any session timeout this engine can obtain from the node, in
    /// milliseconds. The engine asks for <paramref name="maxSerializableLifetimeMs"/> as the session
    /// timeout and the node clamps that request to <paramref name="nodeClampMs"/>, so the ceiling is
    /// the clamp raised to admit the configured lifetime. A non-positive lifetime disables the engine
    /// cap and leaves the node value alone.
    /// </summary>
    internal static int MaxSessionTimeoutMs(int nodeClampMs, int maxSerializableLifetimeMs) =>
        maxSerializableLifetimeMs > 0 && nodeClampMs < maxSerializableLifetimeMs
            ? maxSerializableLifetimeMs
            : nodeClampMs;

    /// <summary>
    /// Age, in milliseconds, past which a transaction whose coordinator session is unknown is treated
    /// as abandoned: the session ceiling plus the reclaim grace. A transaction older than this cannot
    /// still be owned by a live session anywhere in the cluster, whatever a mis-routed answer during a
    /// leadership change might suggest.
    ///
    /// <para>Returns <see langword="null"/> when the operator disabled the release entirely
    /// (<see cref="CamusDBOptions.AbandonedTransactionReleaseAfterMs"/> negative). A positive setting
    /// is used verbatim so a soak or a test can compress the timeline.</para>
    /// </summary>
    internal static long? AbandonedReleaseAgeMs(CamusDBOptions options)
    {
        int configured = options.AbandonedTransactionReleaseAfterMs;

        if (configured < 0)
            return null;

        if (configured > 0)
            return configured;

        int ceiling = MaxSessionTimeoutMs(
            options.Kahuna.MaxTransactionTimeoutMs ?? NodeDefaultMaxTransactionTimeoutMs,
            options.MaxSerializableTransactionLifetimeMs);

        return (long)ceiling + CoordinatorReclaimGraceMs;
    }
}
