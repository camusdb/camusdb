/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The retry policy shared by the catalog and registry startup scans.
///
/// <para><b>The window is the discriminator, not the exception type.</b> Both scans run while the
/// cluster is still assembling itself, and there is no useful way to enumerate in advance every
/// transient a half-formed cluster can produce. Each loop previously retried only
/// <see cref="Kommander.RaftException"/> — the boot race it was written for, where the scan reached a
/// node before the bucket's partition existed. A rejoining node hits a different one: the scan
/// hash-routes the <c>_system/</c> bucket, so on a multi-node cluster it frequently has to reach a
/// peer, and a peer that has not finished restarting yields a transport failure
/// (<c>Grpc.Core.RpcException</c>, "Connection refused") rather than a Raft one. That escaped the
/// filter, propagated out of <c>Program.Main</c>, and killed the process during startup — a node that
/// crashed on boot instead of waiting a few hundred milliseconds for its peer.</para>
///
/// <para>So the classification is by <i>persistence</i> rather than by type, which is what the loops
/// always claimed to do: a fault that clears inside the budget was a blip worth waiting out, and one
/// that outlives the budget is real and is surfaced unchanged. Widening the catch cannot mask a
/// genuine failure — it delays it by at most the budget, on a path where boot latency is not
/// precious.</para>
///
/// <para>Cancellation is the one exception that is never a blip: it means the process is shutting
/// down, and retrying it would spin the loop for the whole budget against a token that will never
/// become good again.</para>
/// </summary>
internal static class StartupLoadRetry
{
    /// <summary>How long a startup scan keeps retrying before it gives up and surfaces the failure.</summary>
    internal const int MaxWaitMs = 30_000;

    /// <summary>Pause between attempts. Short, because the conditions being waited out — an election,
    /// a peer finishing its own boot — resolve in well under a second.</summary>
    internal const int RetryDelayMs = 200;

    /// <summary>
    /// Whether a failed startup scan should be retried: any fault except cancellation, for as long as
    /// the budget has not elapsed.
    /// </summary>
    internal static bool ShouldRetry(Exception exception, long elapsedMs, int budgetMs = MaxWaitMs)
        => exception is not OperationCanceledException && elapsedMs < budgetMs;
}
