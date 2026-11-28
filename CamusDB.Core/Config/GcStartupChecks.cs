/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Core.Config;

/// <summary>
/// Reports a node that runs the server GC in its dynamic adaptation mode together with a write
/// aggregator that dispatches Raft rounds at a short interval.
///
/// <para>The server build turns the mode off, so this state needs
/// <c>DOTNET_GCDynamicAdaptationMode=1</c> in the environment or a changed
/// <c>System.GC.DynamicAdaptationMode</c> in the runtime configuration. The node starts and serves in
/// it. Under load the mode sizes the young generation from the share of memory that survives a
/// collection, and frequent rounds leave little alive, so the leader can settle into a state with
/// many small collections: measured on the bank shape, 13.6% of the time in GC pauses on one node,
/// and 24% with throughput at 0.80x on a three-node leader. Nothing else reports the cause.</para>
///
/// <para>The GC state is passed in because only the running process can read it, and a test passes
/// each combination. The caller reads it with <see cref="ReadDynamicAdaptationMode"/>.</para>
/// </summary>
public static class GcStartupChecks
{
    /// <summary>
    /// Returns one message when dynamic adaptation is on under the server GC and the configured
    /// write aggregator runs short cycles: the exact hold (precise wake on) or no hold at all.
    /// Empty otherwise. Workstation GC has no dynamic adaptation, whatever the setting reports.
    /// </summary>
    /// <param name="kahuna">The node's Kahuna settings, with unset keys left null.</param>
    /// <param name="isClusterMode">Whether the node runs in cluster mode. An unset hold is 1 ms there and 0 on a standalone node.</param>
    /// <param name="isServerGc">Whether the process runs the server GC.</param>
    /// <param name="dynamicAdaptationMode">The mode the GC reports, or null when it reports none.</param>
    public static IReadOnlyList<string> Inspect(KahunaOptionsConfig kahuna, bool isClusterMode, bool isServerGc, long? dynamicAdaptationMode)
    {
        if (!isServerGc || dynamicAdaptationMode is null or 0)
            return [];

        bool preciseWake = kahuna.KeyValueWritePreciseWake ?? EmbeddedKahunaOptionsBuilder.DefaultKeyValueWritePreciseWake;
        int holdMs = kahuna.KeyValueWritePostCompletionHoldMs
            ?? (isClusterMode
                ? EmbeddedKahunaOptionsBuilder.DefaultKeyValueWritePostCompletionHoldMs
                : EmbeddedKahunaOptionsBuilder.DefaultStandaloneKeyValueWritePostCompletionHoldMs);

        if (!preciseWake && holdMs > 0)
            return [];

        string cycle = holdMs == 0
            ? "dispatches each Raft round as soon as the one before it completes (kahuna.key_value_write_post_completion_hold_ms is 0)"
            : "ends its hold at the deadline (kahuna.key_value_write_precise_wake is on)";

        return
        [
            $"The garbage collector runs in dynamic adaptation mode (System.GC.DynamicAdaptationMode = {dynamicAdaptationMode}) " +
            $"and the write aggregator {cycle}. With frequent Raft rounds this mode can shrink the young " +
            "generation of a busy leader until collections take 10% to 25% of its time. CamusDB is built " +
            "with the mode off: remove DOTNET_GCDynamicAdaptationMode from the environment, or set it to 0. " +
            "To keep the mode on, set kahuna.key_value_write_precise_wake to false and " +
            "kahuna.key_value_write_post_completion_hold_ms to 1 or more."
        ];
    }

    /// <summary>
    /// The dynamic adaptation mode the running GC reports, or null when it reports none. The runtime
    /// reports the mode in force: 0 when the environment or the runtime configuration turned it off,
    /// and 0 when a fixed heap count rules it out.
    /// </summary>
    public static long? ReadDynamicAdaptationMode()
    {
        if (!GC.GetConfigurationVariables().TryGetValue("GCDynamicAdaptationMode", out object? value) || value is null)
            return null;

        try
        {
            return Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
        {
            return null;
        }
    }
}
