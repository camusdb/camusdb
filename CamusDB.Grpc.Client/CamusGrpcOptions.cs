
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Grpc.Client;

/// <summary>
/// Tunables for the multiplexing gRPC client. Defaults mirror the Kahuna client batcher, which this
/// client is modeled on. The pool size bounds how many long-lived duplex streams exist per endpoint —
/// <b>not</b> how many transactions can be in flight (many transactions hash onto the same stream and
/// interleave), so a small pool is normal. Coalescing trades a tiny latency delay for fewer, larger
/// writes when a burst of ops arrives together.
/// </summary>
public sealed class CamusGrpcOptions
{
    /// <summary>Number of long-lived <c>BatchExecute</c> streams multiplexed per endpoint.</summary>
    public int ChannelPoolSize { get; set; } = 2;

    /// <summary>
    /// When a pump drain produces fewer than this many ops, wait <see cref="CoalescingDelayMs"/> to let
    /// more accumulate before the next drain. A threshold of 1 (or a zero delay) disables coalescing.
    /// </summary>
    public int CoalescingThreshold { get; set; } = 10;

    /// <summary>Upper bound of the randomized coalescing delay, in milliseconds.</summary>
    public int CoalescingDelayMs { get; set; } = 2;

    /// <summary>
    /// Default deadline applied to an op whose caller supplied no cancellation token. Zero disables the
    /// deadline (an op can then wait forever on a wedged stream). Mirrors Kahuna's operation timeout.
    /// </summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.Zero;
}
