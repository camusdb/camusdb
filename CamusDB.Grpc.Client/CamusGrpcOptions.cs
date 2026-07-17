
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

    // ─── Transport (TLS + keep-alive) ─────────────────────────────────────────

    /// <summary>
    /// Accept any server certificate without chain validation. <b>Dev only</b> — disables TLS trust, so
    /// never enable it against a real deployment. Ignored when
    /// <see cref="TrustedServerCertificateThumbprints"/> is used (pinning takes precedence is not the
    /// rule: insecure wins if set, so do not set both). Mirrors Kahuna's insecure toggle.
    /// </summary>
    public bool AllowInsecureCertificateValidation { get; set; }

    /// <summary>
    /// SHA-256 certificate thumbprints (hex) to pin the server certificate to. When non-empty (and
    /// <see cref="AllowInsecureCertificateValidation"/> is false), the server cert is accepted only if its
    /// SHA-256 hash matches one of these; the OS chain is bypassed. Empty = standard OS chain validation.
    /// </summary>
    public IList<string> TrustedServerCertificateThumbprints { get; } = new List<string>();

    /// <summary>TCP connect timeout for a new channel connection.</summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>HTTP/2 keep-alive ping delay — keeps long-lived batch streams from being idled out.</summary>
    public TimeSpan KeepAlivePingDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long to wait for a keep-alive ping ack before treating the connection as dead.</summary>
    public TimeSpan KeepAlivePingTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Let the handler open more than one HTTP/2 connection so many concurrent batch streams (and the
    /// pool) aren't all funneled onto a single connection's stream limit. On by default.
    /// </summary>
    public bool EnableMultipleHttp2Connections { get; set; } = true;
}
