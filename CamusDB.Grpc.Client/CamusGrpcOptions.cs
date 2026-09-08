
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

    // ─── Learned routing ──────────────────────────────────────────────────────

    /// <summary>
    /// Learned statement-routing configuration for a multi-endpoint connection. Snapshotted at
    /// <c>Connect</c> time — later mutation of this object does not affect an existing connection,
    /// unlike the batching tunables above, because routing state (endpoint pools, the trust map)
    /// cannot safely change underneath in-flight selection.
    /// </summary>
    public CamusRoutingOptions Routing { get; } = new();
}

/// <summary>Whether and how a connection learns per-statement destinations. See <see cref="CamusRoutingOptions.Mode"/>.</summary>
public enum CamusRoutingMode
{
    /// <summary>Rotate over the configured endpoints; never negotiate or learn.</summary>
    Off = 0,

    /// <summary>Negotiate routing metadata and prefer learned destinations for unpinned work.</summary>
    Learned = 1,

    /// <summary>
    /// Behave as <see cref="Learned"/> when the trust map names at least two distinct reachable
    /// addresses, else as <see cref="Off"/>. A single load-balancer URL is not a set of routable
    /// database nodes, so learning against it would only add bookkeeping. The default.
    /// </summary>
    Auto = 2,
}

/// <summary>
/// Tunables for learned statement routing. All values are snapshotted at <c>Connect</c>.
///
/// <para><b>The trust map is the routing authority.</b> <see cref="NodeAddresses"/> maps a
/// server-advertised opaque node identity to an operator-configured client address. Advice naming
/// an identity outside this map is ignored: the client never dials a response-provided address and
/// never derives one from a server identity, so a compromised or confused response cannot steer
/// traffic — or credentials — anywhere the operator did not list.</para>
/// </summary>
public sealed class CamusRoutingOptions
{
    /// <summary>
    /// Routing mode. Default <see cref="CamusRoutingMode.Auto"/>: learning engages by itself when
    /// the operator maps at least two distinct endpoints in <see cref="NodeAddresses"/>, and a
    /// connection with no trust map behaves exactly as <see cref="CamusRoutingMode.Off"/> — no
    /// negotiation, requests byte-identical to a pre-routing client.
    /// </summary>
    public CamusRoutingMode Mode { get; set; } = CamusRoutingMode.Auto;

    /// <summary>
    /// Node identity → client address (e.g. <c>"camus-b:7070"</c> → <c>"https://db-b.internal:9090"</c>).
    /// Addresses named here join the connection's endpoint pool and get the connection's full TLS,
    /// credential and timeout policy.
    /// </summary>
    public IDictionary<string, string> NodeAddresses { get; } = new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>Bound on learned route entries. High-cardinality SQL must not grow the cache without limit.</summary>
    public int RouteCacheMaxEntries { get; set; } = 4_096;

    /// <summary>Bound on the bytes the route cache retains (keys included), same rationale as the entry cap.</summary>
    public long RouteCacheMaxBytes { get; set; } = 4 * 1024 * 1024;

    /// <summary>
    /// Client-side ceiling on a hint's advertised age. The effective TTL of a learned route is the
    /// smaller of this and the server's <c>maxAgeMs</c>, measured monotonically from receipt.
    /// </summary>
    public TimeSpan MaxHintAge { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long a transport-failed endpoint is skipped for future <b>unpinned</b> work. A domain
    /// SQL error never triggers this — only the transport failing does.
    /// </summary>
    public TimeSpan EndpointCooldown { get; set; } = TimeSpan.FromSeconds(1);
}
