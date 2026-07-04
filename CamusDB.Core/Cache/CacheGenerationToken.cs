
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Cache;

/// <summary>
/// An immutable snapshot of per-keyspace generation counters taken at the moment a
/// cacheable query begins executing.
///
/// <para>The token is passed back to <see cref="CachePublishGate.CanPublish"/> once
/// the query finishes materializing. If any keyspace in the snapshot has a higher
/// generation in the gate than it did at snapshot time, a write committed into that
/// keyspace while the query ran and the result must not be published — the live rows
/// are already correct but would be stale on the next cache hit.</para>
///
/// <para><b>Invariant:</b> a token that observed generation N for keyspace K may only
/// publish if K's current generation is still N. Generation gaps are monotone and never
/// reset, so <c>current == snapshot</c> is sufficient; no wraparound is possible.</para>
/// </summary>
public sealed class CacheGenerationToken
{
    /// <summary>
    /// Snapshot of (keyspace → generation) taken just before scan execution started.
    /// Keyspaces not present in the gate at snapshot time are recorded as generation 0
    /// so any future write to that keyspace (which would set generation ≥ 1) is detected.
    /// </summary>
    public IReadOnlyDictionary<string, long> Generations { get; }

    /// <summary>Monotonic timestamp (Environment.TickCount64) when this token was minted.</summary>
    public long MintedAtMs { get; }

    internal CacheGenerationToken(Dictionary<string, long> generations)
    {
        Generations = generations;
        MintedAtMs  = Environment.TickCount64;
    }
}
