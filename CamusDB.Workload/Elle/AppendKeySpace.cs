/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Util;

namespace CamusDB.Workload.Elle;

/// <summary>
/// Generates list-append transactions over a small, rotating set of keys, the shape Jepsen's own
/// list-append generator uses. The rotation and the value uniqueness are <see cref="KeyRotation"/>'s.
///
/// <para>One generator serves all workers behind a lock. The seed makes a single-worker run repeat
/// exactly; with several workers the interleaving decides which worker gets which transaction, and no
/// seed can fix that.</para>
/// </summary>
public sealed class AppendKeySpace
{
    private readonly object _gate = new();
    private readonly DeterministicRandom _rng;
    private readonly KeyRotation _keys;
    private readonly int _maxTxnLength;

    public AppendKeySpace(
        ulong seed, int activeKeys, int maxWritesPerKey, int maxTxnLength,
        KeyDistribution distribution = KeyDistribution.Uniform)
    {
        if (maxTxnLength < 1)
            throw new ArgumentOutOfRangeException(nameof(maxTxnLength), "a transaction needs at least one step");

        _rng = new DeterministicRandom(seed ^ 0xA5A5_5A5A_C3C3_3C3CUL);
        _keys = new KeyRotation(_rng, activeKeys, maxWritesPerKey, distribution);
        _maxTxnLength = maxTxnLength;
    }

    public int ActiveKeys => _keys.ActiveKeys;

    public int MaxWritesPerKey => _keys.MaxWritesPerKey;

    public int MaxTxnLength => _maxTxnLength;

    public KeyDistribution Distribution => _keys.Distribution;

    /// <summary>Keys handed out so far, active ones included.</summary>
    public long KeysUsed
    {
        get
        {
            lock (_gate)
                return _keys.KeysUsed;
        }
    }

    /// <summary>
    /// The next transaction: 1 to <c>maxTxnLength</c> steps on active keys. A read-only transaction has
    /// reads only; otherwise each step is a read or an append with equal odds.
    /// </summary>
    public ElleMicroOp[] NextTxn(bool readOnly)
    {
        lock (_gate)
        {
            int length = 1 + (int)_rng.NextLong(_maxTxnLength);
            ElleMicroOp[] txn = new ElleMicroOp[length];
            for (int i = 0; i < length; i++)
            {
                int slot = _keys.NextSlot();
                long key = _keys.KeyOf(slot);

                if (readOnly || _rng.NextLong(2) == 0)
                {
                    txn[i] = ElleMicroOp.ReadOf(key);
                    continue;
                }

                txn[i] = ElleMicroOp.Append(key, _keys.NextValueFor(slot));
            }
            return txn;
        }
    }
}
