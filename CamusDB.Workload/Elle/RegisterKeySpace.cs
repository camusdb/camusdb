/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Util;

namespace CamusDB.Workload.Elle;

/// <summary>
/// Generates rw-register transactions over a small, rotating set of keys, the shape Jepsen's
/// rw-register generator uses. The rotation and the value uniqueness are <see cref="KeyRotation"/>'s.
///
/// <para>What the checker can infer from this shape, and what the generator does to help it:</para>
/// <list type="bullet">
/// <item>Every register starts as <c>nil</c>. A read of <c>nil</c> is a read of the initial version,
/// so a later write to that key is ordered after the read. The workload never deletes a row, so an
/// absent row always means "no write reached it", never "a write was undone".</item>
/// <item>Every written value is unique across the run. A read of value <c>v</c> names the one
/// transaction that wrote it, which is the only direct edge the rw-register model has.</item>
/// <item>Inside one transaction the steps are ordered. A transaction that reads <c>k</c> and then
/// writes <c>k</c> tells Elle the version it read precedes the one it wrote; that is why a read-write
/// transaction mixes reads and writes on the same few keys instead of writing only.</item>
/// <item>Elle's <c>:linearizable-keys?</c>, <c>:sequential-keys?</c> and <c>:wfr-keys?</c> options are
/// not used: elle-cli does not expose them, and Elle 0.2.7 documented a defect in them. Without them
/// the checker never assumes a per-key order from real time or from the client's process, so an
/// anomaly that needs that order (a stale read of an older version with no other link) is not found
/// by Elle; the end-of-run quiescent check (<see cref="ElleWriteLedger"/>) covers the final state
/// instead.</item>
/// </list>
///
/// <para>One generator serves all workers behind a lock, as the append generator does.</para>
/// </summary>
public sealed class RegisterKeySpace
{
    private readonly object _gate = new();
    private readonly DeterministicRandom _rng;
    private readonly KeyRotation _keys;
    private readonly int _maxTxnLength;

    public RegisterKeySpace(
        ulong seed, int activeKeys, int maxWritesPerKey, int maxTxnLength,
        KeyDistribution distribution = KeyDistribution.Exponential)
    {
        if (maxTxnLength < 1)
            throw new ArgumentOutOfRangeException(nameof(maxTxnLength), "a transaction needs at least one step");

        _rng = new DeterministicRandom(seed ^ 0x5EC0_7D5E_C0ED_1234UL);
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
    /// reads only; otherwise each step is a read or a write with equal odds. A read-write transaction
    /// of two or more steps always ends with a write, so the common shape "read k, write k" occurs
    /// often enough to give Elle version-order information on most keys.
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

                bool write = !readOnly && (i == length - 1 && length > 1 || _rng.NextLong(2) == 0);
                if (!write)
                {
                    txn[i] = ElleMicroOp.RegisterReadOf(key);
                    continue;
                }

                txn[i] = ElleMicroOp.Write(key, _keys.NextValueFor(slot));
            }
            return txn;
        }
    }
}
