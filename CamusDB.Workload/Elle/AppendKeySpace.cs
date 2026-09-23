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
/// list-append generator uses.
///
/// <para>A few keys at a time are active, so concurrent transactions collide on them: contention is
/// what gives Elle dependency edges to find cycles in. Each active slot moves to a fresh key after
/// <c>maxWritesPerKey</c> appends. That keeps every list short, because the checker's cost grows with
/// list length, and it keeps the number of keys in the history growing so the check covers many
/// independent lists.</para>
///
/// <para>Every appended value is unique across the whole run, not only per key, and a retried attempt
/// gets new values. Elle attributes a value it reads to the one transaction that appended it, so a
/// value reused by a failed attempt and a later committed one would make a read of it ambiguous.</para>
///
/// <para>One generator serves all workers behind a lock. The seed makes a single-worker run repeat
/// exactly; with several workers the interleaving decides which worker gets which transaction, and no
/// seed can fix that.</para>
/// </summary>
public sealed class AppendKeySpace
{
    private readonly object _gate = new();
    private readonly DeterministicRandom _rng;
    private readonly int _maxWritesPerKey;
    private readonly int _maxTxnLength;
    private readonly long[] _slotKey;
    private readonly int[] _slotWrites;
    private long _nextKey;
    private long _nextValue;

    public AppendKeySpace(ulong seed, int activeKeys, int maxWritesPerKey, int maxTxnLength)
    {
        if (activeKeys < 1)
            throw new ArgumentOutOfRangeException(nameof(activeKeys), "at least one active key is needed");
        if (maxWritesPerKey < 1)
            throw new ArgumentOutOfRangeException(nameof(maxWritesPerKey), "each key must take at least one append");
        if (maxTxnLength < 1)
            throw new ArgumentOutOfRangeException(nameof(maxTxnLength), "a transaction needs at least one step");

        _rng = new DeterministicRandom(seed ^ 0xA5A5_5A5A_C3C3_3C3CUL);
        _maxWritesPerKey = maxWritesPerKey;
        _maxTxnLength = maxTxnLength;
        _slotKey = new long[activeKeys];
        _slotWrites = new int[activeKeys];
        for (int i = 0; i < activeKeys; i++)
            _slotKey[i] = _nextKey++;
    }

    public int ActiveKeys => _slotKey.Length;

    public int MaxWritesPerKey => _maxWritesPerKey;

    public int MaxTxnLength => _maxTxnLength;

    /// <summary>Keys handed out so far, active ones included.</summary>
    public long KeysUsed
    {
        get
        {
            lock (_gate)
                return _nextKey;
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
                int slot = (int)_rng.NextLong(_slotKey.Length);
                long key = _slotKey[slot];

                if (readOnly || _rng.NextLong(2) == 0)
                {
                    txn[i] = ElleMicroOp.ReadOf(key);
                    continue;
                }

                txn[i] = ElleMicroOp.Append(key, ++_nextValue);
                if (++_slotWrites[slot] >= _maxWritesPerKey)
                {
                    _slotKey[slot] = _nextKey++;
                    _slotWrites[slot] = 0;
                }
            }
            return txn;
        }
    }
}
