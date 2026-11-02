/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Util;

namespace CamusDB.Workload.Elle;

/// <summary>How a transaction picks among the active keys.</summary>
public enum KeyDistribution
{
    /// <summary>Every active key is equally likely.</summary>
    Uniform,

    /// <summary>The first active slot is the hottest and each later slot is half as likely, the shape
    /// Jepsen's generators use by default (<c>:key-dist :exponential</c>, base 2). A hot key is what
    /// makes a rare anomaly show up in a bounded run: most transactions touch it, so most pairs of
    /// transactions are ordered through it.</summary>
    Exponential,
}

/// <summary>
/// The rotating set of active keys that every Elle generator shares.
///
/// <para>A few keys at a time are active, so concurrent transactions collide on them: contention is
/// what gives Elle dependency edges to find cycles in. Each active slot moves to a fresh key after a
/// fixed number of writes. That keeps every list short and every register's version count bounded,
/// because the checker's cost grows with both, and it keeps the number of keys in the history growing
/// so the check covers many independent objects.</para>
///
/// <para>Every written value is unique across the whole run, not only per key, and a retried attempt
/// gets new values. Both checkers attribute a value they read to the one transaction that wrote it, so
/// a value reused by a failed attempt and a later committed one would make a read of it ambiguous.</para>
///
/// <para>Not thread-safe: the owning generator holds a lock around every call.</para>
/// </summary>
internal sealed class KeyRotation
{
    private readonly DeterministicRandom _rng;
    private readonly int _maxWritesPerKey;
    private readonly KeyDistribution _distribution;
    private readonly long[] _slotKey;
    private readonly int[] _slotWrites;
    private long _nextKey;
    private long _nextValue;

    public KeyRotation(DeterministicRandom rng, int activeKeys, int maxWritesPerKey, KeyDistribution distribution)
    {
        if (activeKeys < 1)
            throw new ArgumentOutOfRangeException(nameof(activeKeys), "at least one active key is needed");
        if (maxWritesPerKey < 1)
            throw new ArgumentOutOfRangeException(nameof(maxWritesPerKey), "each key must take at least one write");

        _rng = rng;
        _maxWritesPerKey = maxWritesPerKey;
        _distribution = distribution;
        _slotKey = new long[activeKeys];
        _slotWrites = new int[activeKeys];
        for (int i = 0; i < activeKeys; i++)
            _slotKey[i] = _nextKey++;
    }

    public int ActiveKeys => _slotKey.Length;

    public int MaxWritesPerKey => _maxWritesPerKey;

    public KeyDistribution Distribution => _distribution;

    /// <summary>Keys handed out so far, active ones included.</summary>
    public long KeysUsed => _nextKey;

    /// <summary>Values handed out so far.</summary>
    public long ValuesUsed => _nextValue;

    /// <summary>A slot index drawn from the configured distribution.</summary>
    public int NextSlot() => NextSlot(_rng, _slotKey.Length, _distribution);

    /// <summary>
    /// The slot a step lands on. Under the exponential distribution the draw is the inverse of a
    /// geometric cumulative distribution with base 2: slot 0 takes about half of the draws, slot 1 a
    /// quarter, and so on, with the last slot taking the remainder.
    /// </summary>
    internal static int NextSlot(DeterministicRandom rng, int slots, KeyDistribution distribution)
    {
        if (slots == 1 || distribution == KeyDistribution.Uniform)
            return (int)rng.NextLong(slots);

        // x is uniform over [1, 2^slots); floor(log2 x) is j with probability 2^j / (2^slots - 1), so
        // the hottest slot is the highest j, mapped onto slot 0.
        double x = 1 + rng.NextDouble() * (Math.Pow(2, slots) - 1);
        int j = (int)Math.Floor(Math.Log2(x));
        return Math.Clamp(slots - 1 - j, 0, slots - 1);
    }

    public long KeyOf(int slot) => _slotKey[slot];

    /// <summary>The next unique value, and the rotation of the slot once its key took enough writes.</summary>
    public long NextValueFor(int slot)
    {
        long value = ++_nextValue;
        if (++_slotWrites[slot] >= _maxWritesPerKey)
        {
            _slotKey[slot] = _nextKey++;
            _slotWrites[slot] = 0;
        }
        return value;
    }
}
