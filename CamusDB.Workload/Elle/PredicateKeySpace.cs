/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Workload.Util;

namespace CamusDB.Workload.Elle;

/// <summary>One step of a predicate transaction before it is run: a read of a whole group, or a
/// write of one key in a group.</summary>
public readonly record struct PredicateStep(bool IsGroupRead, long Group, long Key, long Value)
{
    public static PredicateStep GroupRead(long group) => new(true, group, 0, 0);

    public static PredicateStep Write(long group, long key, long value) => new(false, group, key, value);
}

/// <summary>
/// Generates predicate transactions: writes of single rows and reads of whole groups of rows, over a
/// small rotating set of groups. Each group owns a fixed, known universe of <see cref="GroupSize"/>
/// keys, and that is what makes a predicate read checkable by Elle's rw-register model.
///
/// <para>Elle checks point reads and point writes of keys. A read of a group (<c>WHERE grp = g</c>,
/// <c>WHERE k BETWEEN lo AND hi</c>) is not a point read, but the universe of keys the predicate can
/// match is finite and known, so the read is recorded as one register read per key of that universe:
/// the value returned for a present key, and <c>nil</c> for an absent one. The absent keys are the
/// point: a transaction that reads <c>nil</c> for key <c>k</c> read the initial version, so a
/// transaction that inserts <c>k</c> is ordered after it. Two transactions that each read the group
/// and then insert a key the other did not see form the write-skew cycle, an anti-dependency cycle
/// in Adya's terms; a transaction that reads the group twice and sees a key appear is the phantom
/// (G-single). Neither is visible to a
/// point-key history, because a point read of a key that does not yet exist is never issued.</para>
///
/// <para>The dependencies recorded are therefore: a present row <c>(k, v)</c> is a read of version
/// <c>v</c> of <c>k</c>; an absent key of the group is a read of version <c>nil</c>; an insert is a
/// write that moves a key from <c>nil</c> to its first value; an update is a write of a later value.
/// No row is ever deleted, so <c>nil</c> is always the initial version and never a later one; a
/// membership change that removes a row would need a version the model cannot express.</para>
///
/// <para>Every group rotates out after <see cref="MaxWritesPerGroup"/> writes, as a register key
/// does. A group's keys are not all written in its lifetime, so most group reads see absent keys.</para>
/// </summary>
public sealed class PredicateKeySpace
{
    private readonly object _gate = new();
    private readonly DeterministicRandom _rng;
    private readonly KeyRotation _groups;
    private readonly int _groupSize;
    private readonly int _maxTxnLength;

    public PredicateKeySpace(
        ulong seed, int activeGroups, int groupSize, int maxWritesPerGroup, int maxTxnLength,
        KeyDistribution distribution = KeyDistribution.Exponential)
    {
        if (groupSize < 2)
            throw new ArgumentOutOfRangeException(nameof(groupSize), "a group needs at least two keys for a predicate to select among");
        if (maxTxnLength < 1)
            throw new ArgumentOutOfRangeException(nameof(maxTxnLength), "a transaction needs at least one step");

        _rng = new DeterministicRandom(seed ^ 0x9E37_79B9_7F4A_7C15UL);
        _groups = new KeyRotation(_rng, activeGroups, maxWritesPerGroup, distribution);
        _groupSize = groupSize;
        _maxTxnLength = maxTxnLength;
    }

    public int ActiveGroups => _groups.ActiveKeys;

    public int GroupSize => _groupSize;

    public int MaxWritesPerGroup => _groups.MaxWritesPerKey;

    public int MaxTxnLength => _maxTxnLength;

    public KeyDistribution Distribution => _groups.Distribution;

    /// <summary>Groups handed out so far, active ones included.</summary>
    public long GroupsUsed
    {
        get
        {
            lock (_gate)
                return _groups.KeysUsed;
        }
    }

    /// <summary>The first key of a group; the group holds <see cref="GroupSize"/> consecutive keys.</summary>
    public long FirstKeyOf(long group) => group * _groupSize;

    public long LastKeyOf(long group) => group * _groupSize + _groupSize - 1;

    /// <summary>The group a key belongs to.</summary>
    public long GroupOf(long key) => key / _groupSize;

    /// <summary>
    /// The next transaction: 1 to <c>maxTxnLength</c> steps on active groups. A read-only transaction
    /// has group reads only; otherwise each step is a group read or a single-key write with equal
    /// odds, and a transaction of two or more steps ends with a write, so "read the group, then write
    /// into it" is a common shape.
    /// </summary>
    public PredicateStep[] NextTxn(bool readOnly)
    {
        lock (_gate)
        {
            int length = 1 + (int)_rng.NextLong(_maxTxnLength);
            PredicateStep[] txn = new PredicateStep[length];
            for (int i = 0; i < length; i++)
            {
                int slot = _groups.NextSlot();
                long group = _groups.KeyOf(slot);

                bool write = !readOnly && (i == length - 1 && length > 1 || _rng.NextLong(2) == 0);
                if (!write)
                {
                    txn[i] = PredicateStep.GroupRead(group);
                    continue;
                }

                long key = FirstKeyOf(group) + _rng.NextLong(_groupSize);
                txn[i] = PredicateStep.Write(group, key, _groups.NextValueFor(slot));
            }
            return txn;
        }
    }

    /// <summary>
    /// The register steps a predicate transaction is recorded as: one <c>[:r k nil]</c> per key of a
    /// read group, in key order, and one <c>[:w k v]</c> per write. Reads are filled in by the operation
    /// once the rows are known.
    /// </summary>
    public ElleMicroOp[] ToMicroOps(PredicateStep[] txn)
    {
        int count = 0;
        foreach (PredicateStep step in txn)
            count += step.IsGroupRead ? _groupSize : 1;

        ElleMicroOp[] ops = new ElleMicroOp[count];
        int at = 0;
        foreach (PredicateStep step in txn)
        {
            if (step.IsGroupRead)
            {
                long first = FirstKeyOf(step.Group);
                for (int i = 0; i < _groupSize; i++)
                    ops[at++] = ElleMicroOp.RegisterReadOf(first + i);
            }
            else
            {
                ops[at++] = ElleMicroOp.Write(step.Key, step.Value);
            }
        }
        return ops;
    }
}
