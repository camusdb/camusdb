
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;

namespace CamusDB.Core.Util.Trees;

// internal nodes: only use key and next
// external nodes: only use key and value
public sealed class BTreeEntry<TKey, TValue>
{
    private readonly ConcurrentDictionary<HLCTimestamp, (BTreeCommitState, TValue?)> mvccValues = new(); // snapshot of the values seen by each timestamp    

    public TKey Key;

    public BTreeNode<TKey, TValue>? Next; // helper field to iterate over array entries

    public ObjectIdValue NextPageOffset; // the address of the next page offset

    public BTreeEntry(TKey key, HLCTimestamp initialTimestamp, BTreeCommitState commitState, TValue? initialValue, BTreeNode<TKey, TValue>? next)
    {
        Key = key;
        Next = next;

        SetValue(initialTimestamp, commitState, initialValue);
    }

    public void SetValue(HLCTimestamp initialTimestamp, BTreeCommitState commitState, TValue? initialValue)
    {
        //Console.WriteLine("{0} {1}", initialTimestamp, initialValue);

        if (!mvccValues.TryAdd(initialTimestamp, (commitState, initialValue)))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Keys must be unique");
    }    

    public TValue? GetValue(HLCTimestamp timestamp)
    {
        //Console.WriteLine("Get={0}", timestamp);

        if (mvccValues.TryGetValue(timestamp, out (BTreeCommitState commitState, TValue? value) snapshotValue))
            return snapshotValue.value;

        TValue? newestValue = default;

        foreach (KeyValuePair<HLCTimestamp, (BTreeCommitState commitState, TValue? value)> keyValue in mvccValues)
        {
            if (keyValue.Value.commitState == BTreeCommitState.Committed &&  keyValue.Key.CompareTo(timestamp) < 0)
                newestValue = keyValue.Value.value;
        }

        mvccValues.TryAdd(timestamp, (BTreeCommitState.Uncommitted, newestValue));

        return newestValue;
    }

    public bool CanBeSeenBy(HLCTimestamp timestamp)
    {
        //Console.WriteLine("CanBeSeenBy={0}", timestamp);

        if (mvccValues.ContainsKey(timestamp))
            return true;

        foreach (KeyValuePair<HLCTimestamp, (BTreeCommitState commitState, TValue? value)> keyValue in mvccValues)
        {
            if (keyValue.Value.commitState == BTreeCommitState.Committed && keyValue.Key.CompareTo(timestamp) < 0)
                return true;
        }

        return false;
    }
}
