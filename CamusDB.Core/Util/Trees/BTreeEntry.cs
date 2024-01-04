
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using System.Collections.Concurrent;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Util.Trees;

// internal nodes: only use key and next
// external nodes: only use key and value
public sealed class BTreeEntry<TKey, TValue> where TKey : IComparable<TKey>
{
    private readonly IBTreeNodeReader<TKey, TValue>? Reader; // lazy node reader

    private readonly ConcurrentDictionary<HLCTimestamp, BTreeMvccEntry<TValue>> mvccValues = new(); // snapshot of the values seen by each timestamp    

    public TKey Key;

    public ObjectIdValue NextPageOffset; // the address of the next page offset

    public AsyncLazy<BTreeNode<TKey, TValue>?> Next; // helper field to iterate over array entries    

    public BTreeEntry(TKey key, IBTreeNodeReader<TKey, TValue>? reader, BTreeNode<TKey, TValue>? next)
    {
        Key = key;
        Reader = reader;

        if (next is not null)
            Next = new AsyncLazy<BTreeNode<TKey, TValue>?>(() => Task.FromResult<BTreeNode<TKey, TValue>?>(next));
        else
            Next = new AsyncLazy<BTreeNode<TKey, TValue>?>(LoadNode);
    }

    private async Task<BTreeNode<TKey, TValue>?> LoadNode()
    {
        if (NextPageOffset.IsNull())
            return null;

        if (Reader is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Cannot read lazy node because reader is null");

        return await Reader.GetNode(NextPageOffset);
    }

    /// <summary>
    /// Sets or replaces the value and commit state for a specific timestamp
    /// </summary>
    /// <param name="timestamp"></param>
    /// <param name="commitState"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
    public BTreeMvccEntry<TValue> SetValue(HLCTimestamp timestamp, BTreeCommitState commitState, TValue? value)
    {
        //Console.WriteLine("SetV={0} {1} {2}", timestamp, commitState, value);

        if (mvccValues.TryGetValue(timestamp, out BTreeMvccEntry<TValue>? mvccEntry))
        {
            mvccEntry.CommitState = commitState;
            mvccEntry.Value = value;
            return mvccEntry;
        }

        mvccEntry = new(commitState, value);

        if (!mvccValues.TryAdd(timestamp, mvccEntry))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Keys must be unique");

        return mvccEntry;
    }

    /// <summary>
    /// Returns the most recent value in the list of MVCC values. If the transaction is a write,
    /// the last committed value is stored in a snapshot and returned consistently to avoid dirty reads.
    /// </summary>
    /// <param name="txType"></param>
    /// <param name="timestamp"></param>
    /// <returns></returns>
    public TValue? GetValue(TransactionType txType, HLCTimestamp timestamp)
    {
        //Console.WriteLine("Get={0}", timestamp);

        if (mvccValues.TryGetValue(timestamp, out BTreeMvccEntry<TValue>? snapshotValue))
            return snapshotValue.Value;

        TValue? newestValue = default;
        HLCTimestamp recentTimestamp = HLCTimestamp.Zero;

        foreach (KeyValuePair<HLCTimestamp, BTreeMvccEntry<TValue>> keyValue in mvccValues)
        {
            //Console.WriteLine("Get={0} {1} {2} {3}", timestamp, keyValue.Value.CommitState, keyValue.Key.CompareTo(timestamp), keyValue.Value.Value);

            if (keyValue.Value.CommitState == BTreeCommitState.Committed && keyValue.Key.CompareTo(timestamp) <= 0 && keyValue.Key.CompareTo(recentTimestamp) > 0)
            {
                recentTimestamp = keyValue.Key;
                newestValue = keyValue.Value.Value;
            }
        }

        //Console.WriteLine("GetX={0} {1}", timestamp, newestValue);

        if (txType == TransactionType.Write)
            mvccValues.TryAdd(timestamp, new(BTreeCommitState.Uncommitted, newestValue));

        return newestValue;
    }

    /// <summary>
    /// Checks whether a value can be seen by a specific timestamp
    /// </summary>
    /// <param name="timestamp"></param>
    /// <returns></returns>
    public bool CanBeSeenBy(HLCTimestamp timestamp)
    {
        //Console.WriteLine("CanBeSeenBy={0}", timestamp);

        if (mvccValues.ContainsKey(timestamp))
            return true;

        foreach (KeyValuePair<HLCTimestamp, BTreeMvccEntry<TValue>> keyValue in mvccValues)
        {
            if (keyValue.Value.CommitState == BTreeCommitState.Committed && keyValue.Key.CompareTo(timestamp) < 0)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Returns the maximum commited value to the entry
    /// </summary>
    /// <returns></returns>
    internal (HLCTimestamp, TValue?) GetMaxCommitedValue()
    {
        TValue? value = default;
        HLCTimestamp newestValue = HLCTimestamp.Zero;

        foreach (KeyValuePair<HLCTimestamp, BTreeMvccEntry<TValue>> keyValue in mvccValues)
        {
            //Console.WriteLine("GetMaxCommitedValue {0} {1} {2}", keyValue.Value.CommitState, keyValue.Key, keyValue.Value.Value);

            if (keyValue.Value.CommitState == BTreeCommitState.Committed && keyValue.Key.CompareTo(newestValue) > 0)
            {
                newestValue = keyValue.Key;
                value = keyValue.Value.Value;
            }
        }

        return (newestValue, value);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="timestamp"></param>
    /// <returns></returns>
    internal bool HasExpiredEntries(HLCTimestamp timestamp)
    {
        Console.WriteLine(mvccValues.Count);

        if (mvccValues.Count <= 1)
            return false;

        foreach (KeyValuePair<HLCTimestamp, BTreeMvccEntry<TValue>> keyValue in mvccValues)
        {
            Console.WriteLine("HasExpiredEntries {0} {1} {2}", keyValue.Value.CommitState, keyValue.Key, keyValue.Value.Value);

            if (keyValue.Key.CompareTo(timestamp) < 0)
                return true;
        }

        return false;
    }
}
