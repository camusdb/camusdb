
using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using Nito.AsyncEx;

namespace CamusDB.Core.Util.Trees.Experimental;

public enum BTreeLeafType
{
    External,
    Internal
}

public class BPlusTree<TKey, TValue> where TKey : IComparable<TKey> where TValue : IComparable<TValue>
{
    private readonly int maxNodeCapacity;

    private readonly int maxNodeCapacityHalf;

    public ObjectIdValue PageOffset; // page offset to root node    

    public BPlusTreeEntry<TKey, TValue> root;

    private readonly IBPlusTreeNodeReader<TKey, TValue>? reader; // lazy node reader

    private readonly AsyncReaderWriterLock readerWriterLock = new();

    /// <summary>
    /// Initializes an empty B-tree.
    /// </summary>
    /// <param name="rootOffset"></param>
    /// <param name="maxNodeCapacity"></param>
    /// <param name="reader"></param>
    public BPlusTree(ObjectIdValue rootOffset, IBPlusTreeNodeReader<TKey, TValue>? reader = null)
    {
        this.reader = reader;
        this.root = new(default!, reader, null);

        PageOffset = rootOffset;

        //Id = Interlocked.Increment(ref BTreeIncr.CurrentTreeId);
        //Console.WriteLine("Created BTree {0}", Id);

        maxNodeCapacity = BTreeUtils.GetNodeCapacity<TKey, TValue>();
        maxNodeCapacityHalf = maxNodeCapacity / 2;
    }

    public async Task<BPlusTreeMutationDeltas<TKey, TValue>> Put(
        HLCTimestamp txnid,
        BTreeCommitState commitState,
        TKey key,
        TValue value
    )
    {
        using (await WriterLockAsync().ConfigureAwait(false))
        {
            BPlusTreeMutationDeltas<TKey, TValue> deltas = new();

            BPlusTreeNode<TKey, TValue>? node = await root.Next.ConfigureAwait(false);

            if (node is null)
            {
                node = new BPlusTreeNode<TKey, TValue>();
                root.Next = new AsyncLazy<BPlusTreeNode<TKey, TValue>?>(() => Task.FromResult<BPlusTreeNode<TKey, TValue>?>(node));
                deltas.Nodes.Add(node);
            }

            await Insert(root, txnid, commitState, key, value, deltas);

            return deltas;
        }
    }

    private async Task Insert(
        BPlusTreeEntry<TKey, TValue> parent,
        HLCTimestamp txnid,
        BTreeCommitState commitState,
        TKey key,
        TValue value,
        BPlusTreeMutationDeltas<TKey, TValue> deltas
    )
    {
        BPlusTreeNode<TKey, TValue>? node = await parent.Next.ConfigureAwait(false);

        if (node is null)
            return;

        if (node.Type == BTreeLeafType.External)
        {
            BPlusTreeEntry<TKey, TValue> entry;

            for (int i = 0; i < node.Entries.Count; i++)
            {
                BPlusTreeEntry<TKey, TValue> currEntry = node.Entries[i];

                if (Eq(currEntry.Key, key))
                {
                    node.Entries[i].SetValue(txnid, commitState, value);

                    deltas.Nodes.Add(node);
                    deltas.Entries.Add(currEntry);
                    return;
                }

                if (Greater(currEntry.Key, key))
                {
                    entry = new BPlusTreeEntry<TKey, TValue>(key, reader, null);
                    entry.SetValue(txnid, commitState, value);
                    node.Entries.Insert(i, entry);
                    
                    deltas.Nodes.Add(node);
                    deltas.Entries.Add(entry);

                    if (node.Entries.Count == maxNodeCapacity)
                        await Split(parent, deltas);

                    return;
                }
            }

            entry = new BPlusTreeEntry<TKey, TValue>(key, reader, null);
            entry.SetValue(txnid, commitState, value);
            node.Entries.Add(entry);

            deltas.Nodes.Add(node);
            deltas.Entries.Add(entry);

            if (node.Entries.Count == maxNodeCapacity)
                await Split(parent, deltas);

            return;
        }

        if (node.Type == BTreeLeafType.Internal)
        {
            for (int i = 0; i < node.Entries.Count; i++)
            {
                if (Less(key, node.Entries[i].Key) || i == (node.Entries.Count - 1))
                {
                    //Console.WriteLine("{0} {1} {2}", node.Type, key, i);

                    await Insert(node.Entries[i], txnid, commitState, key, value, deltas);

                    if (node.Entries.Count == maxNodeCapacity)
                        await Split(parent, deltas);

                    return;
                }
            }

            throw new Exception("Should not happen");
        }
    }

    private async Task Split(BPlusTreeEntry<TKey, TValue> entry, BPlusTreeMutationDeltas<TKey, TValue> deltas)
    {
        BPlusTreeNode<TKey, TValue>? node = await entry.Next.ConfigureAwait(false);
        if (node is null)
            throw new Exception("internal");

        BPlusTreeNode<TKey, TValue> newRoot = new();

        BPlusTreeNode<TKey, TValue> left = new();
        BPlusTreeNode<TKey, TValue> right = new();

        newRoot.Type = BTreeLeafType.Internal;
        newRoot.Entries.Add(new BPlusTreeEntry<TKey, TValue>(node.Entries[maxNodeCapacityHalf].Key, reader, left));
        newRoot.Entries.Add(new BPlusTreeEntry<TKey, TValue>(node.Entries[maxNodeCapacityHalf + 1].Key, reader, right));

        for (int i = 0; i <= maxNodeCapacityHalf; i++)
            left.Entries.Add(node.Entries[i]);

        for (int i = maxNodeCapacityHalf + 1; i < node.Entries.Count; i++)
            right.Entries.Add(node.Entries[i]);

        deltas.Nodes.Add(newRoot);
        deltas.Nodes.Add(left);
        deltas.Nodes.Add(right);

        entry.Next = new AsyncLazy<BPlusTreeNode<TKey, TValue>?>(() => Task.FromResult<BPlusTreeNode<TKey, TValue>?>(newRoot));
    }

    public async Task Print(HLCTimestamp txnid)
    {
        if (root.Next is null)
            return;

        await Print(await root.Next.ConfigureAwait(false), txnid, 0);
    }

    private async Task Print(BPlusTreeNode<TKey, TValue>? root, HLCTimestamp txnid, int v)
    {
        if (root is null)
            return;

        string pad = new('=', v);

        if (root.Type == BTreeLeafType.External)
        {
            foreach (BPlusTreeEntry<TKey, TValue> entry in root.Entries)
            {
                Console.WriteLine("{0}> {1} Key={2} Value={3}", pad, root.Type, entry.Key, entry.GetValue(TransactionType.Write, txnid));
            }
        }
        else
        {
            foreach (BPlusTreeEntry<TKey, TValue> entry in root.Entries)
            {
                Console.WriteLine("{0}> {1} Key={2}", pad, root.Type, entry.Key);
                await Print(await entry.Next.ConfigureAwait(false), txnid, v + 1);
            }
        }
    }

    /// <summary>
    /// Returns the value associated with the given key.
    /// </summary>
    /// /// <param name="txType"></param>
    /// <param name="txnid"></param>
    /// <param name="key"></param>
    /// <returns>the value associated with the given key if the key is in the symbol table
    /// and {@code null} if the key is not in the symbol table</returns>
    public async Task<TValue?> Get(TransactionType txType, HLCTimestamp txnid, TKey key)
    {
        using (await ReaderLockAsync().ConfigureAwait(false))
        {
            if (root is null)
            {
                //Console.WriteLine("root is null");
                return default;
            }

            return await GetInternal(root, txType, txnid, key).ConfigureAwait(false);
        }
    }

    private async Task<TValue?> GetInternal(BPlusTreeEntry<TKey, TValue> parent, TransactionType txType, HLCTimestamp txnid, TKey key)
    {
        BPlusTreeNode<TKey, TValue>? node = await parent.Next.ConfigureAwait(false);

        if (node is null)
            return default;

        if (node.Type == BTreeLeafType.External)
        {
            Console.WriteLine("{0} {1}", txnid, key);

            BPlusTreeEntry<TKey, TValue>? entry = BinarySearch(node.Entries, node.Entries.Count, key);

            if (entry is not null)
            {
                if (entry.CanBeSeenBy(txnid))
                    return entry.GetValue(txType, txnid);

                Console.WriteLine("cannot be seen");
            }
            else
                Console.WriteLine("doesnt exist");

            return default;
        }

        if (node.Type == BTreeLeafType.Internal)
        {
            for (int i = 0; i < node.Entries.Count; i++)
            {
                BPlusTreeEntry<TKey, TValue> entry = node.Entries[i];

                if (Less(key, entry.Key) || i == (node.Entries.Count - 1))
                    return await GetInternal(entry, txType, txnid, key).ConfigureAwait(false);
            }

            return default;
        }

        throw new Exception("internal");
    }

    /// <summary>
    /// Returns all keys in the symbol table as an <tt>Iterable</tt>.
    /// </summary>
    /// <param name="txnId"></param>
    /// <returns></returns>
    public async IAsyncEnumerable<BPlusTreeNode<TKey, TValue>> NodesTraverse(HLCTimestamp txnId)
    {
        using (await ReaderLockAsync().ConfigureAwait(false))
        {
            await foreach (BPlusTreeNode<TKey, TValue> node in NodesTraverseInternal(root, txnId))
                yield return node;
        }
    }

    private async IAsyncEnumerable<BPlusTreeNode<TKey, TValue>> NodesTraverseInternal(BPlusTreeEntry<TKey, TValue> parent, HLCTimestamp txnId)
    {
        BPlusTreeNode<TKey, TValue>? node = await parent.Next.ConfigureAwait(false);

        if (node is null)
            yield break;

        if (node.Type == BTreeLeafType.External)
            yield return node;
        else
        {
            for (int i = 0; i < node.Entries.Count; i++)
            {
                BPlusTreeEntry<TKey, TValue> entry = node.Entries[i];

                await foreach (BPlusTreeNode<TKey, TValue> childNode in NodesTraverseInternal(entry, txnId))
                    yield return childNode;
            }
        }
    }

    /// <summary>
    /// Allows to traverse all entries in the tree
    /// </summary>
    /// <returns></returns>
    public async IAsyncEnumerable<BPlusTreeEntry<TKey, TValue>> EntriesTraverse(HLCTimestamp txnId)
    {
        using (await ReaderLockAsync().ConfigureAwait(false))
        {
            await foreach (BPlusTreeEntry<TKey, TValue> node in EntriesTraverseInternal(root, txnId))
                yield return node;
        }
    }

    private async IAsyncEnumerable<BPlusTreeEntry<TKey, TValue>> EntriesTraverseInternal(BPlusTreeEntry<TKey, TValue> parent, HLCTimestamp txnId)
    {
        BPlusTreeNode<TKey, TValue>? node = await parent.Next.ConfigureAwait(false);

        if (node is null)
            yield break;

        //using IDisposable readerLock = await node.ReaderLockAsync();

        //node.NumberAccesses++;
        //node.NumberReads++;
        //node.LastAccess = txnId;

        if (node is null)
            yield break;

        if (node.Type == BTreeLeafType.External)
        {
            for (int j = 0; j < node.Entries.Count; j++)
                yield return node.Entries[j];
        }

        // internal node
        else
        {
            for (int j = 0; j < node.Entries.Count; j++)
            {
                BPlusTreeEntry<TKey, TValue> entry = node.Entries[j];

                await foreach (BPlusTreeEntry<TKey, TValue> childEntry in EntriesTraverseInternal(entry, txnId))
                    yield return childEntry;
            }
        }
    }

    static BPlusTreeEntry<TKey, TValue>? BinarySearch(List<BPlusTreeEntry<TKey, TValue>> arr, int length, TKey x)
    {
        int l = 0, r = length - 1;

        while (l <= r)
        {
            int m = l + (r - l) / 2;

            //Console.WriteLine("Z {0} {1}", arr[m].Key, x);

            // Check if x is present at mid
            if (Eq(arr[m].Key, x))
                return arr[m];

            // If x is greater, ignore left half
            if (Less(arr[m].Key, x))
                l = m + 1;

            // If x is smaller, ignore right half
            else
                r = m - 1;
        }

        // If we reach here, then element was not present
        return default;
    }

    // comparison functions - make Comparable instead of Key to avoid casts
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Greater(TKey k1, TKey k2)
    {
        return k1!.CompareTo(k2) > 0;
    }

    // comparison functions - make Comparable instead of Key to avoid casts
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Less(TKey k1, TKey k2)
    {
        return k1!.CompareTo(k2) < 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Eq(TKey k1, TKey k2)
    {
        return k1.CompareTo(k2) == 0;
    }

    /// <summary>
    /// Acquires a read lock. Multiple read locks can be acquired as long as the write lock is not.
    /// Read locks are shared.
    /// </summary>
    /// <returns></returns>
    public async Task<IDisposable> ReaderLockAsync()
    {
        return await readerWriterLock.ReaderLockAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Acquires a write lock. Only one write lock can be acquired while other locks are not.
    /// Write locks are exclusive.
    /// </summary>
    /// <returns></returns>
    public async Task<IDisposable> WriterLockAsync()
    {
        return await readerWriterLock.WriterLockAsync().ConfigureAwait(false);
    }
}
