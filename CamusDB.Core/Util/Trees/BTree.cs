
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Util.Trees;

public class BTreeIncr
{
    public static int CurrentTreeId = -1;

    public static int CurrentNodeId = -1;
}

/**
 * B+Tree 
 *  
 * A B+ tree is an m-ary tree with a variable but often large number of children per node. 
 * A B+ tree consists of a root, internal nodes and leaves. The root may be either a 
 * leaf or a node with two or more children.
 * 
 * The implementation use C# generics to support any type of keys and values in an optimal way.
 * The lazy feature allows to load/unload used/unused nodes to/from disk to save memory.
 * 
 * Additionally, the tree implements MVCC (Multi-Version Concurrency Control). 
 * The entries version the data by timestamp and return a consistent state for 
 * the same transaction id (timestamp).
 */
public class BTree<TKey, TValue> where TKey : IComparable<TKey> where TValue : IComparable<TValue>
{    
    public readonly IBTreeNodeReader<TKey, TValue>? reader; // lazy node reader

    public BTreeNode<TKey, TValue>? root;  // root of the B-tree

    public int maxNodeCapacity;

    private readonly int maxNodeCapacityHalf;

    public int Id;       // unique tree id

    public int height;   // height of the B-tree

    public int size;     // number of key-value pairs in the B-tree

    public int loaded;   // number of loaded nodes

    public ObjectIdValue PageOffset; // page offset to root node

    private readonly AsyncReaderWriterLock readerWriterLock = new();

    /// <summary>
    /// Initializes an empty B+tree.
    /// </summary>
    /// <param name="rootOffset"></param>
    /// <param name="maxNodeCapacity"></param>
    /// <param name="reader"></param>
    public BTree(ObjectIdValue rootOffset, int maxNodeCapacity, IBTreeNodeReader<TKey, TValue>? reader = null)
    {
        if (maxNodeCapacity == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Tree capacity cannot be zero");

        this.reader = reader;
        PageOffset = rootOffset;
        Id = Interlocked.Increment(ref BTreeIncr.CurrentTreeId);

        // Console.WriteLine("Created BTree {0} MaxCapacity={1}", Id, maxNodeCapacity);

        this.maxNodeCapacity = maxNodeCapacity;
        this.maxNodeCapacityHalf = maxNodeCapacity / 2;
    }

    /// <summary>
    /// Returns true if this symbol table is empty.
    /// </summary>
    /// <returns></returns>
    public bool IsEmpty()
    {
        return Size() == 0;
    }

    /// <summary>
    /// Returns the number of key-value pairs in this symbol table.
    /// </summary>
    /// <returns></returns>
    public int Size()
    {
        return size;
    }

    /// <summary>
    /// Returns the height of this B-tree (for debugging).
    /// </summary>
    /// <returns></returns>
    public int Height()
    {
        return height;
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
        if (root is null)
        {
            //Console.WriteLine("root is null");
            return default;
        }

        return await GetInternal(root, txType, txnid, key, height).ConfigureAwait(false);
    }

    private async Task<TValue?> GetInternal(BTreeNode<TKey, TValue>? node, TransactionType txType, HLCTimestamp txnid, TKey key, int ht)
    {
        if (node is null)
            return default;

        BTreeEntry<TKey, TValue>[] children = node.children;

        // external node
        if (ht == 0)
        {
            /*for (int j = 0; j < node.KeyCount; j++)
            {
                BTreeEntry<TKey, TValue> entry = children[j];

                //Console.WriteLine("Z {0} {1}", key, entry.Key);

                // verify if key can be seen by MVCC
                if (!entry.CanBeSeenBy(txnid))
                    continue;

                //Console.WriteLine("X {0} {1}", key, entry.Key);

                if (Eq(key, entry.Key))
                {
                    Console.WriteLine(j);
                    return entry.GetValue(txType, txnid);
                }
            }*/

            BTreeEntry<TKey, TValue>? entry = BinarySearch(children, node.KeyCount, key);

            if (entry is not null && entry.CanBeSeenBy(txnid))
                return entry.GetValue(txType, txnid);
        }

        // internal node
        else
        {
            for (int j = 0; j < node.KeyCount; j++)
            {
                if (j + 1 == node.KeyCount || Less(key, children[j + 1].Key))
                {
                    BTreeEntry<TKey, TValue> entry = children[j];

                    return await GetInternal(await entry.Next.ConfigureAwait(false), txType, txnid, key, ht - 1).ConfigureAwait(false);
                }
            }
        }

        return default;
    }

    /// <summary>
    /// Allows to traverse all entries in the tree
    /// </summary>
    /// <returns></returns>
    public async IAsyncEnumerable<BTreeEntry<TKey, TValue>> EntriesTraverse(HLCTimestamp txnId)
    {
        await foreach (BTreeEntry<TKey, TValue> entry in EntriesTraverseInternal(root, txnId, height))
            yield return entry;
    }

    private async IAsyncEnumerable<BTreeEntry<TKey, TValue>> EntriesTraverseInternal(BTreeNode<TKey, TValue>? node, HLCTimestamp txnId, int ht)
    {
        if (node is null)
            yield break;

        //using IDisposable readerLock = await node.ReaderLockAsync();

        node.NumberAccesses++;
        node.NumberReads++;
        node.LastAccess = txnId;

        BTreeEntry<TKey, TValue>[] children = node.children;

        // external node
        if (ht == 0)
        {
            for (int j = 0; j < node.KeyCount; j++)
                yield return children[j];
        }

        // internal node
        else
        {
            for (int j = 0; j < node.KeyCount; j++)
            {
                BTreeEntry<TKey, TValue> entry = children[j];

                await foreach (BTreeEntry<TKey, TValue> childEntry in EntriesTraverseInternal(await entry.Next.ConfigureAwait(false), txnId, ht - 1))
                    yield return childEntry;
            }
        }
    }

    /// <summary>
    /// Returns all keys in the symbol table as an <tt>Iterable</tt>.
    /// </summary>
    /// <param name="txnId"></param>
    /// <returns></returns>
    public async IAsyncEnumerable<BTreeNode<TKey, TValue>> NodesTraverse(HLCTimestamp txnId)
    {
        await foreach (BTreeNode<TKey, TValue> node in NodesTraverseInternal(root, txnId, height))
            yield return node;
    }

    private async IAsyncEnumerable<BTreeNode<TKey, TValue>> NodesTraverseInternal(BTreeNode<TKey, TValue>? node, HLCTimestamp txnId, int ht)
    {
        if (node is null)
            yield break;

        yield return node;

        if (ht == 0)
            yield break;

        //using IDisposable readerLock = await node.ReaderLockAsync();

        node.NumberAccesses++;
        node.NumberReads++;
        node.LastAccess = txnId;

        for (int j = 0; j < node.KeyCount; j++)
        {
            BTreeEntry<TKey, TValue> entry = node.children[j];

            await foreach (BTreeNode<TKey, TValue> childNode in NodesTraverseInternal(await entry.Next.ConfigureAwait(false), txnId, ht - 1))
                yield return childNode;
        }
    }

    /// <summary>
    /// Returns the entries in the tree in descending order.
    /// </summary>
    /// <returns></returns>
    public async IAsyncEnumerable<BTreeNode<TKey, TValue>> NodesReverseTraverse(HLCTimestamp txnId)
    {        
        await foreach (BTreeNode<TKey, TValue> node in NodesReverseTraverseInternal(root, txnId, height))
           yield return node;     
    }

    private async IAsyncEnumerable<BTreeNode<TKey, TValue>> NodesReverseTraverseInternal(BTreeNode<TKey, TValue>? node, HLCTimestamp txnId, int ht)
    {
        if (node is null)
            yield break;

        //using IDisposable readerLock = await node.ReaderLockAsync();

        node.NumberAccesses++;
        node.NumberReads++;
        node.LastAccess = txnId;

        for (int j = node.KeyCount; j >= 0; j--)
        {
            BTreeEntry<TKey, TValue> entry = node.children[j];

            await foreach (BTreeNode<TKey, TValue> childNode in NodesReverseTraverseInternal(await entry.Next.ConfigureAwait(false), txnId, ht - 1))
                yield return childNode;
        }

        yield return node;
    }

    /// <summary>
    /// Inserts new nodes in the tree. In case of duplicate key the value is overriden    
    /// </summary>
    /// <param name="txnid"></param>
    /// <param name="key"></param>
    /// <param name="commitState"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public async Task<BTreeMutationDeltas<TKey, TValue>> Put(
        HLCTimestamp txnid,
        BTreeCommitState commitState,
        TKey key,
        TValue? value,
        Func<HashSet<BTreeNode<TKey, TValue>>, Task>? persistNodeCallback = null
    )
    {
        BTreeMutationDeltas<TKey, TValue> deltas = new();

        if (root is null) // create root
        {
            root = new BTreeNode<TKey, TValue>(0, maxNodeCapacity)
            {
                CreatedAt = txnid
            };

            deltas.Nodes.Add(root);
            loaded++;
        }

        BTreeNode<TKey, TValue>? split = await Insert(root, txnid, key, commitState, value, height, deltas).ConfigureAwait(false);

        if (split is null)
        {
            if (deltas.Nodes.Count > 0 && persistNodeCallback is not null)
                await persistNodeCallback(deltas.Nodes).ConfigureAwait(false);

            return deltas;
        }

        //Console.WriteLine("Split root node {0} {1}", root.Id, split.Id);

        // need to split root
        BTreeNode<TKey, TValue> newRoot = new(2, maxNodeCapacity)
        {
            CreatedAt = txnid
        };

        deltas.Nodes.Add(root);
        deltas.Nodes.Add(newRoot);
        deltas.Nodes.Add(split);

        loaded++;

        newRoot.children[0] = new BTreeEntry<TKey, TValue>(root.children[0].Key, reader, root, maxNodeCapacity);
        newRoot.children[1] = new BTreeEntry<TKey, TValue>(split.children[0].Key, reader, split, maxNodeCapacity);

        root = newRoot;

        newRoot.PageOffset = root.PageOffset;
        root.PageOffset = new();

        height++;

        if (deltas.Nodes.Count > 0 && persistNodeCallback is not null)
            await persistNodeCallback(deltas.Nodes).ConfigureAwait(false);

        return deltas;
    }

    private async Task<BTreeNode<TKey, TValue>?> Insert(
        BTreeNode<TKey, TValue>? node,
        HLCTimestamp txnid,
        TKey key,
        BTreeCommitState commitState,
        TValue? value,
        int ht,
        BTreeMutationDeltas<TKey, TValue> deltas
    )
    {
        if (node is null)
            throw new ArgumentException("node cannot be null");

        //using IDisposable disposable = await node.WriterLockAsync();

        node.NumberAccesses++;
        node.LastAccess = txnid;

        int j;
        BTreeEntry<TKey, TValue>? newEntry = null;
        BTreeEntry<TKey, TValue>[] children = node.children;

        // external node
        if (ht == 0)
        {
            for (j = 0; j < node.KeyCount; j++)
            {
                BTreeEntry<TKey, TValue> childrenEntry = children[j];

                if (Eq(key, childrenEntry.Key))
                {
                    // Console.WriteLine("SetV={0} {1} {2} {3}", key, txnid, commitState, value);                    

                    node.NumberWrites++;

                    deltas.Nodes.Add(node);
                    deltas.MvccEntries.Add(childrenEntry.SetValue(txnid, commitState, value));
                    return null;
                }

                if (Less(key, childrenEntry.Key))
                    break;
            }

            // Console.WriteLine("Not found in external node SetV={0} {1} {2} {3}", key, txnid, commitState, value);

            size++;
            newEntry = new(key, reader, null, maxNodeCapacity);
            deltas.MvccEntries.Add(newEntry.SetValue(txnid, commitState, value));
        }

        // internal node
        else
        {
            for (j = 0; j < node.KeyCount; j++)
            {
                if ((j + 1 == node.KeyCount) || Less(key, children[j + 1].Key))
                {
                    BTreeEntry<TKey, TValue> entry = children[j++];

                    BTreeNode<TKey, TValue>? next = await entry.Next.ConfigureAwait(false);

                    BTreeNode<TKey, TValue>? split = await Insert(next, txnid, key, commitState, value, ht - 1, deltas).ConfigureAwait(false);

                    if (split == null)
                        return null;

                    newEntry = new(split.children[0].Key, reader, split, maxNodeCapacity);
                    //deltas.MvccEntries.Add(newEntry.SetValue(txnid, commitState, value));
                    break;
                }
            }
        }

        if (newEntry is null)
            throw new Exception(j + " " + node.KeyCount);

        for (int i = node.KeyCount; i > j; i--)
            node.children[i] = node.children[i - 1];

        node.children[j] = newEntry;
        node.KeyCount++;
        node.NumberWrites++;

        deltas.Nodes.Add(node);

        if (node.KeyCount < maxNodeCapacity)
            return null;

        return Split(node, txnid, deltas);
    }

    // split node in half
    private BTreeNode<TKey, TValue> Split(BTreeNode<TKey, TValue> current, HLCTimestamp txnid, BTreeMutationDeltas<TKey, TValue> deltas)
    {
        // Console.WriteLine("Split node {0} {1} [2]", current.Id, current.KeyCount);

        BTreeNode<TKey, TValue> newNode = new(maxNodeCapacityHalf, maxNodeCapacity)
        {
            CreatedAt = txnid
        };

        deltas.Nodes.Add(newNode);
        loaded++;

        current.KeyCount = maxNodeCapacityHalf;
        deltas.Nodes.Add(current);

        for (int j = 0; j < maxNodeCapacityHalf; j++)
            newNode.children[j] = current.children[maxNodeCapacityHalf + j];

        return newNode;
    }

    /// <summary>
    /// Returns the entry associated with the given key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public async Task<(bool found, BTreeMutationDeltas<TKey, TValue> deltas)> Remove(TKey key)
    {        
        BTreeMutationDeltas<TKey, TValue> deltas = new();

        bool found = await Delete(root, key, height, deltas).ConfigureAwait(false);

        if (found)
            size--;

        return (found, deltas);        
    }

    /// <summary>
    /// Delete node entries by key, it doesn't merge yet
    /// </summary>
    /// <param name="node"></param>
    /// <param name="key"></param>
    /// <param name="ht"></param>
    /// <param name="deltas"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    private async Task<bool> Delete(BTreeNode<TKey, TValue>? node, TKey key, int ht, BTreeMutationDeltas<TKey, TValue> deltas)
    {
        if (node is null)
            return false;

        //using IDisposable disposable = await node.WriterLockAsync();

        BTreeEntry<TKey, TValue>[] children = node.children;

        // external node
        if (ht == 0)
        {
            int position = -1;

            for (int j = 0; j < node.KeyCount; j++)
            {
                if (Eq(key, children[j].Key))
                {
                    position = j;
                    break;
                }
            }

            if (position == -1)
                return false;

            for (int j = position; j < node.KeyCount; j++)
                node.children[j] = node.children[j + 1];

            node.KeyCount--;
            deltas.Nodes.Add(node);

            return true;
        }

        // internal node
        else
        {
            for (int j = 0; j < node.KeyCount; j++)
            {
                if (j + 1 == node.KeyCount || Less(key, children[j + 1].Key))
                    return await Delete(await children[j].Next.ConfigureAwait(false), key, ht - 1, deltas).ConfigureAwait(false);
            }
        }

        return false;
    }

    /// <summary>       
    /// Stage 1: Mark
    /// Upon creation of an MVCC version, its mark flag is initially at 0 (false). During the Mark stage,
    /// this flag is set for every accessible object (or those leafs that are visible by transactions) is switched to 1 (true).
    /// To execute this task, a tree traversal is necessary, and utilizing a depth-first search method is effective.
    /// In this scenario, each leaf is viewed as a vertex, and the process involves visiting all vertices (objects)
    /// that can be accessed from a given vertex (object). This continues until all accessible vertices have been explored.
    /// </summary>
    /// <param name="txnid"></param>
    /// <param name="deltas"></param>
    /// <returns></returns>
    public async Task<bool> Mark(HLCTimestamp txnid, BTreeMutationDeltas<TKey, TValue> deltas)
    {
        if (root is null)
            return false;

        return await MarkInternal(root, txnid, height, deltas).ConfigureAwait(false);
    }

    private async Task<bool> MarkInternal(BTreeNode<TKey, TValue>? node, HLCTimestamp txnid, int ht, BTreeMutationDeltas<TKey, TValue> deltas)
    {
        if (node is null)
            return false;

        bool hasExpiredEntries = false;
        BTreeEntry<TKey, TValue>[] children = node.children;

        // external node
        if (ht == 0)
        {
            for (int j = 0; j < node.KeyCount; j++)
            {
                BTreeEntry<TKey, TValue> entry = children[j];

                if (entry.HasExpiredEntries(txnid))
                {
                    deltas.Entries.Add(entry);
                    hasExpiredEntries = true;
                }
            }
        }

        // internal node
        else
        {
            for (int j = 0; j < node.KeyCount; j++)
            {
                BTreeEntry<TKey, TValue> entry = children[j];

                if (entry.Next.IsStarted) // ignore unloaded nodes
                {
                    if (await MarkInternal(await entry.Next.ConfigureAwait(false), txnid, ht - 1, deltas).ConfigureAwait(false))
                        hasExpiredEntries = true;
                }
            }
        }

        node.Mark = node.LastAccess.CompareTo(HLCTimestamp.Zero) != 0 && node.LastAccess.CompareTo(txnid) < 0 && !hasExpiredEntries;

        return hasExpiredEntries;
    }

    /// <summary>       
    /// Stage 2: Sweep
    /// True to its name, this stage involves "clearing out" the leafs that are not accessed recently,
    /// effectively freeing up heap memory from these leafs. It removes all leafs from the heap memory
    /// whose mark flag remains false, while for all accessible objects (those that can be reached),
    /// the mark flag is maintained as true.
    /// </summary>    
    /// <returns></returns>
    public async Task Sweep()
    {
        if (root is null)
            return;

        await SweepInternal(root, height).ConfigureAwait(false);
    }

    private async ValueTask SweepInternal(BTreeNode<TKey, TValue>? node, int ht)
    {
        if (node is null)
            return;

        if (!node.Mark)
            return;

        // external node
        if (ht == 0)
        {
            return;
        }

        // internal node
        else
        {
            BTreeEntry<TKey, TValue>[] children = node.children;

            for (int j = 0; j < node.KeyCount; j++)
            {
                BTreeEntry<TKey, TValue> entry = children[j];

                if (entry.Next.IsStarted && !entry.NextPageOffset.IsNull()) // ignore unloaded nodes
                {
                    await SweepInternal(await entry.Next, ht - 1).ConfigureAwait(false);

                    children[j] = new BTreeEntry<TKey, TValue>(entry.Key, reader, null, maxNodeCapacity)
                    {
                        NextPageOffset = entry.NextPageOffset
                    };

                    //Console.WriteLine("Freed {0} {1} {2}", node.Id, j, entry.NextPageOffset);
                }
            }
        }
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

    static BTreeEntry<TKey, TValue>? BinarySearch(BTreeEntry<TKey, TValue>[] arr, int length, TKey x)
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
