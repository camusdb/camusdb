
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using System.Runtime.CompilerServices;

namespace CamusDB.Core.Util.Trees;

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
public sealed class BTree<TKey, TValue> where TKey : IComparable<TKey>
{
    private static int CurrentId = -1;

    public BTreeNode<TKey, TValue>? root;  // root of the B-tree

    public int Id;       // unique tree id

    public int height;   // height of the B-tree

    public int size;     // number of key-value pairs in the B-tree

    public int loaded;   // number of loaded nodes

    public ObjectIdValue PageOffset; // page offset to root node

    public readonly IBTreeNodeReader<TKey, TValue>? Reader; // lazy node reader

    /// <summary>
    /// Initializes an empty B-tree.
    /// </summary>
    /// <param name="rootOffset"></param>
    /// <param name="reader"></param>
    public BTree(ObjectIdValue rootOffset, IBTreeNodeReader<TKey, TValue>? reader = null)
    {
        Reader = reader;
        PageOffset = rootOffset;
        Id = Interlocked.Increment(ref CurrentId);
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
    /// <param name="txnid"></param>
    /// <param name="key"></param>
    /// <returns>the value associated with the given key if the key is in the symbol table
    /// and {@code null} if the key is not in the symbol table</returns>
    public async Task<TValue?> Get(HLCTimestamp txnid, TKey key)
    {
        if (root is null)
        {
            Console.WriteLine("root is null");
            return default;
        }

        return await Search(root, txnid, key, height);
    }

    private async ValueTask<TValue?> Search(BTreeNode<TKey, TValue>? node, HLCTimestamp txnid, TKey key, int ht)
    {
        if (node is null)
            return default;

        using IDisposable disposable = await node.ReaderLockAsync();

        BTreeEntry<TKey, TValue>[] children = node.children;

        //Console.WriteLine("F-1={0}", ht);

        // external node
        if (ht == 0)
        {
            //Console.WriteLine("F0={0}", node.KeyCount);

            for (int j = 0; j < node.KeyCount; j++)
            {
                //Console.WriteLine("F1={0}", txnid);

                // verify if key can be seen by MVCC
                if (!children[j].CanBeSeenBy(txnid))
                    continue;

                //Console.WriteLine("F2={0} {1}", txnid, key);

                if (Eq(key, children[j].Key))
                    return children[j].GetValue(txnid);
            }
        }

        // internal node
        else
        {
            for (int j = 0; j < node.KeyCount; j++)
            {
                if (j + 1 == node.KeyCount || Less(key, children[j + 1].Key))
                {
                    BTreeEntry<TKey, TValue> entry = children[j];

                    if (entry.Next is null && !entry.NextPageOffset.IsNull())
                    {
                        if (Reader is null)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Cannot read lazy node because reader is null");

                        entry.Next = await Reader.GetNode(entry.NextPageOffset);
                        loaded++;
                    }

                    return await Search(entry.Next, txnid, key, ht - 1);
                }
            }
        }

        return default;
    }

    /// <summary>
    /// Allows to traverse all entries in the tree
    /// </summary>
    /// <returns></returns>
    public async IAsyncEnumerable<BTreeEntry<TKey, TValue>> EntriesTraverse()
    {
        await foreach (BTreeEntry<TKey, TValue> entry in EntriesTraverseInternal(root, height))
            yield return entry;
    }

    private async IAsyncEnumerable<BTreeEntry<TKey, TValue>> EntriesTraverseInternal(BTreeNode<TKey, TValue>? node, int ht)
    {
        if (node is null)
            yield break;

        using IDisposable disposable = await node.ReaderLockAsync();

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

                if (entry.Next is null && !entry.NextPageOffset.IsNull())
                {
                    if (Reader is null)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Cannot read lazy node because reader is null");

                    entry.Next = await Reader.GetNode(entry.NextPageOffset);
                    loaded++;
                }

                await foreach (BTreeEntry<TKey, TValue> childEntry in EntriesTraverseInternal(entry.Next, ht - 1))
                    yield return childEntry;
            }
        }
    }

    /// <summary>
    /// Returns all keys in the symbol table as an <tt>Iterable</tt>.
    /// </summary>
    /// <returns></returns>
    public async IAsyncEnumerable<BTreeNode<TKey, TValue>> NodesTraverse()
    {
        await foreach (BTreeNode<TKey, TValue> node in NodesTraverseInternal(root, height))
            yield return node;
    }

    private async IAsyncEnumerable<BTreeNode<TKey, TValue>> NodesTraverseInternal(BTreeNode<TKey, TValue>? node, int ht)
    {
        if (node is null)
            yield break;

        yield return node;

        if (ht == 0)
            yield break;

        using IDisposable disposable = await node.ReaderLockAsync();

        for (int j = 0; j < node.KeyCount; j++)
        {
            BTreeEntry<TKey, TValue> entry = node.children[j];

            if (entry.Next is null && !entry.NextPageOffset.IsNull())
            {
                if (Reader is null)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Cannot read lazy node because reader is null");

                entry.Next = await Reader.GetNode(entry.NextPageOffset);
                loaded++;
            }

            await foreach (BTreeNode<TKey, TValue> childNode in NodesTraverseInternal(entry.Next, ht - 1))
                yield return childNode;
        }
    }

    /// <summary>
    /// Returns the entries in the tree in descending order.
    /// </summary>
    /// <returns></returns>
    public async IAsyncEnumerable<BTreeNode<TKey, TValue>> NodesReverseTraverse()
    {
        await foreach (BTreeNode<TKey, TValue> node in NodesReverseTraverseInternal(root, height))
            yield return node;
    }

    private async IAsyncEnumerable<BTreeNode<TKey, TValue>> NodesReverseTraverseInternal(BTreeNode<TKey, TValue>? node, int ht)
    {
        if (node is null)
            yield break;

        using IDisposable disposable = await node.ReaderLockAsync();

        for (int j = node.KeyCount; j >= 0; j--)
        {
            BTreeEntry<TKey, TValue> entry = node.children[j];

            if (entry.Next is null && !entry.NextPageOffset.IsNull())
            {
                if (Reader is null)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Cannot read lazy node because reader is null");

                entry.Next = await Reader.GetNode(entry.NextPageOffset);
                loaded++;
            }

            await foreach (BTreeNode<TKey, TValue> childNode in NodesReverseTraverseInternal(entry.Next, ht - 1))
                yield return childNode;
        }

        yield return node;
    }

    /// <summary>
    /// Inserts new nodes in the tree. In case of dublicate key the value is overriden    
    /// </summary>
    /// <param name="txnid"></param>
    /// <param name="key"></param>
    /// <param name="commitState"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public async Task<BTreeMutationDeltas<TKey, TValue>> Put(HLCTimestamp txnid, BTreeCommitState commitState, TKey key, TValue? value)
    {
        BTreeMutationDeltas<TKey, TValue> deltas = new();

        if (root is null) // create root
        {
            root = new BTreeNode<TKey, TValue>(0);
            deltas.Nodes.Add(root);
            loaded++;
        }

        BTreeNode<TKey, TValue>? split = await Insert(root, txnid, key, commitState, value, height, deltas);
        size++;

        if (split is null)
            return deltas;

        //Console.WriteLine("need to split root");

        using IDisposable disposable = await root.WriterLockAsync();

        // need to split root
        BTreeNode<TKey, TValue> newRoot = new(2);
        deltas.Nodes.Add(newRoot);
        loaded++;

        newRoot.children[0] = new BTreeEntry<TKey, TValue>(root.children[0].Key, root);
        newRoot.children[1] = new BTreeEntry<TKey, TValue>(split.children[0].Key, split);

        deltas.Nodes.Add(root);

        root = newRoot;

        newRoot.PageOffset = root.PageOffset;
        root.PageOffset = new();

        height++;

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

        using IDisposable disposable = await node.WriterLockAsync();

        int j;
        BTreeEntry<TKey, TValue>? newEntry = null;
        BTreeEntry<TKey, TValue>[] children = node.children;

        // external node
        if (ht == 0)
        {
            for (j = 0; j < node.KeyCount; j++)
            {
                if (Eq(key, children[j].Key))
                {
                    deltas.Nodes.Add(node);
                    deltas.Entries.Add(children[j].SetValue(txnid, commitState, value));                    
                    return null;
                }

                if (Less(key, children[j].Key))
                    break;
            }

            newEntry = new(key, null);
            deltas.Entries.Add(newEntry.SetValue(txnid, commitState, value));            
        }

        // internal node
        else
        {            
            for (j = 0; j < node.KeyCount; j++)
            {
                if ((j + 1 == node.KeyCount) || Less(key, children[j + 1].Key))
                {
                    BTreeEntry<TKey, TValue> entry = children[j++];

                    if (entry.Next is null && !entry.NextPageOffset.IsNull())
                    {
                        if (Reader is null)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Cannot read lazy node because reader is null");

                        entry.Next = await Reader.GetNode(entry.NextPageOffset);
                        loaded++;
                    }

                    BTreeNode<TKey, TValue>? split = await Insert(entry.Next, txnid, key, commitState, value, ht - 1, deltas);

                    if (split == null)
                        return null;

                    newEntry = new(key, null);                    
                    newEntry.Key = split.children[0].Key;
                    newEntry.Next = split;

                    deltas.Entries.Add(newEntry.SetValue(txnid, commitState, value));
                    break;
                }
            }
        }

        if (newEntry is null)
            throw new Exception("?");

        for (int i = node.KeyCount; i > j; i--)
            node.children[i] = node.children[i - 1];

        node.children[j] = newEntry;
        node.KeyCount++;

        deltas.Nodes.Add(node);

        if (node.KeyCount < BTreeConfig.MaxChildren)
            return null;

        return Split(node, deltas);
    }

    // split node in half
    private BTreeNode<TKey, TValue> Split(BTreeNode<TKey, TValue> current, BTreeMutationDeltas<TKey, TValue> deltas)
    {
        BTreeNode<TKey, TValue> newNode = new(BTreeConfig.MaxChildrenHalf);
        deltas.Nodes.Add(newNode);
        loaded++;

        current.KeyCount = BTreeConfig.MaxChildrenHalf;
        deltas.Nodes.Add(current);

        for (int j = 0; j < BTreeConfig.MaxChildrenHalf; j++)
            newNode.children[j] = current.children[BTreeConfig.MaxChildrenHalf + j];

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

        bool found = await Delete(root, key, height, deltas);

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

        using IDisposable disposable = await node.WriterLockAsync();

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
                {
                    if (children[j].Next is null && !children[j].NextPageOffset.IsNull())
                    {
                        if (Reader is null)
                            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Cannot read lazy node because reader is null");

                        children[j].Next = await Reader.GetNode(children[j].NextPageOffset);
                        loaded++;
                    }

                    return await Delete(children[j].Next, key, ht - 1, deltas);
                }
            }
        }

        return false;
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
}
