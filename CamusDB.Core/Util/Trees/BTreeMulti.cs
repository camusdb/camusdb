
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Util.Time;
using YamlDotNet.Core.Tokens;

namespace CamusDB.Core.Util.Trees;

/**
 *  B+Tree Multi is a tree of B+Trees used for multi keys
 *  
 *  Unique values across the index point to other trees where the rowids are unique values 
 *  pointing to page offsets
 */
public sealed class BTreeMulti<TKey> where TKey : IComparable<TKey>
{
    private static int CurrentId = -1;

    public BTreeMultiNode<TKey>? root;       // root of the B-tree

    public int Id; // unique id

    public int height;      // height of the B-tree

    public int size;           // number of key-value pairs in the B-tree

    public int denseSize;           // number of key-value pairs in the B-tree

    public ObjectIdValue PageOffset; // page offset to root node

    public readonly IBTreeNodeReader<ObjectIdValue, ObjectIdValue>? SubTreeReader; // lazy node sub-tree reader

    /// <summary>
    /// Initializes an empty B-tree.
    /// </summary>
    /// <param name="rootOffset"></param>
    /// <param name="subTreeReader"></param>
    public BTreeMulti(ObjectIdValue rootOffset, IBTreeNodeReader<ObjectIdValue, ObjectIdValue>? subTreeReader = null)
    {
        PageOffset = rootOffset;
        SubTreeReader = subTreeReader;
        Id = Interlocked.Increment(ref CurrentId);
    }

    /**
     * Returns true if this symbol table is empty.
     * @return {@code true} if this symbol table is empty; {@code false} otherwise
     */
    public bool IsEmpty()
    {
        return Size() == 0;
    }

    /**
     * Returns the number of key-value pairs in this symbol table without the subtrees     
     */
    public int Size()
    {
        return size;
    }

    /**
     * Returns the number of key-value pairs in this symbol table including the subtrees     
     */
    public int DenseSize()
    {
        return denseSize;
    }

    /**
     * Returns the height of this B-tree (for debugging).
     *
     * @return the height of this B-tree
     */
    public int Height()
    {
        return height;
    }

    /**
     * Returns the value associated with the given key.
     *
     * @param  key the key
     * @return the value associated with the given key if the key is in the symbol table
     *         and {@code null} if the key is not in the symbol table
     */
    public BTree<ObjectIdValue, ObjectIdValue>? Get(TKey key)
    {
        return Search(root, key, height);
    }

    /**
     * Returns the value associated with the given key.
     *
     * @param  key the key
     * @return the value associated with the given key if the key is in the symbol table
     *         and {@code null} if the key is not in the symbol table
     */
    public async IAsyncEnumerable<ObjectIdValue> GetAll(TKey key)
    {
        BTree<ObjectIdValue, ObjectIdValue>? subTree = Search(root, key, height);

        if (subTree is null)
            yield break;

        HLCTimestamp timestamp = HLCTimestamp.Zero;

        await foreach (BTreeEntry<ObjectIdValue, ObjectIdValue> subTreeEntry in subTree.EntriesTraverse(timestamp))
            yield return subTreeEntry.Key;
    }

    private BTree<ObjectIdValue, ObjectIdValue>? Search(BTreeMultiNode<TKey>? node, TKey key, int ht)
    {
        if (node is null)
            return null;

        BTreeMultiEntry<TKey>[] children = node.children;

        // external node
        if (ht == 0)
        {
            for (int j = 0; j < node.KeyCount; j++)
            {
                if (Eq(key, children[j].Key))
                    return children[j].Value;
            }
        }

        // internal node
        else
        {
            for (int j = 0; j < node.KeyCount; j++)
            {
                if (j + 1 == node.KeyCount || Less(key, children[j + 1].Key))
                    return Search(children[j].Next, key, ht - 1);
            }
        }

        return null;
    }

    public IEnumerable<BTreeMultiEntry<TKey>> EntriesTraverse()
    {
        foreach (BTreeMultiEntry<TKey> entry in EntriesTraverseInternal(root, height))
            yield return entry;
    }

    private static IEnumerable<BTreeMultiEntry<TKey>> EntriesTraverseInternal(BTreeMultiNode<TKey>? node, int ht)
    {
        if (node is null)
            yield break;

        BTreeMultiEntry<TKey>[] children = node.children;

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
                foreach (BTreeMultiEntry<TKey> entry in EntriesTraverseInternal(children[j].Next, ht - 1))
                    yield return entry;
            }
        }
    }

    public IEnumerable<BTreeMultiNode<TKey>> NodesTraverse()
    {
        foreach (BTreeMultiNode<TKey> node in NodesTraverseInternal(root, height))
            yield return node;
    }

    private static IEnumerable<BTreeMultiNode<TKey>> NodesTraverseInternal(BTreeMultiNode<TKey>? node, int ht)
    {
        //Console.WriteLine("ht={0}", ht);

        if (node is null)
            yield break;

        yield return node;

        if (ht == 0)
            yield break;

        for (int j = 0; j < node.KeyCount; j++)
        {
            foreach (BTreeMultiNode<TKey> childNode in NodesTraverseInternal(node.children[j].Next, ht - 1))
                yield return childNode;
        }
    }

    public IEnumerable<BTreeNode<int, int>> NodesReverseTraverse()
    {
        foreach (BTreeNode<int, int> node in NodesReverseTraverseInternal(root, height))
            yield return node;
    }

    private static IEnumerable<BTreeNode<int, int>> NodesReverseTraverseInternal(BTreeMultiNode<TKey>? node, int ht)
    {
        //Console.WriteLine("ht={0}", ht);

        if (node is null)
            yield break;

        for (int j = node.KeyCount; j >= 0; j--)
        {
            foreach (BTreeNode<int, int> childNode in NodesReverseTraverseInternal(node.children[j].Next, ht - 1))
                yield return childNode;
        }
    }

    public async Task<Dictionary<int, BTreeMultiDelta<TKey>>> Put(HLCTimestamp txnid, TKey key, ObjectIdValue value)
    {
        return await Put(txnid, key, new BTreeTuple(value, new()));
    }

    public async Task<Dictionary<int, BTreeMultiDelta<TKey>>> Put(HLCTimestamp txnid, TKey key, BTreeTuple value)
    {
        Dictionary<int, BTreeMultiDelta<TKey>> deltas = new();

        if (root is null)
        {
            root = new BTreeMultiNode<TKey>(0);
            deltas.Add(root.Id, new BTreeMultiDelta<TKey>(root, null));
        }

        BTreeMultiNode<TKey>? split = await Insert(txnid, root, key, value, height, deltas);
        denseSize++;

        if (split == null)
            return deltas;

        // need to split root
        BTreeMultiNode<TKey> newRoot = new(2);
        deltas.Add(newRoot.Id, new BTreeMultiDelta<TKey>(newRoot, null));

        newRoot.children[0] = new BTreeMultiEntry<TKey>(root.children[0].Key, root);
        newRoot.children[1] = new BTreeMultiEntry<TKey>(split.children[0].Key, split);

        if (!deltas.ContainsKey(root.Id))
            deltas.Add(root.Id, new BTreeMultiDelta<TKey>(root, null));

        root = newRoot;

        newRoot.PageOffset = root.PageOffset;
        root.PageOffset = new();

        height++;

        return deltas;
    }

    private async Task<BTreeMultiNode<TKey>?> Insert(HLCTimestamp txnid, BTreeMultiNode<TKey>? node, TKey key, BTreeTuple val, int ht, Dictionary<int, BTreeMultiDelta<TKey>> deltas)
    {
        if (node is null)
            throw new ArgumentException("node cannot be null " + ht + " " + (root is null ? "null" : "no null"));

        int j;
        BTreeMultiDelta<TKey>? multiDelta;
        BTreeMultiEntry<TKey>? newEntry = null;
        BTreeMultiEntry<TKey>[] children = node.children;        
        BTreeMutationDeltas<ObjectIdValue, ObjectIdValue> innerDeltas;

        // external node at height 0
        if (ht == 0)
        {
            for (j = 0; j < node.KeyCount; j++)
            {
                BTreeMultiEntry<TKey> child = children[j];

                if (!Eq(key, child.Key)) // same key found
                    continue;

                //if (val is null)
                //    throw new ArgumentException("val cannot be null");

                innerDeltas = await child.Value!.Put(txnid, BTreeCommitState.Committed, val.SlotOne, val.SlotTwo);

                if (deltas.TryGetValue(node.Id, out multiDelta))
                    multiDelta.InnerDeltas = innerDeltas;
                else
                    deltas.Add(node.Id, new BTreeMultiDelta<TKey>(node, innerDeltas));

                return null;
            }

            for (j = 0; j < node.KeyCount; j++)
            {
                if (Less(key, children[j].Key))
                    break;
            }
        }

        // internal node
        else
        {
            for (j = 0; j < node.KeyCount; j++)
            {
                if ((j + 1 == node.KeyCount) || Less(key, children[j + 1].Key))
                {
                    BTreeMultiNode<TKey>? split = await Insert(txnid, children[j++].Next, key, val, ht - 1, deltas);

                    if (split == null)
                        return null;

                    newEntry = new(split.children[0].Key, split);
                    newEntry.Value = new BTree<ObjectIdValue, ObjectIdValue>(new(), SubTreeReader);
                    //size++;
                    break;
                }
            }
        }

        for (int i = node.KeyCount; i > j; i--)
            node.children[i] = node.children[i - 1];

        //if (val is null)
        //throw new ArgumentException("val cannot be null");

        if (newEntry is null)
        {
            newEntry = new(key, null)
            {
                Value = new BTree<ObjectIdValue, ObjectIdValue>(new(), SubTreeReader)
            };
            size++;
        }

        innerDeltas = await newEntry.Value!.Put(txnid, BTreeCommitState.Committed, val.SlotOne, val.SlotTwo);

        if (innerDeltas is null)
            throw new Exception("inner deltas cannot be null");

        node.children[j] = newEntry;
        node.KeyCount++;

        if (!deltas.ContainsKey(node.Id))
            deltas.Add(node.Id, new BTreeMultiDelta<TKey>(node, innerDeltas));
        else
            deltas[node.Id].InnerDeltas = innerDeltas;

        //Console.WriteLine("Node {0} marked as dirty as child added", node.Id);

        if (node.KeyCount < BTreeConfig.MaxChildren)
            return null;

        return Split(node, deltas);
    }

    // split node in half
    private static BTreeMultiNode<TKey> Split(BTreeMultiNode<TKey> current, Dictionary<int, BTreeMultiDelta<TKey>> deltas)
    {
        //Console.WriteLine("split node");

        BTreeMultiNode<TKey> split = new(BTreeConfig.MaxChildrenHalf);
        deltas.Add(split.Id, new BTreeMultiDelta<TKey>(split, null));

        //Console.WriteLine("Node {0} marked as dirty because of split", t.Id);

        current.KeyCount = BTreeConfig.MaxChildrenHalf;

        if (!deltas.ContainsKey(current.Id))
            deltas.Add(current.Id, new BTreeMultiDelta<TKey>(current, null));

        //Console.WriteLine("Node {0} marked as dirty because of split", current.Id);

        for (int j = 0; j < BTreeConfig.MaxChildrenHalf; j++)
        {
            //Console.WriteLine("Assigning child {0} from {1} to {2} at {3}", BTreeConfig.MaxChildrenHalf + j, current.Id, split.Id, j);

            split.children[j] = current.children[BTreeConfig.MaxChildrenHalf + j];
        }

        //Console.WriteLine("end split node {0} {1}", current.KeyCount, split.KeyCount);

        return split;
    }

    /**
     * Returns the entry associated with the given key.
     *
     * @param  key the key
     */
    public (bool found, HashSet<BTreeMultiNode<TKey>> deltas) Remove(TKey key)
    {
        HashSet<BTreeMultiNode<TKey>> deltas = new();

        bool found = Delete(root, key, height);

        if (found)
            size--;

        return (found, deltas);
    }

    private bool Delete(BTreeMultiNode<TKey>? node, TKey key, int ht)
    {
        if (node is null)
            return false;

        BTreeMultiEntry<TKey>[] children = node.children;

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

            BTree<ObjectIdValue, ObjectIdValue>? subTree = children[position].Value;
            if (subTree is not null)
                denseSize -= subTree.size;

            children[position].Value = null;

            for (int j = position; j < node.KeyCount; j++)
                node.children[j] = node.children[j + 1];

            node.KeyCount--;
            return true;
        }

        // internal node
        else
        {
            for (int j = 0; j < node.KeyCount; j++)
            {
                if (j + 1 == node.KeyCount || Less(key, children[j + 1].Key))
                    return Delete(children[j].Next, key, ht - 1);
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
