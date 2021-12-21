
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using System.Collections.Generic;

namespace CamusDB.Core.Util.Trees;

/**
 *  B+Tree 
 *  
 * A B+ tree is an m-ary tree with a variable but often large number of children per node. 
 * A B+ tree consists of a root, internal nodes and leaves. The root may be either a 
 * leaf or a node with two or more children.
 * 
 * The implementation use C# generics to support any type of keys and values in an optimal way.
 * The lazy feature allows to load/unload used/unused nodes to/from disk to save memory.
 */
public sealed class BTree<TKey, TValue> where TKey : IComparable<TKey>
{
    private static int CurrentId = -1;

    public BTreeNode<TKey, TValue>? root;  // root of the B-tree

    public int Id;    // unique tree id

    public int height;      // height of the B-tree

    public int size;           // number of key-value pairs in the B-tree

    public int PageOffset = -1; // page offset to root node

    public SemaphoreSlim WriteLock { get; } = new(1, 1); // global lock

    /**
     * Initializes an empty B-tree.
     */
    public BTree(int rootOffset)
    {
        PageOffset = rootOffset;
        Id = Interlocked.Increment(ref CurrentId);

        //Console.WriteLine("Tree={0}", Id);        
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
     * Returns the number of key-value pairs in this symbol table.
     * @return the number of key-value pairs in this symbol table
     */
    public int Size()
    {
        return size;
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
    public TValue? Get(TKey key)
    {
        if (root is null)
            return default;

        return Search(root, key, height);
    }

    private TValue? Search(BTreeNode<TKey, TValue>? node, TKey key, int ht)
    {
        if (node is null)
            return default;

        BTreeEntry<TKey, TValue>[] children = node.children;

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

        return default;
    }

    public IEnumerable<BTreeEntry<TKey, TValue>> EntriesTraverse()
    {
        foreach (BTreeEntry<TKey, TValue> entry in EntriesTraverseInternal(root, height))
            yield return entry;
    }

    private static IEnumerable<BTreeEntry<TKey, TValue>> EntriesTraverseInternal(BTreeNode<TKey, TValue>? node, int ht)
    {
        if (node is null)
            yield break;

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
                foreach (BTreeEntry<TKey, TValue> entry in EntriesTraverseInternal(children[j].Next, ht - 1))
                    yield return entry;
            }
        }
    }

    public IEnumerable<BTreeNode<TKey, TValue>> NodesTraverse()
    {
        foreach (BTreeNode<TKey, TValue> node in NodesTraverseInternal(root, height))
            yield return node;
    }

    private static IEnumerable<BTreeNode<TKey, TValue>> NodesTraverseInternal(BTreeNode<TKey, TValue>? node, int ht)
    {
        if (node is null)
            yield break;

        yield return node;

        if (ht == 0)
            yield break;

        for (int j = 0; j < node.KeyCount; j++)
        {
            foreach (BTreeNode<TKey, TValue> childNode in NodesTraverseInternal(node.children[j].Next, ht - 1))
                yield return childNode;
        }
    }

    public IEnumerable<BTreeNode<TKey, TValue>> NodesReverseTraverse()
    {
        foreach (BTreeNode<TKey, TValue> node in NodesReverseTraverseInternal(root, height))
            yield return node;
    }

    private static IEnumerable<BTreeNode<TKey, TValue>> NodesReverseTraverseInternal(BTreeNode<TKey, TValue>? node, int ht)
    {
        if (node is null)
            yield break;

        for (int j = node.KeyCount; j >= 0; j--)
        {
            foreach (BTreeNode<TKey, TValue> childNode in NodesReverseTraverseInternal(node.children[j].Next, ht - 1))
                yield return childNode;
        }

        yield return node;
    }

    public List<BTreeNode<TKey, TValue>> Put(TKey key, TValue? value)
    {
        List<BTreeNode<TKey, TValue>> deltas = new();

        if (root is null) // create root
        {
            root = new BTreeNode<TKey, TValue>(0);
            deltas.Add(root);
        }

        BTreeNode<TKey, TValue>? split = Insert(root, key, value, height, deltas);
        size++;

        if (split is null)
            return deltas;

        // need to split root
        BTreeNode<TKey, TValue> newRoot = new(2);
        deltas.Add(newRoot);

        newRoot.children[0] = new BTreeEntry<TKey, TValue>(root.children[0].Key, default, root);
        newRoot.children[1] = new BTreeEntry<TKey, TValue>(split.children[0].Key, default, split);

        root = newRoot;

        newRoot.PageOffset = root.PageOffset;
        root.PageOffset = -1;

        if (root.Dirty == false)
        {
            root.Dirty = true;
            deltas.Add(newRoot);
        }

        height++;

        return deltas;
    }

    private BTreeNode<TKey, TValue>? Insert(BTreeNode<TKey, TValue>? node, TKey key, TValue? val, int ht, List<BTreeNode<TKey, TValue>> deltas)
    {
        if (node is null)
            throw new ArgumentException("node cannot be null");

        int j;
        BTreeEntry<TKey, TValue> newEntry = new(key, val, null);
        BTreeEntry<TKey, TValue>[] children = node.children;

        // external node
        if (ht == 0)
        {
            for (j = 0; j < node.KeyCount; j++)
            {
                if (Eq(key, children[j].Key))
                    throw new Exception("Keys must be unique");

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
                    BTreeNode<TKey, TValue>? split = Insert(children[j++].Next, key, val, ht - 1, deltas);

                    if (split == null)
                        return null;

                    newEntry.Key = split.children[0].Key;
                    newEntry.Next = split;
                    break;
                }
            }
        }

        for (int i = node.KeyCount; i > j; i--)
            node.children[i] = node.children[i - 1];

        node.children[j] = newEntry;
        node.KeyCount++;

        if (node.Dirty == false)
        {
            node.Dirty = true;
            deltas.Add(node);
        }

        if (node.KeyCount < BTreeConfig.MaxChildren)
            return null;

        return Split(node, deltas);
    }

    // split node in half
    private static BTreeNode<TKey, TValue> Split(BTreeNode<TKey, TValue> current, List<BTreeNode<TKey, TValue>> deltas)
    {
        BTreeNode<TKey, TValue> newNode = new(BTreeConfig.MaxChildrenHalf);
        deltas.Add(newNode);        

        current.KeyCount = BTreeConfig.MaxChildrenHalf;

        if (current.Dirty == false)
        {
            current.Dirty = true;
            deltas.Add(current);
        }

        for (int j = 0; j < BTreeConfig.MaxChildrenHalf; j++)
            newNode.children[j] = current.children[BTreeConfig.MaxChildrenHalf + j];

        return newNode;
    }

    /**
     * Returns the entry associated with the given key.
     *
     * @param  key the key
     */
    public (bool found, List<BTreeNode<TKey, TValue>> deltas) Remove(TKey key)
    {
        List<BTreeNode<TKey, TValue>> deltas = new();

        bool found = Delete(root, key, height, deltas);

        if (found)
            size--;

        return (found, deltas);
    }

    /**
     * Delete node entries by key, it doesn't merge yet
     * 
     * @param node the node where to search the value
     */
    private bool Delete(BTreeNode<TKey, TValue>? node, TKey key, int ht, List<BTreeNode<TKey, TValue>> deltas)
    {
        if (node is null)
            return false;

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

            if (node.Dirty == false)
            {
                node.Dirty = true;
                deltas.Add(node);
            }

            return true;
        }

        // internal node
        else
        {
            for (int j = 0; j < node.KeyCount; j++)
            {
                if (j + 1 == node.KeyCount || Less(key, children[j + 1].Key))
                    return Delete(children[j].Next, key, ht - 1, deltas);
            }
        }

        return false;
    }

    // comparison functions - make Comparable instead of Key to avoid casts
    private static bool Less(TKey k1, TKey k2)
    {
        return k1!.CompareTo(k2) < 0;
    }

    private static bool Eq(TKey k1, TKey k2)
    {
        return k1.CompareTo(k2) == 0;
    }
}
