
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
 *  B+Tree structure than can be stored on disk
 */ 
public sealed class BTree<TKey, TValue> where TKey : IComparable<TKey>
{
    // max children per B-tree node = M-1 (must be even and greater than 2)
    public const int MaxChildren = 8;

    public const int MaxChildrenHalf = MaxChildren / 2;

    private static int CurrentId = -1;

    public int Id;

    public BTreeNode<TKey, TValue> root;       // root of the B-tree

    public int height;      // height of the B-tree

    public int size;           // number of key-value pairs in the B-tree

    public int PageOffset = -1; // page offset to root node

    public SemaphoreSlim WriteLock { get; } = new(1, 1);

    /**
     * Initializes an empty B-tree.
     */
    public BTree(int rootOffset)
    {
        root = new BTreeNode<TKey, TValue>(0);
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
        //Console.WriteLine("ht={0}", ht);

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
        //Console.WriteLine("ht={0}", ht);

        if (node is null)
            yield break;

        for (int j = node.KeyCount; j >= 0; j--)
        {
            foreach (BTreeNode<TKey, TValue> childNode in NodesReverseTraverseInternal(node.children[j].Next, ht - 1))
                yield return childNode;
        }

        yield return node;
    }

    public BTreeInsertDeltas<TKey, TValue> Put(TKey key, TValue? value)
    {
        BTreeInsertDeltas<TKey, TValue> deltas = new();

        //Console.WriteLine("Put {0} {1}\nStackTrace: '{2}'", key, value, Environment.StackTrace);

        BTreeNode<TKey, TValue>? u = Insert(root, key, value, height, deltas);
        size++;

        if (u == null)
            return deltas;

        // need to split root
        BTreeNode<TKey, TValue> newRoot = new(2);
        deltas.Deltas.Add(newRoot);
        //Console.WriteLine("Node {0} is now root", newRoot.Id);

        newRoot.children[0] = new BTreeEntry<TKey, TValue>(root.children[0].Key, default, root);
        newRoot.children[1] = new BTreeEntry<TKey, TValue>(u.children[0].Key, default, u);

        root = newRoot;

        newRoot.PageOffset = root.PageOffset;
        root.PageOffset = -1;
        root.Dirty = true;
        deltas.Deltas.Add(newRoot);

        //Console.WriteLine("Node {0} is now root", root.Id);

        height++;

        return deltas;
    }

    private BTreeNode<TKey, TValue>? Insert(BTreeNode<TKey, TValue>? node, TKey key, TValue? val, int ht, BTreeInsertDeltas<TKey, TValue> deltas)
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
                    BTreeNode<TKey, TValue>? u = Insert(children[j++].Next, key, val, ht - 1, deltas);

                    if (u == null)
                        return null;

                    newEntry.Key = u.children[0].Key;
                    newEntry.Next = u;
                    break;
                }
            }
        }

        for (int i = node.KeyCount; i > j; i--)
            node.children[i] = node.children[i - 1];

        node.children[j] = newEntry;
        node.KeyCount++;
        node.Dirty = true;
        deltas.Deltas.Add(node);

        //Console.WriteLine("Node {0} marked as dirty as child added", node.Id);

        if (node.KeyCount < MaxChildren)
            return null;

        return Split(node, deltas);
    }

    // split node in half
    private static BTreeNode<TKey, TValue> Split(BTreeNode<TKey, TValue> current, BTreeInsertDeltas<TKey, TValue> deltas)
    {
        BTreeNode<TKey, TValue> newNode = new(MaxChildrenHalf);
        deltas.Deltas.Add(newNode);

        //Console.WriteLine("Node {0} marked as dirty because of split", t.Id);

        current.KeyCount = MaxChildrenHalf;
        current.Dirty = true;
        deltas.Deltas.Add(current);

        //Console.WriteLine("Node {0} marked as dirty because of split", current.Id);

        for (int j = 0; j < MaxChildrenHalf; j++)
            newNode.children[j] = current.children[MaxChildrenHalf + j];

        return newNode;
    }

    /**
     * Returns the entry associated with the given key.
     *
     * @param  key the key
     */
    public bool Remove(TKey key)
    {
        bool found = Delete(root, key, height);

        if (found)
            size--;

        return found;
    }

    private bool Delete(BTreeNode<TKey, TValue>? node, TKey key, int ht)
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
            node.Dirty = true;
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
    private static bool Less(TKey k1, TKey k2)
    {
        return k1!.CompareTo(k2) < 0;
    }

    private static bool Eq(TKey k1, TKey k2)
    {
        return k1.CompareTo(k2) == 0;
    }
}
