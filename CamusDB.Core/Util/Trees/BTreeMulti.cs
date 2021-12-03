
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
 *  BTreeMulti is a tree of B+Trees used for multi keys
 *  Unique values across the index point to other trees where the rowids are unique values
 */
public sealed class BTreeMulti<T> where T : IComparable<T>
{
    // max children per B-tree node = M-1 (must be even and greater than 2)
    public const int MaxChildren = 8;

    public const int MaxChildrenHalf = MaxChildren / 2;

    private static int CurrentId = -1;

    public BTreeMultiNode<T> root;       // root of the B-tree

    public int Id;

    public int height;      // height of the B-tree

    public int n;           // number of key-value pairs in the B-tree

    public int PageOffset = -1; // page offset to root node

    public SemaphoreSlim WriteLock { get; } = new(1, 1);

    /**
     * Initializes an empty B-tree.
     */
    public BTreeMulti(int rootOffset)
    {
        root = new BTreeMultiNode<T>(0);
        PageOffset = rootOffset;
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
     * Returns the number of key-value pairs in this symbol table.
     * @return the number of key-value pairs in this symbol table
     */
    public int Size()
    {
        return n;
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
    public BTree<int, int?>? Get(T key)
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
    public IEnumerable<int> GetAll(T key)
    {
        BTree<int, int?>? subTree = Search(root, key, height);

        if (subTree is null)
            yield break;

        foreach (BTreeEntry<int, int?> subTreeEntry in subTree.EntriesTraverse())
            yield return subTreeEntry.Key;
    }

    private BTree<int, int?>? Search(BTreeMultiNode<T>? node, T key, int ht)
    {
        if (node is null)
            return null;

        BTreeMultiEntry<T>[] children = node.children;

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

    public IEnumerable<BTreeMultiEntry<T>> EntriesTraverse()
    {
        foreach (BTreeMultiEntry<T> entry in EntriesTraverseInternal(root, height))
            yield return entry;
    }

    private static IEnumerable<BTreeMultiEntry<T>> EntriesTraverseInternal(BTreeMultiNode<T>? node, int ht)
    {
        if (node is null)
            yield break;

        BTreeMultiEntry<T>[] children = node.children;

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
                foreach (BTreeMultiEntry<T> entry in EntriesTraverseInternal(children[j].Next, ht - 1))
                    yield return entry;
            }
        }
    }

    public IEnumerable<BTreeMultiNode<T>> NodesTraverse()
    {
        foreach (BTreeMultiNode<T> node in NodesTraverseInternal(root, height))
            yield return node;
    }

    private static IEnumerable<BTreeMultiNode<T>> NodesTraverseInternal(BTreeMultiNode<T>? node, int ht)
    {
        //Console.WriteLine("ht={0}", ht);

        if (node is null)
            yield break;

        yield return node;

        if (ht == 0)
            yield break;

        for (int j = 0; j < node.KeyCount; j++)
        {
            foreach (BTreeMultiNode<T> childNode in NodesTraverseInternal(node.children[j].Next, ht - 1))
                yield return childNode;
        }
    }

    public IEnumerable<BTreeNode<int, int?>> NodesReverseTraverse()
    {
        foreach (BTreeNode<int, int?> node in NodesReverseTraverseInternal(root, height))
            yield return node;
    }

    private static IEnumerable<BTreeNode<int, int?>> NodesReverseTraverseInternal(BTreeMultiNode<T>? node, int ht)
    {
        //Console.WriteLine("ht={0}", ht);

        if (node is null)
            yield break;

        for (int j = node.KeyCount; j >= 0; j--)
        {
            foreach (BTreeNode<int, int?> childNode in NodesReverseTraverseInternal(node.children[j].Next, ht - 1))
                yield return childNode;
        }
    }

    public void Put(T key, int value)
    {
        Put(key, new BTreeTuple(value, 0));
    }

    public void Put(T key, BTreeTuple value)
    {
        //Console.WriteLine("Inserting in multitree {0} {1} {2}", Id, key, value);

        BTreeMultiNode<T>? u = Insert(root, key, value, height);
        n++;
        if (u == null) return;

        // need to split root
        BTreeMultiNode<T> newRoot = new(2);
        //Console.WriteLine("Node {0} is now root", newRoot.Id);

        newRoot.children[0] = new BTreeMultiEntry<T>(root.children[0].Key, root);
        newRoot.children[1] = new BTreeMultiEntry<T>(u.children[0].Key, u);

        root = newRoot;

        newRoot.PageOffset = root.PageOffset;
        root.PageOffset = -1;
        root.Dirty = true;

        //Console.WriteLine("Node {0} is now root", root.Id);

        height++;
    }

    private BTreeMultiNode<T>? Insert(BTreeMultiNode<T>? node, T key, BTreeTuple val, int ht)
    {
        if (node is null)
            throw new ArgumentException("node cannot be null");

        int j;
        BTreeMultiEntry<T>? newEntry = null;
        BTreeMultiEntry<T>[] children = node.children;

        // external node at height 0
        if (ht == 0)
        {
            for (j = 0; j < node.KeyCount; j++)
            {
                BTreeMultiEntry<T> child = children[j];

                if (!Eq(key, child.Key)) // same key found
                    continue;

                //if (val is null)
                //    throw new ArgumentException("val cannot be null");

                child.Value!.Put(val.SlotOne, val.SlotTwo);
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
                    BTreeMultiNode<T>? u = Insert(children[j++].Next, key, val, ht - 1);

                    if (u == null)
                        return null;

                    newEntry = new(u.children[0].Key, u);
                    newEntry.Value = new BTree<int, int?>(-1);
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
            newEntry = new(key, null);
            newEntry.Value = new BTree<int, int?>(-1);
        }

        newEntry.Value!.Put(val.SlotOne, val.SlotTwo);

        node.children[j] = newEntry;
        node.KeyCount++;
        node.Dirty = true;

        //Console.WriteLine("Node {0} marked as dirty as child added", node.Id);

        if (node.KeyCount < MaxChildren)
            return null;

        return Split(node);
    }

    // split node in half
    private static BTreeMultiNode<T> Split(BTreeMultiNode<T> current)
    {
        BTreeMultiNode<T> t = new(MaxChildrenHalf);

        //Console.WriteLine("Node {0} marked as dirty because of split", t.Id);

        current.KeyCount = MaxChildrenHalf;
        current.Dirty = true;

        //Console.WriteLine("Node {0} marked as dirty because of split", current.Id);

        for (int j = 0; j < MaxChildrenHalf; j++)
            t.children[j] = current.children[MaxChildrenHalf + j];

        return t;
    }

    // comparison functions - make Comparable instead of Key to avoid casts
    private static bool Less(T k1, T k2)
    {
        return k1!.CompareTo(k2) < 0;
    }

    private static bool Eq(T k1, T k2)
    {
        return k1.CompareTo(k2) == 0;
    }
}
