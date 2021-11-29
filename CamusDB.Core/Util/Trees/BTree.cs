
using System.Collections;
using System.Collections.Generic;

namespace CamusDB.Core.Util.Trees;

public sealed class BTree
{
    // max children per B-tree node = M-1
    // (must be even and greater than 2)
    public const int MaxChildren = 4;

    public BTreeNode root;       // root of the B-tree

    public int height;      // height of the B-tree

    public int n;           // number of key-value pairs in the B-tree

    public int PageOffset = -1; // page offset to root node

    public SemaphoreSlim WriteLock { get; } = new(1, 1);

    /**
     * Initializes an empty B-tree.
     */
    public BTree(int rootOffset)
    {
        root = new BTreeNode(0);
        PageOffset = rootOffset;
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
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    public int? Get(int key)
    {
        //if (key == null) throw new ArgumentException("argument to get() is null");
        return Search(root, key, height);
    }

    private int? Search(BTreeNode? node, int key, int ht)
    {
        if (node is null)
            return null;

        BTreeEntry[] children = node.children;

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

    public IEnumerable EntriesTraverse()
    {
        foreach (BTreeEntry entry in EntriesTraverseInternal(root, height))
            yield return entry;
    }

    private static IEnumerable EntriesTraverseInternal(BTreeNode? node, int ht)
    {
        if (node is null)
            yield break;

        BTreeEntry[] children = node.children;

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
                foreach (BTreeEntry entry in EntriesTraverseInternal(children[j].Next, ht - 1))
                    yield return entry;
            }
        }
    }

    public IEnumerable NodesTraverse()
    {
        foreach (BTreeNode node in NodesTraverseInternal(root, height))
            yield return node;
    }

    private static IEnumerable NodesTraverseInternal(BTreeNode? node, int ht)
    {
        //Console.WriteLine("ht={0}", ht);

        if (node is null)
            yield break;

        yield return node;

        if (ht == 0)
            yield break;

        for (int j = 0; j < node.KeyCount; j++)
        {
            foreach (BTreeNode childNode in NodesTraverseInternal(node.children[j].Next, ht - 1))
                yield return childNode;
        }
    }

    public IEnumerable NodesReverseTraverse()
    {
        foreach (BTreeNode node in NodesReverseTraverseInternal(root, height))
            yield return node;
    }

    private static IEnumerable NodesReverseTraverseInternal(BTreeNode? node, int ht)
    {
        //Console.WriteLine("ht={0}", ht);

        if (node is null)
            yield break;

        for (int j = node.KeyCount; j >= 0; j--)
        {
            foreach (BTreeNode childNode in NodesReverseTraverseInternal(node.children[j].Next, ht - 1))
                yield return childNode;
        }

        yield return node;
    }

    public void Put(int key, int value)
    {
        //if (key == null) throw new IllegalArgumentException("argument key to put() is null");
        BTreeNode? u = Insert(root, key, value, height);
        n++;
        if (u == null) return;

        // need to split root
        BTreeNode newRoot = new(2);
        newRoot.children[0] = new BTreeEntry(root.children[0].Key, null, root);
        newRoot.children[1] = new BTreeEntry(u.children[0].Key, null, u);

        root = newRoot;

        newRoot.PageOffset = root.PageOffset;
        root.PageOffset = -1;
        root.Dirty = true;

        height++;
    }

    private BTreeNode? Insert(BTreeNode? node, int key, int? val, int ht)
    {
        if (node is null)
            throw new ArgumentException("h cannot be null");

        int j;
        BTreeEntry t = new(key, val, null);

        // external node
        if (ht == 0)
        {
            for (j = 0; j < node.KeyCount; j++)
                if (Less(key, node.children[j].Key))
                    break;
        }

        // internal node
        else
        {
            for (j = 0; j < node.KeyCount; j++)
            {
                if ((j + 1 == node.KeyCount) || Less(key, node.children[j + 1].Key))
                {
                    BTreeNode? u = Insert(node.children[j++].Next, key, val, ht - 1);

                    if (u == null)
                        return null;

                    t.Key = u.children[0].Key;
                    t.Next = u;
                    break;
                }
            }
        }

        for (int i = node.KeyCount; i > j; i--)
            node.children[i] = node.children[i - 1];

        node.children[j] = t;
        node.KeyCount++;
        node.Dirty = true;

        if (node.KeyCount < MaxChildren)
            return null;

        return Split(node);
    }

    // split node in half
    private static BTreeNode Split(BTreeNode current)
    {
        int half = MaxChildren / 2;

        BTreeNode t = new(half);

        current.KeyCount = half;
        current.Dirty = true;

        for (int j = 0; j < half; j++)
            t.children[j] = current.children[half + j];

        return t;
    }

    // comparison functions - make Comparable instead of Key to avoid casts
    private static bool Less(int k1, int k2)
    {
        //return k1.compareTo(k2) < 0;
        return k1 < k2;
    }

    private static bool Eq(int k1, int k2)
    {
        return k1 == k2;
    }
}
