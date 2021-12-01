
using System.Collections;
using System.Collections.Generic;

namespace CamusDB.Core.Util.Trees;

/**
 *  BTreeMulti is a tree of B+Trees 
 *  Unique values point to other trees where the rowids are unique values
 */
public sealed class BTreeMulti
{
    // max children per B-tree node = M-1 (must be even and greater than 2)
    public const int MaxChildren = 8;

    public const int MaxChildrenHalf = MaxChildren / 2;

    public BTreeMultiNode root;       // root of the B-tree

    public int height;      // height of the B-tree

    public int n;           // number of key-value pairs in the B-tree

    public int PageOffset = -1; // page offset to root node

    public SemaphoreSlim WriteLock { get; } = new(1, 1);

    /**
     * Initializes an empty B-tree.
     */
    public BTreeMulti(int rootOffset)
    {
        root = new BTreeMultiNode(0);
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
     */
    public BTree? Get(int key)
    {
        return Search(root, key, height);
    }

    private BTree? Search(BTreeMultiNode? node, int key, int ht)
    {
        if (node is null)
            return null;

        BTreeMultiEntry[] children = node.children;

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

    private static IEnumerable EntriesTraverseInternal(BTreeMultiNode? node, int ht)
    {
        if (node is null)
            yield break;

        BTreeMultiEntry[] children = node.children;

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
        foreach (BTreeMultiNode node in NodesTraverseInternal(root, height))
            yield return node;
    }

    private static IEnumerable NodesTraverseInternal(BTreeMultiNode? node, int ht)
    {
        //Console.WriteLine("ht={0}", ht);

        if (node is null)
            yield break;

        yield return node;

        if (ht == 0)
            yield break;

        for (int j = 0; j < node.KeyCount; j++)
        {
            foreach (BTreeMultiNode childNode in NodesTraverseInternal(node.children[j].Next, ht - 1))
                yield return childNode;
        }
    }

    public IEnumerable NodesReverseTraverse()
    {
        foreach (BTreeMultiNode node in NodesReverseTraverseInternal(root, height))
            yield return node;
    }

    private static IEnumerable NodesReverseTraverseInternal(BTreeMultiNode? node, int ht)
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
        BTreeMultiNode? u = Insert(root, key, value, height);
        n++;
        if (u == null) return;

        // need to split root
        BTreeMultiNode newRoot = new(2);
        //Console.WriteLine("Node {0} is now root", newRoot.Id);

        newRoot.children[0] = new BTreeMultiEntry(root.children[0].Key, root);
        newRoot.children[1] = new BTreeMultiEntry(u.children[0].Key, u);

        root = newRoot;

        newRoot.PageOffset = root.PageOffset;
        root.PageOffset = -1;
        root.Dirty = true;

        //Console.WriteLine("Node {0} is now root", root.Id);

        height++;
    }

    private BTreeMultiNode? Insert(BTreeMultiNode? node, int key, int? val, int ht)
    {
        if (node is null)
            throw new ArgumentException("node cannot be null");

        int j;
        BTreeMultiEntry? newEntry = null;
        BTreeMultiEntry[] children = node.children;        

        // external node at height 0
        if (ht == 0)
        {
            for (j = 0; j < node.KeyCount; j++)
            {
                BTreeMultiEntry child = children[j];

                if (!Eq(key, child.Key)) // same key found
                    continue;

                if (val is null)
                    throw new ArgumentException("val cannot be null");

                //FindChildToInsert(node, key, val.Value);

                child.Value!.Put(val.Value, key);

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
                    BTreeMultiNode? u = Insert(children[j++].Next, key, val, ht - 1);

                    if (u == null)
                        return null;

                    newEntry = new(u.children[0].Key, u);
                    newEntry.Value = new BTree(0);
                    break;
                }
            }
        }

        for (int i = node.KeyCount; i > j; i--)
            node.children[i] = node.children[i - 1];

        if (val is null)
            throw new ArgumentException("val cannot be null");

        if (newEntry is null)
        {
            newEntry = new(key, null);
            newEntry.Value = new BTree(0);
        }

        newEntry.Value!.Put(val.Value, key);

        node.children[j] = newEntry;
        node.KeyCount++;
        node.Dirty = true;

        //Console.WriteLine("Node {0} marked as dirty as child added", node.Id);

        if (node.KeyCount < MaxChildren)
            return null;

        return Split(node);
    }

    // split node in half
    private static BTreeMultiNode Split(BTreeMultiNode current)
    {
        BTreeMultiNode t = new(MaxChildrenHalf);

        //Console.WriteLine("Node {0} marked as dirty because of split", t.Id);

        current.KeyCount = MaxChildrenHalf;
        current.Dirty = true;

        //Console.WriteLine("Node {0} marked as dirty because of split", current.Id);

        for (int j = 0; j < MaxChildrenHalf; j++)
            t.children[j] = current.children[MaxChildrenHalf + j];

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
