
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using System.Collections.Generic;

namespace CamusDB.Core.Util.Trees;

public sealed class BTree<T> where T : IComparable<T>
{
    // max children per B-tree node = M-1 (must be even and greater than 2)
    public const int MaxChildren = 8;

    public const int MaxChildrenHalf = MaxChildren / 2;

    public static int CurrentId = -1;

    public int Id;

    public BTreeNode<T> root;       // root of the B-tree

    public int height;      // height of the B-tree

    public int n;           // number of key-value pairs in the B-tree

    public int PageOffset = -1; // page offset to root node

    public SemaphoreSlim WriteLock { get; } = new(1, 1);

    /**
     * Initializes an empty B-tree.
     */
    public BTree(int rootOffset)
    {
        root = new BTreeNode<T>(0);
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
    public int? Get(T key)
    {
        return Search(root, key, height);
    }

    private int? Search(BTreeNode<T>? node, T key, int ht)
    {
        if (node is null)
            return null;

        BTreeEntry<T>[] children = node.children;

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

    public IEnumerable<BTreeEntry<T>> EntriesTraverse()
    {
        foreach (BTreeEntry<T> entry in EntriesTraverseInternal(root, height))
            yield return entry;
    }

    private static IEnumerable<BTreeEntry<T>> EntriesTraverseInternal(BTreeNode<T>? node, int ht)
    {
        if (node is null)
            yield break;

        BTreeEntry<T>[] children = node.children;

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
                foreach (BTreeEntry<T> entry in EntriesTraverseInternal(children[j].Next, ht - 1))
                    yield return entry;
            }
        }
    }

    public IEnumerable<BTreeNode<T>> NodesTraverse()
    {
        foreach (BTreeNode<T> node in NodesTraverseInternal(root, height))
            yield return node;
    }

    private static IEnumerable<BTreeNode<T>> NodesTraverseInternal(BTreeNode<T>? node, int ht)
    {
        //Console.WriteLine("ht={0}", ht);

        if (node is null)
            yield break;

        yield return node;

        if (ht == 0)
            yield break;

        for (int j = 0; j < node.KeyCount; j++)
        {
            foreach (BTreeNode<T> childNode in NodesTraverseInternal(node.children[j].Next, ht - 1))
                yield return childNode;
        }
    }

    public IEnumerable<BTreeNode<T>> NodesReverseTraverse()
    {
        foreach (BTreeNode<T> node in NodesReverseTraverseInternal(root, height))
            yield return node;
    }

    private static IEnumerable<BTreeNode<T>> NodesReverseTraverseInternal(BTreeNode<T>? node, int ht)
    {
        //Console.WriteLine("ht={0}", ht);

        if (node is null)
            yield break;

        for (int j = node.KeyCount; j >= 0; j--)
        {
            foreach (BTreeNode<T> childNode in NodesReverseTraverseInternal(node.children[j].Next, ht - 1))
                yield return childNode;
        }

        yield return node;
    }

    public void Put(T key, int value)
    {
        //Console.WriteLine("Put {0} {1}\nStackTrace: '{2}'", key, value, Environment.StackTrace);

        BTreeNode<T>? u = Insert(root, key, value, height);
        n++;
        if (u == null) return;

        // need to split root
        BTreeNode<T> newRoot = new(2);
        //Console.WriteLine("Node {0} is now root", newRoot.Id);

        newRoot.children[0] = new BTreeEntry<T>(root.children[0].Key, null, root);
        newRoot.children[1] = new BTreeEntry<T>(u.children[0].Key, null, u);

        root = newRoot;

        newRoot.PageOffset = root.PageOffset;
        root.PageOffset = -1;
        root.Dirty = true;

        //Console.WriteLine("Node {0} is now root", root.Id);

        height++;
    }

    private BTreeNode<T>? Insert(BTreeNode<T>? node, T key, int? val, int ht)
    {
        if (node is null)
            throw new ArgumentException("node cannot be null");

        int j;
        BTreeEntry<T> newEntry = new(key, val, null);
        BTreeEntry<T>[] children = node.children;

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
                    BTreeNode<T>? u = Insert(children[j++].Next, key, val, ht - 1);

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

        //Console.WriteLine("Node {0} marked as dirty as child added", node.Id);

        if (node.KeyCount < MaxChildren)
            return null;

        return Split(node);
    }

    // split node in half
    private static BTreeNode<T> Split(BTreeNode<T> current)
    {
        BTreeNode<T> newNode = new(MaxChildrenHalf);

        //Console.WriteLine("Node {0} marked as dirty because of split", t.Id);

        current.KeyCount = MaxChildrenHalf;
        current.Dirty = true;

        //Console.WriteLine("Node {0} marked as dirty because of split", current.Id);

        for (int j = 0; j < MaxChildrenHalf; j++)
            newNode.children[j] = current.children[MaxChildrenHalf + j];

        return newNode;
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
