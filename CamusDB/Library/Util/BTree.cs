
using System;
using System.Collections;
using System.Collections.Generic;

namespace CamusDB.Library.Util;

class Integer
{
    private readonly int _v;

    public int Value => _v;

    public Integer(int v)
    {
        _v = v;
    }

    public override string ToString() => _v.ToString();
}

// The Comparer must provide a strict weak ordering.
//
// If !(x < y) and !(y < x), we treat this to mean x == y 
// (i.e. we can only hold one of either x or y in the tree).

class IntegerComparer : Comparer<Integer>
{
    public override int Compare(Integer? x, Integer? y)
    {
        if (x is null || y is null)
            return 0;

        return x.Value < y.Value ? -1 : x.Value > y.Value ? 1 : 0;
    }
}

// helper B-tree node data type
public class Node
{
    public int m;                             // number of children

    public Entry[] children = new Entry[BTree.M];   // the array of children

    // create a node with k children
    public Node(int k)
    {
        m = k;
    }
}

// internal nodes: only use key and next
// external nodes: only use key and value
public class Entry
{
    public int key;

    public string? val;

    public Node? next;     // helper field to iterate over array entries

    public Entry(int key, string? val, Node? next)
    {
        this.key = key;
        this.val = val;
        this.next = next;
    }
}

public class BTree
{
    // max children per B-tree node = M-1
    // (must be even and greater than 2)
    public const int M = 4;

    private Node root;       // root of the B-tree

    private int height;      // height of the B-tree
    private int n;           // number of key-value pairs in the B-tree

    /**
     * Initializes an empty B-tree.
     */
    public BTree()
    {
        root = new Node(0);
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
    public string? Get(int key)
    {
        //if (key == null) throw new ArgumentException("argument to get() is null");
        return Search(root, key, height);
    }

    private string? Search(Node? x, int key, int ht)
    {
        if (x is null)
            return null;

        Entry[] children = x.children;

        // external node
        if (ht == 0)
        {
            for (int j = 0; j < x.m; j++)
            {
                if (Eq(key, children[j].key))
                    return children[j].val;
            }
        }

        // internal node
        else
        {
            for (int j = 0; j < x.m; j++)
            {
                if (j + 1 == x.m || Less(key, children[j + 1].key))
                    return Search(children[j].next, key, ht - 1);
            }
        }

        return null;
    }

    public IEnumerable Traverse()
    {
        foreach (Entry entry in InternalTraverse(root, height))
            yield return entry;
    }

    private static IEnumerable InternalTraverse(Node? node, int ht)
    {
        if (node is null)
            yield break;

        Entry[] children = node.children;

        // external node
        if (ht == 0)
        {
            for (int j = 0; j < node.m; j++)
                yield return children[j];
        }

        // internal node
        else
        {
            for (int j = 0; j < node.m; j++)
            {
                foreach (Entry entry in InternalTraverse(children[j].next, ht - 1))
                    yield return entry;
            }
        }
    }

    public void Put(int key, string val)
    {
        //if (key == null) throw new IllegalArgumentException("argument key to put() is null");
        Node? u = Insert(root, key, val, height);
        n++;
        if (u == null) return;

        // need to split root
        Node t = new Node(2);
        t.children[0] = new Entry(root.children[0].key, null, root);
        t.children[1] = new Entry(u.children[0].key, null, u);
        root = t;
        height++;
    }

    private Node? Insert(Node? h, int key, string? val, int ht)
    {
        if (h is null)
            throw new ArgumentException("h cannot be null");

        int j;
        Entry t = new(key, val, null);

        // external node
        if (ht == 0)
        {
            for (j = 0; j < h.m; j++)
                if (Less(key, h.children[j].key))
                    break;
        }

        // internal node
        else
        {
            for (j = 0; j < h.m; j++)
            {
                if ((j + 1 == h.m) || Less(key, h.children[j + 1].key))
                {
                    Node? u = Insert(h.children[j++].next, key, val, ht - 1);
                    if (u == null) return null;
                    t.key = u.children[0].key;
                    t.next = u;
                    break;
                }
            }
        }

        for (int i = h.m; i > j; i--)
            h.children[i] = h.children[i - 1];

        h.children[j] = t;
        h.m++;

        if (h.m < M)
            return null;

        return Split(h);
    }

    // split node in half
    private Node Split(Node h)
    {
        Node t = new Node(M / 2);
        h.m = M / 2;
        for (int j = 0; j < M / 2; j++)
            t.children[j] = h.children[M / 2 + j];

        return t;
    }

    // comparison functions - make Comparable instead of Key to avoid casts
    private bool Less(int k1, int k2)
    {
        //return k1.compareTo(k2) < 0;
        return k1 < k2;
    }

    private bool Eq(int k1, int k2)
    {
        return k1 == k2;
    }
}
