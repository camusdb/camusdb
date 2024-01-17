

using System.Xml.Linq;

namespace CamusDB.Core.Util.Experimental;

internal class BPlusEntry
{
    public int Key { get; set; }

    public int Value { get; set; }

    public BPlusTreeNode? Next { get; set; }
}

internal enum BTreeLeafType
{
    External,
    Internal
}

internal class BPlusTreeNode
{
    public BTreeLeafType Type { get; set; } = BTreeLeafType.External;

    public List<BPlusEntry> Entries { get; set; } = new();
}

public class BPlusTree
{
    private BPlusTreeNode? root;

    public BPlusTree()
    {
    }

    public void Put(int key, int value)
    {
        if (root is null)
            root = new BPlusTreeNode();

        BPlusTreeNode? node = Insert(root, key, value);

        if (node is null)
            return;

        if (node.Entries.Count < 4)
            return;

        BPlusTreeNode newRoot = new();

        BPlusTreeNode left = new();
        BPlusTreeNode right = new();

        newRoot.Type = BTreeLeafType.Internal;
        newRoot.Entries.Add(new BPlusEntry { Key = node.Entries[1].Key, Value = 0, Next = left });
        newRoot.Entries.Add(new BPlusEntry { Key = node.Entries[2].Key, Value = 0, Next = right });

        for (int i = 0; i <= 1; i++)
            left.Entries.Add(node.Entries[i]);

        for (int i = 2; i < node.Entries.Count; i++)
            right.Entries.Add(node.Entries[i]);

        root = newRoot;
    }

    private BPlusTreeNode? Insert(BPlusTreeNode node, int key, int value)
    {
        if (node is null)
            return null;

        if (node.Type == BTreeLeafType.External)
        {            
            for (int i = 0; i < node.Entries.Count; i++)
            {
                if (node.Entries[i].Key == key)
                {
                    node.Entries[i].Value = value;
                    return node;
                }

                if (node.Entries[i].Key > key)
                {
                    node.Entries.Insert(i, new BPlusEntry { Key = key, Value = value });
                    return node;
                }
            }

            node.Entries.Add(new BPlusEntry { Key = key, Value = value });

            if (node.Entries.Count < 4)
                return null;

            return Split(node);            
        }

        if (node.Type == BTreeLeafType.Internal)
        {
            for (int i = 0; i < node.Entries.Count; i++)
            {
                if (key < node.Entries[i].Key || i == (node.Entries.Count - 1))
                {
                    Console.WriteLine("{0} {1}", key, i);

                    BPlusTreeNode? split = Insert(node.Entries[i].Next!, key, value);

                    if (split is null || split.Entries.Count < 4)
                        return null;

                    return Split(split);
                }
            }

            throw new Exception("Should not happen");
        }

        return null;
    }

    private BPlusTreeNode? Split(BPlusTreeNode node)
    {
        BPlusTreeNode newRoot = new();

        BPlusTreeNode left = new();
        BPlusTreeNode right = new();

        newRoot.Type = BTreeLeafType.Internal;
        newRoot.Entries.Add(new BPlusEntry { Key = node.Entries[1].Key, Value = 0, Next = left });
        newRoot.Entries.Add(new BPlusEntry { Key = node.Entries[2].Key, Value = 0, Next = right });

        for (int i = 0; i <= 1; i++)
            left.Entries.Add(node.Entries[i]);

        for (int i = 2; i < node.Entries.Count; i++)
            right.Entries.Add(node.Entries[i]);

        return newRoot;
    }

    public void Print()
    {
        if (root is null)
            return;

        Print(root, 0);
    }

    private void Print(BPlusTreeNode root, int v)
    {
        string pad = new string('=', v);

        if (root.Type == BTreeLeafType.External)
        {            
            foreach (BPlusEntry entry in root.Entries)
            {
                Console.WriteLine("{0}> Key={1} Value={2}", pad, entry.Key, entry.Value);
            }
        }
        else
        {
            foreach (BPlusEntry entry in root.Entries)
            {
                Console.WriteLine("{0}> Key={1}", pad, entry.Key);
                Print(entry.Next!, v + 1);
            }
        }
    }
}
