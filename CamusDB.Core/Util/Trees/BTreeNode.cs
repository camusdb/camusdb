
namespace CamusDB.Core.Util.Trees;

// helper B-tree node data type
public sealed class BTreeNode
{
    public int KeyCount;         // number of children

    public int PageOffset = -1;       // on-disk offset

    public bool Dirty = true; // whether the node must be persisted

    public BTreeEntry[] children = new BTreeEntry[BTree.MaxChildren];   // the array of children

    // create a node with k children
    public BTreeNode(int keyCount)
    {
        //Console.WriteLine("Allocated new node {0}", keyCount);
        KeyCount = keyCount;
    }
}
