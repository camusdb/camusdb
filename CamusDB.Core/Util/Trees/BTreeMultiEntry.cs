
namespace CamusDB.Core.Util.Trees;

// internal nodes: only use key and next
// external nodes: only use key and value
public sealed class BTreeMultiEntry
{
    public int Key;

    public int NumberValues = 0;

    public BTree<int>? Value;

    public BTreeMultiNode? Next;     // helper field to iterate over array entries

    public BTreeMultiEntry(int key, BTreeMultiNode? next)
    {
        Key = key;        
        Next = next;
    }
}
