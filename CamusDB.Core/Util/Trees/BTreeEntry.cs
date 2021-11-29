
namespace CamusDB.Core.Util.Trees;

// internal nodes: only use key and next
// external nodes: only use key and value
public sealed class BTreeEntry
{
    public int Key;

    public int? Value;

    public BTreeNode? Next;     // helper field to iterate over array entries    

    public BTreeEntry(int key, int? value, BTreeNode? next)
    {
        this.Key = key;
        this.Value = value;
        this.Next = next;
    }
}
