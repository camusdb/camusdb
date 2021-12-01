
namespace CamusDB.Core.Util.Trees;

// internal nodes: only use key and next
// external nodes: only use key and value
public sealed class BTreeMultiEntry
{
    public int Key;

    public int NumberValues = 0;

    public int[] Values = new int[8];

    public BTreeMultiNode? Next;     // helper field to iterate over array entries    

    public BTreeMultiEntry(int key, int? value, BTreeMultiNode? next)
    {
        Key = key;
        if (value is not null)
            Values[NumberValues++] = value.Value;
        Next = next;
    }
}
