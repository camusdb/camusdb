
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Util.Trees.Experimental;

public class BPlusTreeNode<TKey, TValue> where TKey : IComparable<TKey> where TValue : IComparable<TValue>
{
    public BTreeLeafType Type { get; set; } = BTreeLeafType.External;

    public List<BPlusTreeEntry<TKey, TValue>> Entries { get; set; } = new();

    public ObjectIdValue PageOffset; // page offset on disk
}
