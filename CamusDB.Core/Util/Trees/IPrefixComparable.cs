
namespace CamusDB.Core.Util.Trees;

public interface IPrefixComparable<TSubKey>
{
    public int IsPrefixedBy(TSubKey? other);
}
