
namespace CamusDB.Core.SQLParser;

public enum NodeType
{
    Number = 1,
    String = 2,
    Identifier = 3,
    IdentifierList = 4,
    ExprEquals = 5,
    ExprNotEquals = 6,
    ExprLessThan = 7,
    ExprGreaterThan = 8,
    Select = 20,
}
