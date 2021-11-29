
namespace CamusDB.Core;

public class CamusDBException : Exception
{
    public string Code { get; }

    public CamusDBException(string code, string? msg) : base(msg)
    {
        Code = code;
    }
}
