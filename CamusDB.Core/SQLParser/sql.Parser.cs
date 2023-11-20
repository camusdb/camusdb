
using System.Text;

namespace CamusDB.Core.SQLParser;

internal partial class sqlParser
{
    public sqlParser() : base(null) { }

    public NodeAst Parse(string s)
    {
        byte[] inputBuffer = Encoding.Default.GetBytes(s);
        MemoryStream stream = new(inputBuffer);
        Scanner = new sqlScanner(stream);
        Parse();

        return CurrentSemanticValue.n;
    }
}
