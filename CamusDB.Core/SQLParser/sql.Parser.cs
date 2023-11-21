
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
