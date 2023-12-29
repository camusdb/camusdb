
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Entrypoint for the SQL Parser
/// </summary>
internal partial class sqlParser
{
    public sqlParser() : base(null) { }

    public NodeAst Parse(string sqlStatement)
    {
        byte[] inputBuffer = Encoding.Default.GetBytes(sqlStatement);

        MemoryStream stream = new(inputBuffer);
        var scanner = new sqlScanner(stream);

        Scanner = scanner;

        Parse();

        if (!string.IsNullOrEmpty(scanner.YYError))
            throw new CamusDBException(CamusDBErrorCodes.SqlSyntaxError, scanner.YYError);

        return CurrentSemanticValue.n;
    }
}
