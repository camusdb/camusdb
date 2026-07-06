
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Entrypoint for the SQL Parser
/// </summary>
internal partial class sqlParser
{
    public sqlParser() : base(null) { }

    public NodeAst Parse(string sqlStatement)
    {
        // Feed the SQL string straight into the scanner via SetSource(string). This avoids the
        // per-parse Encoding.Default.GetBytes(...) byte[] copy plus MemoryStream that the previous
        // path allocated, and reads the statement's chars directly — which also sidesteps the
        // Encoding.Default round-trip that could mangle non-ASCII identifiers/string literals.
        var scanner = new sqlScanner();
        scanner.SetSource(sqlStatement, 0);

        Scanner = scanner;

        Parse();

        if (!string.IsNullOrEmpty(scanner.YYError))
            throw new CamusDBException(CamusDBErrorCodes.SqlSyntaxError, scanner.YYError);

        return CurrentSemanticValue.n;
    }
}
