
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Creates a new parser instance 
/// </summary>
public static class SQLParserProcessor
{
    public static NodeAst Parse(string sql)
    {
        sqlParser sqlParser = new();
        return sqlParser.Parse(sql);
    }
}

