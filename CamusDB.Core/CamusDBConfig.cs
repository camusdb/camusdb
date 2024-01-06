
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDB.Core;

public static class CamusDBConfig
{
    public static string DataDirectory = Path.GetFullPath("Data");

    #region bufferpool

    public const int PageLayoutVersion = 1;

    public const int PageSize = 4096;

    public static int BufferPoolSize = 65536;

    #endregion

    #region system schema

    public const string PrimaryKeyInternalName = "~pk";

    #endregion   

    #region keys
    public readonly static byte[] SchemaKey = Encoding.Unicode.GetBytes("schema");

    public readonly static byte[] SystemKey = Encoding.Unicode.GetBytes("system");
    #endregion
}
