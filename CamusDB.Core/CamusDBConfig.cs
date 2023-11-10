


using System.Text;
using CamusDB.Core.Util.ObjectIds;
/**
* This file is part of CamusDB  
*
* For the full copyright and license information, please view the LICENSE.txt
* file that was distributed with this source code.
*/
namespace CamusDB.Core;

public static class CamusDBConfig
{
    public readonly static string DataDirectory = Path.GetFullPath("Data");    

    #region bufferpool

    public const int PageLayoutVersion = 1;

    public const int PageSize = 4096;    

    public const int InitialPagesRead = 1024;

    public readonly static ObjectIdValue TableSpaceHeaderPage = new(1, 1, 1);

    public const int FlushToDiskInterval = 1000;

    #endregion

    #region system schema

    public const string PrimaryKeyInternalName = "~pk";

    #endregion   

    #region keys
    public readonly static byte[] SchemaKey = Encoding.UTF8.GetBytes("schema");

    public readonly static byte[] SystemKey = Encoding.UTF8.GetBytes("system");
    #endregion
}
