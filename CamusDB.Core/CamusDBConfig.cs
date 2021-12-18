
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core;

public static class CamusDBConfig
{
    public static string DataDirectory = Path.GetFullPath("Data");

    public const int InitialTableSpaceSize = PageSize * TotalPages; // 4096 blocks of 512 size

    #region bufferpool

    public const int PageLayoutVersion = 1;

    public const int PageSize = 512;

    public const int TotalPages = 4096;

    public const int InitialPagesRead = 1024;

    public const int TableSpaceHeaderPage = 0;

    public const int FlushToDiskInterval = 1000;

    #endregion

    #region system schema

    public const int SystemHeaderPage = 1;

    public const string PrimaryKeyInternalName = "~pk";

    #endregion

    #region system schema

    public const int SchemaHeaderPage = 1;

    #endregion

    #region journal

    public const int JournalFlushInterval = 1000;

    #endregion
}
