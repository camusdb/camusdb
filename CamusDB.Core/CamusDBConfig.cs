
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core;

public static class CamusDBConfig
{
    public const string DataDirectory = "Data";

    public const int InitialTableSpaceSize = PageSize * TotalPages; // 4096 blocks of 1024 size

    #region bufferpool

    public const int PageLayoutVersion = 1;

    public const int PageSize = 1024;

    public const int TotalPages = 4096;

    public const int TableSpaceHeaderPage = 0;

    public const int DataOffset = 2 + 4 * 3; // 2 version + 4 checksum + 4 next page + 4 data length

    public const int LengthOffset = 2 + 4 * 2; // 2 version + 4 checksum + 4 next page + 4 data length

    public const int NextPageOffset = 2 + 4 * 1; // 2 version + 4 checksum + 4 next page + 4 data length

    public const int ChecksumOffset = 2 + 4 * 0; // 2 version + 4 checksum + 4 next page + 4 data length

    public const int RowIdOffset = 14;

    public const int FreePageOffset = 10;

    #endregion

    #region system schema

    public const int SystemHeaderPage = 1;

    public const string PrimaryKeyInternalName = "~pk";

    #endregion

    #region system schema

    public const int SchemaHeaderPage = 1;

    #endregion
}
