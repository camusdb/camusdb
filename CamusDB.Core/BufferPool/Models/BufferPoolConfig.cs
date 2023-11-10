
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.BufferPool.Models;

public class BufferPoolConfig
{
    public const int PageLayoutOffset = 0;

    public const int ChecksumOffset = 2; // 2 version + 4 checksum + 4 next page + 4 data length

    public const int LastSequenceOffset = 6; // 2 version + 4 checksum + 4 next page + 4 data length

    public const int NextPageOffset = 10; // 2 version + 4 checksum + 4 next page + 4 data length
    
    public const int LengthOffset = 22; // 2 version + 4 checksum + 4 next page + 4 data length

    public const int DataOffset = 26; // 2 version + 4 checksum + 12 next page + 4 data length            

    public const int RowIdOffset = 14;

    public const int FreePageOffset = 10;
}
