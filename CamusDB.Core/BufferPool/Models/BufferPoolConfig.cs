
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

    public const int ChecksumOffset = 2 + 4 * 0; // 2 version + 4 checksum + 4 next page + 4 data length

    public const int LastSequenceOffset = 2 + 4 * 1; // 2 version + 4 checksum + 4 next page + 4 data length

    public const int NextPageOffset = 2 + 4 * 2; // 2 version + 4 checksum + 4 next page + 4 data length
    
    public const int LengthOffset = 2 + 4 * 3; // 2 version + 4 checksum + 4 next page + 4 data length

    public const int DataOffset = 2 + 4 * 4; // 2 version + 4 checksum + 4 next page + 4 data length            

    public const int RowIdOffset = 14;

    public const int FreePageOffset = 10;
}
