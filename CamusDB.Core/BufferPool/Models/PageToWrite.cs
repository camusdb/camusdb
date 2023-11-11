
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.BufferPool.Models;

public readonly struct PageToWrite
{
    public ObjectIdValue Offset { get; }    

    public byte[] Buffer { get; }

    public PageToWrite(ObjectIdValue offset, byte[] buffer)
    {
        Offset = offset;
        Buffer = buffer;
    }
}

