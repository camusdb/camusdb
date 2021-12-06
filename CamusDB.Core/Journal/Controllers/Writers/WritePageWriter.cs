
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Serializer.Models;

namespace CamusDB.Core.Journal.Controllers;

public class WritePageWriter
{
    public static byte[] Generate(uint sequence, uint relatedSequence, byte[] data)
    {
        byte[] journal = new byte[
            SerializatorTypeSizes.TypeInteger32 + // LSN (4 bytes)
            SerializatorTypeSizes.TypeInteger16 + // journal type (2 bytes)
            SerializatorTypeSizes.TypeInteger32 + // related LSN (4 bytes)
            data.Length
        ];

        int pointer = 0;
        Serializator.WriteUInt32(journal, sequence, ref pointer);
        Serializator.WriteInt16(journal, JournalLogTypes.InsertSlots, ref pointer);
        Serializator.WriteUInt32(journal, relatedSequence, ref pointer);

        Buffer.BlockCopy(data, 0, journal, pointer, data.Length);

        return journal;
    }
}

