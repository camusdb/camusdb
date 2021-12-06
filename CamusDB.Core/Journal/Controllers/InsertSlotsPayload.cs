
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Serializer.Models;

namespace CamusDB.Core.Journal.Controllers;

public static class InsertSlotsPayload
{
    public static byte[] Generate(uint sequence, uint relatedSequence, BTreeTuple rowTuple)
    {
        byte[] journal = new byte[
            SerializatorTypeSizes.TypeInteger32 + // LSN (4 bytes)
            SerializatorTypeSizes.TypeInteger16 + // journal type (2 bytes)
            SerializatorTypeSizes.TypeInteger32 + // related LSN (4 bytes)
            SerializatorTypeSizes.TypeInteger32 + // slot1 (4 bytes)
            SerializatorTypeSizes.TypeInteger32   // slot2 (4 bytes)
        ];

        //var b = Encoding.UTF8.GetBytes("hello");

        int pointer = 0;
        Serializator.WriteUInt32(journal, sequence, ref pointer);
        Serializator.WriteInt16(journal, JournalScheduleTypes.InsertSlots, ref pointer);
        Serializator.WriteUInt32(journal, relatedSequence, ref pointer);
        Serializator.WriteInt32(journal, rowTuple.SlotOne, ref pointer);
        Serializator.WriteInt32(journal, rowTuple.SlotTwo, ref pointer);

        return journal;
    }
}

