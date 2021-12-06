
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Serializer.Models;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.Journal.Controllers;

public class WritePagePayload
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
        Serializator.WriteInt16(journal, JournalScheduleTypes.InsertSlots, ref pointer);
        Serializator.WriteUInt32(journal, relatedSequence, ref pointer);

        Buffer.BlockCopy(data, 0, journal, pointer, data.Length);        

        return journal;
    }
}

