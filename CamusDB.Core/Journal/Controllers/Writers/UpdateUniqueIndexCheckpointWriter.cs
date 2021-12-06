
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Serializer.Models;

namespace CamusDB.Core.Journal.Controllers.Writers;

public static class UpdateUniqueIndexCheckpointWriter
{
    public static byte[] Generate(uint sequence, uint relatedSequence, TableIndexSchema index)
    {
        byte[] journal = new byte[
            SerializatorTypeSizes.TypeInteger32 + // LSN (4 bytes)
            SerializatorTypeSizes.TypeInteger16 + // journal type (2 bytes)
            SerializatorTypeSizes.TypeInteger32 + // related LSN (4 bytes)
            index.Column.Length // size of name
        ];

        int pointer = 0;
        Serializator.WriteUInt32(journal, sequence, ref pointer);
        Serializator.WriteInt16(journal, JournalLogTypes.UpdateUniqueIndexCheckpoint, ref pointer);
        Serializator.WriteUInt32(journal, relatedSequence, ref pointer);
        Serializator.WriteString(journal, index.Column, ref pointer);

        return journal;
    }
}
