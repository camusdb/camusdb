
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
//using CamusDB.Core.Journal.Models.Readers;
using CamusDB.Core.Journal.Models.Logs;

namespace CamusDB.Core.Journal.Controllers.Readers;

public static class InsertTicketReader
{
    public static async Task<InsertLog> Deserialize(FileStream journal)
    {
        byte[] buffer = new byte[
            SerializatorTypeSizes.TypeInteger16   // number fields (2 bytes)
        ];

        int readBytes = await journal.ReadAsync(buffer, 0, 2);
        if (readBytes != 2)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidJournalData,
                "Invalid journal data when reading insert ticket log"
            );

        int pointer = 0;

        short numberFields = Serializator.ReadInt16(buffer, ref pointer);
        if (numberFields == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidJournalData,
                "Invalid journal data when reading insert ticket log"
            );
       
        readBytes = await journal.ReadAsync(buffer, 0, 2);
        if (readBytes != 2)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidJournalData,
                "Invalid journal data when reading insert ticket log"
            );

        pointer = 0;
        int tableLength = Serializator.ReadInt16(buffer, ref pointer);

        buffer = new byte[tableLength];

        await journal.ReadAsync(buffer, 0, tableLength);

        pointer = 0;
        string tableName = Serializator.ReadString(buffer, tableLength, ref pointer);

        //Console.WriteLine(tableName);

        for (int i = 0; i < numberFields; i++)
        {

        }

        //throw new Exception(length.ToString());
        //throw new Exception(numberFields.ToString());

        return new InsertLog(tableName, new());
    }
}
