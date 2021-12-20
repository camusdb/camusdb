
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal abstract class IndexBaseReader
{
    protected static ColumnValue UnserializeKey(byte[] nodeBuffer, ref int pointer)
    {
        ColumnType type = (ColumnType) Serializator.ReadInt16(nodeBuffer, ref pointer);

        switch (type)
        {
            case ColumnType.Id:
                {
                    ObjectIdValue value = Serializator.ReadObjectId(nodeBuffer, ref pointer);
                    return new ColumnValue(ColumnType.Id, value.ToString());
                }

            case ColumnType.Integer:
                {
                    int value = Serializator.ReadInt32(nodeBuffer, ref pointer);
                    return new ColumnValue(ColumnType.Integer, value.ToString());
                }

            /*case ColumnType.String:
                Serializator.WriteInt16(nodeBuffer, (int)ColumnType.String, ref pointer);
                Serializator.WriteInt32(nodeBuffer, columnValue.Value.Length, ref pointer);
                Serializator.WriteString(nodeBuffer, columnValue.Value, ref pointer);
                break;*/

            default:
                throw new Exception("Can't use this type as index");
        }
    }

    protected static BTreeTuple? UnserializeTuple(byte[] nodeBuffer, ref int pointer)
    {
        int slotOne = Serializator.ReadInt32(nodeBuffer, ref pointer);
        int slotTwo = Serializator.ReadInt32(nodeBuffer, ref pointer);

        if (slotOne == 0 && slotTwo == 0)
            return null;

        return new BTreeTuple(slotOne, slotTwo);
    }
}