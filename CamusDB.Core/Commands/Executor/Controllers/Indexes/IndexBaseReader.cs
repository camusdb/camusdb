
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Indexes;

internal abstract class IndexBaseReader
{
    protected static ColumnValue UnserializeKey(byte[] nodeBuffer, ref int pointer)
    {
        ColumnType type = (ColumnType)Serializator.ReadInt16(nodeBuffer, ref pointer);

        switch (type)
        {
            case ColumnType.Id:
                {
                    ObjectIdValue value = Serializator.ReadObjectId(nodeBuffer, ref pointer);
                    return new ColumnValue(ColumnType.Id, value.ToString());
                }

            case ColumnType.Integer64:
                {
                    long value = Serializator.ReadInt64(nodeBuffer, ref pointer);
                    return new ColumnValue(ColumnType.Integer64, value);
                }

            case ColumnType.String:
                {
                    string value = Serializator.ReadString(nodeBuffer, ref pointer);
                    return new ColumnValue(ColumnType.String, value);
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Can't use this type as index: " + type);
        }
    }
}