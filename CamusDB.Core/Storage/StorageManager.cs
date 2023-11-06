
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using RocksDbSharp;
using Config = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.Storage;

public sealed class StorageManager
{   
    private readonly RocksDb dbHandler;    

    public StorageManager(RocksDb dbHandler)
    {
        this.dbHandler = dbHandler;        
    }

    public byte[] Read(int offset)
    {
        //(int, MemoryMappedViewAccessor) tablespace = await GetTablespace(offset);
        //tablespace.Item2.ReadArray<byte>(offset - tablespace.Item1, buffer, 0, length);

        byte[]? buffer = dbHandler.Get(BitConverter.GetBytes(offset));
        if (buffer is not null)
            return buffer;

        return new byte[Config.PageSize];
    }

    public void Write(int offset, byte[] buffer)
    {
        //(int, MemoryMappedViewAccessor) tablespace = await GetTablespace(offset);
        //tablespace.Item2.WriteArray<byte>(offset - tablespace.Item1, buffer, 0, length);

        dbHandler.Put(BitConverter.GetBytes(offset), buffer);
    }        
}
