
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using RocksDbSharp;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.ObjectIds;
using Config = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.Storage;

public sealed class StorageManager
{
    private readonly RocksDb dbHandler;

    public StorageManager(RocksDb dbHandler)
    {
        this.dbHandler = dbHandler;
    }

    public byte[] Read(ObjectIdValue offset)
    {
        //(int, MemoryMappedViewAccessor) tablespace = await GetTablespace(offset);
        //tablespace.Item2.ReadArray<byte>(offset - tablespace.Item1, buffer, 0, length);

        byte[]? buffer = dbHandler.Get(offset.ToBytes());
        if (buffer is not null)
            return buffer;

        return new byte[Config.PageSize];
    }

    public void Write(ObjectIdValue offset, byte[] buffer)
    {
        //(int, MemoryMappedViewAccessor) tablespace = await GetTablespace(offset);
        //tablespace.Item2.WriteArray<byte>(offset - tablespace.Item1, buffer, 0, length);

        dbHandler.Put(offset.ToBytes(), buffer);
    }

    internal void Dispose()
    {
        //throw new NotImplementedException();
    }

    internal void WriteBatch(List<PageToWrite> pagesToWrite)
    {
        using WriteBatch batch = new();

        foreach (PageToWrite page in pagesToWrite)
            batch.Put(page.Offset.ToBytes(), page.Buffer);

        dbHandler.Write(batch);
    }
}
