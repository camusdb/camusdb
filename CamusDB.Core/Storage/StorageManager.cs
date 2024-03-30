
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using RocksDbSharp;
using CamusDB.Core.BufferPool.Models;
using CamusDB.Core.Util.ObjectIds;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.Storage;

/// <summary>
/// The StorageManager is an abstraction that allows communication with the active storage engine.
/// At this moment, only RocksDb is available, but other storage engines could be implemented in the future.
/// </summary>
public sealed class StorageManager
{
    private static readonly object _lock = new();

    private readonly RocksDb dbHandler;

    public StorageManager(string name)
    {
        string path = Path.Combine(CamusConfig.DataDirectory, name);

        DbOptions options = new DbOptions()
                                .SetCreateIfMissing(true)
                                .SetWalDir(path) // using WAL
                                .SetWalRecoveryMode(Recovery.AbsoluteConsistency) // setting recovery mode to Absolute Consistency
                                .SetAllowConcurrentMemtableWrite(true);

        this.dbHandler = RocksDb.Open(options, path);
    }

    public StorageManager(RocksDb dbHandler)
    {
        this.dbHandler = dbHandler;
    }

    public void Put(byte[] key, byte[] value)
    {
        dbHandler.Put(key, value);
    }

    public byte[]? Get(byte[] key)
    {
        return dbHandler.Get(key);
    }

    public byte[] Read(ObjectIdValue offset)
    {
        //(int, MemoryMappedViewAccessor) tablespace = await GetTablespace(offset);
        //tablespace.Item2.ReadArray<byte>(offset - tablespace.Item1, buffer, 0, length);

        byte[]? buffer = dbHandler.Get(offset.ToBytes());
        if (buffer is not null)
            return buffer;

        return new byte[CamusConfig.PageSize];
    }

    public void Write(ObjectIdValue offset, byte[] buffer)
    {
        //(int, MemoryMappedViewAccessor) tablespace = await GetTablespace(offset);
        //tablespace.Item2.WriteArray<byte>(offset - tablespace.Item1, buffer, 0, length);

        dbHandler.Put(offset.ToBytes(), buffer);
    }

    internal void WriteBatch(List<BufferPageOperation> pageOperations)
    {
        using WriteBatch batch = new();

        //File.AppendAllText("c:\\tmp\\data.txt", $"{pageOperations.Count}\n");
        //System.IO.File.AppendAllText("/tmp/a.txt", $"{pageOperations.Count}\n");

        foreach (BufferPageOperation pageOperation in pageOperations)
        {
            byte[] offset = pageOperation.Offset.ToBytes();

            if (pageOperation.Operation == BufferPageOperationType.InsertOrUpdate)
                batch.Put(offset, pageOperation.Buffer);
            else
                batch.Delete(offset);

            //File.AppendAllText("c:\\tmp\\data.txt", $"{pageOperation.Operation}, {pageOperation.Offset}, {pageOperation.Buffer.Length}\n");
            //System.IO.File.AppendAllText("/tmp/a.txt", $"{pageOperation.Operation}, {pageOperation.Offset}, {pageOperation.Buffer.Length}\n");
        }

        dbHandler.Write(batch);
    }

    internal void Delete(ObjectIdValue offset)
    {
        dbHandler.Remove(offset.ToBytes());
    }

    internal void Dispose()
    {
        dbHandler.Dispose();
    }
}
