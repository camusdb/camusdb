
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.Data.Sqlite;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.BufferPool.Models;
using CamusConfig = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.Storage;

/// <summary>
/// The StorageManager is an abstraction that allows communication with the active storage engine.
/// At this moment, only RocksDb is available, but other storage engines could be implemented in the future.
/// </summary>
public sealed class StorageManager : IDisposable
{
    private const string selectQuery = "SELECT value FROM storage WHERE key = @key";
    
    private const string insertQuery = "INSERT INTO storage (key, value) VALUES (@key, @value) ON CONFLICT(key) DO UPDATE SET value=excluded.value";
    
    private const string deleteQuery = "DELETE FROM storage WHERE key = @key";
    
    private readonly object _lock = new();
    
    private readonly string name;
    
    private SqliteConnection? connection;

    public StorageManager(string name)
    {
        this.name = name;
    }

    private void TryOpenDatabase()
    {
        if (connection is not null)
            return;

        lock (_lock)
        {
            if (connection is not null)
                return;
            
            string path = Path.Combine(CamusConfig.DataDirectory, name);
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            string connectionString = $"Data Source={path}/database.db";
            connection = new(connectionString);

            connection.Open();

            const string createTableQuery = "CREATE TABLE IF NOT EXISTS storage (key TEXT COLLATE BINARY PRIMARY KEY, value TEXT COLLATE BINARY);";
            using SqliteCommand command1 = new(createTableQuery, connection);
            command1.ExecuteNonQuery();
            
            const string enableWalQuery = "PRAGMA journal_mode=WAL;";
            using SqliteCommand command2 = new(enableWalQuery, connection);
            command2.ExecuteNonQuery();
            
            const string enableSynchronousQuery = "PRAGMA synchronous=NORMAL;";
            using SqliteCommand command3 = new(enableSynchronousQuery, connection);
            command3.ExecuteNonQuery();
            
            const string tempStoreQuery = "PRAGMA temp_store=MEMORY;";
            using SqliteCommand command4 = new(tempStoreQuery, connection);
            command4.ExecuteNonQuery();
            
            const string checpointQuery = "PRAGMA wal_checkpoint(FULL);";
            using SqliteCommand command6 = new(checpointQuery, connection);
            command6.ExecuteNonQuery();
            
            //const string vacuumQuery = "VACUUM;";
            //using SqliteCommand command5 = new(vacuumQuery, connection);
            //command5.ExecuteNonQuery();
        }
    }

    public void Put(string key, byte[] value)
    {
        TryOpenDatabase();
        
        using SqliteCommand command = new(insertQuery, connection);
        
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@value", Convert.ToBase64String(value));
        
        command.ExecuteNonQuery();
    }

    public byte[]? Get(string key)
    {
        TryOpenDatabase();
        
        using SqliteCommand command = new(selectQuery, connection);
        
        command.Parameters.AddWithValue("@key", key);

        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            string retrievedData = (string)reader["value"];
            return Convert.FromBase64String(retrievedData);
        }

        return null;
    }

    public byte[] Read(ObjectIdValue offset)
    {
        TryOpenDatabase();
        
        using SqliteCommand command = new(selectQuery, connection);
        
        command.Parameters.AddWithValue("@key", offset.ToString());

        using SqliteDataReader reader = command.ExecuteReader();
        while (reader.Read())
        {
            string retrievedData = (string)reader["value"];
            return Convert.FromBase64String(retrievedData);
        }

        return new byte[CamusConfig.PageSize];
    }

    public void Write(ObjectIdValue offset, ReadOnlySpan<byte> buffer)
    {
        TryOpenDatabase();
        
        using SqliteCommand command = new(insertQuery, connection);
        
        command.Parameters.AddWithValue("@key", offset.ToString());
        command.Parameters.AddWithValue("@value", Convert.ToBase64String(buffer));
        
        command.ExecuteNonQuery();
    }

    internal void WriteBatch(List<BufferPageOperation> pageOperations)
    {
        TryOpenDatabase();
        
        using SqliteTransaction transaction = connection!.BeginTransaction();
        
        using SqliteCommand insertCommand =  new(insertQuery, connection);
        insertCommand.Transaction = transaction;
        
        using SqliteCommand deleteCommand =  new(deleteQuery, connection);
        deleteCommand.Transaction = transaction;

        foreach (BufferPageOperation pageOperation in pageOperations)
        {
            ObjectIdValue offset = pageOperation.Offset;

            if (pageOperation.Operation == BufferPageOperationType.InsertOrUpdate)
            {
                insertCommand.Parameters.Clear();
                insertCommand.Parameters.AddWithValue("@key", offset.ToString());
                insertCommand.Parameters.AddWithValue("@value", Convert.ToBase64String(pageOperation.Buffer.AsSpan()));
                insertCommand.ExecuteNonQuery();
            }
            else
            {
                deleteCommand.Parameters.Clear();
                deleteCommand.Parameters.AddWithValue("@key", offset.ToString());
                deleteCommand.ExecuteNonQuery();
            }
        }
        
        transaction.Commit();
    }

    internal void Delete(ObjectIdValue offset)
    {
        using SqliteCommand deleteCommand =  new(deleteQuery, connection);
        deleteCommand.Parameters.AddWithValue("@key", offset.ToString());
        deleteCommand.ExecuteNonQuery();
    }

    public void Dispose()
    {
        lock (_lock)
        {
            connection?.Dispose();
            connection = null;
        }
    }
}
