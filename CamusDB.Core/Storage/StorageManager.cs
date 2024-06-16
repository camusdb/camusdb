
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
        
        string path = Path.Combine(CamusConfig.DataDirectory, name);
        if (!Directory.Exists(path))
            Directory.CreateDirectory(path);

        string connectionString = $"Data Source={path}/database.db";
        connection = new(connectionString);
        
        //Console.WriteLine(connectionString);
        
        connection.Open();
        
        const string createTableQuery = "CREATE TABLE IF NOT EXISTS storage (id INTEGER PRIMARY KEY, key VARCHAR(32), value TEXT);";
        using SqliteCommand command1 = new(createTableQuery, connection);
        command1.ExecuteNonQuery();
        
        const string createIndexQuery = "CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_key ON storage(key);";
        using SqliteCommand command2 = new(createIndexQuery, connection);
        command2.ExecuteNonQuery();
    }

    public void Put(string key, byte[] value)
    {
        TryOpenDatabase();
        
        const string insertQuery = "INSERT INTO storage (key, value) VALUES (@key, @value) ON CONFLICT(key) DO UPDATE SET value=excluded.value";
        using SqliteCommand command = new(insertQuery, connection);
        
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@value", Convert.ToBase64String(value));
        
        command.ExecuteNonQuery();
    }

    public byte[]? Get(string key)
    {
        TryOpenDatabase();
        
        const string selectQuery = "SELECT value FROM storage WHERE key = @key";
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
        
        const string selectQuery = "SELECT value FROM storage WHERE key = @key";
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

    public void Write(ObjectIdValue offset, byte[] buffer)
    {
        TryOpenDatabase();
        
        const string insertQuery = "INSERT INTO storage (key, value) VALUES (@key, @value) ON CONFLICT(key) DO UPDATE SET value=excluded.value";
        using SqliteCommand command = new(insertQuery, connection);
        
        command.Parameters.AddWithValue("@key", offset.ToString());
        command.Parameters.AddWithValue("@value", Convert.ToBase64String(buffer));
        
        command.ExecuteNonQuery();
    }

    internal void WriteBatch(List<BufferPageOperation> pageOperations)
    {
        TryOpenDatabase();
        
        using SqliteTransaction transaction = connection!.BeginTransaction();
        
        const string insertQuery = "INSERT INTO storage (key, value) VALUES (@key, @value) ON CONFLICT(key) DO UPDATE SET value=excluded.value";
        using SqliteCommand insertCommand =  new(insertQuery, connection);
        insertCommand.Transaction = transaction;
        
        const string deleteQuery = "DELETE FROM storage WHERE key = @key";
        using SqliteCommand deleteCommand =  new(deleteQuery, connection);
        deleteCommand.Transaction = transaction;

        foreach (BufferPageOperation pageOperation in pageOperations)
        {
            ObjectIdValue offset = pageOperation.Offset;

            if (pageOperation.Operation == BufferPageOperationType.InsertOrUpdate)
            {
                insertCommand.Parameters.Clear();
                insertCommand.Parameters.AddWithValue("@key", offset.ToString());
                insertCommand.Parameters.AddWithValue("@value", Convert.ToBase64String(pageOperation.Buffer));
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
        //dbHandler.Remove(offset.ToBytes());
    }

    public void Dispose()
    {
        connection?.Dispose();
        connection = null;
    }
}
