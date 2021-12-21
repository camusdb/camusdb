
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using Config = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.Storage;

public class StorageManager : IDisposable
{
    private const int NumberOfTablespaces = 8;

    private readonly string path;

    private readonly string type;

    private readonly List<FileInfo> files;

    private int totalspaceSize = 0;    

    private readonly SemaphoreSlim semaphore = new(1, 1);

    private MemoryMappedFile[] memoryFiles = new MemoryMappedFile[NumberOfTablespaces];

    private MemoryMappedViewAccessor[] tablespaces = new MemoryMappedViewAccessor[NumberOfTablespaces];

    public StorageManager(string path, string type)
    {
        this.path = path;
        this.type = type;

        files = DataDirectory.GetFiles(path, type);
    }

    public async Task Initialize()
    {
        Console.WriteLine("{0} {1}", path, type);

        if (files.Count == 0)
            files.Add(await AddTablespace(path, type));

        if (files.Count > memoryFiles.Length)
        {
            int tablespaceOffsets = files.Count + (NumberOfTablespaces - files.Count % NumberOfTablespaces);
            Array.Resize<MemoryMappedFile>(ref memoryFiles, tablespaceOffsets);
            Array.Resize<MemoryMappedViewAccessor>(ref tablespaces, tablespaceOffsets);
        }

        // Calculate total available space
        totalspaceSize = tablespaces.Length * Config.TableSpaceSize;

        /*Console.WriteLine(numberTablespaces);
        Console.WriteLine(totalspaceSize);
        Console.WriteLine(totalspaceSize / Config.TableSpaceSize);
        Console.WriteLine((Config.TableSpaceSize + 1) / (float)totalspaceSize);*/

        int index = 0;

        for (int i = 0; i < files.Count; i++)
        {
            FileInfo file = files[i];

            MemoryMappedFile memoryFile = MemoryMappedFile.CreateFromFile(file.FullName, FileMode.Open);
            MemoryMappedViewAccessor accessor = memoryFile.CreateViewAccessor();

            memoryFiles[index] = memoryFile;
            tablespaces[index++] = accessor;
        }
    }

    private async Task<FileInfo> AddTablespace(string path, string type)
    {
        string next = DataDirectory.GetNextFile(files, type);

        string tablespacePath = Path.Combine(path, type + next);

        // @todo initialize the tablespace in a faster way
        await File.WriteAllBytesAsync(tablespacePath, new byte[Config.TableSpaceSize]);

        return new(tablespacePath);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetPageOffset(int offset)
    {
        int index = 0;

        for (int i = 0; i < totalspaceSize; i += Config.TableSpaceSize)
        {
            if (offset >= i && offset < (i + Config.TableSpaceSize))
                return index;

            index++;
        }

        return -1;
    }

    private async ValueTask<(int, MemoryMappedViewAccessor)> GetTablespace(int offset)
    {
        int index = GetPageOffset(offset);

        //Console.WriteLine("{0} {1} {2}", offset, index, Config.TableSpaceSize);

        if (index >= 0 && index < tablespaces.Length && tablespaces[index] != null)
            return (index * Config.TableSpaceSize, tablespaces[index]);

        try
        {
            await semaphore.WaitAsync();

            FileInfo file = await AddTablespace(path, type);
            files.Add(file);

            MemoryMappedFile memoryFile = MemoryMappedFile.CreateFromFile(file.FullName, FileMode.Open);
            MemoryMappedViewAccessor accessor = memoryFile.CreateViewAccessor();

            if (index < tablespaces.Length)
            {
                memoryFiles[index] = memoryFile;
                tablespaces[index] = accessor;
            }

            return (index * Config.TableSpaceSize, accessor);
        }
        finally
        {
            semaphore.Release();
        }
    }

    public async Task Read(int offset, byte[] buffer, int length)
    {
        (int, MemoryMappedViewAccessor) tablespace = await GetTablespace(offset);
        tablespace.Item2.ReadArray<byte>(offset - tablespace.Item1, buffer, 0, length);        
    }

    public async Task Write(int offset, byte[] buffer, int length)
    {
        (int, MemoryMappedViewAccessor) tablespace = await GetTablespace(offset);
        tablespace.Item2.WriteArray<byte>(offset - tablespace.Item1, buffer, 0, length);        
    }

    public void Flush()
    {
        for (int i = 0; i < tablespaces.Length; i++)
        {
            if (tablespaces[i] != null)
                tablespaces[i].Flush();
        }
    }

    public void Dispose()
    {
        for (int i = 0; i < tablespaces.Length; i++)
        {
            if (tablespaces[i] != null)
                tablespaces[i].Dispose();
        }

        foreach (MemoryMappedFile memoryFile in memoryFiles)
        {
            if (memoryFile != null)
                memoryFile.Dispose();
        }
    }
}
