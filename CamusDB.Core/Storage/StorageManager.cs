
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.IO;
using System.IO.MemoryMappedFiles;
using Config = CamusDB.Core.CamusDBConfig;

namespace CamusDB.Core.Storage;

public class StorageManager : IDisposable
{
	private readonly string path;

	private readonly string type;

	private readonly List<MemoryMappedFile> memoryFiles = new();

	private readonly List<MemoryMappedViewAccessor> accessors = new();

	public StorageManager(string path, string type)
	{
		this.path = path;
		this.type = type;
	}

	public async Task Initialize()
    {
		Console.WriteLine("{0} {1}", path, type);

		List<FileInfo> files = DataDirectory.GetFiles(path, type);

		if (files.Count == 0)
		{
			int next = DataDirectory.GetNextFile(files, type);
			await File.WriteAllBytesAsync(Path.Combine(path, type + next), new byte[Config.TableSpaceSize]);
			files = DataDirectory.GetFiles(path, type);
		}

		foreach (FileInfo file in files)
		{
			MemoryMappedFile memoryFile = MemoryMappedFile.CreateFromFile(file.FullName, FileMode.Open);
			MemoryMappedViewAccessor accessor = memoryFile.CreateViewAccessor();

			memoryFiles.Add(memoryFile);
			accessors.Add(accessor);
		}		
	}

	public void Read(int offset, byte[] buffer, int length)
    {
		foreach (MemoryMappedViewAccessor accessor in accessors)
			accessor.ReadArray<byte>(offset, buffer, 0, length);
	}

	public void Write(int offset, byte[] buffer, int length)
    {
		foreach (MemoryMappedViewAccessor accessor in accessors)
			accessor.WriteArray<byte>(offset, buffer, 0, length);
	}

	public void Flush()
    {
		foreach (MemoryMappedViewAccessor accessor in accessors)
			accessor.Flush();
	}

	public void Dispose()
	{
		foreach (MemoryMappedViewAccessor accessor in accessors)
		{
			if (accessor != null)
				accessor.Dispose();
		}

		foreach (MemoryMappedFile memoryFile in memoryFiles)
		{
			if (memoryFile != null)
				memoryFile.Dispose();
		}
	}
}
