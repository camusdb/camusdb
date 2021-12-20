
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO.MemoryMappedFiles;

namespace CamusDB.Core.Storage;

public class StorageManager : IDisposable
{
	private readonly MemoryMappedFile memoryFile;

	private readonly MemoryMappedViewAccessor accessor;

	public StorageManager(MemoryMappedFile memoryFile)
	{
		this.memoryFile = memoryFile;
		this.accessor = memoryFile.CreateViewAccessor();
	}

	public void Read(int offset, byte[] buffer, int length)
    {
		accessor.ReadArray<byte>(offset, buffer, 0, length);
	}

	public void Write(int offset, byte[] buffer, int length)
    {
		accessor.WriteArray<byte>(offset, buffer, 0, length);
	}

	public void Flush()
    {
		accessor.Flush();
	}

	public void Dispose()
	{
		if (accessor != null)
			accessor.Dispose();

		if (memoryFile != null)
			memoryFile.Dispose();
	}
}
