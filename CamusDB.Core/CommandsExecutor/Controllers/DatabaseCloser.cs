
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.BufferPool;
using CamusDB.Core.Serializer;
using System.IO.MemoryMappedFiles;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.BufferPool.Models;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class DatabaseCloser
{
    public async ValueTask Close(string name)
    {
        if (!DatabaseDescriptors.Descriptors.TryGetValue(name, out DatabaseDescriptor? databaseDescriptor))
            return;

        try
        {
            await DatabaseDescriptors.Semaphore.WaitAsync();

            if (databaseDescriptor.TableSpace is not null)
                databaseDescriptor.TableSpace.Dispose();

            if (databaseDescriptor.SchemaSpace is not null)
                databaseDescriptor.SchemaSpace.Dispose();

            if (databaseDescriptor.SystemSpace is not null)
                databaseDescriptor.SystemSpace.Dispose();

            DatabaseDescriptors.Descriptors.Remove(name);                                

            Console.WriteLine("Database {0} closed", name);            
        }
        finally
        {
            DatabaseDescriptors.Semaphore.Release();
        }
    }
}

