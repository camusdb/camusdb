﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class DatabaseCloser
{
    private readonly DatabaseDescriptors databaseDescriptors;

    public DatabaseCloser(DatabaseDescriptors databaseDescriptors)
    {
        this.databaseDescriptors = databaseDescriptors;
    }

    public async ValueTask Close(string name)
    {
        if (!databaseDescriptors.Descriptors.TryGetValue(name, out DatabaseDescriptor? databaseDescriptor))
            return;

        try
        {
            await databaseDescriptors.Semaphore.WaitAsync();

            if (databaseDescriptor.TableSpace is not null)
                databaseDescriptor.TableSpace.Dispose();

            if (databaseDescriptor.SchemaSpace is not null)
                databaseDescriptor.SchemaSpace.Dispose();

            if (databaseDescriptor.SystemSpace is not null)
                databaseDescriptor.SystemSpace.Dispose();

            databaseDescriptors.Descriptors.Remove(name);

            File.Delete(Path.Combine(CamusDBConfig.DataDirectory, name, "camus.lock"));

            Console.WriteLine("Database {0} closed", name);
        }
        finally
        {
            databaseDescriptors.Semaphore.Release();
        }
    }
}

