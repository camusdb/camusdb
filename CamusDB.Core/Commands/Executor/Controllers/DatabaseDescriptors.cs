
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using System.Collections.Concurrent;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class DatabaseDescriptors
{    
    public readonly ConcurrentDictionary<string, AsyncLazy<DatabaseDescriptor>> Descriptors = new();
}
