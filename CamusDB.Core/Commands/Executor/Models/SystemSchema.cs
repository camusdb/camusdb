﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class SystemSchema
{    
    public Dictionary<string, DatabaseTableObject> Tables { get; set; } = new();

    public Dictionary<string, DatabaseIndexObject> Indexes { get; set; } = new();
}
