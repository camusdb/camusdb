
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using CamusDB.Core.Serializer;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Journal.Models.Logs;
using Config = CamusDB.Core.CamusDBConfig;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Serializer.Models;

namespace CamusDB.Core.Journal;

public sealed class JournalManager
{
    public JournalWriter Writer { get; }

    public JournalManager(string databaseName)
    {
        Writer = new JournalWriter(databaseName);
    }
}
