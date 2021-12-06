﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Serializer;
using CamusDB.Core.Util.Trees;
using CamusDB.Core.Journal.Models;
using CamusDB.Core.Serializer.Models;

namespace CamusDB.Core.Journal.Controllers.Readers;

public static class InsertTicketReader
{
    public static int Deserialize(uint sequence, FileStream journal)
    {
        return 0;
    }
}