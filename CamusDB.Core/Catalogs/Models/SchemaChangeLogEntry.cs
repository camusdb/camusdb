
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.Core.Catalogs.Models;

public sealed class SchemaChangeLogEntry
{
    public HLCTimestamp Ts { get; set; }

    public string Database { get; set; } = "";

    public long FromVersion { get; set; }

    public long ToVersion { get; set; }

    public SchemaOp Op { get; set; }

    public byte[] Payload { get; set; } = [];
}
