
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Attributes;

namespace CamusDB.Core.Journal.Models.Logs;

[JournalSerializable(JournalLogTypes.WritePage)]
public sealed class WritePageLog : IJournalLog
{
    [JournalField(0)]
    public uint Sequence { get; }

    [JournalField(0)]
    public uint SubSequence { get; }

    [JournalField(1)]
    public byte[] Data { get; }

    public WritePageLog(uint sequence, uint subSequence, byte[] data)
    {
        Sequence = sequence;
        SubSequence = subSequence;
        Data = data;
    }
}

