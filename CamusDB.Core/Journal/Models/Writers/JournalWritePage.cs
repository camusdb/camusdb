
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Journal.Models.Writers;

public sealed class JournalWritePage
{
    public uint Sequence { get; }

    public byte[] Data { get; }

    public JournalWritePage(uint sequence, byte[] data)
    {
        Sequence = sequence;
        Data = data;
    }
}

