
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Journal.Models;

namespace CamusDB.Core.Journal.Attributes;

[AttributeUsage(AttributeTargets.Class)]
public sealed class JournalSerializable : Attribute
{
    public JournalLogTypes Type { get; }

    public JournalSerializable(JournalLogTypes type)
    {
        Type = type;
    }
}