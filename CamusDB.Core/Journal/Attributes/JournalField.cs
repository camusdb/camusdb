
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using SystemAttribute = System.Attribute;

namespace CamusDB.Core.Journal.Attributes;

[AttributeUsage(AttributeTargets.Property)]
public class JournalField : SystemAttribute
{
    public int Number { get; }

    public JournalField(int number)
    {
        Number = number;
    }
}

