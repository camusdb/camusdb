
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class ConstraintInfo
{    
    public ConstraintType Type { get; }

    public string Name { get; }

    public ColumnIndexInfo[] Columns { get; }

    public ConstraintInfo(ConstraintType type, string name, ColumnIndexInfo[] columns)
	{
        Name = name;
        Type = type;
        Columns = columns;
	}
}
