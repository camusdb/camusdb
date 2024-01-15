
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class ColumnIndexInfo
{
    public string Name { get; }

    public OrderType Order { get; }

    public ColumnIndexInfo(string name, OrderType order)
	{
        Name = name;
        Order = order;
	}
}
