﻿
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

public readonly struct QueryOrderBy
{
    public string ColumnName { get; }

    public OrderType Type { get; }

    public QueryOrderBy(string columnName, OrderType type)
	{
        ColumnName = columnName;
        Type = type;
	}
}

