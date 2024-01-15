
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Models;

public sealed class CreateTableColumn
{
    public string? Name { get; set; }

    public string? Type { get; set; }

    public bool NotNull { get; set; }

    public ColumnValue? DefaultValue { get; set; }
}
