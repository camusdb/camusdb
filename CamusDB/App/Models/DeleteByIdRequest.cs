
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Models;

public sealed class DeleteByIdRequest
{
    public string? DatabaseName { get; set; }

    public string? TableName { get; set; }

    public int Id { get; set; }
}
