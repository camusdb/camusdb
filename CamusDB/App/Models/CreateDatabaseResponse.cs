
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

public sealed class CreateDatabaseResponse
{
    public string Status { get; set; }

    public CreateDatabaseResponse(string status)
    {
        Status = status;
    }
}
