
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

public enum SchemaOp
{
    CreateTable = 0,
    DropTable = 1,
    AddColumn = 2,
    DropColumn = 3,
    AddIndex = 4,
    DropIndex = 5,
    SetElementState = 6
}
