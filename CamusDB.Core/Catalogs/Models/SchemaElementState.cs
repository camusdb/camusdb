/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

public enum SchemaElementState
{
    Absent = 0,
    DeleteOnly = 1,
    WriteOnly = 2,
    Public = 3
}
