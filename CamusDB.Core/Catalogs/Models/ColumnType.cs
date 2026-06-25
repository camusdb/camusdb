
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

// IMPORTANT: These integer values are persisted in schema JSON. Never renumber or reuse existing values.
// New members must be appended with new, previously-unused integers.
public enum ColumnType
{
    Null = 0,
    Id = 1,
    Integer64 = 2,
    String = 3,
    Bool = 4,
    Float64 = 5,
    Float32 = 6,
    Bytes = 7,
    Date = 8,
    DateTime = 9,
    Array = 10,
}
