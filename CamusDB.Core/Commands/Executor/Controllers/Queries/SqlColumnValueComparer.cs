
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// SQL-aware equality comparer for <see cref="ColumnValue"/>, consistent with
/// <see cref="ColumnValue.CompareTo"/> semantics:
/// - NULL is never equal to anything (SQL three-valued logic).
/// - Cross-type values are never equal (strict type matching, matching CompareTo behaviour).
/// - Float32 equality is computed at float (single) precision to match CompareTo.
/// - String/Id are ordinal, consistent with the key-encoding invariant.
/// </summary>
public sealed class SqlColumnValueComparer : IEqualityComparer<ColumnValue>
{
    public static readonly SqlColumnValueComparer Instance = new();

    public bool Equals(ColumnValue? x, ColumnValue? y)
    {
        if (x is null) return y is null;
        if (y is null) return false;
        if (x.Type == ColumnType.Null || y.Type == ColumnType.Null) return false;
        if (x.Type != y.Type) return false;
        try { return x.CompareTo(y) == 0; }
        catch (ArgumentException) { return false; }
    }

    /// <summary>
    /// <see cref="ColumnValueHash"/> of the value. A NULL hashes as 0; it never equals anything, so
    /// its bucket does not matter.
    /// </summary>
    public int GetHashCode(ColumnValue obj)
        => obj.Type == ColumnType.Null ? 0 : ColumnValueHash.Of(obj);
}
