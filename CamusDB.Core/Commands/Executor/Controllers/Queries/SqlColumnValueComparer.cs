
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

    public int GetHashCode(ColumnValue obj)
    {
        if (obj.Type == ColumnType.Null) return 0;
        return obj.Type switch
        {
            ColumnType.Integer64 => HashCode.Combine(obj.Type, obj.LongValue),
            ColumnType.Float64   => HashCode.Combine(obj.Type, obj.FloatValue),
            ColumnType.Float32   => HashCode.Combine(obj.Type, (float)obj.FloatValue),
            ColumnType.Bool      => HashCode.Combine(obj.Type, obj.BoolValue),
            ColumnType.String    => HashCode.Combine(obj.Type, obj.StrValue),
            ColumnType.Id        => HashCode.Combine(obj.Type, obj.StrValue),
            ColumnType.Date      => HashCode.Combine(obj.Type, obj.LongValue),
            ColumnType.DateTime  => HashCode.Combine(obj.Type, obj.LongValue),
            ColumnType.Bytes     => BytesHashCode(obj.BytesValue),
            _                    => HashCode.Combine(obj.Type),
        };
    }

    private static int BytesHashCode(byte[]? bytes)
    {
        HashCode h = new();
        h.AddBytes(bytes ?? []);
        return h.ToHashCode();
    }
}
