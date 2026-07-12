
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Helpers for turning a parsed index column list (<see cref="ColumnIndexInfo"/>, which carries a
/// per-column ASC/DESC <see cref="OrderType"/>) into the direction vector persisted on
/// <see cref="TableIndexSchema.ColumnDirections"/>.
///
/// Descending is honored by the key encoder for types with a fixed-width, self-delimiting body
/// (Integer64, Float64, Float32, Bool, Date, DateTime, Uuid, Id) — a descending column simply inverts
/// its field's encoded ordinal order.
///
/// String and Bytes do not yet have a descending encoding: they encode through a prefix-free,
/// terminator-delimited form whose terminator must sort before content for ascending
/// prefix-correctness, which a naive descending inversion would break — so a descending column of one
/// of these types is rejected at DDL time by <see cref="RejectDescendingOnUnsupportedType"/>.
/// </summary>
internal static class IndexColumnOrder
{
    /// <summary>
    /// Projects the per-column <see cref="OrderType"/> into a direction vector aligned with the
    /// index's column list. Returns null when every column is ascending so the persisted form
    /// stays in its compact, backward-compatible all-ascending shape.
    /// </summary>
    internal static OrderType[]? Extract(ReadOnlySpan<ColumnIndexInfo> columns)
    {
        bool anyDescending = false;
        OrderType[] directions = new OrderType[columns.Length];

        for (int i = 0; i < columns.Length; i++)
        {
            directions[i] = columns[i].Order;
            if (columns[i].Order == OrderType.Descending)
                anyDescending = true;
        }

        return anyDescending ? directions : null;
    }

    /// <summary>
    /// Returns true when descending index encoding is supported for <paramref name="type"/>. Types
    /// with a fixed-width, self-delimiting key body qualify; the terminator-delimited types
    /// String/Bytes do not (see the class summary).
    /// </summary>
    internal static bool SupportsDescending(ColumnType type)
        => type is not (ColumnType.String or ColumnType.Bytes or ColumnType.Array);

    /// <summary>
    /// Throws <see cref="CamusDBException"/> if any column requests descending order on a type whose
    /// descending encoding is not yet supported (String/Bytes). Called at every DDL entry point
    /// that creates an index. <paramref name="resolveType"/> returns the column's declared type, or
    /// null when the column is unknown (its non-existence is reported by the caller's own validation).
    /// </summary>
    internal static void RejectDescendingOnUnsupportedType(
        ReadOnlySpan<ColumnIndexInfo> columns,
        string indexName,
        Func<string, ColumnType?> resolveType)
    {
        foreach (ColumnIndexInfo column in columns)
        {
            if (column.Order != OrderType.Descending)
                continue;

            ColumnType? type = resolveType(column.Name);
            if (type is not null && !SupportsDescending(type.Value))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Descending index columns are not supported for type {type} (column '{column.Name}' in index '{indexName}')"
                );
        }
    }
}
