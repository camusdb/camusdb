
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Estimates the serialized byte-width of a row for <c>NetworkFactor</c> cost computation.
///
/// Widths are rough per-type constants intended to give the cost model a meaningful
/// ordering: wide rows (large strings/bytes columns) cost more to ship than narrow rows.
/// Exact sizes depend on column values and the Kahuna KV serialisation format; this
/// estimator uses fixed per-type averages, not measured samples.
///
/// Default row width when schema is unavailable: <see cref="DefaultRowWidthBytes"/>.
/// </summary>
internal static class RowWidthEstimator
{
    /// <summary>Fixed overhead per row: row-ID, column-count tag, version tag.</summary>
    private const int RowOverheadBytes = 20;

    /// <summary>Fallback when schema is null or has no columns.</summary>
    internal const int DefaultRowWidthBytes = 100;

    // Per-type average encoded byte sizes.
    private const int IdBytes       = 12;   // 12-byte BSON ObjectId
    private const int Int64Bytes    = 8;
    private const int Float64Bytes  = 8;
    private const int Float32Bytes  = 4;
    private const int BoolBytes     = 1;
    private const int DateBytes     = 8;    // stored as int64 (days / ms)
    private const int DateTimeBytes = 8;
    private const int StringAvgBytes = 64;  // average short-to-medium string (UTF-8)
    private const int BytesAvgBytes  = 128; // average bytes payload
    private const int ArrayAvgBytes  = 64;  // rough estimate for scalar arrays

    /// <summary>
    /// Estimates the average serialised width (bytes) of one row of <paramref name="table"/>.
    /// Returns <see cref="DefaultRowWidthBytes"/> when the schema is unavailable.
    /// </summary>
    public static int Estimate(TableDescriptor? table)
    {
        if (table?.Schema?.Columns is not { Count: > 0 } cols)
            return DefaultRowWidthBytes;

        int total = RowOverheadBytes;
        foreach (TableColumnSchema col in cols)
        {
            if (col.State != SchemaElementState.Public)
                continue;

            total += col.Type switch
            {
                ColumnType.Id       => IdBytes,
                ColumnType.Integer64 => Int64Bytes,
                ColumnType.Float64  => Float64Bytes,
                ColumnType.Float32  => Float32Bytes,
                ColumnType.Bool     => BoolBytes,
                ColumnType.Date     => DateBytes,
                ColumnType.DateTime => DateTimeBytes,
                ColumnType.String   => EstimateStringWidth(col.MaxLength),
                ColumnType.Bytes    => EstimateBytesWidth(col.MaxLength),
                ColumnType.Array    => ArrayAvgBytes,
                _                   => 0,
            };
        }

        return Math.Max(total, 1);
    }

    private static int EstimateStringWidth(int? maxLength)
    {
        // Heuristic: average fill is ~25% of max, capped at StringAvgBytes.
        if (maxLength is null or <= 0)
            return StringAvgBytes;
        return Math.Min((int)Math.Ceiling(maxLength.Value * 0.25), StringAvgBytes);
    }

    private static int EstimateBytesWidth(int? maxLength)
    {
        // Cap at BytesAvgBytes regardless of MaxLength — a 10 MB column and a 128-byte column
        // are indistinguishable here. When TOAST out-of-line storage lands the in-row cost
        // drops to a fixed pointer size; update this estimate at that point.
        if (maxLength is null or <= 0)
            return BytesAvgBytes;
        return Math.Min((int)Math.Ceiling(maxLength.Value * 0.25), BytesAvgBytes);
    }
}
