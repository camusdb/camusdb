
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using Kommander.Time;

namespace CamusDB.App.Models;

/// <summary>
/// Output column descriptor included in every row-producing query response.
/// The <see cref="Type"/> integer matches the <see cref="ColumnType"/> enum value so clients
/// can decode positional row values without per-cell type information.
/// </summary>
public sealed class ColumnSchemaDto
{
    public string Name { get; set; } = "";
    public ColumnType Type { get; set; }
}

public sealed class ExecuteSQLQueryResponse
{
    public string Status { get; set; }

    public int Total { get; set; }

    /// <summary>
    /// Ordered output column schema. Always present for row-producing results, even when
    /// <see cref="Rows"/> is empty. Clients use this to decode rows positionally.
    /// </summary>
    public List<ColumnSchemaDto> Columns { get; set; } = [];

    /// <summary>
    /// Positional rows: <c>Rows[r][c]</c> is the compact-raw value for <c>Columns[c]</c> in row
    /// <c>r</c>. Values are JSON-native (number/string/bool/null) for scalar types; Bytes is
    /// base64; Date/DateTime are raw <see cref="DateTime.Ticks"/>; Uuid is <c>[high, low]</c>;
    /// Array is a JSON array of element values.
    /// </summary>
    public List<object?[]> Rows { get; set; } = [];

    public string? Code { get; set; }

    public string? Message { get; set; }

    public HLCTimestamp? CausalToken { get; set; }

    /// <summary>
    /// Total server-side processing time for this request in milliseconds — measured from the
    /// moment the controller began handling it (request-body parse, SQL parse, execution, row
    /// materialization, and commit) until the response was built. Excludes network transit and
    /// client time, so a large gap between this and the client's observed latency isolates
    /// network/connection overhead.
    /// </summary>
    public double? ServerTimeMs { get; set; }

    /// <summary>
    /// Cache resolution for this query: <c>hit</c>, <c>miss</c>, <c>bypass</c>,
    /// <c>stale-revalidated</c>, or <c>evicted-before-publish</c>.
    /// Null when the query did not carry a cache hint.
    /// </summary>
    public string? CacheStatus { get; set; }

    /// <summary>
    /// Why the cache was bypassed or the entry was not published.
    /// Null when <see cref="CacheStatus"/> is not <c>bypass</c> or <c>evicted-before-publish</c>.
    /// </summary>
    public string? CacheBypassReason { get; set; }

    /// <summary>HLC timestamp at which the cached entry was computed. Non-null only on a hit.</summary>
    public HLCTimestamp? CachedAtHlc { get; set; }

    /// <summary>Approximate age of the cache entry in milliseconds. Non-null only on a hit.</summary>
    public long? AgeMs { get; set; }

    /// <summary>Logical cache family name from the query hint. Non-null when the cache path was entered.</summary>
    public string? CacheName { get; set; }

    public ExecuteSQLQueryResponse(string status, int total, List<ColumnSchemaDto> columns, List<object?[]> rows)
    {
        Status = status;
        Total = total;
        Columns = columns;
        Rows = rows;
    }

    public ExecuteSQLQueryResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}
