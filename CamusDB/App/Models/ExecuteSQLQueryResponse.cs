
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using Kommander.Time;

namespace CamusDB.App.Models;

public sealed class ExecuteSQLQueryResponse
{
    public string Status { get; set; }

    public int Total { get; set; }

    public List<Dictionary<string, ColumnValue>>? Rows { get; set; }

    public string? Code { get; set; }

    public string? Message { get; set; }

    public HLCTimestamp? CausalToken { get; set; }

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

    public ExecuteSQLQueryResponse(string status, int total, List<Dictionary<string, ColumnValue>> rows)
    {
        Status = status;
        Total = total;
        Rows = rows;
    }

    public ExecuteSQLQueryResponse(string status, string code, string message)
    {
        Status = status;
        Code = code;
        Message = message;
    }
}
