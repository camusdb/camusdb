
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.Ttl;

/// <summary>
/// Decides whether one row has expired, against a run's fixed horizon.
///
/// <para><b>The horizon comes from the HLC, not the wall clock.</b> Whether a row is past its deadline is
/// a cluster-wide decision made concurrently on several nodes, and two nodes' wall clocks disagree. The
/// run captures one HLC instant and every worker tests against that same value, so no two workers can
/// reach opposite conclusions about a row on their shared span boundary.</para>
///
/// <para><b>NULL never expires.</b> That is the standard choice and it is also the useful one: it gives
/// callers an explicit "keep this forever" value that needs no separate flag column. A row with no
/// expiry instant has not failed to have one — it has declined to have one.</para>
///
/// <para><b>The grace period is subtracted from the horizon, not added to the row.</b> Same arithmetic,
/// but it keeps the row's stored value untouched in the comparison, so a row is judged only against
/// numbers the run already fixed.</para>
/// </summary>
internal static class TtlExpiryPredicate
{
    /// <summary>
    /// Converts a run horizon and grace period into the cutoff instant, in Unix epoch milliseconds.
    /// Rows whose expiry value is strictly less than this are eligible.
    /// </summary>
    public static long CutoffEpochMs(HLCTimestamp horizon, long graceMs) => horizon.L - graceMs;

    /// <summary>
    /// Whether <paramref name="row"/> is expired at <paramref name="cutoffEpochMs"/>.
    ///
    /// <para>Returns false — never throws — when the column is absent, NULL, or of an unexpected type.
    /// Configuration is validated at <c>ALTER TABLE</c> time, so a surprise here means the schema changed
    /// underneath a running sweep; refusing to delete is the only safe reading of an ambiguous row, and a
    /// background job must not take the whole span down over one row it cannot interpret.</para>
    /// </summary>
    public static bool IsExpired(Dictionary<string, ColumnValue> row, string expirationColumn, long cutoffEpochMs)
    {
        if (!row.TryGetValue(expirationColumn, out ColumnValue? value) || value is null)
            return false;

        return value.Type switch
        {
            // Date and DateTime hold UTC DateTime.Ticks; Integer64 holds epoch milliseconds directly.
            ColumnType.Date or ColumnType.DateTime => TicksToEpochMs(value.LongValue) < cutoffEpochMs,
            ColumnType.Integer64 => value.LongValue < cutoffEpochMs,
            _ => false, // includes ColumnType.Null — "keep forever"
        };
    }

    private const long TicksPerMillisecond = 10_000L;
    private static readonly long UnixEpochTicks = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

    private static long TicksToEpochMs(long ticks) => (ticks - UnixEpochTicks) / TicksPerMillisecond;
}
