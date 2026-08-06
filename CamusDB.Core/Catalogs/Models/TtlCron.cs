
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Interprets the <c>ttl_job_cron</c> storage parameter: how often a table's TTL sweep runs.
///
/// <para><b>Why a cron field and not an interval.</b> The parameter name, its accepted values and its
/// <c>@daily</c> default are CockroachDB's, so a user who knows CRDB's row-level TTL can configure
/// CamusDB's without relearning it. CockroachDB accepts full 5-field CRON expressions; this
/// implementation accepts only the <c>@macro</c> forms and rejects everything else with a message that
/// says so. That is a deliberate subset, not a parse failure: the accepted vocabulary can widen later
/// without renaming the parameter or changing what an existing value means.</para>
///
/// <para><b>Cadence, not a wall-clock schedule.</b> A macro is resolved to an interval and the sweep is
/// paced by that interval — <c>@daily</c> means "about once a day", not "at 00:00". Aligning to real
/// calendar boundaries would mean ordering cluster-wide work by wall clock, which this engine
/// deliberately never does; run scheduling is driven by the HLC-stamped run manifest instead.</para>
/// </summary>
public static class TtlCron
{
    /// <summary>The supported macro spellings, rendered for an error message.</summary>
    public const string SupportedForMessage = "'@hourly', '@daily', '@weekly', '@monthly'";

    /// <summary>Returns true when <paramref name="value"/> is a macro this engine understands.</summary>
    public static bool IsSupported(string? value) => TryGetIntervalMs(value, out _);

    /// <summary>
    /// Resolves a macro to its sweep interval in milliseconds. Returns false for a null/empty value, an
    /// unknown macro, or a full CRON expression (which is valid in CockroachDB but not yet here) — the
    /// caller turns that into a user-facing rejection at configuration time, never at sweep time.
    /// </summary>
    public static bool TryGetIntervalMs(string? value, out long intervalMs)
    {
        switch (value?.Trim().ToLowerInvariant())
        {
            case "@hourly":
                intervalMs = 3_600_000L;
                return true;

            case "@daily":
            case "@midnight": // CockroachDB accepts this spelling of @daily
                intervalMs = 86_400_000L;
                return true;

            case "@weekly":
                intervalMs = 604_800_000L;
                return true;

            case "@monthly":
                // 30 days. A calendar month would require anchoring to a wall-clock date, which this
                // engine avoids for anything that orders work across nodes.
                intervalMs = 2_592_000_000L;
                return true;

            default:
                intervalMs = 0;
                return false;
        }
    }
}
