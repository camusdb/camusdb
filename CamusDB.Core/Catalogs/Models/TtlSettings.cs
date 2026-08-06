
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// A table's row-level TTL configuration, resolved once from the free-form
/// <see cref="TableSchema.Settings"/> bag against the node's defaults.
///
/// <para><b>Why resolve in one place.</b> Every value here is "table parameter if present, node default
/// otherwise". Re-deriving that at each use site is how a sweep ends up reading the node default for one
/// knob and the table's for another — a discrepancy that only shows up as a table sweeping at the wrong
/// rate, which nobody notices. <see cref="Resolve"/> is the only place the fallback is applied.</para>
///
/// <para>Parsing is total: a value that survived <see cref="TableSettings.Canonicalize"/> is
/// well-formed, so a malformed one here means the bag was written by an older or hand-edited engine and
/// falls back to the default rather than throwing. Configuration is validated at ALTER time; a sweep
/// must not be the thing that discovers a bad value.</para>
/// </summary>
public sealed record TtlSettings
{
    /// <summary>
    /// The column whose value decides expiry, or null when TTL is not configured for the table.
    /// Null here is the master "TTL off" signal — every other member is meaningless without it.
    /// </summary>
    public string? ExpirationColumn { get; init; }

    /// <summary>Whether the sweep is paused while keeping the configuration (<c>ttl_pause</c>).</summary>
    public bool Paused { get; init; }

    /// <summary>Sweep cadence in milliseconds, resolved from the <c>ttl_job_cron</c> macro.</summary>
    public long JobIntervalMs { get; init; }

    /// <summary>Rows read per batch while scanning a span.</summary>
    public int SelectBatchSize { get; init; }

    /// <summary>Rows deleted per transaction.</summary>
    public int DeleteBatchSize { get; init; }

    /// <summary>Scan rate cap in rows/second; <c>0</c> is unlimited.</summary>
    public int SelectRateLimit { get; init; }

    /// <summary>Delete rate cap in rows/second; <c>0</c> is unlimited.</summary>
    public int DeleteRateLimit { get; init; }

    /// <summary>Extra delay past expiry before a row is eligible, in milliseconds.</summary>
    public long GraceMs { get; init; }

    /// <summary>
    /// True when a sweep should actually run for this table: TTL is configured and not paused.
    /// The engine-level master switch (<c>CamusDBOptions.TtlEnabled</c>) is checked separately by the
    /// scheduler, since it decides whether any loop starts at all.
    /// </summary>
    public bool IsActive => ExpirationColumn is not null && !Paused;

    /// <summary>
    /// Resolves a table's TTL configuration from its settings bag, falling back to the node defaults
    /// for anything the table does not override.
    /// </summary>
    public static TtlSettings Resolve(IReadOnlyDictionary<string, string>? settings, CamusDBOptions options)
    {
        string? column = null;
        bool paused = false;
        long intervalMs = ResolveCron(options.TtlDefaultJobCron);
        int selectBatch = options.TtlDefaultSelectBatchSize;
        int deleteBatch = options.TtlDefaultDeleteBatchSize;
        int selectRate = options.TtlDefaultSelectRateLimit;
        int deleteRate = options.TtlDefaultDeleteRateLimit;
        long graceMs = 0;

        if (settings is not null)
        {
            if (settings.TryGetValue(TableSettings.TtlExpirationExpressionKey, out string? rawColumn) &&
                !string.IsNullOrWhiteSpace(rawColumn))
                column = rawColumn;

            if (settings.TryGetValue(TableSettings.TtlPauseKey, out string? rawPause))
                paused = string.Equals(rawPause, "true", StringComparison.OrdinalIgnoreCase);

            if (settings.TryGetValue(TableSettings.TtlJobCronKey, out string? rawCron) &&
                TtlCron.TryGetIntervalMs(rawCron, out long cronMs))
                intervalMs = cronMs;

            selectBatch = ResolveInt(settings, TableSettings.TtlSelectBatchSizeKey, selectBatch);
            deleteBatch = ResolveInt(settings, TableSettings.TtlDeleteBatchSizeKey, deleteBatch);
            selectRate = ResolveInt(settings, TableSettings.TtlSelectRateLimitKey, selectRate);
            deleteRate = ResolveInt(settings, TableSettings.TtlDeleteRateLimitKey, deleteRate);
            graceMs = ResolveLong(settings, TableSettings.TtlGraceMsKey, graceMs);
        }

        return new TtlSettings
        {
            ExpirationColumn = column,
            Paused = paused,
            JobIntervalMs = intervalMs,
            SelectBatchSize = selectBatch,
            DeleteBatchSize = deleteBatch,
            SelectRateLimit = selectRate,
            DeleteRateLimit = deleteRate,
            GraceMs = graceMs,
        };
    }

    private static long ResolveCron(string? cron) =>
        TtlCron.TryGetIntervalMs(cron, out long ms) ? ms : 86_400_000L;

    private static int ResolveInt(IReadOnlyDictionary<string, string> settings, string key, int fallback) =>
        settings.TryGetValue(key, out string? raw) &&
        int.TryParse(raw, System.Globalization.NumberStyles.Integer,
            System.Globalization.CultureInfo.InvariantCulture, out int parsed) && parsed >= 0
            ? parsed
            : fallback;

    private static long ResolveLong(IReadOnlyDictionary<string, string> settings, string key, long fallback) =>
        settings.TryGetValue(key, out string? raw) &&
        long.TryParse(raw, System.Globalization.NumberStyles.Integer,
            System.Globalization.CultureInfo.InvariantCulture, out long parsed) && parsed >= 0
            ? parsed
            : fallback;
}
