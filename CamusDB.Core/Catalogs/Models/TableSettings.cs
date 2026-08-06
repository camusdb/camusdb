
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// The value shapes a table storage parameter may take. The kind is what decides whether a value is
/// well-formed, so a new parameter is one entry in <see cref="TableSettings.Recognized"/> rather than a
/// new branch in the validator.
/// </summary>
public enum TableSettingKind
{
    /// <summary><c>true</c> or <c>false</c>.</summary>
    Boolean,

    /// <summary>A non-negative integer.</summary>
    NonNegativeInteger,

    /// <summary>A positive integer (rejects zero).</summary>
    PositiveInteger,

    /// <summary>A bare column name.</summary>
    ColumnName,

    /// <summary>A supported <see cref="TtlCron"/> macro.</summary>
    CronMacro,
}

/// <summary>
/// Canonical validation for table storage parameters (<c>ALTER TABLE t SET (key = value)</c> and
/// <c>RESET (key)</c>). The single integrity boundary for settings, invoked by every command entry
/// point (SQL and direct ticket) so an unknown key, a malformed value, an empty set, or a duplicate key
/// is rejected regardless of how the change is submitted. Storage-parameter names are treated
/// case-insensitively (like ordinary SQL identifiers) and canonicalized to a stable lowercase spelling
/// for persistence.
///
/// <para><b>This pass is context-free, and that is not the whole story.</b> It can tell that
/// <c>ttl_expiration_expression = 'expires_at'</c> is shaped correctly; it cannot tell whether the table
/// has a column called <c>expires_at</c> of a type that can expire. That check needs the live
/// <see cref="CommandsExecutor.Models.TableDescriptor"/> and therefore runs in
/// <c>CommandExecutor.AlterTableSettings</c> under the schema semaphore. Both passes run at ALTER time:
/// a misconfigured TTL must fail in the user's session, not silently in a background sweep where nobody
/// sees it.</para>
///
/// <para><b>Parameter names follow CockroachDB.</b> Its row-level TTL is the closest prior art and uses
/// the same storage-parameter frame, so a user who knows CRDB's TTL can configure CamusDB's without
/// relearning it. Where CamusDB cannot yet express something CRDB can, the parameter is accepted with a
/// documented narrower value grammar rather than renamed — widening later then costs nothing.</para>
/// </summary>
public static class TableSettings
{
    /// <summary>Per-table auto-analyze opt-out.</summary>
    public const string SqlStatsAutomaticCollectionEnabledKey = TableSchema.SqlStatsAutomaticCollectionEnabledKey;

    /// <summary>
    /// Names the column whose value decides when a row expires. Presence of this key is what enables
    /// row-level TTL for the table. CockroachDB accepts an arbitrary SQL expression here and marks this
    /// its recommended parameter; CamusDB currently accepts a bare column name, which is the same
    /// parameter with a narrower value grammar.
    /// </summary>
    public const string TtlExpirationExpressionKey = "ttl_expiration_expression";

    /// <summary>Stops the sweep without discarding the configuration.</summary>
    public const string TtlPauseKey = "ttl_pause";

    /// <summary>How often the sweep runs, as a cron macro (see <see cref="TtlCron"/>).</summary>
    public const string TtlJobCronKey = "ttl_job_cron";

    /// <summary>Rows read per batch while scanning a span.</summary>
    public const string TtlSelectBatchSizeKey = "ttl_select_batch_size";

    /// <summary>Rows deleted per transaction.</summary>
    public const string TtlDeleteBatchSizeKey = "ttl_delete_batch_size";

    /// <summary>Scan rate cap in rows/second; <c>0</c> is unlimited.</summary>
    public const string TtlSelectRateLimitKey = "ttl_select_rate_limit";

    /// <summary>Delete rate cap in rows/second; <c>0</c> is unlimited.</summary>
    public const string TtlDeleteRateLimitKey = "ttl_delete_rate_limit";

    /// <summary>
    /// Extra delay past expiry before a row becomes eligible, absorbing clock skew and late writers.
    /// A CamusDB extension with no CockroachDB equivalent: CRDB users fold a grace period into the
    /// expiration expression, which a bare column name cannot express.
    /// </summary>
    public const string TtlGraceMsKey = "ttl_grace_ms";

    /// <summary>
    /// Engine-set marker that row-level TTL is active on this table, mirroring CockroachDB's <c>ttl</c>
    /// parameter. Never accepted from a user — it is written when TTL is configured and is the target of
    /// <c>RESET (ttl)</c>, which clears every TTL parameter at once.
    /// </summary>
    public const string TtlKey = "ttl";

    /// <summary>The storage-parameter keys the engine recognizes, with the value shape each accepts.</summary>
    public static readonly IReadOnlyDictionary<string, TableSettingKind> Recognized =
        new Dictionary<string, TableSettingKind>(StringComparer.Ordinal)
        {
            [SqlStatsAutomaticCollectionEnabledKey] = TableSettingKind.Boolean,
            [TtlExpirationExpressionKey]            = TableSettingKind.ColumnName,
            [TtlPauseKey]                           = TableSettingKind.Boolean,
            [TtlJobCronKey]                         = TableSettingKind.CronMacro,
            [TtlSelectBatchSizeKey]                 = TableSettingKind.PositiveInteger,
            [TtlDeleteBatchSizeKey]                 = TableSettingKind.PositiveInteger,
            [TtlSelectRateLimitKey]                 = TableSettingKind.NonNegativeInteger,
            [TtlDeleteRateLimitKey]                 = TableSettingKind.NonNegativeInteger,
            [TtlGraceMsKey]                         = TableSettingKind.NonNegativeInteger,
            [TtlKey]                                = TableSettingKind.Boolean,
        };

    /// <summary>Every TTL parameter, so <c>RESET (ttl)</c> can clear the whole group.</summary>
    public static readonly IReadOnlySet<string> TtlKeys =
        new HashSet<string>(StringComparer.Ordinal)
        {
            TtlExpirationExpressionKey, TtlPauseKey, TtlJobCronKey,
            TtlSelectBatchSizeKey, TtlDeleteBatchSizeKey,
            TtlSelectRateLimitKey, TtlDeleteRateLimitKey,
            TtlGraceMsKey, TtlKey,
        };

    /// <summary>
    /// Keys a user may not set directly. <c>ttl</c> is derived from whether TTL is configured; letting a
    /// user set it would let the marker disagree with the configuration it is supposed to describe.
    /// </summary>
    private static readonly IReadOnlySet<string> EngineOwned =
        new HashSet<string>(StringComparer.Ordinal) { TtlKey };

    /// <summary>
    /// Whether a key is maintained by the engine rather than set by a user. Such keys must be omitted
    /// from any rendering meant to be replayed, since the settings grammar rejects them on input.
    /// </summary>
    public static bool IsEngineOwned(string key) => EngineOwned.Contains(key);

    /// <summary>
    /// Validates and canonicalizes a set of <c>key = value</c> pairs: lowercases each key, rejects an
    /// unrecognized key, an engine-owned key, a value that does not match the key's
    /// <see cref="TableSettingKind"/>, an empty set, and a duplicate key (case-insensitive). Returns a
    /// fresh ordinal-keyed dictionary with lowercase keys and canonical values. Throws
    /// <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.InvalidInput"/> on any violation.
    /// </summary>
    public static Dictionary<string, string> Canonicalize(IReadOnlyList<KeyValuePair<string, string>> raw)
    {
        if (raw.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ALTER TABLE SET requires at least one setting");

        var result = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (KeyValuePair<string, string> pair in raw)
        {
            string key = (pair.Key ?? "").Trim().ToLowerInvariant();

            if (!Recognized.TryGetValue(key, out TableSettingKind kind))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unknown table setting '{pair.Key}'; supported: {string.Join(", ", SettableKeys())}");

            if (EngineOwned.Contains(key))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Table setting '{key}' is set by the engine and cannot be assigned directly");

            if (!result.TryAdd(key, CanonicalizeValue(key, kind, pair.Value)))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Duplicate table setting '{key}'");
        }

        return result;
    }

    /// <summary>
    /// Validates and canonicalizes the key list of an <c>ALTER TABLE … RESET (…)</c>. Expands the
    /// group key <c>ttl</c> to every TTL parameter, so one <c>RESET (ttl)</c> removes the whole
    /// configuration rather than leaving orphaned tuning behind a cleared expiration column. Unlike
    /// <see cref="Canonicalize"/> this accepts engine-owned keys — clearing <c>ttl</c> is the point.
    /// </summary>
    public static HashSet<string> CanonicalizeResetKeys(IReadOnlyList<string> raw)
    {
        if (raw.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ALTER TABLE RESET requires at least one setting");

        var result = new HashSet<string>(StringComparer.Ordinal);

        foreach (string rawKey in raw)
        {
            string key = (rawKey ?? "").Trim().ToLowerInvariant();

            if (!Recognized.ContainsKey(key))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unknown table setting '{rawKey}'; supported: {string.Join(", ", Recognized.Keys)}");

            if (key == TtlKey)
                result.UnionWith(TtlKeys);
            else if (!result.Add(key))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Duplicate table setting '{key}'");
        }

        return result;
    }

    private static IEnumerable<string> SettableKeys()
    {
        foreach (string key in Recognized.Keys)
        {
            if (!EngineOwned.Contains(key))
                yield return key;
        }
    }

    private static string CanonicalizeValue(string key, TableSettingKind kind, string? rawValue)
    {
        string value = (rawValue ?? "").Trim();

        switch (kind)
        {
            case TableSettingKind.Boolean:
                value = value.ToLowerInvariant();
                if (value != "true" && value != "false")
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Table setting '{key}' must be true or false");
                return value;

            case TableSettingKind.NonNegativeInteger:
            case TableSettingKind.PositiveInteger:
            {
                // Parsed as long so an oversized value is diagnosed rather than overflowing, then
                // range-checked against int — the type every consumer of these settings actually uses.
                // Accepting a value the resolver would silently discard is worse than rejecting it: the
                // user sees their setting stored and the sweep quietly runs on the default instead.
                if (!long.TryParse(value, System.Globalization.NumberStyles.Integer,
                        System.Globalization.CultureInfo.InvariantCulture, out long parsed))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Table setting '{key}' must be an integer, got '{rawValue}'");

                long floor = kind == TableSettingKind.PositiveInteger ? 1 : 0;
                if (parsed < floor)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Table setting '{key}' must be >= {floor}, got {parsed}");

                // ttl_grace_ms is a duration and legitimately exceeds int range; the rest are counts and
                // rates read as int.
                if (!string.Equals(key, TtlGraceMsKey, StringComparison.Ordinal) && parsed > int.MaxValue)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Table setting '{key}' must be <= {int.MaxValue}, got {parsed}");

                return parsed.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            case TableSettingKind.ColumnName:
            {
                if (value.Length == 0)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Table setting '{key}' must name a column");

                // Say "not supported yet" rather than "invalid": CockroachDB accepts an expression here
                // and a user arriving from it will reasonably try one. The distinction tells them the
                // parameter is right and the value grammar is narrower, not that they misspelled it.
                if (!IsBareIdentifier(value))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Table setting '{key}' must be a bare column name; expressions are not supported yet, got '{rawValue}'");

                return value;
            }

            case TableSettingKind.CronMacro:
            {
                if (!TtlCron.IsSupported(value))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Table setting '{key}' must be one of {TtlCron.SupportedForMessage}; " +
                        $"full CRON expressions are not supported yet, got '{rawValue}'");

                return value.ToLowerInvariant();
            }

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Unhandled table setting kind for '{key}'");
        }
    }

    private static bool IsBareIdentifier(string value)
    {
        if (value.Length == 0 || (!char.IsLetter(value[0]) && value[0] != '_'))
            return false;

        foreach (char c in value)
        {
            if (!char.IsLetterOrDigit(c) && c != '_')
                return false;
        }

        return true;
    }
}
