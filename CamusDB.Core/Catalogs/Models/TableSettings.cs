
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Canonical validation for table storage parameters (<c>ALTER TABLE t SET (key = value)</c>). The
/// single integrity boundary for settings, invoked by every command entry point (SQL and direct
/// ticket) so an unknown key, non-boolean value, empty set, or duplicate key is rejected regardless
/// of how the change is submitted. Storage-parameter names are treated case-insensitively (like
/// ordinary SQL identifiers) and canonicalized to a stable lowercase spelling for persistence.
/// </summary>
public static class TableSettings
{
    /// <summary>The storage-parameter keys the engine recognizes (canonical lowercase spelling).</summary>
    public static readonly IReadOnlySet<string> RecognizedKeys =
        new HashSet<string>(StringComparer.Ordinal) { TableSchema.SqlStatsAutomaticCollectionEnabledKey };

    /// <summary>
    /// Validates and canonicalizes a set of <c>key = value</c> pairs: lowercases each key, rejects an
    /// unrecognized key, a non-boolean value, an empty set, and a duplicate key (case-insensitive).
    /// Returns a fresh ordinal-keyed dictionary with lowercase keys and <c>"true"</c>/<c>"false"</c>
    /// values. Throws <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.InvalidInput"/>
    /// on any violation.
    /// </summary>
    public static Dictionary<string, string> Canonicalize(IReadOnlyList<KeyValuePair<string, string>> raw)
    {
        if (raw.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ALTER TABLE SET requires at least one setting");

        var result = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (KeyValuePair<string, string> pair in raw)
        {
            string key = (pair.Key ?? "").Trim().ToLowerInvariant();

            if (!RecognizedKeys.Contains(key))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Unknown table setting '{pair.Key}'; supported: {string.Join(", ", RecognizedKeys)}");

            string value = (pair.Value ?? "").Trim().ToLowerInvariant();
            if (value != "true" && value != "false")
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Table setting '{key}' must be true or false");

            if (!result.TryAdd(key, value))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Duplicate table setting '{key}'");
        }

        return result;
    }
}
