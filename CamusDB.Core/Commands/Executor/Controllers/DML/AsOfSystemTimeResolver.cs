
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text.RegularExpressions;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// Resolves the value of an <c>AS OF SYSTEM TIME</c> clause into the <see cref="HLCTimestamp"/>
/// snapshot the query should read at. The value node comes straight from the grammar and is one of:
/// <list type="bullet">
/// <item>a string relative offset into the past — <c>'-10s'</c>, <c>'-500ms'</c>, <c>'-2m'</c>,
///   <c>'-1h'</c>, <c>'-1d'</c> — resolved against <paramref name="now"/>;</item>
/// <item>a string absolute timestamp — <c>'2026-07-19 20:00:00+00:00'</c> or ISO-8601 — parsed as UTC;</item>
/// <item>a bare integer literal, interpreted as Unix epoch milliseconds;</item>
/// <item>a <c>@placeholder</c> bound to a string or integer parameter.</item>
/// </list>
/// <para>
/// Absolute instants are pinned with the maximal logical/node components (<c>C = uint.MaxValue</c>,
/// <c>N = int.MaxValue</c>) so the snapshot is inclusive of every revision committed within that
/// millisecond — Kahuna resolves an as-of read by selecting the highest revision whose
/// <c>LastModified</c> orders at or before the snapshot, and the order is (physical ms, logical, node).
/// </para>
/// <para>
/// Only past snapshots are allowed: a value that resolves to a future physical millisecond, or to an
/// instant at or before the Unix epoch, is rejected with <see cref="CamusDBErrorCodes.InvalidAsOfSystemTime"/>.
/// Whether history that old is still retained is a storage concern — a snapshot older than Kahuna's
/// retained revisions simply reads as an empty result rather than an error.
/// </para>
/// </summary>
internal static class AsOfSystemTimeResolver
{
    /// <summary>
    /// Matches a relative duration offset such as <c>-10s</c> or <c>-500ms</c>. A leading sign is
    /// captured so a positive offset (a future point) can be rejected with a specific message.
    /// </summary>
    private static readonly Regex DurationPattern =
        new(@"^([+-]?)(\d+)\s*(ms|s|m|h|d)$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static HLCTimestamp Resolve(
        NodeAst valueNode,
        IReadOnlyDictionary<string, ColumnValue>? parameters,
        HLCTimestamp now)
    {
        HLCTimestamp resolved = valueNode.nodeType switch
        {
            NodeType.String => ResolveString(StripQuotes(valueNode.yytext), now),
            NodeType.Integer => FromEpochMillis(ParseLong(valueNode.yytext)),
            NodeType.Placeholder => ResolvePlaceholder(valueNode.yytext, parameters, now),
            _ => throw Invalid("AS OF SYSTEM TIME requires a string, integer, or parameter value."),
        };

        // Compare on the physical millisecond only: absolute snapshots deliberately max out the
        // logical/node components, so a same-millisecond snapshot must not read as "future".
        if (resolved.L > now.L)
            throw Invalid("AS OF SYSTEM TIME resolves to a future instant; only past snapshots are supported.");

        if (resolved.L <= 0)
            throw Invalid("AS OF SYSTEM TIME resolves to an instant at or before the Unix epoch.");

        return resolved;
    }

    private static HLCTimestamp ResolveString(string value, HLCTimestamp now)
    {
        string trimmed = value.Trim();

        if (trimmed.Length == 0)
            throw Invalid("AS OF SYSTEM TIME value is empty.");

        Match duration = DurationPattern.Match(trimmed);
        if (duration.Success)
            return ResolveRelative(duration, now);

        return ResolveAbsoluteString(trimmed);
    }

    private static HLCTimestamp ResolveRelative(Match duration, HLCTimestamp now)
    {
        if (duration.Groups[1].Value == "+")
            throw Invalid("AS OF SYSTEM TIME offset must be negative (into the past), e.g. '-10s'.");

        if (!long.TryParse(duration.Groups[2].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long magnitude))
            throw Invalid($"AS OF SYSTEM TIME offset '{duration.Value}' is out of range.");

        try
        {
            TimeSpan offset = duration.Groups[3].Value.ToLowerInvariant() switch
            {
                "ms" => TimeSpan.FromMilliseconds(magnitude),
                "s" => TimeSpan.FromSeconds(magnitude),
                "m" => TimeSpan.FromMinutes(magnitude),
                "h" => TimeSpan.FromHours(magnitude),
                "d" => TimeSpan.FromDays(magnitude),
                _ => throw Invalid("AS OF SYSTEM TIME offset unit must be one of ms, s, m, h, d."),
            };

            // HLCTimestamp - TimeSpan subtracts whole milliseconds from the physical component.
            return now - offset;
        }
        catch (OverflowException)
        {
            throw Invalid($"AS OF SYSTEM TIME offset '{duration.Value}' is too large.");
        }
    }

    private static HLCTimestamp ResolveAbsoluteString(string value)
    {
        if (!DateTimeOffset.TryParse(
                value,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out DateTimeOffset parsed))
        {
            throw Invalid(
                $"Invalid AS OF SYSTEM TIME value '{value}'. Expected a negative duration such as '-10s' " +
                "or an absolute timestamp such as '2026-07-19 20:00:00+00:00'.");
        }

        return FromEpochMillis(parsed.ToUnixTimeMilliseconds());
    }

    private static HLCTimestamp ResolvePlaceholder(
        string? key,
        IReadOnlyDictionary<string, ColumnValue>? parameters,
        HLCTimestamp now)
    {
        if (key is null || parameters is null || !parameters.TryGetValue(key, out ColumnValue? value) || value is null)
            throw Invalid($"AS OF SYSTEM TIME placeholder '{key}' has no bound value.");

        return value.Type switch
        {
            ColumnType.String or ColumnType.Id when value.StrValue is not null => ResolveString(value.StrValue, now),
            ColumnType.Integer64 => FromEpochMillis(value.LongValue),
            ColumnType.DateTime => FromEpochMillis(TicksToEpochMillis(value.LongValue)),
            _ => throw Invalid($"AS OF SYSTEM TIME placeholder '{key}' must be a string, integer (epoch ms), or datetime."),
        };
    }

    /// <summary>
    /// Builds a snapshot timestamp for an absolute instant. The logical counter and node id are set to
    /// their maxima so the snapshot includes every revision committed within <paramref name="epochMs"/>.
    /// </summary>
    private static HLCTimestamp FromEpochMillis(long epochMs) => new(int.MaxValue, epochMs, uint.MaxValue);

    private static long TicksToEpochMillis(long utcTicks) =>
        new DateTimeOffset(new DateTime(utcTicks, DateTimeKind.Utc)).ToUnixTimeMilliseconds();

    private static long ParseLong(string? raw)
    {
        if (raw is null || !long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out long value))
            throw Invalid($"Invalid AS OF SYSTEM TIME integer value '{raw}'.");

        return value;
    }

    private static string StripQuotes(string? raw)
        => string.IsNullOrEmpty(raw) ? "" : SqlStringLiteral.Decode(raw);

    private static CamusDBException Invalid(string message) =>
        new(CamusDBErrorCodes.InvalidAsOfSystemTime, message);
}
