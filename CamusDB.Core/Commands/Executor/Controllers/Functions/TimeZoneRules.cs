/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Collections.Frozen;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;
using NodaTime;
using NodaTime.TimeZones;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// The time zone rules behind the zone argument of <c>date_trunc</c>, <c>date_part</c> and
/// <c>to_char</c>: zone name lookup, the wall-clock reading of an instant in a zone, and the instant
/// that starts a wall-clock time.
/// </summary>
/// <remarks>
/// <para>The rules come from the IANA time zone database that NodaTime embeds in its assembly, never
/// from the operating system. Every node that runs the same CamusDB build therefore uses the same
/// rules, on every platform, including the browser build. The OS tzdata can differ by node, and a
/// Windows node in invariant-globalization mode cannot resolve IANA names at all.</para>
/// <para>A build that bumps the NodaTime package can change the rules. <see cref="DataVersion"/> names
/// them. A query fragment whose expressions take a zone argument carries the coordinator's version,
/// and a peer with a different version refuses it, so one statement never mixes two sets of
/// rules.</para>
/// <para>A zone name is an IANA name or one of its aliases (<c>America/New_York</c>,
/// <c>US/Eastern</c>, <c>UTC</c>), matched without regard to case, as PostgreSQL does. A numeric
/// offset such as <c>+05:30</c> is refused: PostgreSQL reads it with the POSIX sign (west positive)
/// and ISO-8601 with the opposite sign, so either reading gives a wrong local time to callers who
/// expect the other.</para>
/// </remarks>
internal static class TimeZoneRules
{
    private static readonly IDateTimeZoneProvider Provider = DateTimeZoneProviders.Tzdb;

    /// <summary>Every zone id and alias, keyed without regard to case. Built on first use.</summary>
    private static readonly Lazy<FrozenDictionary<string, string>> CanonicalIds = new(
        static () => Provider.Ids.ToFrozenDictionary(static id => id, static id => id, StringComparer.OrdinalIgnoreCase));

    /// <summary>
    /// Zones already resolved, keyed by the name as written, without regard to case. Only names that
    /// resolve are added, so the cache holds at most one entry per zone id.
    /// </summary>
    private static readonly ConcurrentDictionary<string, DateTimeZone> ResolvedZones = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// The version of the embedded zone database, for example <c>TZDB: 2026f (mapping: 49)</c>.
    /// Reading it loads the database once per process.
    /// </summary>
    internal static string DataVersion => Provider.VersionId;

    /// <summary>
    /// <see cref="DataVersion"/> when one of <paramref name="expressions"/> calls a function with its
    /// zone argument, otherwise null. A query fragment request carries the result: a peer on other zone
    /// rules refuses only the fragments that use them, and serves every other fragment as before, also
    /// during a rolling upgrade.
    /// </summary>
    internal static string? DataVersionIfUsedBy(IEnumerable<NodeAst?> expressions)
    {
        foreach (NodeAst? expression in expressions)
        {
            if (ScalarFunctionEvaluator.ContainsTimeZoneDependentCall(expression))
                return DataVersion;
        }

        return null;
    }

    /// <summary>
    /// The zone named by argument <paramref name="argIndex"/>. The caller has already returned NULL for
    /// a NULL argument. A name that is not a known zone throws <see cref="CamusDBErrorCodes.InvalidInput"/>
    /// with the name.
    /// </summary>
    internal static DateTimeZone Resolve(string calledName, int argIndex, ColumnValue argument)
    {
        ScalarFunctionArguments.RequireString(calledName, argIndex, argument);
        string name = argument.StrValue!;

        if (ResolvedZones.TryGetValue(name, out DateTimeZone? zone))
            return zone;

        if (!CanonicalIds.Value.TryGetValue(name, out string? id))
            throw UnknownZone(calledName, name);

        zone = Provider[id];
        ResolvedZones.TryAdd(name, zone);
        return zone;
    }

    /// <summary>
    /// What a wall clock in <paramref name="zone"/> shows at the instant <paramref name="utcTicks"/>,
    /// with the offset and the zone abbreviation in force then. A reading outside the years 1-9999
    /// throws <see cref="CamusDBErrorCodes.InvalidInput"/>.
    /// </summary>
    internal static WallClockReading Read(string calledName, long utcTicks, DateTimeZone zone)
    {
        ZoneInterval interval = zone.GetZoneInterval(ToInstant(utcTicks));
        long wallTicks = utcTicks + interval.WallOffset.Ticks;

        if (wallTicks < DateTime.MinValue.Ticks || wallTicks > DateTime.MaxValue.Ticks)
            throw OutOfRange(calledName, zone);

        return new WallClockReading(new DateTime(wallTicks, DateTimeKind.Unspecified), interval);
    }

    /// <summary>
    /// The instant at which the unit that starts at <paramref name="wallClock"/> on a clock in
    /// <paramref name="zone"/> starts, as UTC ticks. <c>date_trunc</c> calls it with the truncated
    /// wall-clock time of a value and the zone interval of that value. The result is never later than
    /// the value, and truncating the result again gives the same result.
    /// </summary>
    /// <remarks>
    /// <para>A time that a spring-forward change skips maps to the instant of the change: that is the
    /// first instant of the local day, or hour, that holds it. A day in a zone that moves the clock from
    /// 00:00 to 01:00 starts at 01:00.</para>
    /// <para>A fall-back change repeats some local times. When <paramref name="keepValueOccurrence"/> is
    /// clear, a repeated time maps to its first occurrence, so a day whose midnight repeats still starts
    /// once. <c>date_trunc</c> sets it for the units shorter than a day, and then each occurrence is its
    /// own unit: a repeated time maps to the occurrence that has the value's offset, and a unit that
    /// started before the change, at a time the change does not repeat, starts again at the change for
    /// a value after it. In New York 01:30 EST truncates to 01:00 EST. In Lord Howe, which moves back
    /// from 02:00 to 01:30, 01:45 after the change truncates to the change itself, because 01:00 at the
    /// later offset never happens. Truncating to the minute never moves a value by an hour.</para>
    /// </remarks>
    internal static long StartOfWallClock(
        string calledName,
        DateTime wallClock,
        DateTimeZone zone,
        ZoneInterval valueInterval,
        bool keepValueOccurrence)
    {
        ZoneLocalMapping mapping = zone.MapLocal(LocalDateTime.FromDateTime(wallClock));

        Instant start = mapping.Count switch
        {
            1 => mapping.Single().ToInstant(),
            2 => (keepValueOccurrence && mapping.Last().Offset == valueInterval.WallOffset ? mapping.Last() : mapping.First()).ToInstant(),
            _ => mapping.LateInterval.Start,
        };

        if (keepValueOccurrence
            && valueInterval.HasStart
            && start < valueInterval.Start
            && zone.GetZoneInterval(valueInterval.Start - Duration.Epsilon).WallOffset > valueInterval.WallOffset)
            start = valueInterval.Start;

        long utcTicks = start.ToUnixTimeTicks() + DateTime.UnixEpoch.Ticks;

        if (utcTicks < DateTime.MinValue.Ticks || utcTicks > DateTime.MaxValue.Ticks)
            throw OutOfRange(calledName, zone);

        return utcTicks;
    }

    private static Instant ToInstant(long utcTicks) => Instant.FromUnixTimeTicks(utcTicks - DateTime.UnixEpoch.Ticks);

    private static CamusDBException UnknownZone(string calledName, string name)
    {
        string hint = name.Length > 0 && (name[0] == '+' || name[0] == '-' || char.IsAsciiDigit(name[0]))
            ? "; a numeric offset is not accepted, use an IANA zone name such as 'Asia/Kolkata'"
            : "; use an IANA zone name such as 'America/New_York' or 'UTC'";

        return new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' does not know time zone '{name}'{hint}");
    }

    private static CamusDBException OutOfRange(string calledName, DateTimeZone zone)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' result is out of range in time zone '{zone.Id}'");
}

/// <summary>
/// What a wall clock in one zone shows at one instant: the local date and time (kind
/// <see cref="DateTimeKind.Unspecified"/>), and the zone interval in force then, which gives the offset
/// from UTC and the zone abbreviation, such as <c>EST</c>, <c>IST</c> or <c>-05</c>, as the IANA
/// database spells it.
/// </summary>
internal readonly record struct WallClockReading(DateTime WallClock, ZoneInterval Interval)
{
    public Offset Offset => Interval.WallOffset;

    public string Abbreviation => Interval.Name;
}
