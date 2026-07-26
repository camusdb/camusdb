/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;

namespace CamusDB.Workload.Util;

/// <summary>Parses the compact duration strings used on the command line: <c>5m</c>, <c>30s</c>, <c>250ms</c>, <c>1h</c>.</summary>
public static class DurationParser
{
    public static TimeSpan Parse(string value)
    {
        string v = value.Trim().ToLowerInvariant();
        if (v.EndsWith("ms", StringComparison.Ordinal))
            return TimeSpan.FromMilliseconds(Num(v[..^2]));
        if (v.EndsWith('s'))
            return TimeSpan.FromSeconds(Num(v[..^1]));
        if (v.EndsWith('m'))
            return TimeSpan.FromMinutes(Num(v[..^1]));
        if (v.EndsWith('h'))
            return TimeSpan.FromHours(Num(v[..^1]));
        // Bare number = seconds.
        return TimeSpan.FromSeconds(Num(v));
    }

    private static double Num(string s)
        => double.Parse(s, CultureInfo.InvariantCulture);
}
