/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using System.Globalization;
using System.Reflection;
using Kahuna;

namespace CamusDB.App.Services;

/// <summary>
/// Renders a resolved <see cref="EmbeddedKahunaOptions"/> as a flat name-to-value map for reporting.
///
/// <para>Separate from the controller that serves it so it can be tested against a plain options
/// instance. Rendering settings to strings has nothing to do with a running engine, and a test that
/// stands one up to check formatting pays for a process-wide meter registration and a background
/// engine it does not need.</para>
///
/// <para>Values are read by reflection rather than from a hand-written list. A list does not fail to
/// compile when an option is added; it just stops reporting it, and a setting missing from a run
/// manifest is indistinguishable from one that was never configured.</para>
/// </summary>
public static class EngineSettingsReader
{
    public static IReadOnlyDictionary<string, string> Describe(EmbeddedKahunaOptions options)
    {
        SortedDictionary<string, string> settings = new(StringComparer.Ordinal);

        foreach (PropertyInfo property in options.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!property.CanRead || property.GetIndexParameters().Length > 0)
                continue;

            settings[property.Name] = Describe(property, ReadValue(property, options));
        }

        return settings;
    }

    private static object? ReadValue(PropertyInfo property, EmbeddedKahunaOptions options)
    {
        try
        {
            return property.GetValue(options);
        }
        catch (Exception)
        {
            // A property that throws on read is not worth failing the whole report over; the rest of
            // the settings still describe the run.
            return null;
        }
    }

    /// <summary>
    /// Renders one option value. Scalars are reported exactly; a structured value is reported as its
    /// type name, which says "this is configured" without dumping its shape; a credential is reduced
    /// to whether one is set at all, which is the part that changes behaviour.
    /// </summary>
    private static string Describe(PropertyInfo property, object? value)
    {
        if (IsSecret(property.Name))
            return value is string s && s.Length > 0 ? "***redacted***" : "";

        return value switch
        {
            null => "",
            string text => text,
            bool flag => flag ? "true" : "false",
            TimeSpan span => span.ToString("c", CultureInfo.InvariantCulture),
            Enum enumeration => enumeration.ToString(),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            IEnumerable sequence => $"<{property.PropertyType.Name}:{Count(sequence)}>",
            _ => $"<{value.GetType().Name}>",
        };
    }

    private static int Count(IEnumerable sequence)
    {
        int count = 0;
        foreach (object? _ in sequence)
            count++;
        return count;
    }

    /// <summary>
    /// Names that carry a credential. Matched on whole concepts rather than on substrings: "key"
    /// alone would redact <c>KeyValueWriteLingerMs</c>, which is a batching knob and one of the
    /// settings this report exists to carry.
    /// </summary>
    private static bool IsSecret(string name)
        => name.Contains("Token", StringComparison.OrdinalIgnoreCase)
        || name.Contains("Password", StringComparison.OrdinalIgnoreCase)
        || name.Contains("Secret", StringComparison.OrdinalIgnoreCase)
        || name.Contains("Credential", StringComparison.OrdinalIgnoreCase);
}
