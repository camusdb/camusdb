/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB;

/// <summary>
/// Resolves console log verbosity from environment variables so a running container can be turned
/// up or down without editing <c>appsettings.json</c>, rebuilding the image, or shipping a custom
/// config file. This exists because the host pins a few noisy categories (Kahuna, Kommander, gRPC)
/// to Warning in code, and a code-level filter always beats the equivalent
/// <c>Logging__LogLevel__*</c> configuration entry — a rule with the same category added later
/// wins — so without these hooks those categories could not be raised from outside at all.
///
/// <para>Recognized variables (all optional; unset means "keep the built-in default"):
/// <c>CAMUS_LOG_LEVEL</c> for everything not covered by a more specific rule,
/// <c>CAMUS_LOG_LEVEL_KAHUNA</c>, <c>CAMUS_LOG_LEVEL_KOMMANDER</c>, <c>CAMUS_LOG_LEVEL_GRPC</c>
/// for the pinned categories, and <c>CAMUS_LOG_FILTERS</c> for anything else, as a comma-separated
/// <c>Category=Level</c> list.</para>
///
/// <para>Values are the standard <see cref="LogLevel"/> names and are matched case-insensitively.
/// An unparseable value is ignored rather than fatal: a typo in a deployment variable must not stop
/// the server from booting, and the built-in default is always a safe answer.</para>
/// </summary>
internal static class LogLevelResolver
{
    /// <summary>
    /// Reads <paramref name="variable"/> and returns the level it names, or <c>null</c> when it is
    /// unset, empty, or not a valid level name. Callers treat <c>null</c> as "leave the built-in
    /// default alone", which keeps the no-environment behavior byte-identical to before.
    /// </summary>
    public static LogLevel? FromEnvironment(string variable)
    {
        string? value = Environment.GetEnvironmentVariable(variable);

        if (string.IsNullOrWhiteSpace(value))
            return null;

        if (Enum.TryParse(value.Trim(), ignoreCase: true, out LogLevel level))
            return level;

        Console.Error.WriteLine($"Ignoring {variable}: '{value}' is not a log level (Trace|Debug|Information|Warning|Error|Critical|None).");
        return null;
    }

    /// <summary>
    /// Parses a comma-separated <c>Category=Level</c> list (e.g.
    /// <c>"Microsoft.AspNetCore=Debug,CamusDB.Core.ICamusDB=Trace"</c>) into filter rules. Malformed
    /// or unparseable entries are skipped with a message on stderr so the rest of the list still
    /// applies; the category is passed through verbatim because logging matches it as a namespace
    /// prefix.
    /// </summary>
    public static IEnumerable<(string Category, LogLevel Level)> ParseFilters(string? spec)
    {
        if (string.IsNullOrWhiteSpace(spec))
            yield break;

        foreach (string entry in spec.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            int separator = entry.LastIndexOf('=');

            if (separator <= 0 || separator == entry.Length - 1)
            {
                Console.Error.WriteLine($"Ignoring CAMUS_LOG_FILTERS entry '{entry}': expected Category=Level.");
                continue;
            }

            string category = entry[..separator].Trim();

            if (!Enum.TryParse(entry[(separator + 1)..].Trim(), ignoreCase: true, out LogLevel level))
            {
                Console.Error.WriteLine($"Ignoring CAMUS_LOG_FILTERS entry '{entry}': '{entry[(separator + 1)..]}' is not a log level.");
                continue;
            }

            yield return (category, level);
        }
    }
}
