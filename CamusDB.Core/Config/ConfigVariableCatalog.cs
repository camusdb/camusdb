/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using System.Globalization;
using System.Reflection;
using CamusDB.Core.Config.Models;
using YamlDotNet.Serialization.NamingConventions;

namespace CamusDB.Core.Config;

/// <summary>One configuration setting as reported to an operator, already formatted for display.</summary>
/// <param name="Name">The underscored YAML key, e.g. <c>ttl_span_lease_ms</c> or <c>kahuna.wal_sync_writes</c>.</param>
/// <param name="Value">Effective value, or null for a setting that is genuinely unset.</param>
/// <param name="Type">Coarse type label: <c>bool</c>, <c>int</c>, <c>long</c>, <c>double</c>, <c>string</c>, <c>enum</c>, <c>duration_ms</c>, or <c>list</c>.</param>
/// <param name="Default">The built-in default rendered the same way, or null when the default is unset.</param>
/// <param name="Source">Which layer supplied <paramref name="Value"/>.</param>
/// <param name="Mutability">Whether a change takes effect live (<c>runtime</c>) or requires a restart.</param>
/// <param name="Scope">Whether the fleet must agree on the value (<c>cluster</c>) or it is per-node.</param>
public sealed record ConfigVariable(
    string Name,
    string? Value,
    string Type,
    string? Default,
    ConfigValueSource Source,
    ConfigMutability Mutability,
    ConfigScope Scope);

/// <summary>
/// Renders a <see cref="CamusDBOptions"/> instance as the flat, YAML-named list of settings behind
/// <c>SHOW VARIABLES</c>.
///
/// <para>Enumeration is by reflection rather than a hand-written registry, and that is the whole
/// point of the type: a registry of 150-odd entries drifts the first time someone adds a tunable and
/// forgets the row, and the failure is silent — a missing variable nobody notices. Reflection is
/// complete by construction. What reflection cannot get right is the <em>name</em>, because four
/// options were named differently on the options record than in the file, so those live in
/// <see cref="YamlNameOverrides"/>; a fifth rename that is not added there ships a variable under a
/// name no operator can put in their config, which is what the name-coverage test exists to catch.</para>
///
/// <para>Values are formatted to be copy-pasteable back into <c>config.yml</c> — lowercase booleans,
/// invariant numerics with no separators, durations as whole milliseconds like every <c>*_ms</c> key
/// — so that reading a value here and writing it into a file cannot change its meaning.</para>
///
/// <para>Reporting is not the same as disclosing: the settings in <see cref="RedactedVariables"/>
/// hold key material and are listed with their value masked. They are listed rather than hidden
/// because whether a node <em>has</em> a node secret configured is an operational question, while
/// what that secret is never is.</para>
/// </summary>
public static class ConfigVariableCatalog
{
    /// <summary>Mask substituted for a configured secret. Unset secrets render as the empty string.</summary>
    internal const string RedactedValue = "********";

    /// <summary>
    /// Settings whose value is key material and must never be printed. Membership is explicit rather
    /// than inferred from the name: a heuristic on "key"/"secret"/"password" would both miss
    /// (<c>node_secret</c> is caught, but a future <c>signing_material</c> would not be) and
    /// over-match (<c>backup_mac_key_file</c> and the certificate settings are file <em>paths</em>,
    /// which an operator needs to see to debug a misconfigured deployment).
    /// </summary>
    internal static readonly HashSet<string> RedactedVariables = new(StringComparer.OrdinalIgnoreCase)
    {
        "bootstrap_superuser_password",
        "access_token_server_key",
        "node_secret",
    };

    /// <summary>
    /// Options whose record property name does not underscore to the YAML key an operator writes.
    /// Keyed by CLR property name.
    /// </summary>
    internal static readonly Dictionary<string, string> YamlNameOverrides = new(StringComparer.Ordinal)
    {
        [nameof(CamusDBOptions.DataDirectory)] = "data_dir",
        [nameof(CamusDBOptions.ClusterPartitionCount)] = "initial_partitions",
        [nameof(CamusDBOptions.KeyRangeShardingEnabled)] = "key_range_sharding",
        [nameof(CamusDBOptions.QueryResultCacheSingleFlightWaitMs)] = "query_result_cache_singleflight_wait_ms",
    };

    /// <summary>
    /// Properties of <see cref="CamusDBOptions"/> that are not settings and must not appear as rows:
    /// the nested Kahuna section (flattened separately) and the provenance map (metadata about the
    /// settings, not one of them).
    /// </summary>
    private static readonly HashSet<string> NotVariables = new(StringComparer.Ordinal)
    {
        nameof(CamusDBOptions.Kahuna),
        nameof(CamusDBOptions.ValueSources),
    };

    /// <summary>
    /// The settings of <paramref name="options"/>, ordinal-sorted by name, with every value already
    /// formatted and redacted. Defaults are read from <see cref="CamusDBOptions.Default"/>, so the
    /// caller can show what a setting would be if nothing overrode it.
    /// </summary>
    public static IReadOnlyList<ConfigVariable> Describe(CamusDBOptions options)
    {
        List<ConfigVariable> variables = [];

        foreach (PropertyInfo property in typeof(CamusDBOptions).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (NotVariables.Contains(property.Name) || !IsSetting(property))
                continue;

            variables.Add(Describe(
                YamlName(property.Name),
                property.PropertyType,
                property.GetValue(options),
                property.GetValue(CamusDBOptions.Default),
                options.ValueSources,
                Classification(property)));
        }

        foreach (PropertyInfo property in typeof(KahunaOptionsConfig).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!IsSetting(property))
                continue;

            variables.Add(Describe(
                "kahuna." + YamlName(property.Name),
                property.PropertyType,
                property.GetValue(options.Kahuna),
                property.GetValue(CamusDBOptions.Default.Kahuna),
                options.ValueSources,
                KahunaClassification));
        }

        variables.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        return variables;
    }

    /// <summary>
    /// The declared classification of an options property, read from its
    /// <see cref="ConfigSettingAttribute"/>. Throwing on an unattributed property is deliberate:
    /// the classification is operator-facing, so a setting without one must fail loudly (and the
    /// classification test fails the build first) rather than default to something plausible.
    /// </summary>
    private static ConfigSettingAttribute Classification(PropertyInfo property)
        => property.GetCustomAttribute<ConfigSettingAttribute>()
           ?? throw new InvalidOperationException(
               $"Setting '{property.Name}' carries no [ConfigSetting] classification; every settable " +
               "CamusDBOptions property must declare mutability and scope");

    /// <summary>
    /// Every <c>kahuna.*</c> key is restart-class and node-scoped by rule rather than per-property
    /// attribution: the embedded node is constructed once from these values before anything else
    /// starts and is never rebuilt, so no key in the section can honestly claim runtime mutability.
    /// </summary>
    private static readonly ConfigSettingAttribute KahunaClassification =
        new(ConfigMutability.Restart, ConfigScope.Node);

    private static ConfigVariable Describe(
        string name,
        Type declaredType,
        object? value,
        object? defaultValue,
        IReadOnlyDictionary<string, ConfigValueSource> sources,
        ConfigSettingAttribute classification)
    {
        string? formatted = Format(value);

        if (RedactedVariables.Contains(name) && !string.IsNullOrEmpty(formatted))
            formatted = RedactedValue;

        return new ConfigVariable(
            name,
            formatted,
            TypeLabel(declaredType),
            Format(defaultValue),
            sources.TryGetValue(name, out ConfigValueSource source) ? source : ConfigValueSource.Default,
            classification.Mutability,
            classification.Scope);
    }

    /// <summary>
    /// True for a property that is a setting rather than a view of one. A computed, get-only property
    /// such as <see cref="CamusDBOptions.SpillEffectiveThreshold"/> derives its value from settings
    /// that are already listed; reporting it as a variable would offer the operator a name they cannot
    /// configure and a value that contradicts nothing, and would double-count the underlying setting.
    /// </summary>
    private static bool IsSetting(PropertyInfo property) => property.SetMethod is not null;

    /// <summary>
    /// The YAML key for a CLR property name: YamlDotNet's own underscoring convention — the same one
    /// the deserializer applies, so the two cannot disagree about a name — with the handful of
    /// deliberate renames applied on top.
    /// </summary>
    internal static string YamlName(string propertyName)
        => YamlNameOverrides.TryGetValue(propertyName, out string? overridden)
            ? overridden
            : UnderscoredNamingConvention.Instance.Apply(propertyName);

    /// <summary>
    /// Renders a value the way <c>config.yml</c> would spell it, so a value read out of the engine can
    /// be written back into a file unchanged. Null — a genuinely unset option — stays null and becomes
    /// SQL NULL, which is distinct from an empty string.
    /// </summary>
    private static string? Format(object? value) => value switch
    {
        null => null,
        bool flag => flag ? "true" : "false",
        string text => text,
        // Every duration key in the configuration is an integer millisecond count; rendering the two
        // TimeSpan-typed options as 00:15:00 would make them the only ones that are not.
        TimeSpan span => ((long)span.TotalMilliseconds).ToString(CultureInfo.InvariantCulture),
        // Underscored, not merely lowercased: the configuration file spells these values
        // read_committed and best_effort, and a bare ToLowerInvariant would render readcommitted —
        // a token the config parser rejects, breaking the round trip this formatting exists for.
        Enum enumeration => UnderscoredNamingConvention.Instance.Apply(enumeration.ToString()),
        double number => number.ToString(CultureInfo.InvariantCulture),
        float number => number.ToString(CultureInfo.InvariantCulture),
        IEnumerable items => string.Join(",", items.Cast<object?>().Select(item => item?.ToString() ?? "")),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString(),
    };

    private static string TypeLabel(Type declaredType)
    {
        Type type = Nullable.GetUnderlyingType(declaredType) ?? declaredType;

        if (type.IsEnum)
            return "enum";

        if (type == typeof(TimeSpan))
            return "duration_ms";

        if (type == typeof(bool))
            return "bool";

        if (type == typeof(int))
            return "int";

        if (type == typeof(long))
            return "long";

        if (type == typeof(double) || type == typeof(float))
            return "double";

        if (type == typeof(string))
            return "string";

        return typeof(IEnumerable).IsAssignableFrom(type) ? "list" : type.Name.ToLowerInvariant();
    }
}
