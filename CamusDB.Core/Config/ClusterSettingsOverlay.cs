/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Reflection;
using CamusDB.Core.Config.Models;
using YamlDotNet.Serialization.NamingConventions;

namespace CamusDB.Core.Config;

/// <summary>
/// Binds replicated cluster-setting overlay entries — <c>(yaml key → scalar text)</c> pairs — onto
/// a <see cref="ConfigDefinition"/>, so a runtime change flows through the exact resolution chain a
/// file value does: the same property, the same coercion, the same <see cref="ConfigDefinition.Validate"/>
/// cross-field checks, and finally <see cref="ConfigResolver.Resolve"/>. Applied after the CLI
/// layer, so the cluster value wins by ordering rather than by a re-implemented precedence rule.
///
/// <para>The scalar text is the spelling <see cref="ConfigVariableCatalog"/> reports — lowercase
/// booleans, invariant numerics, underscored enum members, whole-millisecond durations — so a value
/// read out of <c>SHOW VARIABLES</c> can be written back by <c>SET CLUSTER SETTING</c> unchanged.
/// Enum- and duration-typed options need no special parser here because their
/// <see cref="ConfigDefinition"/> fields are already the string/millisecond forms the file uses.</para>
/// </summary>
public static class ClusterSettingsOverlay
{
    /// <summary>How a submitted key classifies, deciding which of the three rejections (or none) it gets.</summary>
    public enum KeyClass
    {
        /// <summary>A runtime-mutable setting with a <see cref="ConfigDefinition"/> field — changeable live.</summary>
        Runtime = 0,

        /// <summary>A real setting whose classification is restart-bound; changeable only via file + restart.</summary>
        RestartOnly = 1,

        /// <summary>No setting by this name exists.</summary>
        Unknown = 2,
    }

    /// <summary>
    /// yaml key → the <see cref="ConfigDefinition"/> property the overlay writes. Built once by
    /// reflection so it can never drift from the definition, exactly like the variable catalog.
    /// </summary>
    private static readonly Dictionary<string, PropertyInfo> DefinitionByYamlKey = BuildDefinitionMap();

    /// <summary>
    /// yaml key → the <see cref="ConfigSettingAttribute"/> declared on the corresponding
    /// <see cref="CamusDBOptions"/> property, which is what decides runtime vs restart.
    /// </summary>
    private static readonly Dictionary<string, ConfigSettingAttribute> ClassificationByYamlKey = BuildClassificationMap();

    private static Dictionary<string, PropertyInfo> BuildDefinitionMap()
    {
        Dictionary<string, PropertyInfo> map = new(StringComparer.OrdinalIgnoreCase);

        foreach (PropertyInfo property in typeof(ConfigDefinition).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.SetMethod is null)
                continue;

            map[UnderscoredNamingConvention.Instance.Apply(property.Name)] = property;
        }

        return map;
    }

    private static Dictionary<string, ConfigSettingAttribute> BuildClassificationMap()
    {
        Dictionary<string, ConfigSettingAttribute> map = new(StringComparer.OrdinalIgnoreCase);

        foreach (PropertyInfo property in typeof(CamusDBOptions).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.SetMethod is null)
                continue;

            ConfigSettingAttribute? classification = property.GetCustomAttribute<ConfigSettingAttribute>();
            if (classification is not null)
                map[ConfigVariableCatalog.YamlName(property.Name)] = classification;
        }

        return map;
    }

    /// <summary>
    /// Classifies a submitted key. The three answers route to three different operator errors,
    /// because they send the operator to three different places: a typo hunt, the config file, or
    /// the value itself. <c>kahuna.*</c> keys are restart-only by the section rule.
    /// </summary>
    public static KeyClass Classify(string key)
    {
        if (key.StartsWith("kahuna.", StringComparison.OrdinalIgnoreCase))
            return KahunaOptionsConfig.AllowedYamlKeys.Contains(key["kahuna.".Length..])
                ? KeyClass.RestartOnly
                : KeyClass.Unknown;

        if (!ClassificationByYamlKey.TryGetValue(key, out ConfigSettingAttribute? classification))
            return KeyClass.Unknown;

        // Runtime-classified but not file-mappable would be a contract violation (the overlay can
        // only write what ConfigDefinition models); treat it as restart-only so the error tells the
        // operator the truth — the setting cannot be changed live.
        if (classification.Mutability != ConfigMutability.Runtime)
            return KeyClass.RestartOnly;

        return DefinitionByYamlKey.ContainsKey(key) ? KeyClass.Runtime : KeyClass.RestartOnly;
    }

    /// <summary>
    /// Writes one overlay entry onto <paramref name="definition"/>, recording cluster provenance
    /// for the key. The caller validates the key with <see cref="Classify"/> first and calls
    /// <see cref="ConfigDefinition.Validate"/> after the last entry, so range and cross-field
    /// checks run against the resulting configuration.
    /// </summary>
    public static void Apply(ConfigDefinition definition, string key, string valueText)
    {
        if (!DefinitionByYamlKey.TryGetValue(key, out PropertyInfo? property))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown cluster setting '{key}'");

        property.SetValue(definition, ParseScalar(key, property.PropertyType, valueText));
        definition.RecordSource(key, ConfigValueSource.Cluster);
    }

    /// <summary>
    /// Parses the catalog's scalar spelling back into the definition's field type — the inverse of
    /// <see cref="ConfigVariableCatalog"/>'s formatting, held to the same invariant-culture rules
    /// so a value cannot mean one thing in a file and another in a statement.
    /// </summary>
    private static object ParseScalar(string key, Type targetType, string valueText)
    {
        try
        {
            if (targetType == typeof(bool))
            {
                return valueText switch
                {
                    "true" => true,
                    "false" => false,
                    _ => throw new FormatException("expected 'true' or 'false'"),
                };
            }

            if (targetType == typeof(int))
                return int.Parse(valueText, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);

            if (targetType == typeof(long))
                return long.Parse(valueText, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);

            if (targetType == typeof(double))
                return double.Parse(valueText, NumberStyles.Float, CultureInfo.InvariantCulture);

            if (targetType == typeof(string))
                return valueText;
        }
        catch (Exception e) when (e is FormatException or OverflowException)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Invalid value '{valueText}' for cluster setting '{key}': {e.Message}");
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Cluster setting '{key}' has unsupported type '{targetType.Name}' and cannot be changed at runtime");
    }
}
