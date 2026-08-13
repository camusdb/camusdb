/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Config;

/// <summary>
/// When a change to a setting takes effect. This is a claim about the code that <em>reads</em> the
/// setting, not a preference: a value latched once in a constructor is <see cref="Restart"/>-class
/// no matter how appealing changing it live would be, and labeling it <see cref="Runtime"/> ships a
/// promise the component does not keep.
/// </summary>
public enum ConfigMutability
{
    /// <summary>
    /// A new value takes effect at the reader's next boundary — the next statement, the next
    /// transaction begin, or the next iteration of a background loop. Only these settings may be
    /// changed with <c>SET CLUSTER SETTING</c>.
    /// </summary>
    Runtime = 0,

    /// <summary>
    /// The value is baked into something built once per process (the data directory, node identity,
    /// the embedded storage node, a component constructed at boot that never re-reads). Changing it
    /// requires editing configuration and restarting the node; a runtime change is rejected.
    /// </summary>
    Restart = 1,
}

/// <summary>
/// Whether the fleet must agree on a setting's value, or the setting is deliberately per-node.
/// </summary>
public enum ConfigScope
{
    /// <summary>
    /// The whole cluster must run the same value (isolation defaults, mutation caps, lease/TTL
    /// policy): nodes disagreeing about it makes a user's transaction behave differently depending
    /// on which node accepted it. Runtime changes to these replicate to every node.
    /// </summary>
    Cluster = 0,

    /// <summary>
    /// Deliberately per-node: tracing switches, local cache sizes, whether <em>this</em> node may
    /// run background refresh work. Other nodes are free to differ.
    /// </summary>
    Node = 1,
}

/// <summary>
/// Classifies one configuration setting on the two operator-facing axes: mutability (can it change
/// without a restart?) and scope (must the fleet agree?). Lives on the option property itself so
/// the classification sits next to the thing it classifies, and both axes are required positional
/// arguments so neither can be defaulted by omission — an unclassified setting fails
/// <c>TestConfigSettingClassification</c> rather than silently defaulting to something plausible.
///
/// <para>The nested <c>kahuna.*</c> section is deliberately not attributed per property: the
/// embedded node is constructed once from <c>EmbeddedKahunaOptionsBuilder</c> before anything else
/// starts and never rebuilt, so every key in it is restart-class and node-scoped by rule (see
/// <see cref="ConfigVariableCatalog"/>).</para>
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
public sealed class ConfigSettingAttribute : Attribute
{
    /// <summary>When a change to this setting takes effect.</summary>
    public ConfigMutability Mutability { get; }

    /// <summary>Whether the fleet must agree on this setting's value.</summary>
    public ConfigScope Scope { get; }

    public ConfigSettingAttribute(ConfigMutability mutability, ConfigScope scope)
    {
        Mutability = mutability;
        Scope = scope;
    }
}
