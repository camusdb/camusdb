
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Config;

/// <summary>
/// Where a node's YAML configuration came from. Reported at startup because a layered lookup
/// (flag, environment, working directory, user home, built-in defaults) otherwise makes
/// "which config is this node actually running?" unanswerable from the console — the first
/// question asked whenever a setting appears not to have taken effect.
/// </summary>
/// <param name="Kind">Which step of the lookup produced the configuration.</param>
/// <param name="Path">
/// Absolute path of the file that was read, or <c>null</c> when no file was found and the
/// built-in defaults are in effect.
/// </param>
public readonly record struct ConfigLocation(ConfigSourceKind Kind, string? Path)
{
    /// <summary>
    /// One-line description for the startup banner, e.g.
    /// <c>"/Users/me/.camusdb/config.yml (user configuration)"</c>.
    /// </summary>
    public string Describe() => Kind switch
    {
        ConfigSourceKind.CommandLine => $"{Path} (--config)",
        ConfigSourceKind.Environment => $"{Path} (CAMUS_CONFIG_PATH)",
        ConfigSourceKind.WorkingDirectory => $"{Path} (working directory)",
        ConfigSourceKind.UserHome => $"{Path} (user configuration)",
        _ => "built-in defaults (no configuration file found)",
    };
}

/// <summary>
/// The step of the configuration lookup that supplied the configuration. The order of the
/// members is the lookup order, highest precedence first.
/// </summary>
public enum ConfigSourceKind
{
    /// <summary>An explicit <c>--config &lt;path&gt;</c> flag. A missing file here is a startup error.</summary>
    CommandLine,

    /// <summary>The <c>CAMUS_CONFIG_PATH</c> environment variable. A missing file here is a startup error.</summary>
    Environment,

    /// <summary>A file found in the current working directory — a per-project config, and how the repo checkout and the container image are configured.</summary>
    WorkingDirectory,

    /// <summary>The user-owned configuration under the home directory, which no repository checkout can overwrite.</summary>
    UserHome,

    /// <summary>No file anywhere; the node runs on built-in defaults. This is the normal state of a freshly installed tool.</summary>
    BuiltInDefaults,
}
