
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config.Models;

namespace CamusDB.Core.Config;

/// <summary>
/// Finds the YAML configuration a node should start with, searching an ordered list of locations
/// and stopping at the first hit.
/// <para>
/// Two properties matter and are easy to lose. First, <b>an explicit location must never fall
/// through</b>: if the operator named a file with <c>--config</c> or <c>CAMUS_CONFIG_PATH</c> and it
/// is not there, that is an error, not a cue to silently start on someone else's settings. Second,
/// <b>no configuration file at all is a valid, supported state</b> — a freshly installed
/// <c>camusdb</c> tool has no file anywhere and must still start on built-in defaults. The previous
/// behavior (a mandatory working-directory-relative <c>Config/config.yml</c>) satisfied neither, and
/// made the server unable to start outside a repository checkout.
/// </para>
/// <para>
/// The working-directory step is what keeps <c>dotnet run --project CamusDB</c> and the container
/// image working unchanged, since both start with the repository's <c>Config/config.yml</c> beside
/// them. The user-home step is the one a globally installed tool relies on: it lives outside any
/// repository, so a <c>git pull</c> or <c>git checkout</c> cannot overwrite an operator's settings.
/// </para>
/// </summary>
public static class ConfigLocator
{
    /// <summary>Config file name looked for in the working directory and the user configuration directory.</summary>
    public const string UserConfigFileName = "config.yml";

    /// <summary>
    /// Working-directory candidates, in order. <c>camusdb.yml</c> is offered first so a project can
    /// keep a node's settings as a single visible file; <c>Config/config.yml</c> is the layout the
    /// repository and the container image already use.
    /// </summary>
    private static readonly string[] WorkingDirectoryCandidates =
    [
        "camusdb.yml",
        Path.Combine("Config", "config.yml"),
    ];

    /// <summary>
    /// Resolves and reads the configuration. Returns the parsed definition together with the
    /// location it came from, so the caller can report it.
    /// </summary>
    /// <param name="explicitPath">Value of the <c>--config</c> flag, or null when not supplied.</param>
    /// <param name="workingDirectory">Directory the working-directory probes are relative to; defaults to the process working directory.</param>
    /// <exception cref="CamusDBException">
    /// The explicitly named file does not exist, or a located file is not valid configuration.
    /// </exception>
    public static (ConfigDefinition Config, ConfigLocation Location) Load(
        string? explicitPath = null,
        string? workingDirectory = null)
    {
        ConfigLocation location = Locate(explicitPath, workingDirectory);

        if (location.Path is null)
            return (new ConfigDefinition(), location);

        string yml = File.ReadAllText(location.Path);
        return (new ConfigReader().Read(yml), location);
    }

    /// <summary>
    /// Runs the lookup without reading the file. Split out from <see cref="Load"/> so the search
    /// order can be tested on its own, and so tooling (e.g. the <c>init</c> command) can report
    /// which file a subsequent run would pick up.
    /// </summary>
    public static ConfigLocation Locate(string? explicitPath = null, string? workingDirectory = null)
    {
        // An explicitly named file is never probed: it either exists or startup fails. Falling
        // through would start the node on a different configuration than the operator named.
        if (!string.IsNullOrWhiteSpace(explicitPath))
            return Explicit(explicitPath, ConfigSourceKind.CommandLine, "--config");

        string? envPath = Environment.GetEnvironmentVariable("CAMUS_CONFIG_PATH");
        if (!string.IsNullOrWhiteSpace(envPath))
            return Explicit(envPath, ConfigSourceKind.Environment, "CAMUS_CONFIG_PATH");

        string cwd = workingDirectory ?? Directory.GetCurrentDirectory();
        foreach (string candidate in WorkingDirectoryCandidates)
        {
            string path = Path.GetFullPath(Path.Combine(cwd, candidate));
            if (File.Exists(path))
                return new ConfigLocation(ConfigSourceKind.WorkingDirectory, path);
        }

        string userPath = UserConfigPath();
        if (File.Exists(userPath))
            return new ConfigLocation(ConfigSourceKind.UserHome, userPath);

        return new ConfigLocation(ConfigSourceKind.BuiltInDefaults, null);
    }

    /// <summary>
    /// The user-owned configuration file: <c>$CAMUS_HOME/config.yml</c> when that variable is set,
    /// otherwise <c>~/.camusdb/config.yml</c> (<c>%APPDATA%\camusdb\config.yml</c> on Windows).
    /// Deliberately outside any repository, so no checkout can replace an operator's settings.
    /// </summary>
    public static string UserConfigPath() => Path.Combine(UserConfigDirectory(), UserConfigFileName);

    /// <summary>Directory holding <see cref="UserConfigPath"/>. See that member for the layout.</summary>
    public static string UserConfigDirectory()
    {
        string? camusHome = Environment.GetEnvironmentVariable("CAMUS_HOME");
        if (!string.IsNullOrWhiteSpace(camusHome))
            return Path.GetFullPath(camusHome);

        if (OperatingSystem.IsWindows())
            return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "camusdb");

        return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".camusdb");
    }

    /// <summary>
    /// Where a node stores its data when nothing configures <c>data_dir</c>:
    /// <c>$CAMUS_HOME/data</c>, else <c>$XDG_DATA_HOME/camusdb</c> or <c>~/.local/share/camusdb</c>
    /// (<c>%LOCALAPPDATA%\camusdb</c> on Windows).
    /// <para>
    /// The previous default rooted the database at <c>./Data</c> relative to the process working
    /// directory, which for an installed tool means a database appearing wherever the user happened
    /// to be standing — and a different database each time they move. The shipped reference config
    /// points at <c>/tmp/camusdb</c>, which the operating system reaps; neither is an acceptable
    /// default for data someone expects to keep.
    /// </para>
    /// </summary>
    public static string DefaultDataDirectory()
    {
        string? camusHome = Environment.GetEnvironmentVariable("CAMUS_HOME");
        if (!string.IsNullOrWhiteSpace(camusHome))
            return Path.Combine(Path.GetFullPath(camusHome), "data");

        if (OperatingSystem.IsWindows())
            return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "camusdb");

        string? xdgDataHome = Environment.GetEnvironmentVariable("XDG_DATA_HOME");
        if (!string.IsNullOrWhiteSpace(xdgDataHome))
            return Path.Combine(Path.GetFullPath(xdgDataHome), "camusdb");

        return Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            ".local", "share", "camusdb");
    }

    private static ConfigLocation Explicit(string path, ConfigSourceKind kind, string origin)
    {
        string full = Path.GetFullPath(path);

        if (!File.Exists(full))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidConfig,
                $"Configuration file '{full}' (from {origin}) does not exist.");

        return new ConfigLocation(kind, full);
    }
}
