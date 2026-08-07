
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;

namespace CamusDB.Tests.Config;

/// <summary>
/// Covers the ordered configuration lookup. Every test here manipulates process-wide environment
/// variables (<c>CAMUS_CONFIG_PATH</c>, <c>CAMUS_HOME</c>), so the whole fixture is
/// <see cref="NonParallelizableAttribute"/>: a concurrent fixture reading those variables would see
/// another test's value.
/// </summary>
[NonParallelizable]
public class TestConfigLocator
{
    private string workingDirectory = "";

    private string? savedConfigPath;

    private string? savedCamusHome;

    [SetUp]
    public void SetUp()
    {
        savedConfigPath = Environment.GetEnvironmentVariable("CAMUS_CONFIG_PATH");
        savedCamusHome = Environment.GetEnvironmentVariable("CAMUS_HOME");

        // Both variables participate in the lookup, so the ambient environment must not leak in.
        Environment.SetEnvironmentVariable("CAMUS_CONFIG_PATH", null);

        workingDirectory = Path.Combine(Path.GetTempPath(), "camusdb-locator-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(workingDirectory);

        // Point the user-configuration probe at a scratch directory so a config.yml in the developer's
        // real home directory cannot decide the outcome of a test.
        Environment.SetEnvironmentVariable("CAMUS_HOME", Path.Combine(workingDirectory, "home"));
    }

    [TearDown]
    public void TearDown()
    {
        Environment.SetEnvironmentVariable("CAMUS_CONFIG_PATH", savedConfigPath);
        Environment.SetEnvironmentVariable("CAMUS_HOME", savedCamusHome);

        if (Directory.Exists(workingDirectory))
            Directory.Delete(workingDirectory, recursive: true);
    }

    private string WriteConfig(string relativePath, string yml)
    {
        string path = Path.Combine(workingDirectory, relativePath);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, yml);
        return path;
    }

    [Test]
    public void TestNoConfigFileAnywhereYieldsBuiltInDefaults()
    {
        (ConfigDefinition config, ConfigLocation location) = ConfigLocator.Load(workingDirectory: workingDirectory);

        Assert.AreEqual(ConfigSourceKind.BuiltInDefaults, location.Kind);
        Assert.IsNull(location.Path);
        Assert.AreEqual("standalone", config.Mode);
    }

    /// <summary>
    /// A freshly installed tool has no configuration file, so the defaults it starts on must pass
    /// the same validation an operator's file does. Without this, "no file" would be a startup crash.
    /// </summary>
    [Test]
    public void TestBuiltInDefaultsAreValid()
    {
        ConfigDefinition config = new();
        ConfigResolver.ApplyEffectiveDefaults(config);

        Assert.DoesNotThrow(() => config.Validate());
    }

    [Test]
    public void TestExplicitPathWins()
    {
        string explicitPath = WriteConfig("elsewhere/mine.yml", "http_port: 6001");
        WriteConfig("camusdb.yml", "http_port: 6002");
        Environment.SetEnvironmentVariable("CAMUS_CONFIG_PATH", WriteConfig("env.yml", "http_port: 6003"));

        (ConfigDefinition config, ConfigLocation location) =
            ConfigLocator.Load(explicitPath, workingDirectory);

        Assert.AreEqual(ConfigSourceKind.CommandLine, location.Kind);
        Assert.AreEqual(explicitPath, location.Path);
        Assert.AreEqual(6001, config.HttpPort);
    }

    /// <summary>
    /// An explicitly named file that is absent must fail, never fall through: starting the node on
    /// a different configuration than the operator named is worse than not starting.
    /// </summary>
    [Test]
    public void TestMissingExplicitPathThrowsRatherThanFallingThrough()
    {
        WriteConfig("camusdb.yml", "http_port: 6002");

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            ConfigLocator.Load(Path.Combine(workingDirectory, "absent.yml"), workingDirectory));

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex!.Code);
        StringAssert.Contains("absent.yml", ex.Message);
    }

    [Test]
    public void TestMissingEnvironmentPathThrowsRatherThanFallingThrough()
    {
        WriteConfig("camusdb.yml", "http_port: 6002");
        Environment.SetEnvironmentVariable("CAMUS_CONFIG_PATH", Path.Combine(workingDirectory, "absent.yml"));

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            ConfigLocator.Load(workingDirectory: workingDirectory));

        Assert.AreEqual(CamusDBErrorCodes.InvalidConfig, ex!.Code);
        StringAssert.Contains("CAMUS_CONFIG_PATH", ex.Message);
    }

    [Test]
    public void TestEnvironmentPathBeatsWorkingDirectory()
    {
        string envPath = WriteConfig("env.yml", "http_port: 6003");
        WriteConfig("camusdb.yml", "http_port: 6002");
        Environment.SetEnvironmentVariable("CAMUS_CONFIG_PATH", envPath);

        (ConfigDefinition config, ConfigLocation location) = ConfigLocator.Load(workingDirectory: workingDirectory);

        Assert.AreEqual(ConfigSourceKind.Environment, location.Kind);
        Assert.AreEqual(6003, config.HttpPort);
    }

    /// <summary>
    /// The repository checkout and the container image both start beside a <c>Config/config.yml</c>,
    /// so this probe is what keeps those two workflows working unchanged.
    /// </summary>
    [Test]
    public void TestWorkingDirectoryConfigSubdirectoryIsFound()
    {
        WriteConfig(Path.Combine("Config", "config.yml"), "http_port: 6004");

        (ConfigDefinition config, ConfigLocation location) = ConfigLocator.Load(workingDirectory: workingDirectory);

        Assert.AreEqual(ConfigSourceKind.WorkingDirectory, location.Kind);
        Assert.AreEqual(6004, config.HttpPort);
    }

    [Test]
    public void TestBareCamusdbYmlBeatsConfigSubdirectory()
    {
        WriteConfig("camusdb.yml", "http_port: 6002");
        WriteConfig(Path.Combine("Config", "config.yml"), "http_port: 6004");

        (ConfigDefinition config, ConfigLocation location) = ConfigLocator.Load(workingDirectory: workingDirectory);

        Assert.AreEqual(ConfigSourceKind.WorkingDirectory, location.Kind);
        Assert.AreEqual(6002, config.HttpPort);
    }

    [Test]
    public void TestUserConfigFoundWhenWorkingDirectoryHasNone()
    {
        WriteConfig(Path.Combine("home", "config.yml"), "http_port: 6005");

        (ConfigDefinition config, ConfigLocation location) = ConfigLocator.Load(workingDirectory: workingDirectory);

        Assert.AreEqual(ConfigSourceKind.UserHome, location.Kind);
        Assert.AreEqual(ConfigLocator.UserConfigPath(), location.Path);
        Assert.AreEqual(6005, config.HttpPort);
    }

    [Test]
    public void TestWorkingDirectoryBeatsUserConfig()
    {
        WriteConfig("camusdb.yml", "http_port: 6002");
        WriteConfig(Path.Combine("home", "config.yml"), "http_port: 6005");

        (ConfigDefinition config, ConfigLocation location) = ConfigLocator.Load(workingDirectory: workingDirectory);

        Assert.AreEqual(ConfigSourceKind.WorkingDirectory, location.Kind);
        Assert.AreEqual(6002, config.HttpPort);
    }

    [Test]
    public void TestCamusHomeRelocatesConfigAndData()
    {
        string home = Path.Combine(workingDirectory, "home");

        Assert.AreEqual(Path.Combine(home, "config.yml"), ConfigLocator.UserConfigPath());
        Assert.AreEqual(Path.Combine(home, "data"), ConfigLocator.DefaultDataDirectory());
    }

    /// <summary>
    /// With nothing configured the data directory must not be relative to the process working
    /// directory: an installed tool would then keep a different database for every directory the
    /// user happens to launch it from.
    /// </summary>
    [Test]
    public void TestDefaultDataDirectoryIsAbsoluteAndNotWorkingDirectoryRelative()
    {
        Environment.SetEnvironmentVariable("CAMUS_HOME", null);

        string dataDirectory = ConfigLocator.DefaultDataDirectory();

        Assert.IsTrue(Path.IsPathRooted(dataDirectory), dataDirectory);
        Assert.AreNotEqual(Path.GetFullPath("Data"), dataDirectory);
    }
}
