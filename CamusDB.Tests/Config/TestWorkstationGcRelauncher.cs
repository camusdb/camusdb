
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using NUnit.Framework;

using CamusDB.Core.Config;

namespace CamusDB.Tests.Config;

/// <summary>
/// Covers when the dev memory profile hands the server over to a workstation-GC child, and what that
/// child is started with. The relaunch itself spawns a full server process, so it is exercised by
/// running the host rather than here; these tests pin the decision and the command line, which are the
/// parts a wrong edit would silently break (a relaunch loop, an operator's GC choice overridden, or a
/// child started without its assembly under the <c>dotnet</c> muxer).
/// </summary>
[TestFixture]
public sealed class TestWorkstationGcRelauncher
{
    private static Func<string, string?> Env(params (string Name, string Value)[] variables)
    {
        Dictionary<string, string> values = variables.ToDictionary(v => v.Name, v => v.Value, StringComparer.Ordinal);
        return name => values.TryGetValue(name, out string? value) ? value : null;
    }

    [Test]
    public void DevProfileUnderServerGcRelaunches()
    {
        Assert.That(WorkstationGcRelauncher.ShouldRelaunch(MemoryProfile.Dev, isServerGc: true, Env()), Is.True);
    }

    [Test]
    public void ProdProfileNeverRelaunches()
    {
        Assert.That(WorkstationGcRelauncher.ShouldRelaunch(MemoryProfile.Prod, isServerGc: true, Env()), Is.False);
    }

    [Test]
    public void DevProfileAlreadyOnWorkstationGcDoesNotRelaunch()
    {
        Assert.That(WorkstationGcRelauncher.ShouldRelaunch(MemoryProfile.Dev, isServerGc: false, Env()), Is.False);
    }

    [Test]
    public void RelaunchedChildNeverRelaunchesAgain()
    {
        // Even if the child somehow came up on server GC, the marker stops a second spawn.
        Func<string, string?> env = Env((WorkstationGcRelauncher.RelaunchedVariable, "1"));

        Assert.That(WorkstationGcRelauncher.ShouldRelaunch(MemoryProfile.Dev, isServerGc: true, env), Is.False);
    }

    [TestCase("DOTNET_gcServer", "1")]
    [TestCase("DOTNET_gcServer", "0")]
    [TestCase("COMPlus_gcServer", "1")]
    public void ExplicitGcModeInEnvironmentIsKept(string variable, string value)
    {
        Assert.That(WorkstationGcRelauncher.ShouldRelaunch(MemoryProfile.Dev, isServerGc: true, Env((variable, value))), Is.False);
    }

    [Test]
    public void EmptyGcServerVariableIsNotAnExplicitChoice()
    {
        Assert.That(WorkstationGcRelauncher.ShouldRelaunch(MemoryProfile.Dev, isServerGc: true, Env(("DOTNET_gcServer", ""))), Is.True);
    }

    [Test]
    public void AppHostIsStartedDirectlyWithArgumentsAndWorkstationGc()
    {
        string[] args = ["--memory-profile", "dev", "--data-dir", "/tmp/a dir with spaces"];

        ProcessStartInfo? info = WorkstationGcRelauncher.BuildStartInfo("/usr/local/bin/camusdb", "/opt/camusdb/CamusDB.dll", args);

        Assert.That(info, Is.Not.Null);
        Assert.That(info!.FileName, Is.EqualTo("/usr/local/bin/camusdb"));
        Assert.That(info.ArgumentList, Is.EqualTo(args));
        Assert.That(info.UseShellExecute, Is.False);
        Assert.That(info.RedirectStandardOutput || info.RedirectStandardError || info.RedirectStandardInput, Is.False);
        Assert.That(info.Environment["DOTNET_gcServer"], Is.EqualTo("0"));
        Assert.That(info.Environment[WorkstationGcRelauncher.RelaunchedVariable], Is.EqualTo("1"));
    }

    [TestCase("/usr/local/share/dotnet/dotnet")]
    [TestCase("/usr/share/dotnet/DOTNET")]
    public void DotnetMuxerGetsTheEntryAssemblyFirst(string muxer)
    {
        ProcessStartInfo? info = WorkstationGcRelauncher.BuildStartInfo(muxer, "/opt/camusdb/CamusDB.dll", ["--memory-profile", "dev"]);

        Assert.That(info, Is.Not.Null);
        Assert.That(info!.ArgumentList, Is.EqualTo(new[] { "/opt/camusdb/CamusDB.dll", "--memory-profile", "dev" }));
    }

    [Test]
    public void NoRelaunchWithoutAProcessPathOrUnderTheMuxerWithoutAnAssembly()
    {
        Assert.That(WorkstationGcRelauncher.BuildStartInfo(null, "/opt/camusdb/CamusDB.dll", []), Is.Null);
        Assert.That(WorkstationGcRelauncher.BuildStartInfo("/usr/local/share/dotnet/dotnet", "", []), Is.Null);
    }
}
