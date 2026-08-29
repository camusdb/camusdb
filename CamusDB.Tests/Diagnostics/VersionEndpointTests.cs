/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using CamusDB.App.Controllers;
using CamusDB.App.Models;
using Microsoft.AspNetCore.Mvc;
using NUnit.Framework;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// Covers the version surface a benchmark harness records in its run manifest. The value of the
/// endpoint is that it reports what the process loaded rather than what a project file declares, so
/// the tests check that the versions are real and specific enough to tell two builds apart.
/// </summary>
[TestFixture]
public sealed class VersionEndpointTests
{
    private static VersionResponse Get()
    {
        VersionController controller = new();
        JsonResult result = controller.GetVersion();
        Assert.That(result.Value, Is.InstanceOf<VersionResponse>());
        return (VersionResponse)result.Value!;
    }

    [Test]
    public void ReportsTheServerAndRuntimeVersions()
    {
        VersionResponse version = Get();

        Assert.That(version.Server, Is.Not.Empty);
        Assert.That(version.Runtime, Is.Not.Empty);
    }

    [Test]
    public void ReportsTheEngineAssemblyItLoaded()
    {
        VersionResponse version = Get();

        Assert.That(version.Components.Select(c => c.Name), Does.Contain("CamusDB.Core"));
        Assert.That(version.Components.All(c => !string.IsNullOrWhiteSpace(c.Version)), Is.True);
    }

    [Test]
    public void ReportsOnlyTheFamiliesWorthRecording()
    {
        // A manifest that listed every loaded assembly would bury the four that change behaviour.
        VersionResponse version = Get();

        Assert.That(version.Components.All(c =>
            c.Name.StartsWith("CamusDB", StringComparison.Ordinal) ||
            c.Name.StartsWith("Kahuna", StringComparison.Ordinal) ||
            c.Name.StartsWith("Kommander", StringComparison.Ordinal) ||
            c.Name.StartsWith("Nixie", StringComparison.Ordinal)), Is.True);
    }

    [Test]
    public void OrdersComponentsSoTwoRunsCompareAsText()
    {
        VersionResponse version = Get();
        List<string> names = version.Components.Select(c => c.Name).ToList();

        Assert.That(names, Is.EqualTo(names.OrderBy(n => n, StringComparer.Ordinal).ToList()));
    }

    [Test]
    public void ReturnsTheSameAnswerOnEveryCall()
    {
        // The answer is cached: it is asked once per run, but /ping-class endpoints get polled.
        Assert.That(Get(), Is.EqualTo(Get()));
    }
}
