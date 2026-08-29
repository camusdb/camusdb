/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Reflection;
using CamusDB.App.Models;
using Microsoft.AspNetCore.Mvc;

namespace CamusDB.App.Controllers;

/// <summary>
/// Reports which build of the server and of its embedded storage/consensus assemblies this node is
/// running, so a benchmark harness can record what the image actually loaded instead of what a
/// project file says it should have.
///
/// <para>Unauthenticated, on the same footing as <c>/ping</c> and <c>/v1/cluster/health</c>: a probe
/// has to work before credentials exist, and the response carries version metadata only — no
/// configuration, no data, and no topology. Treat it like the rest of that surface and keep it on a
/// trusted interface.</para>
///
/// <para>The answer is computed once and cached. Enumerating loaded assemblies is cheap but not free,
/// and the set cannot change in a way this endpoint reports differently: an assembly loaded later
/// only adds to it, which the next process start picks up.</para>
/// </summary>
[ApiController]
public sealed class VersionController : ControllerBase
{
    private static VersionResponse? cached;

    [HttpGet]
    [Route("/v1/version")]
    public JsonResult GetVersion() => new(cached ??= Build());

    private static VersionResponse Build()
    {
        List<ComponentVersion> components = new();

        foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            string? name = assembly.GetName().Name;
            if (name is null || !IsReportable(name))
                continue;

            components.Add(new ComponentVersion(name, VersionOf(assembly)));
        }

        components.Sort((a, b) => string.CompareOrdinal(a.Name, b.Name));

        return new VersionResponse(
            Server: VersionOf(typeof(VersionController).Assembly),
            Runtime: Environment.Version.ToString(),
            Components: components);
    }

    /// <summary>
    /// The families worth reporting: this product and the three sibling libraries whose version
    /// changes the measured behaviour. Everything else would be noise in a run manifest.
    /// </summary>
    private static bool IsReportable(string assemblyName)
        => assemblyName.StartsWith("CamusDB", StringComparison.Ordinal)
        || assemblyName.StartsWith("Kahuna", StringComparison.Ordinal)
        || assemblyName.StartsWith("Kommander", StringComparison.Ordinal)
        || assemblyName.StartsWith("Nixie", StringComparison.Ordinal);

    /// <summary>
    /// Prefers the informational version, which for a package reference is the NuGet package version
    /// (<c>1.4.14</c>), over the assembly version, which is usually rounded to the major (<c>1.0.0.0</c>)
    /// and so cannot distinguish two builds a benchmark must not compare.
    /// </summary>
    private static string VersionOf(Assembly assembly)
    {
        string? informational = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (!string.IsNullOrWhiteSpace(informational))
            return informational;

        return assembly.GetName().Version?.ToString() ?? "unknown";
    }
}
