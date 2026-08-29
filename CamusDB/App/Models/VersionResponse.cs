/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

/// <summary>One assembly this process actually loaded, and the version it reports.</summary>
/// <param name="Name">Simple assembly name, e.g. <c>Kahuna.Core</c>.</param>
/// <param name="Version">Informational version, which for a NuGet package is its package version.</param>
public sealed record ComponentVersion(string Name, string Version);

/// <summary>
/// The versions a node is running: the server itself plus the storage and consensus assemblies it
/// loaded.
///
/// <para>It exists for one reason: a performance comparison between two runs is only valid when both
/// ran the same code, and the dependency versions are the ones most likely to differ silently. A
/// harness that records a package version from a checked-in project file records what the repository
/// intends, not what the running image loaded — a locally built package, a stale image layer, or a
/// transitive bump all break that assumption without changing any file.</para>
///
/// <para>Reported versions come from the assemblies present in the process, so a component that is
/// loaded lazily and has not been used yet is simply absent rather than wrong.</para>
/// </summary>
/// <param name="Server">The CamusDB server's own informational version.</param>
/// <param name="Runtime">The .NET runtime version the process is executing on.</param>
/// <param name="Components">Loaded CamusDB, Kahuna, Kommander and Nixie assemblies.</param>
public sealed record VersionResponse(string Server, string Runtime, IReadOnlyList<ComponentVersion> Components);
