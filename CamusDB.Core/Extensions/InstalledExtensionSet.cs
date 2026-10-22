/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Extensions;

/// <summary>
/// The extensions installed in one database: the replicated records (<see cref="Installed"/>) and,
/// per engine, their resolution against the assemblies that engine loaded
/// (<see cref="Resolve"/>). One instance belongs to one <c>Schema</c> and is never replaced, so a
/// statement's function scope can hold it without pinning the rest of the schema.
///
/// <para><b>Published snapshots.</b> The schema apply replaces the whole map
/// (<see cref="Publish"/>) under the schema lock; readers on the statement path take no lock and
/// always see one complete map. <see cref="Stamp"/> moves with every publish, and the caches that hold
/// work derived from function calls (the result cache, the bound-query cache, the plan cache) compare
/// it, so they never serve a result computed with a function set that is no longer installed.</para>
///
/// <para><b>Resolution is cached per snapshot.</b> The first statement after a publish builds the
/// <see cref="InstalledExtensionSet"/> once; later statements compare two references.</para>
/// </summary>
internal sealed class InstalledExtensionCatalog
{
    private static readonly IReadOnlyDictionary<string, ExtensionDefinition> NoneInstalled =
        new Dictionary<string, ExtensionDefinition>(StringComparer.OrdinalIgnoreCase);

    private volatile IReadOnlyDictionary<string, ExtensionDefinition> installed = NoneInstalled;

    private long stamp;

    private volatile ResolvedSet? resolved;

    /// <summary>The installed records keyed by extension name (case-insensitive).</summary>
    public IReadOnlyDictionary<string, ExtensionDefinition> Installed => installed;

    /// <summary>
    /// A value that changes whenever <see cref="Installed"/> changes in this process: 0 while nothing
    /// is installed, and otherwise the schema version that published the current map, which only grows.
    /// A database that never installs an extension keeps 0, so the caches that include the stamp key
    /// exactly as they did before extensions existed. Returning to 0 after the last drop is safe: a
    /// result computed while nothing was installed used no extension.
    /// </summary>
    public long Stamp => Interlocked.Read(ref stamp);

    /// <summary>
    /// Replaces the installed records. Called by the schema apply (under the schema lock, with the
    /// delta's target version as the stamp) and by the schema load. The map must not be changed after
    /// it is published.
    /// </summary>
    public void Publish(IReadOnlyDictionary<string, ExtensionDefinition> extensions, long newStamp)
    {
        installed = extensions;
        Interlocked.Exchange(ref stamp, extensions.Count == 0 ? 0 : newStamp);

        if (extensions.Count > 0)
            ExtensionRegistry.MarkLoaded();
    }

    /// <summary>
    /// The installed extensions resolved against <paramref name="registry"/>. Built once per published
    /// snapshot and registry. When an installed extension has no matching build on this node, the
    /// build logs one error that names the database and the extension; the statements that use the
    /// extension fail with <see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/>.
    /// </summary>
    public InstalledExtensionSet Resolve(ExtensionRegistry registry, string? databaseName, ILogger? logger)
    {
        IReadOnlyDictionary<string, ExtensionDefinition> snapshot = installed;

        if (snapshot.Count == 0)
            return InstalledExtensionSet.Empty;

        ResolvedSet? cached = resolved;

        if (cached is not null && ReferenceEquals(cached.Snapshot, snapshot) && ReferenceEquals(cached.Registry, registry))
            return cached.Set;

        InstalledExtensionSet set = InstalledExtensionSet.Build(snapshot, registry);
        resolved = new ResolvedSet(snapshot, registry, set);

        if (logger is not null)
        {
            foreach ((string name, string reason) in set.Missing)
                logger.LogError(
                    "Database {Database} has extension {Extension} installed, but {Reason}; statements that use it fail on this node until the same file is deployed",
                    databaseName, name, reason);
        }

        return set;
    }

    private sealed record ResolvedSet(IReadOnlyDictionary<string, ExtensionDefinition> Snapshot, ExtensionRegistry Registry, InstalledExtensionSet Set);
}

/// <summary>
/// The members a database can use on this node: for each installed extension, the loaded build with
/// the same name, version and SHA-256, and the functions and procedures it registered, by plain name
/// and by the qualified form <c>extension.member</c>. Immutable.
///
/// <para><b>Missing builds.</b> An installed extension that this node does not load (no file with that
/// name and version, or a file with another hash) is listed in <see cref="Missing"/>, and each member
/// name its record lists maps to the reason. Looking such a name up throws
/// <see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/> instead of "function not found", so the
/// caller learns which deployment is wrong. The hash comparison is what keeps two nodes from running
/// two builds of one version.</para>
///
/// <para><b>No shadowing.</b> The loader refuses a member that shadows a built-in, and
/// <c>CREATE EXTENSION</c> refuses a member that another installed extension defines, so a plain
/// name resolves to at most one member and the lookup order never matters.</para>
/// </summary>
internal sealed class InstalledExtensionSet
{
    public static readonly InstalledExtensionSet Empty = new();

    private readonly Dictionary<string, ScalarFunctionDescriptor> functions = new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<string, ExtensionProcedure> procedures = new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<string, LoadedExtension> bound = new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<string, string> missing = new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<string, string> missingFunctions = new(StringComparer.OrdinalIgnoreCase);

    private readonly Dictionary<string, string> missingProcedures = new(StringComparer.OrdinalIgnoreCase);

    private InstalledExtensionSet() { }

    /// <summary>The installed extensions this node cannot bind, with the reason, keyed by extension name.</summary>
    public IReadOnlyDictionary<string, string> Missing => missing;

    /// <summary>The installed extensions this node binds, keyed by extension name.</summary>
    public IReadOnlyDictionary<string, LoadedExtension> Bound => bound;

    public static InstalledExtensionSet Build(IReadOnlyDictionary<string, ExtensionDefinition> installed, ExtensionRegistry registry)
    {
        InstalledExtensionSet set = new();

        foreach (ExtensionDefinition record in installed.Values)
        {
            LoadedExtension? build = registry.Find(record.Name, record.Version);
            string? reason = null;

            if (build is null)
            {
                IReadOnlyList<LoadedExtension> versions = registry.GetVersions(record.Name);

                reason = versions.Count == 0
                    ? $"this node does not load extension '{record.Name}'"
                    : $"this node loads extension '{record.Name}' versions {string.Join(", ", versions.Select(v => v.Version))}, not the installed version {record.Version}";
            }
            else if (!string.Equals(build.Sha256, record.Sha256, StringComparison.OrdinalIgnoreCase))
            {
                reason = $"this node loads a different build of extension '{record.Name}' {record.Version} " +
                         $"(sha256 {build.Sha256}, installed sha256 {record.Sha256})";
            }

            if (reason is not null)
            {
                set.missing[record.Name] = reason;

                foreach (ExtensionMemberDefinition member in record.Members)
                {
                    Dictionary<string, string>? target = member.Kind switch
                    {
                        ExtensionMemberKind.Function => set.missingFunctions,
                        ExtensionMemberKind.Procedure => set.missingProcedures,
                        _ => null,
                    };

                    if (target is null)
                        continue;

                    target[member.Name] = record.Name;
                    target[record.Name + "." + member.Name] = record.Name;
                }

                continue;
            }

            set.bound[record.Name] = build!;

            foreach ((string name, ScalarFunctionDescriptor descriptor) in build!.FunctionTable)
            {
                set.functions[name] = descriptor;
                set.functions[build.Name + "." + name] = descriptor;
            }

            foreach ((string name, ExtensionProcedure procedure) in build.ProcedureTable)
            {
                set.procedures[name] = procedure;
                set.procedures[build.Name + "." + name] = procedure;
            }
        }

        return set;
    }

    /// <summary>
    /// Resolves a function by plain or qualified name. Returns false for a name that no installed
    /// extension defines, and throws <see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/> for a
    /// member of an installed extension this node does not bind.
    /// </summary>
    public bool TryGetFunction(string name, out ScalarFunctionDescriptor descriptor)
    {
        if (functions.TryGetValue(name, out descriptor!))
            return true;

        if (missingFunctions.TryGetValue(name, out string? extension))
            throw NotLoaded(extension, "function '" + name + "'");

        return false;
    }

    /// <summary>True when an installed extension defines a function of this name, whether or not this node binds it.</summary>
    public bool HasFunction(string name) => functions.ContainsKey(name) || missingFunctions.ContainsKey(name);

    /// <summary>Resolves a procedure the same way <see cref="TryGetFunction"/> resolves a function.</summary>
    public bool TryGetProcedure(string name, out ExtensionProcedure procedure)
    {
        if (procedures.TryGetValue(name, out procedure!))
            return true;

        if (missingProcedures.TryGetValue(name, out string? extension))
            throw NotLoaded(extension, "procedure '" + name + "'");

        return false;
    }

    /// <summary>
    /// The loaded build of the installed extension <paramref name="extensionName"/>. Throws
    /// <see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/> when it is installed but not bound here,
    /// and returns null when it is not installed at all.
    /// </summary>
    public LoadedExtension? GetBound(string extensionName, string usedBy)
    {
        if (bound.TryGetValue(extensionName, out LoadedExtension? build))
            return build;

        if (missing.ContainsKey(extensionName))
            throw NotLoaded(extensionName, usedBy);

        return null;
    }

    private CamusDBException NotLoaded(string extension, string usedBy) =>
        new(CamusDBErrorCodes.ExtensionNotLoadedOnNode,
            $"Cannot use {usedBy}: extension '{extension}' is installed in this database, but {missing[extension]}. " +
            "Deploy the same extension file on every node.");
}
