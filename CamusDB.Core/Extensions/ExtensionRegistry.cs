/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Reflection;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Extensions;

namespace CamusDB.Core.Extensions;

/// <summary>How much the server checked an extension assembly before loading it.</summary>
public enum ExtensionTrust
{
    /// <summary>Passed the static SAFE check, and its load context refuses assemblies outside the
    /// SAFE allow-list (<c>extension_assemblies</c>).</summary>
    Safe = 0,

    /// <summary>Loaded without any check (<c>unsafe_extension_assemblies</c>).</summary>
    Unsafe = 1
}

/// <summary>
/// One loaded extension assembly: its manifest identity (name and version), where it came from, the
/// hash of its bytes, and everything it registered. One per (name, version) on an engine; a node can
/// load two versions of one extension side by side, each in its own load context.
///
/// <para><b>Available, not installed.</b> Loading makes the extension available on this node only.
/// Its members resolve in a database only while that database's installed record
/// (<see cref="Catalogs.Models.ExtensionDefinition"/>) names this exact name, version and
/// <see cref="Sha256"/>; see <see cref="InstalledExtensionSet"/>.</para>
///
/// <para><see cref="Sha256"/> is the hash of the exact bytes the server verified and loaded (the file
/// is read once, and those bytes are both checked and loaded). It is how a node with a different build
/// of the same name and version is told apart: such a node does not bind the database's record.</para>
/// </summary>
public sealed class LoadedExtension
{
    /// <summary>The extension name from the manifest.</summary>
    public required string Name { get; init; }

    /// <summary>The manifest version, as written.</summary>
    public required string Version { get; init; }

    /// <summary>The assembly's simple name.</summary>
    public required string AssemblyName { get; init; }

    /// <summary>The configured absolute path.</summary>
    public required string Path { get; init; }

    /// <summary>The lowercase hex SHA-256 of the loaded bytes.</summary>
    public required string Sha256 { get; init; }

    /// <summary>SAFE or UNSAFE.</summary>
    public required ExtensionTrust Trust { get; init; }

    /// <summary>The names of the extensions this one requires, from the manifest.</summary>
    public required IReadOnlyList<string> Requires { get; init; }

    /// <summary>The SQL function names the assembly registered, aliases included.</summary>
    public required IReadOnlyList<string> Functions { get; init; }

    /// <summary>The full CLR names of the trigger types the assembly defines.</summary>
    public required IReadOnlyList<string> Triggers { get; init; }

    /// <summary>The SQL procedure names the assembly registered.</summary>
    public required IReadOnlyList<string> Procedures { get; init; }

    /// <summary>The loaded assembly. Null only in registries built by tests without a load.</summary>
    internal Assembly? Assembly { get; init; }

    /// <summary>The parsed manifest version, for ordering.</summary>
    internal ExtensionVersion ParsedVersion { get; init; }

    internal Dictionary<string, ScalarFunctionDescriptor> FunctionTable { get; } = new(StringComparer.OrdinalIgnoreCase);

    internal List<ExtensionTriggerType> TriggerTypes { get; } = [];

    internal Dictionary<string, ExtensionProcedure> ProcedureTable { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>The members, with their kinds, as an installed record lists them.</summary>
    internal List<Catalogs.Models.ExtensionMemberDefinition> BuildMembers()
    {
        List<Catalogs.Models.ExtensionMemberDefinition> members = new(Functions.Count + Procedures.Count + Triggers.Count);

        foreach (string name in Functions)
            members.Add(new() { Kind = Catalogs.Models.ExtensionMemberKind.Function, Name = name });

        foreach (string name in Procedures)
            members.Add(new() { Kind = Catalogs.Models.ExtensionMemberKind.Procedure, Name = name });

        foreach (string name in Triggers)
            members.Add(new() { Kind = Catalogs.Models.ExtensionMemberKind.Trigger, Name = name });

        return members;
    }

    /// <summary>
    /// Finds a trigger type of this extension by full CLR name. A public type of that name that is
    /// not a trigger reports <see cref="TriggerTypeLookup.NotATrigger"/>, so <c>CREATE TRIGGER</c> can
    /// say why it refused.
    /// </summary>
    internal TriggerTypeLookup FindTriggerType(string fullName, out ExtensionTriggerType? type)
    {
        foreach (ExtensionTriggerType candidate in TriggerTypes)
        {
            if (string.Equals(candidate.FullName, fullName, StringComparison.Ordinal))
            {
                type = candidate;
                return TriggerTypeLookup.Found;
            }
        }

        type = null;

        return Assembly?.GetType(fullName, throwOnError: false) is { IsPublic: true }
            ? TriggerTypeLookup.NotATrigger
            : TriggerTypeLookup.NotFound;
    }

    /// <summary>"name version" as messages print it.</summary>
    public override string ToString() => Name + " " + Version;
}

/// <summary>
/// A trigger type discovered in a loaded extension. The instance is created on first use, once per
/// engine, and shared by every statement that fires the trigger, so trigger classes must be
/// thread-safe (stated on <see cref="ICamusTrigger"/>).
/// </summary>
internal sealed class ExtensionTriggerType
{
    private readonly Lazy<ICamusTrigger> instance;

    public ExtensionTriggerType(LoadedExtension source, Type type, Func<ICamusTrigger> factory)
    {
        Source = source;
        Type = type;
        instance = new Lazy<ICamusTrigger>(factory, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public LoadedExtension Source { get; }

    public Type Type { get; }

    public string FullName => Type.FullName!;

    /// <summary>The shared instance. The first access runs the type's constructor; an exception from
    /// it surfaces on every access, because a failed <see cref="Lazy{T}"/> caches the exception.</summary>
    public ICamusTrigger Instance => instance.Value;
}

/// <summary>A procedure discovered in a loaded extension, with its argument-count bounds.</summary>
internal sealed class ExtensionProcedure
{
    private readonly Lazy<ICamusProcedure> instance;

    public ExtensionProcedure(LoadedExtension source, string name, int minArguments, int maxArguments, Func<ICamusProcedure> factory)
    {
        Source = source;
        Name = name;
        MinArguments = minArguments;
        MaxArguments = maxArguments;
        instance = new Lazy<ICamusProcedure>(factory, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public LoadedExtension Source { get; }

    public string Name { get; }

    public int MinArguments { get; }

    /// <summary>The largest accepted count, or <see cref="int.MaxValue"/> for no limit.</summary>
    public int MaxArguments { get; }

    public ICamusProcedure Instance => instance.Value;
}

/// <summary>The outcome of looking up a trigger type by its full CLR name.</summary>
internal enum TriggerTypeLookup
{
    Found,
    NotFound,
    Ambiguous,
    NotATrigger
}

/// <summary>
/// Everything one engine loaded from its extension assemblies, immutable after construction.
///
/// <para><b>One per engine, never static.</b> The <c>CommandExecutor</c> builds it from its
/// <see cref="CamusDBOptions"/> and owns it; two engines in one process (tests, the in-process
/// cluster) each see only their own extensions.</para>
///
/// <para><b>No global name table.</b> A loaded extension is only available; its members resolve in a
/// database where it is installed, through that database's <see cref="InstalledExtensionSet"/>. The
/// registry answers which (name, version) pairs this node loads, and the boot only refuses a member
/// name that would shadow a built-in or that one assembly defines twice. Two extensions may define
/// the same member name: <c>CREATE EXTENSION</c> refuses to install both in one database.</para>
/// </summary>
internal sealed class ExtensionRegistry
{
    public static readonly ExtensionRegistry Empty = new([]);

    private static volatile bool anyLoaded;

    /// <summary>
    /// True once any engine in this process loaded at least one extension. A process fact, not a
    /// setting: it only turns on <see cref="ExtensionScopedCursor"/> wrapping, which a process that never
    /// loads an extension does not need. It never turns off again.
    /// </summary>
    public static bool AnyLoaded => anyLoaded;

    /// <summary>Called by the loader when it built a non-empty registry, and by the database open when
    /// a database has an installed extension (the node may load none, and a statement there must still
    /// reach the scope that reports the missing extension).</summary>
    internal static void MarkLoaded() => anyLoaded = true;

    private readonly Dictionary<string, List<LoadedExtension>> byName = new(StringComparer.OrdinalIgnoreCase);

    public ExtensionRegistry(IReadOnlyList<LoadedExtension> extensions)
    {
        Extensions = extensions;

        foreach (LoadedExtension extension in extensions)
        {
            if (!byName.TryGetValue(extension.Name, out List<LoadedExtension>? versions))
                byName[extension.Name] = versions = [];

            versions.Add(extension);
        }

        foreach (List<LoadedExtension> versions in byName.Values)
            versions.Sort((a, b) => b.ParsedVersion.CompareTo(a.ParsedVersion));
    }

    /// <summary>The loaded assemblies, in configuration order.</summary>
    public IReadOnlyList<LoadedExtension> Extensions { get; }

    public bool IsEmpty => Extensions.Count == 0;

    /// <summary>The loaded versions of <paramref name="name"/>, highest first, or an empty list.</summary>
    public IReadOnlyList<LoadedExtension> GetVersions(string name) =>
        byName.TryGetValue(name, out List<LoadedExtension>? versions) ? versions : [];

    /// <summary>The loaded build with exactly this name and version text, or null.</summary>
    public LoadedExtension? Find(string name, string version)
    {
        foreach (LoadedExtension extension in GetVersions(name))
        {
            if (string.Equals(extension.Version, version, StringComparison.Ordinal))
                return extension;
        }

        return null;
    }
}
