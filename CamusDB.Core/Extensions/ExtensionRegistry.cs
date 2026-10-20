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
/// One loaded extension assembly as an operator sees it in <c>SHOW EXTENSIONS</c>: where it came
/// from, its identity, and what it registered.
///
/// <para><see cref="Sha256"/> is the hash of the exact bytes the server verified and loaded (the file
/// is read once, and those bytes are both checked and loaded). Triggers record it when they are
/// created and compare it when they bind, which is how a node with a different build of the same
/// extension is detected.</para>
/// </summary>
public sealed class LoadedExtension
{
    /// <summary>The assembly's simple name.</summary>
    public required string Name { get; init; }

    /// <summary>The assembly version.</summary>
    public required string Version { get; init; }

    /// <summary>The configured absolute path.</summary>
    public required string Path { get; init; }

    /// <summary>The lowercase hex SHA-256 of the loaded bytes.</summary>
    public required string Sha256 { get; init; }

    /// <summary>SAFE or UNSAFE.</summary>
    public required ExtensionTrust Trust { get; init; }

    /// <summary>The SQL function names the assembly registered, aliases included.</summary>
    public required IReadOnlyList<string> Functions { get; init; }

    /// <summary>The full CLR names of the trigger types the assembly defines.</summary>
    public required IReadOnlyList<string> Triggers { get; init; }

    /// <summary>The SQL procedure names the assembly registered.</summary>
    public required IReadOnlyList<string> Procedures { get; init; }

    /// <summary>The loaded assembly. Null only in registries built by tests without a load.</summary>
    internal Assembly? Assembly { get; init; }
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
/// cluster) each see only their own extensions. Statement evaluation reaches it through
/// <see cref="ExtensionFunctionScope"/>, because the expression evaluator is static.</para>
///
/// <para>Function names are case-insensitive. The loader refuses a name that collides with a
/// built-in, a table function or another extension, so a lookup here never shadows a built-in.</para>
/// </summary>
internal sealed class ExtensionRegistry
{
    public static readonly ExtensionRegistry Empty = new([], new Dictionary<string, ScalarFunctionDescriptor>(), [], new Dictionary<string, ExtensionProcedure>());

    private static volatile bool anyLoaded;

    /// <summary>
    /// True once any engine in this process loaded at least one extension. A process fact, not a
    /// setting: it only turns on <see cref="ExtensionScopedCursor"/> wrapping, which a process that never
    /// loads an extension does not need. It never turns off again.
    /// </summary>
    public static bool AnyLoaded => anyLoaded;

    /// <summary>Called by the loader when it built a non-empty registry.</summary>
    internal static void MarkLoaded() => anyLoaded = true;

    private readonly Dictionary<string, ScalarFunctionDescriptor> functions;

    private readonly IReadOnlyList<ExtensionTriggerType> triggerTypes;

    private readonly Dictionary<string, ExtensionProcedure> procedures;

    public ExtensionRegistry(
        IReadOnlyList<LoadedExtension> extensions,
        IReadOnlyDictionary<string, ScalarFunctionDescriptor> functions,
        IReadOnlyList<ExtensionTriggerType> triggerTypes,
        IReadOnlyDictionary<string, ExtensionProcedure> procedures)
    {
        Extensions = extensions;
        this.functions = new Dictionary<string, ScalarFunctionDescriptor>(functions, StringComparer.OrdinalIgnoreCase);
        this.triggerTypes = triggerTypes;
        this.procedures = new Dictionary<string, ExtensionProcedure>(procedures, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>The loaded assemblies, in configuration order.</summary>
    public IReadOnlyList<LoadedExtension> Extensions { get; }

    public bool IsEmpty => Extensions.Count == 0;

    public bool TryGetFunction(string name, out ScalarFunctionDescriptor descriptor) =>
        functions.TryGetValue(name, out descriptor!);

    public bool HasFunction(string name) => functions.ContainsKey(name);

    public bool TryGetProcedure(string name, out ExtensionProcedure procedure) =>
        procedures.TryGetValue(name, out procedure!);

    /// <summary>
    /// Finds a trigger type by its full CLR name across all loaded assemblies. A name defined in two
    /// assemblies is ambiguous. A public type of that name that is not a trigger reports
    /// <see cref="TriggerTypeLookup.NotATrigger"/>, so <c>CREATE TRIGGER</c> can say why it refused.
    /// </summary>
    public TriggerTypeLookup FindTriggerType(string fullName, out ExtensionTriggerType? type)
    {
        type = null;
        int matches = 0;

        foreach (ExtensionTriggerType candidate in triggerTypes)
        {
            if (!string.Equals(candidate.FullName, fullName, StringComparison.Ordinal))
                continue;

            type = candidate;
            matches++;
        }

        if (matches > 1)
        {
            type = null;
            return TriggerTypeLookup.Ambiguous;
        }

        if (matches == 1)
            return TriggerTypeLookup.Found;

        foreach (LoadedExtension extension in Extensions)
        {
            if (extension.Assembly?.GetType(fullName, throwOnError: false) is { IsPublic: true })
                return TriggerTypeLookup.NotATrigger;
        }

        return TriggerTypeLookup.NotFound;
    }
}
