/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

#if !CAMUSDB_BROWSER

using System.Collections.Immutable;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Reflection.PortableExecutable;

namespace CamusDB.Core.Extensions;

/// <summary>
/// What the server reads from an extension file before it loads it: the assembly's simple name and
/// version, the version of the contract assembly it was compiled against, and its
/// <c>[assembly: CamusExtension]</c> manifest (null when the assembly carries none).
/// </summary>
internal sealed record ExtensionAssemblyIdentity(string Name, Version Version, Version? ContractVersion, ExtensionManifestData? Manifest);

/// <summary>
/// The raw arguments of an assembly's <c>CamusExtensionAttribute</c>, as the metadata holds them.
/// Not validated here: the loader checks the name and parses the version, so every rule and its
/// error message live in one place.
/// </summary>
internal sealed record ExtensionManifestData(string? Name, string? Version, IReadOnlyList<string> Requires);

/// <summary>
/// The SAFE-mode check for an extension assembly, modeled on SQL Server's CLR permission sets. It
/// reads the metadata tables of the file with <see cref="MetadataReader"/> and runs no extension
/// code, so a hostile static constructor cannot run before the check decides.
///
/// <para><b>A guardrail, not a security boundary.</b> .NET has no in-process sandbox. The check
/// catches accidental use of a dangerous API and gives a reviewer a list to read; it does not contain
/// a determined attacker. A SAFE extension can still loop forever or allocate without limit, and the
/// list cannot name every framework member that reaches a file or the network (a
/// <c>StreamReader</c> opened on a path, an XML reader opened on a URI). The operator's control of
/// which files the server loads is the real boundary.</para>
///
/// <para>The rules, all reported together so one boot lists every problem:</para>
/// <list type="bullet">
/// <item>Every AssemblyRef must be on <see cref="AllowedAssemblies"/>: the contract and the
/// compute-only framework facades.</item>
/// <item>No TypeRef inside a banned namespace (<see cref="BannedNamespaces"/>), no reference to a
/// banned type (<see cref="BannedTypes"/>), and no MemberRef to a banned member
/// (<see cref="BannedMembers"/>).</item>
/// <item><b>Reflection.</b> <c>System.Reflection</c> is a banned namespace except for its attribute
/// types: every compiled assembly references <c>System.Reflection.Assembly*Attribute</c> for its own
/// metadata, and an attribute type carries no capability. A ban of single members cannot hold here:
/// <c>typeof(string).Module.GetType("System.IO.File")</c>, then <c>GetMethod</c> and
/// <c>CreateDelegate</c>, reaches a banned type with no reference to it. Every such path goes through
/// a <c>MethodInfo</c>, a <c>Module</c>, a <c>ConstructorInfo</c> or another type of the namespace,
/// and a member that returns or takes one names that type in its signature, which puts a TypeRef to
/// it in the file. The reflection members that need no such type (<c>Delegate.CreateDelegate</c> by
/// name, <c>Type.GetType</c>, <c>Type.InvokeMember</c>) are banned one by one.</item>
/// <item><b>Code that outlives the call.</b> Threads, the thread pool, timers, <c>Parallel</c> and
/// <c>Task.Run</c> start code that no statement waits for and no cancellation reaches.</item>
/// <item><b>Host disclosure.</b> The environment variables, the command line, the process path and
/// the user and machine names can hold secrets of the deployment.</item>
/// <item>No P/Invoke (a non-empty ImplMap table) and no <c>/unsafe</c> code (a module-level
/// <c>UnverifiableCodeAttribute</c>). A walk of the IL for <c>calli</c> and pointer types is not
/// done.</item>
/// </list>
///
/// <para>The same <see cref="AllowedAssemblies"/> list is enforced at run time by
/// <see cref="ExtensionLoadContext"/>, which stops a dynamic load from going around this static
/// check.</para>
/// </summary>
internal static class ExtensionAssemblyVerifier
{
    /// <summary>The contract assembly's simple name.</summary>
    public const string ContractAssemblyName = "CamusDB.Extensions.Abstractions";

    /// <summary>
    /// Assemblies a SAFE extension may reference: the contract plus the framework facades that carry
    /// compute-only types. A netstandard2.0 library references only <c>netstandard</c>; a net10
    /// library references <c>System.Runtime</c> and the facades it uses. A banned type reached through
    /// an allowed facade is still caught by the type and member rules.
    /// </summary>
    public static readonly ImmutableHashSet<string> AllowedAssemblies = ImmutableHashSet.Create(
        StringComparer.OrdinalIgnoreCase,
        ContractAssemblyName,
        "netstandard",
        "mscorlib",
        "System.Private.CoreLib",
        "System.Runtime",
        "System.Runtime.Extensions",
        "System.Runtime.Numerics",
        "System.Collections",
        "System.Collections.Concurrent",
        "System.Linq",
        "System.Memory",
        "System.Threading",
        "System.Text.RegularExpressions",
        "System.Text.Encoding.Extensions");

    /// <summary>Namespaces whose types a SAFE extension may not reference, including nested
    /// namespaces.</summary>
    public static readonly ImmutableArray<string> BannedNamespaces =
    [
        "System.Net",
        "System.Runtime.Loader",
        "System.Runtime.InteropServices",
        "System.Reflection",
        "System.IO.MemoryMappedFiles",
        "System.IO.Pipes",
        "System.IO.IsolatedStorage",
        "System.Timers",
        "Microsoft.Win32",
    ];

    /// <summary>Single types a SAFE extension may not reference.</summary>
    public static readonly ImmutableHashSet<string> BannedTypes = ImmutableHashSet.Create(
        StringComparer.Ordinal,
        "System.IO.File",
        "System.IO.FileStream",
        "System.IO.Directory",
        "System.IO.FileInfo",
        "System.IO.DirectoryInfo",
        "System.IO.FileSystemInfo",
        "System.IO.FileSystemWatcher",
        "System.IO.DriveInfo",
        "System.IO.RandomAccess",
        "System.Diagnostics.Process",
        "System.Diagnostics.Debugger",
        "System.AppDomain",
        "System.Activator",
        "System.Reflection.Assembly",
        // Reads and writes any memory, with no /unsafe flag.
        "System.Runtime.CompilerServices.Unsafe",
        // An array returned to the shared pool that the pool did not hand out corrupts the buffers
        // of other requests.
        "System.Buffers.ArrayPool`1",
        "System.Threading.Thread",
        "System.Threading.ThreadPool",
        "System.Threading.Timer",
        "System.Threading.Tasks.Parallel",
        "System.Threading.Tasks.TaskFactory",
        "System.Threading.Tasks.TaskFactory`1");

    /// <summary>Single members a SAFE extension may not call, as <c>Namespace.Type.Member</c>.</summary>
    public static readonly ImmutableHashSet<string> BannedMembers = ImmutableHashSet.Create(
        StringComparer.Ordinal,
        "System.Environment.Exit",
        "System.Environment.FailFast",
        "System.Environment.set_ExitCode",
        "System.Environment.SetEnvironmentVariable",
        "System.Environment.GetEnvironmentVariable",
        "System.Environment.GetEnvironmentVariables",
        "System.Environment.ExpandEnvironmentVariables",
        "System.Environment.GetCommandLineArgs",
        "System.Environment.get_CommandLine",
        "System.Environment.get_ProcessPath",
        "System.Environment.get_ProcessId",
        "System.Environment.get_MachineName",
        "System.Environment.get_UserName",
        "System.Environment.get_UserDomainName",
        "System.Environment.get_CurrentDirectory",
        "System.Environment.set_CurrentDirectory",
        "System.Environment.get_SystemDirectory",
        "System.Environment.GetFolderPath",
        "System.Environment.GetLogicalDrives",
        "System.Environment.get_StackTrace",
        "System.Type.GetType",
        "System.Type.GetTypeFromProgID",
        "System.Type.GetTypeFromCLSID",
        "System.Type.InvokeMember",
        "System.Delegate.CreateDelegate",
        "System.Delegate.DynamicInvoke",
        "System.Reflection.MethodBase.Invoke",
        "System.Threading.Tasks.Task.Run",
        "System.Threading.Tasks.Task.Start",
        "System.Threading.Tasks.Task.get_Factory",
        "System.Threading.Tasks.Task`1.get_Factory",
        "System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject",
        "System.GC.Collect",
        "System.GC.TryStartNoGCRegion",
        "System.GC.AddMemoryPressure");

    /// <summary>
    /// Types inside a banned namespace that the compiler itself emits for ordinary code, and that
    /// carry no capability: <c>InAttribute</c> marks <c>in</c> parameters and <c>ref readonly</c>
    /// returns.
    /// </summary>
    private static readonly ImmutableHashSet<string> CompilerEmittedExemptions = ImmutableHashSet.Create(
        StringComparer.Ordinal,
        "System.Runtime.InteropServices.InAttribute");

    /// <summary>
    /// True for a type of <c>System.Reflection</c> that is an attribute: the compiler writes
    /// <c>AssemblyTitleAttribute</c>, <c>DefaultMemberAttribute</c> and their kind into every
    /// assembly, and an attribute only describes metadata. Only a top-level type qualifies, and only in
    /// the namespace itself, not in <c>System.Reflection.Emit</c>.
    /// </summary>
    private static bool IsReflectionAttribute(string ns, string fullName) =>
        ns == "System.Reflection" && !fullName.Contains('+') && fullName.EndsWith("Attribute", StringComparison.Ordinal);

    /// <summary>
    /// Reads the identity of the file. Throws <see cref="CamusDBErrorCodes.ExtensionNotAnAssembly"/>
    /// when the bytes are not a .NET assembly.
    /// </summary>
    public static ExtensionAssemblyIdentity ReadIdentity(byte[] image, string path)
    {
        try
        {
            using PEReader pe = new(new MemoryStream(image, writable: false));

            if (!pe.HasMetadata)
                throw NotAnAssembly(path, "the file has no .NET metadata");

            MetadataReader reader = pe.GetMetadataReader();

            if (!reader.IsAssembly)
                throw NotAnAssembly(path, "the file is a module, not an assembly");

            AssemblyDefinition definition = reader.GetAssemblyDefinition();
            Version? contract = null;

            foreach (AssemblyReferenceHandle handle in reader.AssemblyReferences)
            {
                AssemblyReference reference = reader.GetAssemblyReference(handle);

                if (reader.StringComparer.Equals(reference.Name, ContractAssemblyName))
                    contract = reference.Version;
            }

            return new ExtensionAssemblyIdentity(reader.GetString(definition.Name), definition.Version, contract, ReadManifest(reader, definition));
        }
        catch (BadImageFormatException ex)
        {
            throw NotAnAssembly(path, ex.Message);
        }
    }

    /// <summary>The full name of the contract's manifest attribute.</summary>
    private const string ManifestAttributeName = "CamusDB.Extensions.CamusExtensionAttribute";

    /// <summary>
    /// Decodes the assembly-level <c>CamusExtensionAttribute</c> from the metadata blob, or returns
    /// null when the assembly has none. The blob decoder reads constants only; no extension code runs.
    /// A second manifest attribute cannot occur: the attribute's usage forbids it, and the compiler
    /// enforces that.
    /// </summary>
    private static ExtensionManifestData? ReadManifest(MetadataReader reader, AssemblyDefinition definition)
    {
        foreach (CustomAttributeHandle handle in definition.GetCustomAttributes())
        {
            CustomAttribute attribute = reader.GetCustomAttribute(handle);

            if (AttributeTypeName(reader, attribute) != ManifestAttributeName)
                continue;

            CustomAttributeValue<string> value = attribute.DecodeValue(ManifestTypeProvider.Instance);

            string? name = value.FixedArguments.Length > 0 ? value.FixedArguments[0].Value as string : null;
            string? version = value.FixedArguments.Length > 1 ? value.FixedArguments[1].Value as string : null;
            List<string> requires = [];

            foreach (CustomAttributeNamedArgument<string> named in value.NamedArguments)
            {
                if (named.Name != "Requires" || named.Value is not ImmutableArray<CustomAttributeTypedArgument<string>> items)
                    continue;

                foreach (CustomAttributeTypedArgument<string> item in items)
                {
                    if (item.Value is string required)
                        requires.Add(required);
                }
            }

            return new ExtensionManifestData(name, version, requires);
        }

        return null;
    }

    /// <summary>
    /// The smallest type provider the attribute blob decoder needs for the manifest's arguments
    /// (strings and a string array). Types are named by their full name; an enum argument cannot
    /// occur in the manifest, so its underlying type is never asked for.
    /// </summary>
    private sealed class ManifestTypeProvider : ICustomAttributeTypeProvider<string>
    {
        public static readonly ManifestTypeProvider Instance = new();

        public string GetPrimitiveType(PrimitiveTypeCode typeCode) => typeCode.ToString();

        public string GetSystemType() => "System.Type";

        public string GetSZArrayType(string elementType) => elementType + "[]";

        public string GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind) => DefinitionName(reader, handle);

        public string GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind) => TypeReferenceName(reader, handle);

        public string GetTypeFromSerializedName(string name) => name;

        public PrimitiveTypeCode GetUnderlyingEnumType(string type) => PrimitiveTypeCode.Int32;

        public bool IsSystemType(string type) => type == "System.Type";
    }

    /// <summary>
    /// Returns every SAFE-rule finding for the image, in metadata order. An empty list means the
    /// assembly passes.
    /// </summary>
    public static IReadOnlyList<string> Verify(byte[] image)
    {
        using PEReader pe = new(new MemoryStream(image, writable: false));
        MetadataReader reader = pe.GetMetadataReader();

        List<string> findings = [];
        HashSet<string> seen = new(StringComparer.Ordinal);

        void Add(string finding)
        {
            if (seen.Add(finding))
                findings.Add(finding);
        }

        foreach (AssemblyReferenceHandle handle in reader.AssemblyReferences)
        {
            string name = reader.GetString(reader.GetAssemblyReference(handle).Name);

            if (!AllowedAssemblies.Contains(name))
                Add($"assembly reference {name}");
        }

        foreach (TypeReferenceHandle handle in reader.TypeReferences)
        {
            string fullName = TypeReferenceName(reader, handle);

            if (BannedTypes.Contains(fullName))
            {
                Add($"type {fullName}");
                continue;
            }

            string ns = TypeReferenceNamespace(reader, handle);

            if (CompilerEmittedExemptions.Contains(fullName) || IsReflectionAttribute(ns, fullName))
                continue;

            foreach (string banned in BannedNamespaces)
            {
                if (ns.Equals(banned, StringComparison.Ordinal) ||
                    ns.StartsWith(banned + ".", StringComparison.Ordinal))
                {
                    Add($"type {fullName} (namespace {banned})");
                    break;
                }
            }
        }

        foreach (MemberReferenceHandle handle in reader.MemberReferences)
        {
            MemberReference member = reader.GetMemberReference(handle);

            if (member.Parent.Kind != HandleKind.TypeReference)
                continue;

            string fullName = TypeReferenceName(reader, (TypeReferenceHandle)member.Parent) + "." + reader.GetString(member.Name);

            if (BannedMembers.Contains(fullName))
                Add($"member {fullName}");
        }

        if (reader.GetTableRowCount(TableIndex.ImplMap) > 0)
            Add("P/Invoke declarations (DllImport)");

        foreach (CustomAttributeHandle handle in reader.GetModuleDefinition().GetCustomAttributes())
        {
            if (AttributeTypeName(reader, reader.GetCustomAttribute(handle)) == "System.Security.UnverifiableCodeAttribute")
                Add("unsafe code (compiled with /unsafe)");
        }

        return findings;
    }

    private static string TypeReferenceNamespace(MetadataReader reader, TypeReferenceHandle handle)
    {
        TypeReference type = reader.GetTypeReference(handle);

        // A nested type has an empty namespace; its enclosing type carries it.
        if (type.ResolutionScope.Kind == HandleKind.TypeReference)
            return TypeReferenceNamespace(reader, (TypeReferenceHandle)type.ResolutionScope);

        return reader.GetString(type.Namespace);
    }

    private static string TypeReferenceName(MetadataReader reader, TypeReferenceHandle handle)
    {
        TypeReference type = reader.GetTypeReference(handle);
        string name = reader.GetString(type.Name);

        if (type.ResolutionScope.Kind == HandleKind.TypeReference)
            return TypeReferenceName(reader, (TypeReferenceHandle)type.ResolutionScope) + "+" + name;

        string ns = reader.GetString(type.Namespace);
        return ns.Length == 0 ? name : ns + "." + name;
    }

    private static string? AttributeTypeName(MetadataReader reader, CustomAttribute attribute)
    {
        EntityHandle parent = attribute.Constructor.Kind switch
        {
            HandleKind.MemberReference => reader.GetMemberReference((MemberReferenceHandle)attribute.Constructor).Parent,
            HandleKind.MethodDefinition => reader.GetMethodDefinition((MethodDefinitionHandle)attribute.Constructor).GetDeclaringType(),
            _ => default,
        };

        return parent.Kind switch
        {
            HandleKind.TypeReference => TypeReferenceName(reader, (TypeReferenceHandle)parent),
            HandleKind.TypeDefinition => DefinitionName(reader, (TypeDefinitionHandle)parent),
            _ => null,
        };
    }

    private static string DefinitionName(MetadataReader reader, TypeDefinitionHandle handle)
    {
        TypeDefinition type = reader.GetTypeDefinition(handle);
        string ns = reader.GetString(type.Namespace);
        string name = reader.GetString(type.Name);
        return ns.Length == 0 ? name : ns + "." + name;
    }

    private static CamusDBException NotAnAssembly(string path, string reason) =>
        new(CamusDBErrorCodes.ExtensionNotAnAssembly, $"Extension '{path}' is not a .NET assembly: {reason}");
}

#endif
