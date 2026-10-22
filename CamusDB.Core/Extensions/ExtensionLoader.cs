/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq.Expressions;
using System.Reflection;
using System.Security.Cryptography;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Extensions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Extensions;

/// <summary>
/// Loads the extension assemblies an engine's <see cref="CamusDBOptions"/> lists and builds its
/// <see cref="ExtensionRegistry"/>. Runs once, from the <c>CommandExecutor</c> constructor; any
/// failure throws a <see cref="CamusDBException"/> with a <c>CADB080x</c> code and the path, so a
/// misconfigured node refuses to start instead of starting without the code it was told to run.
///
/// <para><b>Order per file:</b> check the path → read the bytes once → SHA-256 of those bytes → read
/// the identity and the <c>[assembly: CamusExtension]</c> manifest from metadata (not a .NET assembly,
/// contract too new, manifest missing or invalid) → SAFE check of the same bytes → load the same bytes
/// into a new <see cref="ExtensionLoadContext"/> → discover members. Hashing, checking and loading one
/// byte array closes the window in which a file could change between the check and the load.</para>
///
/// <para><b>Versions.</b> The registry is keyed by the manifest's (name, version). Two files may carry
/// two versions of one extension; each loads into its own context, which is what lets a rolling
/// upgrade load the new version on every node before any database moves to it. Two files with the
/// same name and version fail the boot.</para>
///
/// <para><b>Discovery</b> uses reflection, at startup only: public static <see cref="CamusFunctionAttribute"/>
/// methods (bound by <see cref="ExtensionFunctionBinder"/>), public concrete <see cref="ICamusTrigger"/>
/// types, and public <see cref="CamusProcedureAttribute"/> types. Trigger and procedure instances are
/// created later, on first use, through constructors compiled here; nothing in extension code runs
/// during the load except the runtime's own type loading.</para>
///
/// <para><b>Names.</b> A function name that collides with a built-in, a table function or an aggregate
/// fails the boot, and so does a member name that one extension defines twice. A built-in is never
/// shadowed. A name shared by two extensions is not a boot error: the extensions are only available,
/// and <c>CREATE EXTENSION</c> refuses to install both in one database.</para>
/// </summary>
internal static class ExtensionLoader
{
    public static ExtensionRegistry Load(CamusDBOptions options, ILogger logger)
    {
        IReadOnlyList<string> safe = options.ExtensionAssemblies;
        IReadOnlyList<string> unsafePaths = options.UnsafeExtensionAssemblies;

        if (safe.Count == 0 && unsafePaths.Count == 0)
            return ExtensionRegistry.Empty;

#if CAMUSDB_BROWSER
        throw new CamusDBException(
            CamusDBErrorCodes.FeatureNotSupported,
            "C# extensions are not available in the browser build");
#else
        List<(string Path, ExtensionTrust Trust)> entries = new(safe.Count + unsafePaths.Count);
        HashSet<string> seenPaths = new(StringComparer.Ordinal);

        foreach (string path in safe)
            entries.Add((CheckPath(path, seenPaths), ExtensionTrust.Safe));

        foreach (string path in unsafePaths)
            entries.Add((CheckPath(path, seenPaths), ExtensionTrust.Unsafe));

        Version hostContract = typeof(ICamusTrigger).Assembly.GetName().Version ?? new Version(0, 0);

        List<LoadedExtension> extensions = new(entries.Count);
        HashSet<(string, string)> identities = [];

        foreach ((string path, ExtensionTrust trust) in entries)
        {
            if (!File.Exists(path))
                throw new CamusDBException(CamusDBErrorCodes.ExtensionFileNotFound, $"Extension file '{path}' does not exist");

            byte[] image = File.ReadAllBytes(path);
            string sha256 = Convert.ToHexStringLower(SHA256.HashData(image));

            ExtensionAssemblyIdentity identity = ExtensionAssemblyVerifier.ReadIdentity(image, path);

            CheckContractVersion(identity.ContractVersion, hostContract, path);

            (string name, ExtensionVersion version, IReadOnlyList<string> requires) = CheckManifest(identity.Manifest, path);

            if (!identities.Add((name.ToLowerInvariant(), version.Text)))
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionDuplicateAssembly,
                    $"Extension '{path}': another configured file is also extension '{name}' version {version.Text}; a node loads one build of each version");

            if (trust == ExtensionTrust.Safe)
            {
                IReadOnlyList<string> findings = ExtensionAssemblyVerifier.Verify(image);

                if (findings.Count > 0)
                    throw new CamusDBException(
                        CamusDBErrorCodes.ExtensionVerificationFailed,
                        $"Extension '{path}' failed the SAFE check ({findings.Count} finding(s)): " +
                        string.Join("; ", findings) +
                        ". Review the code, then list it under unsafe_extension_assemblies if it must load.");
            }
            else
                logger.LogWarning(
                    "Loading extension {Name} {Version} from {Path} WITHOUT the SAFE check (unsafe_extension_assemblies); it runs with the server's full rights",
                    name, version.Text, path);

            Assembly assembly;

            try
            {
                ExtensionLoadContext context = new(name + "@" + version.Text, path, trust);
                using MemoryStream stream = new(image, writable: false);
                assembly = context.LoadFromStream(stream);
            }
            catch (Exception ex) when (ex is BadImageFormatException or FileLoadException)
            {
                throw new CamusDBException(CamusDBErrorCodes.ExtensionNotAnAssembly, $"Extension '{path}' could not be loaded: {ex.Message}", ex);
            }

            Type[] types = GetTypes(assembly, path);

            List<string> functionNames = [];
            List<string> triggerNames = [];
            List<string> procedureNames = [];

            // The record is built before its members so each member can point at it; the name lists
            // and member tables are filled in place below.
            LoadedExtension extension = new()
            {
                Name = name,
                Version = version.Text,
                ParsedVersion = version,
                AssemblyName = identity.Name,
                Path = path,
                Sha256 = sha256,
                Trust = trust,
                Requires = requires,
                Functions = functionNames,
                Triggers = triggerNames,
                Procedures = procedureNames,
                Assembly = assembly,
            };

            foreach (Type type in types)
            {
                if (!type.IsPublic && !type.IsNestedPublic)
                    continue;

                foreach (MethodInfo method in type.GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly))
                {
                    CamusFunctionAttribute? attribute = method.GetCustomAttribute<CamusFunctionAttribute>();
                    if (attribute is null)
                        continue;

                    ScalarFunctionDescriptor descriptor = ExtensionFunctionBinder.Bind(method, attribute);

                    foreach (string functionName in descriptor.AllNames)
                    {
                        CheckFunctionName(functionName, path, extension);
                        extension.FunctionTable[functionName] = descriptor;
                        functionNames.Add(functionName);
                    }
                }

                if (type.IsAbstract || type.IsInterface)
                    continue;

                if (typeof(ICamusTrigger).IsAssignableFrom(type))
                {
                    extension.TriggerTypes.Add(new ExtensionTriggerType(extension, type, CompileFactory<ICamusTrigger>(type, path)));
                    triggerNames.Add(type.FullName!);
                }

                CamusProcedureAttribute? procedure = type.GetCustomAttribute<CamusProcedureAttribute>();

                if (procedure is not null)
                {
                    if (!typeof(ICamusProcedure).IsAssignableFrom(type))
                        throw new CamusDBException(
                            CamusDBErrorCodes.ExtensionInvalidSignature,
                            $"Extension '{path}': [CamusProcedure] type {type.FullName} does not implement {nameof(ICamusProcedure)}");

                    if (string.IsNullOrWhiteSpace(procedure.Name) || procedure.Name.Contains('.'))
                        throw new CamusDBException(CamusDBErrorCodes.ExtensionInvalidSignature, $"Extension '{path}': procedure {type.FullName} has an invalid name '{procedure.Name}'");

                    if (extension.ProcedureTable.ContainsKey(procedure.Name))
                        throw new CamusDBException(
                            CamusDBErrorCodes.ExtensionDuplicateMember,
                            $"Extension '{path}': procedure '{procedure.Name}' is defined twice");

                    int max = procedure.MaxArguments < 0 ? int.MaxValue : procedure.MaxArguments;

                    if (procedure.MinArguments < 0 || procedure.MinArguments > max)
                        throw new CamusDBException(
                            CamusDBErrorCodes.ExtensionInvalidSignature,
                            $"Extension '{path}': procedure '{procedure.Name}' has an invalid argument range {procedure.MinArguments}..{procedure.MaxArguments}");

                    extension.ProcedureTable[procedure.Name] = new ExtensionProcedure(
                        extension, procedure.Name, procedure.MinArguments, max, CompileFactory<ICamusProcedure>(type, path));
                    procedureNames.Add(procedure.Name);
                }
            }

            extensions.Add(extension);

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "Loaded extension {Name} {Version} (assembly {Assembly}, {Trust}) from {Path}, sha256 {Sha256}: functions [{Functions}], triggers [{Triggers}], procedures [{Procedures}]",
                    name, version.Text, identity.Name, trust, path, sha256,
                    string.Join(", ", functionNames), string.Join(", ", triggerNames), string.Join(", ", procedureNames));
        }

        ExtensionRegistry.MarkLoaded();
        return new ExtensionRegistry(extensions);
#endif
    }

    /// <summary>
    /// Refuses an extension compiled against a newer contract than the server carries. The contract
    /// is versioned with the server, and its compatibility key follows SemVer: the major number from
    /// 1.0 on, and major.minor while the major is 0, because a 0.x minor release may break the
    /// contract. An extension whose key is older than or equal to the server's loads: the load
    /// context hands every extension the server's copy of the contract. An extension compiled
    /// against a newer key could use members this server lacks, so it is refused here with a clear
    /// error instead of failing later at type load. An extension that does not reference the contract
    /// at all (<paramref name="contract"/> null) registers nothing but is not an error.
    /// </summary>
    internal static void CheckContractVersion(Version? contract, Version host, string path)
    {
        if (contract is null || CompatibilityKey(contract).CompareTo(CompatibilityKey(host)) <= 0)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.ExtensionContractMismatch,
            $"Extension '{path}' was compiled against CamusDB.Extensions.Abstractions {contract}, " +
            $"which is newer than the {DescribeKey(host)} this server carries; rebuild it against {DescribeKey(host)} or upgrade the server");
    }

    /// <summary>The part of a contract version that decides compatibility; see <see cref="CheckContractVersion"/>.</summary>
    private static (int Major, int Minor) CompatibilityKey(Version version) =>
        version.Major == 0 ? (0, version.Minor) : (version.Major, 0);

    private static string DescribeKey(Version version) =>
        version.Major == 0 ? $"0.{version.Minor}.x" : $"{version.Major}.x";

#if !CAMUSDB_BROWSER
    private static string CheckPath(string path, HashSet<string> seenPaths)
    {
        if (string.IsNullOrWhiteSpace(path) || !Path.IsPathFullyQualified(path))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionPathInvalid,
                $"Extension path '{path}' is not absolute; a relative path changes meaning with the working directory");

        if (!seenPaths.Add(path))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionPathInvalid,
                $"Extension path '{path}' is listed more than once (a path may be SAFE or UNSAFE, not both)");

        return path;
    }

    private static void CheckFunctionName(string name, string path, LoadedExtension extension)
    {
        if (name.Contains('.'))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionInvalidSignature,
                $"Extension '{path}': function name '{name}' contains a dot; a dot separates the extension name in a qualified call");

        if (ScalarFunctionEvaluator.IsBuiltInScalarFunction(name) || TableFunctionRegistry.IsTableFunction(name) ||
            CommandsExecutor.Controllers.Queries.QueryExpressionClassifier.IsReservedAggregateName(name))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionDuplicateMember,
                $"Extension '{path}': function '{name}' collides with a built-in function; extensions cannot shadow built-ins");

        if (extension.FunctionTable.ContainsKey(name))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionDuplicateMember,
                $"Extension '{path}': function '{name}' is defined twice");
    }

    /// <summary>
    /// Checks the manifest an extension file must carry and returns its parts. The name must be a
    /// plain SQL identifier, because it appears unquoted in <c>CREATE EXTENSION</c> and before the dot
    /// of a qualified call. The version must be SemVer without build metadata. A required name must
    /// be an identifier too, and an extension cannot require itself.
    /// </summary>
    private static (string Name, ExtensionVersion Version, IReadOnlyList<string> Requires) CheckManifest(ExtensionManifestData? manifest, string path)
    {
        if (manifest is null)
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionManifestInvalid,
                $"Extension '{path}' has no manifest; add [assembly: CamusExtension(\"<name>\", \"<major.minor.patch>\")] to the assembly");

        if (!IsExtensionName(manifest.Name))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionManifestInvalid,
                $"Extension '{path}': manifest name '{manifest.Name}' is not a plain identifier (a letter or underscore, then letters, digits and underscores)");

        if (!ExtensionVersion.TryParse(manifest.Version, out ExtensionVersion version))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionManifestInvalid,
                $"Extension '{path}': manifest version '{manifest.Version}' is not a SemVer version (major.minor.patch, optional -prerelease, no +build)");

        List<string> requires = new(manifest.Requires.Count);

        foreach (string required in manifest.Requires)
        {
            if (!IsExtensionName(required))
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionManifestInvalid,
                    $"Extension '{path}': required extension name '{required}' is not a plain identifier");

            if (string.Equals(required, manifest.Name, StringComparison.OrdinalIgnoreCase))
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionManifestInvalid,
                    $"Extension '{path}': extension '{manifest.Name}' requires itself");

            if (!requires.Contains(required, StringComparer.OrdinalIgnoreCase))
                requires.Add(required);
        }

        return (manifest.Name!, version, requires);
    }
#endif

    /// <summary>True when <paramref name="name"/> is a plain SQL identifier: the shape an extension name must have.</summary>
    internal static bool IsExtensionName(string? name)
    {
        if (string.IsNullOrEmpty(name) || !(char.IsAsciiLetter(name[0]) || name[0] == '_'))
            return false;

        foreach (char c in name)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c != '_')
                return false;
        }

        return true;
    }

#if !CAMUSDB_BROWSER
    private static Type[] GetTypes(Assembly assembly, string path)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            string reasons = string.Join("; ", ex.LoaderExceptions.Where(e => e is not null).Select(e => e!.Message).Distinct());

            // A refused assembly surfaces here when a type's base or field type lives in it.
            CamusDBException? refused = ex.LoaderExceptions.OfType<CamusDBException>().FirstOrDefault()
                ?? ex.LoaderExceptions.Select(e => e?.InnerException).OfType<CamusDBException>().FirstOrDefault();

            if (refused is not null)
                throw refused;

            throw new CamusDBException(CamusDBErrorCodes.ExtensionNotAnAssembly, $"Extension '{path}' has types that cannot load: {reasons}", ex);
        }
    }

    /// <summary>A constructor call compiled once, so creating the instance later needs no
    /// reflection (and no <c>Activator</c>, which SAFE code may not use either).</summary>
    private static Func<T> CompileFactory<T>(Type type, string path)
    {
        ConstructorInfo? constructor = type.GetConstructor(Type.EmptyTypes);

        if (constructor is null)
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionInvalidSignature,
                $"Extension '{path}': type {type.FullName} needs a public parameterless constructor");

        return Expression.Lambda<Func<T>>(Expression.Convert(Expression.New(constructor), typeof(T))).Compile();
    }
#endif
}
