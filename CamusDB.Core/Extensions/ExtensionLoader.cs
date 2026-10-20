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
/// the identity from metadata (not a .NET assembly, contract too new) → SAFE check of the same bytes
/// → load the same bytes into a new <see cref="ExtensionLoadContext"/> → discover members. Hashing,
/// checking and loading one byte array closes the window in which a file could change between the
/// check and the load.</para>
///
/// <para><b>Discovery</b> uses reflection, at startup only: public static <see cref="CamusFunctionAttribute"/>
/// methods (bound by <see cref="ExtensionFunctionBinder"/>), public concrete <see cref="ICamusTrigger"/>
/// types, and public <see cref="CamusProcedureAttribute"/> types. Trigger and procedure instances are
/// created later, on first use, through constructors compiled here; nothing in extension code runs
/// during the load except the runtime's own type loading.</para>
///
/// <para><b>Names.</b> A function or procedure name that collides with a built-in, a table function
/// or another extension's member fails the boot. A built-in is never shadowed.</para>
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
        Dictionary<string, ScalarFunctionDescriptor> functions = new(StringComparer.OrdinalIgnoreCase);
        Dictionary<string, string> functionOwners = new(StringComparer.OrdinalIgnoreCase);
        List<ExtensionTriggerType> triggers = [];
        Dictionary<string, ExtensionProcedure> procedures = new(StringComparer.OrdinalIgnoreCase);
        HashSet<string> simpleNames = new(StringComparer.OrdinalIgnoreCase);

        foreach ((string path, ExtensionTrust trust) in entries)
        {
            if (!File.Exists(path))
                throw new CamusDBException(CamusDBErrorCodes.ExtensionFileNotFound, $"Extension file '{path}' does not exist");

            byte[] image = File.ReadAllBytes(path);
            string sha256 = Convert.ToHexStringLower(SHA256.HashData(image));

            ExtensionAssemblyIdentity identity = ExtensionAssemblyVerifier.ReadIdentity(image, path);

            CheckContractVersion(identity.ContractVersion, hostContract, path);

            if (!simpleNames.Add(identity.Name))
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionDuplicateAssembly,
                    $"Extension '{path}': another configured extension is also named '{identity.Name}'; a node loads one version of an extension");

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
                    "Loading extension {Name} from {Path} WITHOUT the SAFE check (unsafe_extension_assemblies); it runs with the server's full rights",
                    identity.Name, path);

            Assembly assembly;

            try
            {
                ExtensionLoadContext context = new(identity.Name, path, trust);
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
            // are filled in place below.
            LoadedExtension extension = new()
            {
                Name = identity.Name,
                Version = identity.Version.ToString(),
                Path = path,
                Sha256 = sha256,
                Trust = trust,
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

                    foreach (string name in descriptor.AllNames)
                    {
                        CheckFunctionName(name, path, functionOwners);
                        functions[name] = descriptor;
                        functionOwners[name] = identity.Name;
                        functionNames.Add(name);
                    }
                }

                if (type.IsAbstract || type.IsInterface)
                    continue;

                if (typeof(ICamusTrigger).IsAssignableFrom(type))
                {
                    triggers.Add(new ExtensionTriggerType(extension, type, CompileFactory<ICamusTrigger>(type, path)));
                    triggerNames.Add(type.FullName!);
                }

                CamusProcedureAttribute? procedure = type.GetCustomAttribute<CamusProcedureAttribute>();

                if (procedure is not null)
                {
                    if (!typeof(ICamusProcedure).IsAssignableFrom(type))
                        throw new CamusDBException(
                            CamusDBErrorCodes.ExtensionInvalidSignature,
                            $"Extension '{path}': [CamusProcedure] type {type.FullName} does not implement {nameof(ICamusProcedure)}");

                    if (string.IsNullOrWhiteSpace(procedure.Name))
                        throw new CamusDBException(CamusDBErrorCodes.ExtensionInvalidSignature, $"Extension '{path}': procedure {type.FullName} has an empty name");

                    if (procedures.ContainsKey(procedure.Name))
                        throw new CamusDBException(
                            CamusDBErrorCodes.ExtensionDuplicateMember,
                            $"Extension '{path}': procedure '{procedure.Name}' is already defined by extension '{procedures[procedure.Name].Source.Name}'");

                    int max = procedure.MaxArguments < 0 ? int.MaxValue : procedure.MaxArguments;

                    if (procedure.MinArguments < 0 || procedure.MinArguments > max)
                        throw new CamusDBException(
                            CamusDBErrorCodes.ExtensionInvalidSignature,
                            $"Extension '{path}': procedure '{procedure.Name}' has an invalid argument range {procedure.MinArguments}..{procedure.MaxArguments}");

                    procedures[procedure.Name] = new ExtensionProcedure(
                        extension, procedure.Name, procedure.MinArguments, max, CompileFactory<ICamusProcedure>(type, path));
                    procedureNames.Add(procedure.Name);
                }
            }

            extensions.Add(extension);

            if (logger.IsEnabled(LogLevel.Information))
                logger.LogInformation(
                    "Loaded extension {Name} {Version} ({Trust}) from {Path}, sha256 {Sha256}: functions [{Functions}], triggers [{Triggers}], procedures [{Procedures}]",
                    extension.Name, extension.Version, trust, path, sha256,
                    string.Join(", ", functionNames), string.Join(", ", triggerNames), string.Join(", ", procedureNames));
        }

        ExtensionRegistry.MarkLoaded();
        return new ExtensionRegistry(extensions, functions, triggers, procedures);
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

    private static void CheckFunctionName(string name, string path, Dictionary<string, string> owners)
    {
        if (ScalarFunctionEvaluator.IsBuiltInScalarFunction(name) || TableFunctionRegistry.IsTableFunction(name) ||
            CommandsExecutor.Controllers.Queries.QueryExpressionClassifier.IsReservedAggregateName(name))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionDuplicateMember,
                $"Extension '{path}': function '{name}' collides with a built-in function; extensions cannot shadow built-ins");

        if (owners.TryGetValue(name, out string? owner))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionDuplicateMember,
                $"Extension '{path}': function '{name}' is already defined by extension '{owner}'");
    }

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
