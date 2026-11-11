/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

#if !CAMUSDB_BROWSER

using System.Reflection;
using System.Runtime.Loader;
using CamusDB.Extensions;

namespace CamusDB.Core.Extensions;

/// <summary>
/// The load context of one extension assembly. Each extension gets its own context, so two
/// extensions can carry different versions of a private dependency without a conflict.
///
/// <para><b>One contract.</b> A request for <c>CamusDB.Extensions.Abstractions</c> always returns the
/// server's own copy, whatever copy sits next to the extension. Without that the extension's
/// <see cref="ICamusTrigger"/> would be a different type from the server's, and no cast between them
/// could succeed.</para>
///
/// <para><b>SAFE mode.</b> A request for any assembly outside
/// <see cref="ExtensionAssemblyVerifier.AllowedAssemblies"/> <b>throws</b>
/// <see cref="CamusDBErrorCodes.ExtensionAssemblyRefused"/> (the runtime hands the caller a
/// <see cref="FileLoadException"/> whose inner exception is that error). Returning <c>null</c> instead would not
/// refuse anything: the runtime then falls back to <see cref="AssemblyLoadContext.Default"/>, where the
/// whole framework lives. An allowed framework name returns <c>null</c> on purpose, so it resolves
/// from the default context, which holds the shared framework. Native libraries are refused in SAFE
/// mode for the same reason P/Invoke is.</para>
///
/// <para><b>UNSAFE mode.</b> Private dependencies resolve from the extension's folder through
/// <see cref="AssemblyDependencyResolver"/> (which reads the extension's <c>.deps.json</c> when there is
/// one); anything else falls back to the default context. A resolved file must have the bytes it had
/// at boot (<see cref="ExtensionDependencyHash"/>): an assembly is loaded from the bytes that were
/// checked, and a native library is checked and then loaded from its path. A file that is new or
/// changed since the boot is refused.</para>
///
/// <para>Not collectible: the server never unloads an extension, and a collectible context would make
/// every compiled adapter delegate pin it anyway.</para>
/// </summary>
internal sealed class ExtensionLoadContext : AssemblyLoadContext
{
    private static readonly Assembly ContractAssembly = typeof(ICamusTrigger).Assembly;

    private readonly AssemblyDependencyResolver resolver;

    private readonly ExtensionTrust trust;

    private readonly string extensionName;

    private readonly ExtensionDependencyHash dependencies;

    public ExtensionLoadContext(string extensionName, string assemblyPath, ExtensionTrust trust, ExtensionDependencyHash dependencies)
        : base("camusdb-extension:" + extensionName, isCollectible: false)
    {
        this.extensionName = extensionName;
        this.trust = trust;
        this.dependencies = dependencies;
        resolver = new AssemblyDependencyResolver(assemblyPath);
    }

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        string? name = assemblyName.Name;

        if (string.Equals(name, ExtensionAssemblyVerifier.ContractAssemblyName, StringComparison.OrdinalIgnoreCase))
            return ContractAssembly;

        if (trust == ExtensionTrust.Safe)
        {
            if (name is null || !ExtensionAssemblyVerifier.AllowedAssemblies.Contains(name))
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionAssemblyRefused,
                    $"SAFE extension '{extensionName}' tried to load assembly '{assemblyName.FullName}', which is not on the SAFE allow-list");

            return null;
        }

        string? path = resolver.ResolveAssemblyToPath(assemblyName);

        if (path is null)
            return null;

        using MemoryStream stream = new(dependencies.ReadVerified(path, extensionName), writable: false);
        return LoadFromStream(stream);
    }

    protected override IntPtr LoadUnmanagedDll(string unmanagedDllName)
    {
        if (trust == ExtensionTrust.Safe)
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionAssemblyRefused,
                $"SAFE extension '{extensionName}' tried to load native library '{unmanagedDllName}'");

        string? path = resolver.ResolveUnmanagedDllToPath(unmanagedDllName);

        if (path is null)
            return IntPtr.Zero;

        dependencies.VerifyFile(path, extensionName);
        return LoadUnmanagedDllFromPath(path);
    }
}

#endif
