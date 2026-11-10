/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

#if !CAMUSDB_BROWSER

using System.Reflection;
using System.Runtime.Loader;
using CamusDB.Extensions.Inline;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// The load context of the compiled code of one inline function source. It is collectible, so the
/// code of a dropped or replaced function leaves the process once nothing calls it any more.
///
/// <para><b>What it resolves.</b> The budget assembly resolves to the server's own copy, so the
/// thread-static budget of the compiled code is the one the server's adapter sets. A framework
/// assembly on <see cref="InlineFunctionImageVerifier.AllowedAssemblies"/> resolves from the default
/// context. Every other name <b>throws</b> <see cref="CamusDBErrorCodes.ExtensionAssemblyRefused"/>:
/// returning null would not refuse it, because the runtime then falls back to the default context,
/// where the whole framework and the server live. A native library is always refused.</para>
///
/// <para>The image passed the positive metadata check before it reaches this class, so a refusal here
/// means that check has an error. It is the same pair of checks an operator's SAFE extension gets,
/// with a much shorter list.</para>
/// </summary>
internal sealed class InlineFunctionLoadContext : AssemblyLoadContext
{
    private static readonly Assembly BudgetAssembly = typeof(CallBudget).Assembly;

    private static readonly Assembly ContractAssembly = typeof(CamusDB.Extensions.ITriggerContext).Assembly;


    private readonly string unitName;

    public InlineFunctionLoadContext(string unitName)
        : base("camusdb-inline:" + unitName, isCollectible: true)
    {
        this.unitName = unitName;
    }

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        string? name = assemblyName.Name;

        if (string.Equals(name, InlineFunctionImageVerifier.BudgetAssemblyName, StringComparison.OrdinalIgnoreCase))
            return BudgetAssembly;

        // The trigger contract: the row and context interfaces that a trigger wrapper implements
        // against. The server's own copy, so that the types are the ones the engine passes in.
        if (string.Equals(name, InlineFunctionImageVerifier.ContractAssemblyName, StringComparison.OrdinalIgnoreCase))
            return ContractAssembly;

        if (name is null || !InlineFunctionImageVerifier.AllowedAssemblies.Contains(name) && !InlineFunctionImageVerifier.TriggerAssemblies.Contains(name))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionAssemblyRefused,
                $"Inline function code '{unitName}' tried to load assembly '{assemblyName.FullName}', which an inline function may not use");

        return null;
    }

    protected override IntPtr LoadUnmanagedDll(string unmanagedDllName) =>
        throw new CamusDBException(
            CamusDBErrorCodes.ExtensionAssemblyRefused,
            $"Inline function code '{unitName}' tried to load native library '{unmanagedDllName}'");
}

#endif
