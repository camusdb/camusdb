
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;
using CamusDB.Core.Config;

namespace CamusDB;

/// <summary>
/// Runs a <c>memory_profile: dev</c> node under workstation GC by starting the server again as a child
/// process with <c>DOTNET_gcServer=0</c>, then waiting for it and passing its exit code through.
/// <para>
/// The dev profile caps the caches the node allocates, but an idle dev node was measured at 2.1 GB of
/// GC-committed memory against a live managed heap of ~160 MB: server GC keeps one heap per core and
/// does not hand the space a past burst grew back to the OS. The GC mode is fixed by the runtime before
/// <c>Main</c> runs and no API changes it afterwards, so the only way a setting resolved from
/// <c>config.yml</c> or <c>--memory-profile</c> can select it is a fresh process. The parent does no
/// work of its own: it never builds the host, opens a store or binds a port.
/// </para>
/// <para>
/// An operator who sets <c>DOTNET_gcServer</c> (or the legacy <c>COMPlus_gcServer</c>) has chosen the
/// GC mode, and the choice is kept whatever the profile. The child carries
/// <see cref="RelaunchedVariable"/> so it never relaunches again, even when the variable that pins
/// workstation GC is lost on the way.
/// </para>
/// <para>
/// Signals: on Unix the parent forwards SIGINT, SIGTERM and SIGQUIT to the child and stays alive until
/// the child exits, so <c>docker stop</c>, systemd and <c>kill</c> aimed at the parent still trigger the
/// node's graceful shutdown. A Ctrl+C on a terminal reaches both processes, so the child sees SIGINT
/// twice; the host's shutdown handler treats a repeated stop request as a no-op. A SIGKILL to the parent
/// cannot be forwarded and leaves the child running.
/// </para>
/// </summary>
internal static class WorkstationGcRelauncher
{
    /// <summary>Set to <c>1</c> in the child's environment; marks a process that is already the relaunched node.</summary>
    internal const string RelaunchedVariable = "CAMUSDB_WORKSTATION_GC_RELAUNCH";

    /// <summary>The environment variables through which an operator selects the GC mode explicitly.</summary>
    private static readonly string[] GcServerVariables = ["DOTNET_gcServer", "COMPlus_gcServer"];

    /// <summary>
    /// The signal the parent received before the child's pid was known, or 0. Forwarded as soon as the
    /// child starts, so a stop request that lands during the spawn is not lost.
    /// </summary>
    private static int pendingSignal;

    /// <summary>The child's pid once it started, or 0.</summary>
    private static int childPid;

    /// <summary>True in the relaunched child; the startup banner was already printed by the parent.</summary>
    internal static bool IsRelaunchedChild => Environment.GetEnvironmentVariable(RelaunchedVariable) == "1";

    /// <summary>True when the GC mode came from the environment rather than from the build default.</summary>
    internal static bool GcModeSetByEnvironment => IsGcModeSetByEnvironment(Environment.GetEnvironmentVariable);

    /// <summary>
    /// True when this process should hand over to a workstation-GC child: the dev profile is selected,
    /// the runtime started with server GC, the operator did not pick the GC mode, and this process is
    /// not already a relaunched child.
    /// </summary>
    internal static bool ShouldRelaunch(MemoryProfile profile, bool isServerGc, Func<string, string?> getEnvironmentVariable)
    {
        if (profile != MemoryProfile.Dev || !isServerGc)
            return false;

        if (!string.IsNullOrEmpty(getEnvironmentVariable(RelaunchedVariable)))
            return false;

        return !IsGcModeSetByEnvironment(getEnvironmentVariable);
    }

    /// <summary>
    /// Builds the child's start info, or returns null when this process cannot be started again. Under
    /// the <c>dotnet</c> muxer (<c>dotnet CamusDB.dll</c>) the entry assembly has to be passed back as
    /// the first argument; an app host or a global-tool shim is started directly. The arguments are
    /// passed through unchanged, so the child resolves the same configuration.
    /// </summary>
    internal static ProcessStartInfo? BuildStartInfo(string? processPath, string? entryAssemblyPath, string[] args)
    {
        if (string.IsNullOrEmpty(processPath))
            return null;

        ProcessStartInfo info = new(processPath) { UseShellExecute = false };

        if (string.Equals(Path.GetFileNameWithoutExtension(processPath), "dotnet", StringComparison.OrdinalIgnoreCase))
        {
            if (string.IsNullOrEmpty(entryAssemblyPath))
                return null;

            info.ArgumentList.Add(entryAssemblyPath);
        }

        foreach (string arg in args)
            info.ArgumentList.Add(arg);

        info.Environment["DOTNET_gcServer"] = "0";
        info.Environment[RelaunchedVariable] = "1";

        return info;
    }

    /// <summary>
    /// Starts the workstation-GC child with inherited standard streams and waits for it. Returns the
    /// child's exit code, or null when the child could not be started, in which case the caller runs
    /// the node in this process under server GC.
    /// </summary>
    internal static async Task<int?> TryRunChildAsync(string[] args)
    {
        ProcessStartInfo? info = BuildStartInfo(Environment.ProcessPath, Assembly.GetEntryAssembly()?.Location, args);
        if (info is null)
            return null;

        // Registered before the spawn: a stop request that arrives while the child starts must not take
        // the parent down with its default action and leave an unsupervised child behind.
        using PosixSignalRegistration sigInt = PosixSignalRegistration.Create(PosixSignal.SIGINT, OnSignal);
        using PosixSignalRegistration sigTerm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, OnSignal);
        using PosixSignalRegistration sigQuit = PosixSignalRegistration.Create(PosixSignal.SIGQUIT, OnSignal);

        Process? child;

        try
        {
            child = Process.Start(info);
        }
        catch (Win32Exception ex)
        {
            Console.Error.WriteLine($"Could not restart under workstation GC ({ex.Message}); continuing with server GC.");
            return null;
        }

        if (child is null)
            return null;

        using (child)
        {
            Volatile.Write(ref childPid, child.Id);

            int pending = Interlocked.Exchange(ref pendingSignal, 0);
            if (pending != 0)
                Forward(pending);

            await child.WaitForExitAsync().ConfigureAwait(false);
            return child.ExitCode;
        }
    }

    private static bool IsGcModeSetByEnvironment(Func<string, string?> getEnvironmentVariable)
    {
        foreach (string variable in GcServerVariables)
        {
            if (!string.IsNullOrEmpty(getEnvironmentVariable(variable)))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Keeps the parent alive and passes the signal on. On Windows a console control event already
    /// reaches every process attached to the console, so there is nothing to forward.
    /// </summary>
    private static void OnSignal(PosixSignalContext context)
    {
        context.Cancel = true;

        if (OperatingSystem.IsWindows())
            return;

        int signal = context.Signal switch
        {
            PosixSignal.SIGINT => 2,
            PosixSignal.SIGQUIT => 3,
            PosixSignal.SIGTERM => 15,
            _ => 0
        };

        if (signal == 0)
            return;

        if (Volatile.Read(ref childPid) == 0)
        {
            Interlocked.Exchange(ref pendingSignal, signal);

            // The child may have started between the pid read and the store; re-check so the signal
            // is not left pending after the spawn path already looked.
            if (Volatile.Read(ref childPid) == 0)
                return;

            signal = Interlocked.Exchange(ref pendingSignal, 0);
            if (signal == 0)
                return;
        }

        Forward(signal);
    }

    private static void Forward(int signal)
    {
        int pid = Volatile.Read(ref childPid);
        if (pid != 0)
            _ = kill(pid, signal);
    }

    [DllImport("libc", EntryPoint = "kill", SetLastError = true)]
    private static extern int kill(int pid, int sig);
}
