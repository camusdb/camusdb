/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Config.Models;
using CamusDB.Core.Transactions;

namespace CamusDB.Core;

/// <summary>
/// The last remnants of process-wide configuration. Everything that can be injected already is:
/// components take a <see cref="CamusDBOptions"/> by constructor, and an engine keeps the configuration
/// it was built with. What is left here has no injection point yet.
///
/// <para><b>Do not add members, and do not reach for <see cref="Ambient"/> in new code.</b> A new
/// tunable is a property on <see cref="CamusDBOptions"/>, a field on <c>ConfigDefinition</c>, an entry
/// in <c>ConfigReader.AllowedRootKeys</c> and a mapping in <c>ConfigResolver</c>, and it is passed to
/// whatever needs it. Notably there are no per-knob setters here any more: a component's configuration
/// is fixed when it is constructed, so a test that assigns a knob after building an engine would be
/// writing a no-op. Build the engine with the options it should have instead.</para>
///
/// <para>What remains, and why:</para>
/// <list type="bullet">
///   <item><see cref="Ambient"/> — read by the regex helpers reached from the static expression
///   evaluator, which has no per-query context to carry options through. Giving the evaluator such a
///   context is the remaining work.</item>
///   <item><see cref="DataDirectory"/> — an async-local override so concurrently running tests get
///   isolated directories without a shared static.</item>
/// </list>
/// </summary>
public static class CamusDBConfig
{
    private static CamusDBOptions ambient = CamusDBOptions.Default;

    // Per-async-context data-directory override, retained until callers pass options explicitly.
    // Setting it in a test's set-up affects only that test's async execution context, which is what
    // keeps concurrently-running fixtures from stamping on each other's temp directory.
    private static readonly AsyncLocal<string?> TestDataDirectoryOverride = new();

    /// <summary>
    /// The options instance every member here reads through. Migrated code should take this by
    /// constructor instead of reaching for the ambient value.
    ///
    /// <para>Public only so that not-yet-migrated callers outside this assembly — chiefly tests that
    /// still configure an engine by assigning to the statics above — can hand the very instance they
    /// configured to a component that now requires one. Passing this is a transitional step, not the
    /// destination: the point of the migration is that a test builds its own options.</para>
    /// </summary>
    public static CamusDBOptions Ambient => ambient;

    /// <summary>
    /// Installs the resolved configuration as the ambient instance. Called once at composition time
    /// (host startup) while migration is in progress; it exists so the facade and injected options
    /// cannot disagree. Public only because the host composes outside this assembly — it is not an
    /// extension point, and it disappears with the rest of this type.
    /// </summary>
    public static void SetAmbient(CamusDBOptions options)
        => Interlocked.Exchange(ref ambient, options);

    /// <inheritdoc cref="CamusDBOptions.DataDirectory"/>
    public static string DataDirectory
    {
        get => TestDataDirectoryOverride.Value ?? ambient.DataDirectory;
        set => TestDataDirectoryOverride.Value = value;
    }

    /// <inheritdoc cref="CamusDBConstants.PrimaryKeyInternalName"/>
    public const string PrimaryKeyInternalName = CamusDBConstants.PrimaryKeyInternalName;

    /// <inheritdoc cref="CamusDBConstants.DefaultStringMaxLength"/>
    public const int DefaultStringMaxLength = CamusDBConstants.DefaultStringMaxLength;

    /// <inheritdoc cref="CamusDBConstants.DefaultBytesMaxLength"/>
    public const int DefaultBytesMaxLength = CamusDBConstants.DefaultBytesMaxLength;

    /// <inheritdoc cref="CamusDBConstants.MaxCommentLength"/>
    public const int MaxCommentLength = CamusDBConstants.MaxCommentLength;

    /// <inheritdoc cref="CamusDBConstants.MaxPasswordBytes"/>
    public const int MaxPasswordBytes = CamusDBConstants.MaxPasswordBytes;
}

/// <summary>
/// The three decode-backing policies for <see cref="CamusDBOptions.BorrowedDecode"/>: let the scanner
/// choose per query (<see cref="Adaptive"/>), or force one path globally for A/B measurement and as a
/// kill-switch (<see cref="ForceBorrowed"/> / <see cref="ForceEager"/>).
/// </summary>
public enum BorrowedDecodePolicy
{
    /// <summary>Scanner opts into borrowed decode for filtered, non-row-retaining scans; eager otherwise. Production default.</summary>
    Adaptive,

    /// <summary>Never use borrowed decode — the eager kill-switch / A/B baseline.</summary>
    ForceEager,

    /// <summary>Always use borrowed decode, on every scan — the A/B measurement mode.</summary>
    ForceBorrowed,
}
