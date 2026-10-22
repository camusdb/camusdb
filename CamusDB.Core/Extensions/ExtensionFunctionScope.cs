/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.Extensions;

/// <summary>
/// Ambient (per-async-flow) carrier of the <see cref="ExtensionScopeFrame"/> of the statement this
/// flow runs: the engine's loaded extensions and the database whose installed extensions the
/// statement may call. It is how extension functions reach the expression evaluator, which is static
/// and takes no context: <c>ScalarFunctionEvaluator</c> reads the built-in table first and this scope
/// on a miss. The same pattern as <c>AuthorizationContext</c>.
///
/// <para><b>Why not a static registry.</b> Several engines run in one process (tests, the in-process
/// cluster), each built with its own <see cref="CamusDBOptions"/>, and one engine serves many
/// databases, each with its own installed extensions. A static table could answer neither question,
/// and the configuration rule forbids a static setting.</para>
///
/// <para><b>Where it is set.</b> The <c>CommandExecutor</c> calls <see cref="Enter"/> at the start of
/// every entry point that can evaluate an expression, with the frame of the database the ticket names,
/// and at the start of each background job that evaluates one (column backfills, constraint
/// validation, materialized-view refresh). The call is made in the synchronous part of the entry
/// method, so the value also reaches the caller's flow: a query returns a lazy cursor, and the caller
/// later pulls rows from it in its own flow. An <see cref="AsyncLocal{T}"/> value set inside an awaited
/// async method does not flow back out, so a set placed after the first await would be lost for that
/// cursor.</para>
///
/// <para><b>The database arrives later, by mutation.</b> At the synchronous entry the database is
/// only a name; it is opened later, inside awaited code. The database opener therefore attaches the
/// opened database to the frame already in the flow (<see cref="ExtensionScopeFrame.Attach"/>). That
/// is a write to the frame object, not to the <see cref="AsyncLocal{T}"/>, so every flow that holds the
/// frame, the caller's included, sees it. A frame attaches only the database it was entered for, so
/// a statement that opens a second database does not move its own scope.</para>
///
/// <para><b>Cursors.</b> Entering the scope in the entry method is not enough for a cursor that the
/// caller pulls later: by then the caller may be in another flow, or inside another engine. The query
/// entry points therefore also wrap the cursor (<see cref="ExtensionScopedCursor"/>), so each row is
/// evaluated in the frame of the statement that created the cursor.</para>
///
/// <para><b>Failure is closed.</b> A flow that never entered a scope, or whose frame has no database,
/// resolves built-ins only, and a call to an extension function fails as "Function not found". It
/// never resolves to another engine's or another database's function.</para>
///
/// <para><b>Cost.</b> Each engine keeps one frame per opened database name and reuses it, so the
/// <see cref="AsyncLocal{T}"/> write is skipped when the flow already holds that frame. Built-in
/// function lookups never read the scope.</para>
/// </summary>
internal static class ExtensionFunctionScope
{
    private static readonly AsyncLocal<ExtensionScopeFrame?> current = new();

    /// <summary>The frame of the statement this flow runs for, or <c>null</c>.</summary>
    public static ExtensionScopeFrame? Current => current.Value;

    /// <summary>The installed extensions the current statement may call, or <c>null</c> with no frame.</summary>
    public static InstalledExtensionSet? CurrentInstalled => current.Value?.Installed;

    /// <summary>
    /// Makes <paramref name="frame"/> the scope of this flow. Skips the write when the flow already
    /// holds the same frame, so repeated entries from one request cost nothing.
    /// </summary>
    public static void Enter(ExtensionScopeFrame? frame)
    {
        if (!ReferenceEquals(current.Value, frame))
            current.Value = frame;
    }
}

/// <summary>
/// The function scope of a statement: the engine's <see cref="ExtensionRegistry"/> and, once the
/// database is open, that database's <see cref="InstalledExtensionCatalog"/>. Created by
/// <see cref="ExtensionScopeFrames"/>; see <see cref="ExtensionFunctionScope"/> for how it travels.
///
/// <para>Holds the small catalog object, not the database descriptor, so a frame that outlives its
/// database pins no tables or storage.</para>
/// </summary>
internal sealed class ExtensionScopeFrame
{
    private readonly ExtensionScopeFrames? owner;

    private volatile InstalledExtensionCatalog? catalog;

    internal ExtensionScopeFrame(ExtensionScopeFrames? owner, ExtensionRegistry registry, string? databaseName, ILogger? logger)
    {
        this.owner = owner;
        Registry = registry;
        DatabaseName = databaseName;
        Logger = logger;
    }

    /// <summary>The loaded extensions of the engine.</summary>
    public ExtensionRegistry Registry { get; }

    /// <summary>The database the statement names, or null for a statement that names none.</summary>
    public string? DatabaseName { get; }

    internal ILogger? Logger { get; }

    /// <summary>The installed extensions of the attached database, resolved for this engine.</summary>
    public InstalledExtensionSet Installed =>
        catalog?.Resolve(Registry, DatabaseName, Logger) ?? InstalledExtensionSet.Empty;

    /// <summary>
    /// Attaches the opened database. A no-op unless <paramref name="databaseName"/> is the database
    /// this frame was entered for, so opening another database inside a statement does not change the
    /// statement's scope. The first attach also keeps the frame for reuse by later statements.
    /// </summary>
    public void Attach(string databaseName, InstalledExtensionCatalog databaseCatalog)
    {
        if (!string.Equals(DatabaseName, databaseName, StringComparison.OrdinalIgnoreCase))
            return;

        if (!ReferenceEquals(catalog, databaseCatalog))
            catalog = databaseCatalog;

        owner?.Keep(this);
    }
}

/// <summary>
/// The engine's reusable <see cref="ExtensionScopeFrame"/> objects, one per database name. A frame is
/// kept only after a database of that name opened, so names that never resolve to a database leave
/// nothing behind.
/// </summary>
internal sealed class ExtensionScopeFrames
{
    private readonly ConcurrentDictionary<string, ExtensionScopeFrame> frames = new(StringComparer.OrdinalIgnoreCase);

    private readonly ExtensionRegistry registry;

    private readonly ILogger? logger;

    private readonly ExtensionScopeFrame noDatabase;

    public ExtensionScopeFrames(ExtensionRegistry registry, ILogger? logger)
    {
        this.registry = registry;
        this.logger = logger;
        noDatabase = new ExtensionScopeFrame(null, registry, null, logger);
    }

    /// <summary>The frame for a statement on <paramref name="databaseName"/>; a new, not yet kept frame for a name not seen before.</summary>
    public ExtensionScopeFrame For(string? databaseName)
    {
        if (string.IsNullOrEmpty(databaseName))
            return noDatabase;

        return frames.TryGetValue(databaseName, out ExtensionScopeFrame? frame)
            ? frame
            : new ExtensionScopeFrame(this, registry, databaseName, logger);
    }

    /// <summary>
    /// The installed extensions of <paramref name="database"/> resolved for this engine, for code that
    /// holds the database and needs no ambient scope (trigger binding, <c>CALL</c>, extension DDL).
    /// </summary>
    public InstalledExtensionSet Resolve(CommandsExecutor.Models.DatabaseDescriptor database) =>
        database.Schema.ExtensionCatalog.Resolve(registry, database.Name, logger);

    internal void Keep(ExtensionScopeFrame frame)
    {
        if (frame.DatabaseName is { } name && !frames.ContainsKey(name))
            frames.TryAdd(name, frame);
    }
}
