/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>When a trigger fires relative to the row write. Persisted by value: never renumber.</summary>
public enum TriggerFireTiming
{
    /// <summary>Before validation and the write; the trigger can change NEW.</summary>
    Before = 0,

    /// <summary>After the statement wrote all its rows, once per row.</summary>
    After = 1
}

/// <summary>The statement kinds a trigger fires on. A flag set, persisted by value: never renumber.</summary>
[Flags]
public enum TriggerEventSet
{
    None = 0,
    Insert = 1,
    Update = 2,
    Delete = 4
}

/// <summary>
/// One row trigger on a table: a C# extension type that fires on INSERT, UPDATE or DELETE. Created by
/// <c>CREATE TRIGGER</c> and stored in <see cref="TableSchema.Triggers"/>, so it rides the table's own
/// meta key (<c>{dbId}/meta/table:{tableId}</c>) and its replicated schema deltas: there is no
/// separate key family for triggers.
///
/// <para><b>Only metadata is stored.</b> The trigger's code is never in the database; the record names
/// the CLR type and the installed extension (<see cref="ExtensionDefinition"/>) that defines it, and
/// nothing else. The build is fixed by the extension's record, which holds the version and the SHA-256
/// in one place: <c>ALTER EXTENSION ... UPDATE</c> moves every trigger of the extension to the new
/// build in the same delta, with no trigger dropped or re-created. Each node binds the record to the
/// build it loads for the installed version on the first DML that fires it, and refuses the DML when
/// it loads no such build (<see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/>). The hash covers
/// the root assembly only: a private dependency that an UNSAFE extension loads from its folder is not
/// hashed, so two nodes with the same root file and different dependency files are not told
/// apart.</para>
///
/// <para><b>Immutable once created.</b> A change is a drop and a new create. The apply replaces the
/// table's list instead of editing an element. The per-node binding cached in <see cref="Binding"/>
/// is keyed by the extension record it was resolved against, so an update of the extension makes the
/// next DML bind again.</para>
///
/// <para>A recovered (relinked) table comes back without its triggers, the same as without its
/// foreign keys: they are re-created by the user. A materialized view never has triggers.</para>
/// </summary>
public sealed class TriggerDefinition
{
    /// <summary>The trigger name, unique per table (case-insensitive).</summary>
    public string Name { get; set; } = "";

    /// <summary>BEFORE or AFTER.</summary>
    public TriggerFireTiming Timing { get; set; }

    /// <summary>The events that fire it; never <see cref="TriggerEventSet.None"/>.</summary>
    public TriggerEventSet Events { get; set; }

    /// <summary>The full CLR name of the trigger type, as <c>EXTERNAL NAME</c> gave it.</summary>
    public string TypeName { get; set; } = "";

    /// <summary>The name of the installed extension that defines the type.</summary>
    public string ExtensionName { get; set; } = "";

    /// <summary>When the trigger was created, on the creating node's clock.</summary>
    public HLCTimestamp CreatedAt { get; set; }

    /// <summary>True when the trigger fires on <paramref name="events"/>.</summary>
    public bool FiresOn(TriggerEventSet events) => (Events & events) != 0;

    /// <summary>
    /// This node's resolved binding of the record to a loaded trigger type, or the reason it cannot
    /// bind. Not persisted; replaced when the extension record it was resolved against changes. See
    /// <c>TriggerRuntime</c>.
    /// </summary>
    [JsonIgnore]
    internal object? Binding { get; set; }
}
