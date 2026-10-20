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
/// the CLR type, the assembly that defined it on the node that created it, and the SHA-256 of that
/// assembly's bytes. Each node binds the record to its own loaded extensions on the first DML that
/// fires it, and refuses the DML when the type is missing or the hash differs
/// (<see cref="CamusDBErrorCodes.TriggerBindingMismatch"/>), so two nodes cannot fire different builds
/// of the defining assembly. The hash covers that root assembly only: a private dependency that an
/// UNSAFE extension loads from its folder is not hashed, so two nodes with the same root file and
/// different dependency files are not told apart.</para>
///
/// <para><b>Immutable once created.</b> A change is a drop and a new create. The apply replaces the
/// table's list instead of editing an element, so the per-node binding cached in
/// <see cref="Binding"/> goes away with the record it belongs to.</para>
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

    /// <summary>The simple name of the assembly that defined the type on the creating node.</summary>
    public string AssemblyName { get; set; } = "";

    /// <summary>The lowercase hex SHA-256 of that assembly's bytes.</summary>
    public string AssemblySha256 { get; set; } = "";

    /// <summary>When the trigger was created, on the creating node's clock.</summary>
    public HLCTimestamp CreatedAt { get; set; }

    /// <summary>True when the trigger fires on <paramref name="events"/>.</summary>
    public bool FiresOn(TriggerEventSet events) => (Events & events) != 0;

    /// <summary>
    /// This node's resolved binding of the record to a loaded trigger type, or the reason it cannot
    /// bind. Not persisted, and set at most once per record instance; see
    /// <c>TriggerBinder</c>.
    /// </summary>
    [JsonIgnore]
    internal object? Binding { get; set; }
}
