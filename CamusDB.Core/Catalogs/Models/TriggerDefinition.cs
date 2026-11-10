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
/// One row trigger on a table: a C# extension type, or an inline C# body
/// (<c>CREATE TRIGGER ... LANGUAGE csharp</c>), that fires on INSERT, UPDATE or DELETE. Created by
/// <c>CREATE TRIGGER</c> and stored in <see cref="TableSchema.Triggers"/>, so it rides the table's own
/// meta key (<c>{dbId}/meta/table:{tableId}</c>) and its replicated schema deltas: there is no
/// separate key family for triggers.
///
/// <para><b>Only metadata is stored for the DLL form.</b> The trigger's code is never in the database; the record names
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
/// <para><b>The inline form</b> stores its C# body in <see cref="Source"/> and no type. Each node compiles
/// the body at the first fire against its own table schema and function catalog, and its SQL runs with
/// the rights of <see cref="Owner"/>.</para>
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

    /// <summary>
    /// For a typed trigger (<c>ICamusTrigger&lt;TRow&gt;</c>), the columns its row class maps, as the
    /// table spelled them when the trigger was created; null for an untyped trigger. Recorded so the
    /// schema-change validation can refuse a <c>DROP COLUMN</c> or a <c>RENAME COLUMN</c> of a mapped
    /// column on any node, without the extension loaded: a working trigger cannot break silently. The
    /// list is not updated by <c>ALTER EXTENSION ... UPDATE</c>; a new build whose row class maps other
    /// columns is checked when a node binds it, and a mismatch fails the DML with
    /// <see cref="CamusDBErrorCodes.TriggerRowMappingInvalid"/>.
    /// </summary>
    public List<string>? RowColumns { get; set; }

    /// <summary>
    /// The language of an inline body, <c>csharp</c>, and empty for a trigger of an extension DLL.
    /// An inline trigger has <see cref="Source"/> and no <see cref="TypeName"/>; the two forms share
    /// this record and the schema operations, and <c>TriggerRuntime</c> binds each by its form.
    /// </summary>
    public string Language { get; set; } = "";

    /// <summary>The C# body of an inline trigger, as written between the dollar quotes.</summary>
    public string Source { get; set; } = "";

    /// <summary>The SHA-256 of <see cref="Source"/>, lowercase hex, computed when the trigger was created.</summary>
    public string SourceSha256 { get; set; } = "";

    /// <summary>The C# language version the body was accepted with (<c>CSharp12</c>).</summary>
    public string LanguageVersion { get; set; } = "";

    /// <summary>The revision of the allowed list the body was accepted under.</summary>
    public int RuleRevision { get; set; }

    /// <summary>
    /// The user who created an inline trigger, and its id. The SQL a body runs uses this user's rights,
    /// not the rights of the statement that fired the trigger: the author of a trigger can only do
    /// what the author may do. Null for a DLL trigger and before authentication existed.
    /// </summary>
    public string? Owner { get; set; }

    /// <summary>The immutable id of <see cref="Owner"/>; a user created again under the same name does not match it.</summary>
    public string? OwnerId { get; set; }

    /// <summary>True for a trigger whose body is inline C#.</summary>
    [JsonIgnore]
    public bool IsInline => Source.Length > 0;

    /// <summary>When the trigger was created, on the creating node's clock.</summary>
    public HLCTimestamp CreatedAt { get; set; }

    /// <summary>True when the trigger fires on <paramref name="events"/>.</summary>
    public bool FiresOn(TriggerEventSet events) => (Events & events) != 0;

    private object? binding;

    /// <summary>
    /// This node's resolved binding of the record: a loaded trigger type or a compiled inline unit, or
    /// the reason it cannot bind. Not persisted; replaced when what it was resolved against changes
    /// (the extension record, the table's columns, the inline functions of the database). See
    /// <c>TriggerRuntime</c>.
    /// </summary>
    [JsonIgnore]
    internal object? Binding
    {
        get => Volatile.Read(ref binding);
        set => Volatile.Write(ref binding, value);
    }

    /// <summary>
    /// Publishes <paramref name="next"/> as the binding when the binding is still
    /// <paramref name="expected"/>, and returns the binding that was there. An inline binding holds a
    /// reference on compiled code, so two statements that bind the same record at the same time must
    /// agree on one winner: the loser gives its reference back.
    /// </summary>
    internal object? ExchangeBinding(object? next, object? expected) =>
        Interlocked.CompareExchange(ref binding, next, expected);
}
