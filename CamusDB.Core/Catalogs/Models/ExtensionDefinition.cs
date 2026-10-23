/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>The kind of an extension member. Persisted by value: never renumber.</summary>
public enum ExtensionMemberKind
{
    /// <summary>A SQL scalar function, by SQL name. Each alias is its own member.</summary>
    Function = 0,

    /// <summary>A procedure that <c>CALL</c> runs, by SQL name.</summary>
    Procedure = 1,

    /// <summary>A row trigger type, by full CLR name, that <c>CREATE TRIGGER ... EXTERNAL NAME</c> names.</summary>
    Trigger = 2,

    /// <summary>An aggregate function, by SQL name.</summary>
    Aggregate = 3,

    /// <summary>A set-returning function used as a relation in <c>FROM</c>, by SQL name.</summary>
    TableFunction = 4
}

/// <summary>Rules over <see cref="ExtensionMemberKind"/> that several catalog checks share.</summary>
public static class ExtensionMemberKinds
{
    /// <summary>
    /// True for the kinds that a SQL call names: a scalar function, an aggregate and a table function
    /// share one name space per database, because a call is resolved by name before its kind is known.
    /// </summary>
    public static bool IsCallable(ExtensionMemberKind kind) =>
        kind is ExtensionMemberKind.Function or ExtensionMemberKind.Aggregate or ExtensionMemberKind.TableFunction;

    /// <summary>The word a message uses for a member of <paramref name="kind"/>.</summary>
    public static string Describe(ExtensionMemberKind kind) => kind switch
    {
        ExtensionMemberKind.Function => "function",
        ExtensionMemberKind.Procedure => "procedure",
        ExtensionMemberKind.Trigger => "trigger type",
        ExtensionMemberKind.Aggregate => "aggregate",
        ExtensionMemberKind.TableFunction => "table function",
        _ => kind.ToString(),
    };
}

/// <summary>One member of an installed extension, as the record lists it.</summary>
public sealed class ExtensionMemberDefinition
{
    public ExtensionMemberKind Kind { get; set; }

    /// <summary>The SQL name of a function, an aggregate, a table function or a procedure, or the full CLR name of a trigger type.</summary>
    public string Name { get; set; } = "";
}

/// <summary>
/// One C# extension installed in a database by <c>CREATE EXTENSION</c>. Stored at
/// <c>{dbId}/meta/extension:{name}</c>, in the <c>{dbId}/meta</c> bucket, so the schema load reads it
/// and a database drop purges it with the rest of the bucket. Changes replicate through the schema
/// log (<see cref="SchemaOp.CreateExtension"/>, <see cref="SchemaOp.UpdateExtension"/>,
/// <see cref="SchemaOp.DropExtension"/>).
///
/// <para><b>Available versus installed.</b> The extension's code is never in the database. A node
/// makes an extension <i>available</i> by loading its assembly at boot; this record makes it
/// <i>installed</i> in one database and fixes which build the database uses: the name, the version
/// and the SHA-256 of the assembly bytes. Each node binds the record to the loaded assembly with the
/// same name, version and hash. A node that loads no such assembly still opens the database; a
/// statement there that calls a member fails with
/// <see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/>.</para>
///
/// <para><b>One hash, in one place.</b> A trigger names its extension and not a hash of its own, so
/// <c>ALTER EXTENSION ... UPDATE</c> moves every function, procedure and trigger of the extension to
/// the new build in one replicated delta, with no trigger re-created.</para>
///
/// <para><b>Immutable once published.</b> The apply replaces the record (and the map that holds it)
/// instead of editing it, so a statement that resolved names against the old record keeps a
/// consistent view, and the per-node binding cached for a record goes away with it.</para>
/// </summary>
public sealed class ExtensionDefinition
{
    /// <summary>The extension name from the manifest, as the creating node loaded it.</summary>
    public string Name { get; set; } = "";

    /// <summary>The installed version, exactly as the manifest wrote it.</summary>
    public string Version { get; set; } = "";

    /// <summary>The lowercase hex SHA-256 of the assembly bytes of that version on the proposing node.</summary>
    public string Sha256 { get; set; } = "";

    /// <summary>The extensions the installed version requires, from its manifest.</summary>
    public List<string> Requires { get; set; } = [];

    /// <summary>The members of the installed version, with their kinds.</summary>
    public List<ExtensionMemberDefinition> Members { get; set; } = [];

    /// <summary>When the extension was installed, on the proposing node's clock.</summary>
    public HLCTimestamp InstalledAt { get; set; }

    /// <summary>When the installed version last changed (equal to <see cref="InstalledAt"/> before any update).</summary>
    public HLCTimestamp UpdatedAt { get; set; }

    /// <summary>True when the record lists a member of <paramref name="kind"/> named <paramref name="name"/>.
    /// Function and procedure names compare case-insensitively; trigger type names compare exactly.</summary>
    public bool HasMember(ExtensionMemberKind kind, string name)
    {
        StringComparison comparison = kind == ExtensionMemberKind.Trigger ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase;

        foreach (ExtensionMemberDefinition member in Members)
        {
            if (member.Kind == kind && string.Equals(member.Name, name, comparison))
                return true;
        }

        return false;
    }
}
