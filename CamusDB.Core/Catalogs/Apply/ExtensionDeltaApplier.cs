/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// Applies the committed deltas that install, update and remove a C# extension in a database
/// (<see cref="SchemaOp.CreateExtension"/>, <see cref="SchemaOp.UpdateExtension"/>,
/// <see cref="SchemaOp.DropExtension"/>).
///
/// <para><b>Metadata only.</b> The apply never looks at the engine's loaded assemblies: the proposer
/// resolved the build and recorded its version, hash and members. A node binds the record to its own
/// files when a statement resolves a member, and a node without the file still applies the delta, so
/// a committed delta applies everywhere.</para>
///
/// <para><b>Rules checked in log order.</b> A required extension must be installed, and no other
/// installed extension may define one of the record's function or procedure names. The proposer
/// checks the same rules before it proposes; checking them again here orders two concurrent
/// statements against each other. A drop is refused while another installed extension requires it.
/// Every check runs before the first change.</para>
///
/// <para><b>Idempotent.</b> A re-delivered create or update finds the record it left behind (same
/// version and timestamp) and returns; a re-delivered drop finds nothing to remove. An update or a
/// drop proposed against a record that a later statement already replaced is a no-op, so it cannot
/// undo that statement.</para>
///
/// <para>The installed map is replaced, never edited in place (see
/// <see cref="CamusDB.Core.Extensions.InstalledExtensionCatalog"/>). Each arm returns null: an
/// extension is not a relation, and the proposer persists its own meta key afterwards.</para>
/// </summary>
internal static class ExtensionDeltaApplier
{
    internal static TableSchema? ApplyCreateExtension(Schema schema, SchemaExtensionPayload payload, long toVersion)
    {
        ExtensionDefinition extension = RequireRecord(payload, "CreateExtension");

        if (schema.Extensions.TryGetValue(extension.Name, out ExtensionDefinition? existing))
        {
            if (IsSameRecord(existing, extension))
                return null;

            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionAlreadyInstalled,
                $"Extension '{extension.Name}' is already installed (version {existing.Version})");
        }

        CheckRules(schema, extension);
        Publish(schema, extension.Name, extension, toVersion);
        return null;
    }

    internal static TableSchema? ApplyUpdateExtension(Schema schema, SchemaExtensionPayload payload, long toVersion)
    {
        ExtensionDefinition extension = RequireRecord(payload, "UpdateExtension");

        if (!schema.Extensions.TryGetValue(extension.Name, out ExtensionDefinition? existing))
            return null;

        if (IsSameRecord(existing, extension) || !string.Equals(existing.Version, payload.FromVersion, StringComparison.Ordinal))
            return null;

        CheckRules(schema, extension);
        Publish(schema, extension.Name, extension, toVersion);
        return null;
    }

    internal static TableSchema? ApplyDropExtension(Schema schema, SchemaDropExtensionPayload payload, long toVersion)
    {
        if (!schema.Extensions.TryGetValue(payload.ExtensionName, out ExtensionDefinition? existing) ||
            existing.InstalledAt != payload.InstalledAt)
            return null;

        foreach (ExtensionDefinition other in schema.Extensions.Values)
        {
            if (!ReferenceEquals(other, existing) && other.Requires.Contains(existing.Name, StringComparer.OrdinalIgnoreCase))
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionHasDependents,
                    $"Cannot drop extension '{existing.Name}': extension '{other.Name}' requires it");
        }

        Publish(schema, existing.Name, null, toVersion);
        return null;
    }

    internal static bool WasCreateApplied(Schema schema, SchemaExtensionPayload payload) =>
        payload.Extension is { } extension
        && schema.Extensions.TryGetValue(extension.Name, out ExtensionDefinition? existing)
        && IsSameRecord(existing, extension);

    /// <summary>
    /// Applied when the record the delta carries is installed, and also when the installed record
    /// no longer has the version the update replaced (or is gone): the apply then was, or will be, a
    /// no-op, and a proposer must not wait for a change that cannot happen.
    /// </summary>
    internal static bool WasUpdateApplied(Schema schema, SchemaExtensionPayload payload) =>
        payload.Extension is not { } extension
        || !schema.Extensions.TryGetValue(extension.Name, out ExtensionDefinition? existing)
        || IsSameRecord(existing, extension)
        || !string.Equals(existing.Version, payload.FromVersion, StringComparison.Ordinal);

    internal static bool WasDropApplied(Schema schema, SchemaDropExtensionPayload payload) =>
        !schema.Extensions.TryGetValue(payload.ExtensionName, out ExtensionDefinition? existing)
        || existing.InstalledAt != payload.InstalledAt;

    /// <summary>The extension an extension delta targets, for the checkpoint that persists its key.</summary>
    internal static string GetExtensionName(SchemaChangeLogEntry entry) => entry.Op switch
    {
        SchemaOp.CreateExtension or SchemaOp.UpdateExtension =>
            SchemaDeltaApplier.DecodePayload<SchemaExtensionPayload>(entry).Extension?.Name ?? "",
        SchemaOp.DropExtension => SchemaDeltaApplier.DecodePayload<SchemaDropExtensionPayload>(entry).ExtensionName,
        _ => throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Schema operation '{entry.Op}' does not target an extension")
    };

    /// <summary>
    /// The install rules for <paramref name="extension"/> against the other installed extensions: each
    /// required extension is installed, and no other installed extension defines one of its procedure
    /// names, or one of its function, aggregate and table-function names (one shared name space).
    /// Trigger types need no such check: a trigger names its extension.
    /// </summary>
    internal static void CheckRules(Schema schema, ExtensionDefinition extension)
    {
        foreach (string required in extension.Requires)
        {
            if (!schema.Extensions.ContainsKey(required))
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionRequirementMissing,
                    $"Extension '{extension.Name}' {extension.Version} requires extension '{required}', which is not installed in this database");
        }

        foreach (ExtensionDefinition other in schema.Extensions.Values)
        {
            if (string.Equals(other.Name, extension.Name, StringComparison.OrdinalIgnoreCase))
                continue;

            foreach (ExtensionMemberDefinition member in extension.Members)
            {
                if (member.Kind == ExtensionMemberKind.Procedure && other.HasMember(member.Kind, member.Name))
                    throw new CamusDBException(
                        CamusDBErrorCodes.ExtensionMemberConflict,
                        $"Extension '{extension.Name}' defines procedure '{member.Name}', which installed extension '{other.Name}' already defines");

                if (!ExtensionMemberKinds.IsCallable(member.Kind))
                    continue;

                // Functions, aggregates and table functions share one name space.
                foreach (ExtensionMemberDefinition theirs in other.Members)
                {
                    if (ExtensionMemberKinds.IsCallable(theirs.Kind) && string.Equals(theirs.Name, member.Name, StringComparison.OrdinalIgnoreCase))
                        throw new CamusDBException(
                            CamusDBErrorCodes.ExtensionMemberConflict,
                            $"Extension '{extension.Name}' defines {ExtensionMemberKinds.Describe(member.Kind)} '{member.Name}', " +
                            $"which installed extension '{other.Name}' already defines as {ExtensionMemberKinds.Describe(theirs.Kind)}");
                }
            }
        }
    }

    private static bool IsSameRecord(ExtensionDefinition a, ExtensionDefinition b) =>
        string.Equals(a.Version, b.Version, StringComparison.Ordinal)
        && string.Equals(a.Sha256, b.Sha256, StringComparison.OrdinalIgnoreCase)
        && a.InstalledAt == b.InstalledAt
        && a.UpdatedAt == b.UpdatedAt;

    private static ExtensionDefinition RequireRecord(SchemaExtensionPayload payload, string op) =>
        payload.Extension is { Name.Length: > 0 } extension
            ? extension
            : throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"A {op} delta carries no extension record");

    /// <summary>Publishes a copy of the installed map with <paramref name="name"/> set to <paramref name="record"/>, or removed when it is null.</summary>
    private static void Publish(Schema schema, string name, ExtensionDefinition? record, long stamp)
    {
        Dictionary<string, ExtensionDefinition> next = new(schema.Extensions, StringComparer.OrdinalIgnoreCase);

        if (record is null)
            next.Remove(name);
        else
            next[name] = record;

        schema.ExtensionCatalog.Publish(next, stamp);
    }
}
