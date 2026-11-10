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
/// Applies the committed deltas that add, replace, remove and give away an inline SQL function
/// (<see cref="SchemaOp.CreateFunction"/>, <see cref="SchemaOp.ReplaceFunction"/>,
/// <see cref="SchemaOp.DropFunction"/>, <see cref="SchemaOp.SetFunctionOwner"/>).
///
/// <para><b>Metadata only, and never the compiler.</b> The apply runs inside the commit pipeline of
/// the schema partition, so it only swaps records. The proposer compiled the body before it proposed;
/// a node compiles a function at its first call. A node whose compiler refuses a body still applies
/// the delta, so a committed delta applies everywhere.</para>
///
/// <para><b>Rules checked in log order.</b> A create is refused when the name is taken by a function
/// or by a function, an aggregate or a table function of an installed extension. The proposer checks
/// the same rule (and, on its own build, the built-in names) before it proposes; checking the
/// replicated part again here orders a <c>CREATE FUNCTION</c> against a concurrent
/// <c>CREATE EXTENSION</c>. Every check runs before the first change.</para>
///
/// <para><b>Idempotent.</b> A re-delivered create or replace finds the record it left behind and
/// returns. A replace, a drop or an owner change that was proposed against a record a later statement
/// already replaced or removed is a no-op, so it cannot undo that statement.</para>
///
/// <para>The function map is replaced, never edited in place (see
/// <see cref="CamusDB.Core.Extensions.Inline.InlineFunctionCatalog"/>), and a record is never changed
/// after it is published: an owner change publishes a copy. Each arm returns null: a function is not
/// a relation, and the proposer persists its own meta key afterwards.</para>
/// </summary>
internal static class InlineFunctionDeltaApplier
{
    internal static TableSchema? ApplyCreateFunction(Schema schema, SchemaFunctionPayload payload, long toVersion)
    {
        InlineFunctionDefinition function = RequireRecord(payload, "CreateFunction");

        if (schema.Functions.TryGetValue(function.Name, out InlineFunctionDefinition? existing))
        {
            if (existing.IsSameRecord(function))
                return null;

            throw new CamusDBException(
                CamusDBErrorCodes.FunctionAlreadyExists,
                $"Function '{function.Name}' already exists; use CREATE OR REPLACE FUNCTION to change it");
        }

        RequireNameFreeOfExtensions(schema, function.Name);
        Publish(schema, function.Name, function, toVersion);
        return null;
    }

    internal static TableSchema? ApplyReplaceFunction(Schema schema, SchemaFunctionPayload payload, long toVersion)
    {
        InlineFunctionDefinition function = RequireRecord(payload, "ReplaceFunction");

        if (!IsCurrent(schema, function, payload.FromGeneration, out _))
            return null;

        Publish(schema, function.Name, function, toVersion);
        return null;
    }

    internal static TableSchema? ApplyDropFunction(Schema schema, SchemaDropFunctionPayload payload, long toVersion)
    {
        if (!schema.Functions.TryGetValue(payload.FunctionName, out InlineFunctionDefinition? existing) ||
            existing.CreatedAt != payload.CreatedAt)
            return null;

        Publish(schema, existing.Name, null, toVersion);
        return null;
    }

    internal static TableSchema? ApplySetFunctionOwner(Schema schema, SchemaSetFunctionOwnerPayload payload, long toVersion)
    {
        if (!schema.Functions.TryGetValue(payload.FunctionName, out InlineFunctionDefinition? existing) ||
            existing.CreatedAt != payload.CreatedAt || WasOwnerSet(existing, payload))
            return null;

        Publish(schema, existing.Name, WithOwner(existing, payload), toVersion);
        return null;
    }

    internal static bool WasCreateApplied(Schema schema, SchemaFunctionPayload payload) =>
        payload.Function is { } function
        && schema.Functions.TryGetValue(function.Name, out InlineFunctionDefinition? existing)
        && existing.IsSameRecord(function);

    /// <summary>
    /// Applied when the record the delta carries is published, and also when the function is gone or
    /// is no longer at the generation the replace was proposed against: the apply then was, or will
    /// be, a no-op, and a proposer must not wait for a change that cannot happen.
    /// </summary>
    internal static bool WasReplaceApplied(Schema schema, SchemaFunctionPayload payload) =>
        payload.Function is not { } function
        || !schema.Functions.TryGetValue(function.Name, out InlineFunctionDefinition? existing)
        || existing.IsSameRecord(function)
        || !IsCurrent(schema, function, payload.FromGeneration, out _);

    internal static bool WasDropApplied(Schema schema, SchemaDropFunctionPayload payload) =>
        !schema.Functions.TryGetValue(payload.FunctionName, out InlineFunctionDefinition? existing)
        || existing.CreatedAt != payload.CreatedAt;

    internal static bool WasSetOwnerApplied(Schema schema, SchemaSetFunctionOwnerPayload payload) =>
        !schema.Functions.TryGetValue(payload.FunctionName, out InlineFunctionDefinition? existing)
        || existing.CreatedAt != payload.CreatedAt
        || WasOwnerSet(existing, payload);

    /// <summary>The function a function delta targets, for the checkpoint that persists its key.</summary>
    internal static string GetFunctionName(SchemaChangeLogEntry entry) => entry.Op switch
    {
        SchemaOp.CreateFunction or SchemaOp.ReplaceFunction =>
            SchemaDeltaApplier.DecodePayload<SchemaFunctionPayload>(entry).Function?.Name ?? "",
        SchemaOp.DropFunction => SchemaDeltaApplier.DecodePayload<SchemaDropFunctionPayload>(entry).FunctionName,
        SchemaOp.SetFunctionOwner => SchemaDeltaApplier.DecodePayload<SchemaSetFunctionOwnerPayload>(entry).FunctionName,
        _ => throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Schema operation '{entry.Op}' does not target a function")
    };

    /// <summary>True for the four operations this class applies.</summary>
    internal static bool IsFunctionOp(SchemaOp op) =>
        op is SchemaOp.CreateFunction or SchemaOp.ReplaceFunction or SchemaOp.DropFunction or SchemaOp.SetFunctionOwner;

    /// <summary>
    /// The installed extension that defines <paramref name="name"/> as a function, an aggregate or a
    /// table function, or null. Read from the replicated records, so every node gives the same answer.
    /// </summary>
    internal static ExtensionDefinition? ExtensionThatDefines(Schema schema, string name)
    {
        foreach (ExtensionDefinition extension in schema.Extensions.Values)
        {
            foreach (ExtensionMemberDefinition member in extension.Members)
            {
                if (ExtensionMemberKinds.IsCallable(member.Kind) && string.Equals(member.Name, name, StringComparison.OrdinalIgnoreCase))
                    return extension;
            }
        }

        return null;
    }

    private static void RequireNameFreeOfExtensions(Schema schema, string name)
    {
        if (ExtensionThatDefines(schema, name) is { } extension)
            throw new CamusDBException(
                CamusDBErrorCodes.FunctionAlreadyExists,
                $"Cannot create function '{name}': the installed extension '{extension.Name}' defines a function of that name");
    }

    /// <summary>
    /// True when the published record of <paramref name="function"/>'s name is the function the
    /// replace was proposed against: the same creation time and the generation the proposer saw.
    /// </summary>
    private static bool IsCurrent(Schema schema, InlineFunctionDefinition function, long? fromGeneration, out InlineFunctionDefinition? existing) =>
        schema.Functions.TryGetValue(function.Name, out existing)
        && existing.CreatedAt == function.CreatedAt
        && existing.Generation == fromGeneration;

    private static bool WasOwnerSet(InlineFunctionDefinition existing, SchemaSetFunctionOwnerPayload payload) =>
        existing.UpdatedAt == payload.UpdatedAt
        && string.Equals(existing.OwnerId, payload.OwnerId, StringComparison.Ordinal)
        && string.Equals(existing.Owner, payload.Owner, StringComparison.Ordinal);

    private static InlineFunctionDefinition WithOwner(InlineFunctionDefinition existing, SchemaSetFunctionOwnerPayload payload) => new()
    {
        Name = existing.Name,
        Parameters = existing.Parameters,
        ReturnType = existing.ReturnType,
        ReturnElementType = existing.ReturnElementType,
        NullInput = existing.NullInput,
        Language = existing.Language,
        Source = existing.Source,
        SourceSha256 = existing.SourceSha256,
        LanguageVersion = existing.LanguageVersion,
        RuleRevision = existing.RuleRevision,
        Owner = payload.Owner,
        OwnerId = payload.OwnerId,
        CreatedAt = existing.CreatedAt,
        UpdatedAt = payload.UpdatedAt,
        Generation = existing.Generation,
    };

    private static InlineFunctionDefinition RequireRecord(SchemaFunctionPayload payload, string op) =>
        payload.Function is { Name.Length: > 0 } function
            ? function
            : throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"A {op} delta carries no function record");

    /// <summary>Publishes a copy of the function map with <paramref name="name"/> set to <paramref name="record"/>, or removed when it is null.</summary>
    private static void Publish(Schema schema, string name, InlineFunctionDefinition? record, long version)
    {
        Dictionary<string, InlineFunctionDefinition> next = new(schema.Functions, StringComparer.OrdinalIgnoreCase);

        if (record is null)
            next.Remove(name);
        else
            next[name] = record;

        schema.FunctionCatalog.Publish(next, version);
    }
}
