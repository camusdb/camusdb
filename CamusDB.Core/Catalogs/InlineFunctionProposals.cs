/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor.Models;
using Kommander.Time;

namespace CamusDB.Core.Catalogs;

/// <summary>
/// Proposes the replicated changes to a database's inline SQL functions: create, replace, drop and
/// owner change. Each operation builds its delta and validates it under the schema lock, releases the
/// lock, and hands the delta to <see cref="SchemaChangePublisher"/>, which waits for the local apply
/// and persists the record's meta key. The same shape as <see cref="ExtensionCatalog"/>.
///
/// <para><b>Checks run under the lock, in the critical section that builds the delta.</b> Whether the
/// function exists, whether its name is free, who owns it, and whether a schema object calls it: a
/// check made before the lock would be a check-then-act that a concurrent <c>CREATE VIEW</c> or
/// <c>CREATE FUNCTION</c> could pass in between. The rules that need SQL parsing, the session's
/// principal or this node's function tables live with the DDL service, so they arrive as callbacks
/// that run inside the lock, with the schema and the current record.</para>
/// </summary>
internal sealed class InlineFunctionProposals
{
    private readonly SchemaChangePublisher publisher;

    public InlineFunctionProposals(SchemaChangePublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(publisher);

        this.publisher = publisher;
    }

    /// <summary>
    /// Proposes a <see cref="SchemaOp.CreateFunction"/> delta for <paramref name="function"/>, or, when
    /// <paramref name="replace"/> is true and the function exists, a
    /// <see cref="SchemaOp.ReplaceFunction"/> delta, and waits for it to apply locally.
    ///
    /// <para><paramref name="checkNew"/> runs under the lock for a function that does not exist yet
    /// (the name and count rules). <paramref name="checkReplace"/> runs under the lock with the current
    /// record and the new one (the ownership and dependency rules). A replace keeps the creation time
    /// and the owner of the current record and takes the next generation.</para>
    /// </summary>
    /// <returns>True when a function was replaced, false when one was created.</returns>
    internal async Task<bool> CreateOrReplaceAsync(
        DatabaseDescriptor database,
        InlineFunctionDefinition function,
        bool replace,
        Action<Schema> checkNew,
        Action<Schema, InlineFunctionDefinition, InlineFunctionDefinition> checkReplace)
    {
        SchemaChangeLogEntry entry;
        bool replaced;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (database.Schema.Functions.TryGetValue(function.Name, out InlineFunctionDefinition? existing))
            {
                if (!replace)
                    throw new CamusDBException(
                        CamusDBErrorCodes.FunctionAlreadyExists,
                        $"Function '{existing.Name}' already exists; use CREATE OR REPLACE FUNCTION to change it");

                checkReplace(database.Schema, existing, function);

                // The function stays the same function: a drop is matched on the creation time, and the
                // owner changes only through ALTER FUNCTION ... OWNER TO.
                function.Name = existing.Name;
                function.CreatedAt = existing.CreatedAt;
                function.Owner = existing.Owner;
                function.OwnerId = existing.OwnerId;
                function.Generation = existing.Generation + 1;

                entry = SchemaChangeEntryFactory.ReplaceFunctionEntry(database, function, existing.Generation);
                replaced = true;
            }
            else
            {
                checkNew(database.Schema);

                function.Generation = 0;
                entry = SchemaChangeEntryFactory.CreateFunctionEntry(database, function);
                replaced = false;
            }

            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
        return replaced;
    }

    /// <summary>
    /// Proposes a <see cref="SchemaOp.DropFunction"/> delta and waits for it to apply locally.
    /// <paramref name="check"/> runs under the lock with the current record and throws when the caller
    /// may not drop it or while an object calls it.
    /// </summary>
    internal async Task DropAsync(DatabaseDescriptor database, string functionName, Action<Schema, InlineFunctionDefinition> check)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            InlineFunctionDefinition existing = Require(database, functionName);

            check(database.Schema, existing);

            entry = SchemaChangeEntryFactory.DropFunctionEntry(database, existing);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a <see cref="SchemaOp.SetFunctionOwner"/> delta and waits for it to apply locally.
    /// <paramref name="check"/> runs under the lock with the current record and throws when the caller
    /// may not give the function away.
    /// </summary>
    internal async Task SetOwnerAsync(
        DatabaseDescriptor database, string functionName, string? owner, string? ownerId, HLCTimestamp now, Action<Schema, InlineFunctionDefinition> check)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            InlineFunctionDefinition existing = Require(database, functionName);

            check(database.Schema, existing);

            entry = SchemaChangeEntryFactory.SetFunctionOwnerEntry(database, existing, owner, ownerId, now);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    private static InlineFunctionDefinition Require(DatabaseDescriptor database, string functionName) =>
        database.Schema.Functions.TryGetValue(functionName, out InlineFunctionDefinition? existing)
            ? existing
            : throw new CamusDBException(
                CamusDBErrorCodes.FunctionNotFound,
                $"Function '{functionName}' does not exist in database '{database.Name}'");
}
