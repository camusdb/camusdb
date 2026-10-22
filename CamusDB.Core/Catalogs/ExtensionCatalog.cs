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

namespace CamusDB.Core.Catalogs;

/// <summary>
/// Proposes the replicated changes to a database's installed C# extensions: install, update and
/// remove. Each operation builds its delta and validates it under the schema lock, releases the lock,
/// and hands the delta to <see cref="SchemaChangePublisher"/>, which waits for the local apply and
/// persists the record's meta key.
///
/// <para><b>Checks run under the lock, in the critical section that builds the delta.</b> Whether the
/// extension is installed, whether its requirements are met, whether another installed extension
/// defines one of its member names, and whether a schema object depends on it: a check made before
/// taking the lock would be a check-then-act that a concurrent <c>CREATE VIEW</c> or
/// <c>CREATE TRIGGER</c> could pass in between. The dependency rule needs SQL parsing and lives with
/// the DDL service, so it arrives as a callback that runs inside the lock.</para>
/// </summary>
internal sealed class ExtensionCatalog
{
    private readonly SchemaChangePublisher publisher;

    public ExtensionCatalog(SchemaChangePublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(publisher);

        this.publisher = publisher;
    }

    /// <summary>Proposes a <see cref="SchemaOp.CreateExtension"/> delta and waits for it to apply locally.</summary>
    internal async Task CreateExtensionAsync(DatabaseDescriptor database, ExtensionDefinition extension)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (database.Schema.Extensions.TryGetValue(extension.Name, out ExtensionDefinition? existing))
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionAlreadyInstalled,
                    $"Extension '{extension.Name}' is already installed (version {existing.Version}); use ALTER EXTENSION {extension.Name} UPDATE to change the version");

            entry = SchemaChangeEntryFactory.CreateExtensionEntry(database, extension);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    /// <summary>
    /// Proposes a <see cref="SchemaOp.UpdateExtension"/> delta that replaces the installed record with
    /// <paramref name="extension"/>, and waits for it to apply locally. <paramref name="checkDependents"/>
    /// runs under the lock with the installed and the new record, and throws when an object uses a
    /// member the new version lacks. Returns false, and proposes nothing, when the installed record
    /// already has the target version and build.
    /// </summary>
    internal async Task<bool> UpdateExtensionAsync(
        DatabaseDescriptor database, ExtensionDefinition extension, Action<Schema, ExtensionDefinition, ExtensionDefinition> checkDependents)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            ExtensionDefinition installed = RequireInstalled(database, extension.Name);

            if (string.Equals(installed.Version, extension.Version, StringComparison.Ordinal) &&
                string.Equals(installed.Sha256, extension.Sha256, StringComparison.OrdinalIgnoreCase))
                return false;

            checkDependents(database.Schema, installed, extension);

            // The install time stays with the extension across updates: a drop is matched on it.
            extension.InstalledAt = installed.InstalledAt;

            entry = SchemaChangeEntryFactory.UpdateExtensionEntry(database, extension, installed.Version);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Proposes a <see cref="SchemaOp.DropExtension"/> delta and waits for it to apply locally.
    /// <paramref name="checkDependents"/> runs under the lock and throws while an object uses the
    /// extension; the apply itself refuses while another installed extension requires it.
    /// </summary>
    internal async Task DropExtensionAsync(DatabaseDescriptor database, string extensionName, Action<Schema, ExtensionDefinition> checkDependents)
    {
        SchemaChangeLogEntry entry;

        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            ExtensionDefinition installed = RequireInstalled(database, extensionName);

            checkDependents(database.Schema, installed);

            entry = SchemaChangeEntryFactory.DropExtensionEntry(database, installed);
            SchemaDeltaApplier.ValidateSchemaDelta(database, entry);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }

        await publisher.ReplicateAndWaitLocalApplyAsync(database, entry).ConfigureAwait(false);
    }

    private static ExtensionDefinition RequireInstalled(DatabaseDescriptor database, string extensionName) =>
        database.Schema.Extensions.TryGetValue(extensionName, out ExtensionDefinition? installed)
            ? installed
            : throw new CamusDBException(
                CamusDBErrorCodes.ExtensionNotInstalled,
                $"Extension '{extensionName}' is not installed in database '{database.Name}'");
}
