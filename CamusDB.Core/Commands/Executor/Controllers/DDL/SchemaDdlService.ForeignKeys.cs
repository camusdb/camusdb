/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// <c>ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY</c> and <c>DROP CONSTRAINT</c> of a foreign key.
///
/// <para><b>ADD is a staged rollout, in this order:</b></para>
/// <list type="number">
/// <item>Resolve the constraint against the live schema, under <c>SchemaDdlSemaphore</c>.</item>
/// <item>When no index can serve the parent-side probe, build <c>~fk_{name}</c> with the staged index
/// build and its backfill, and publish it, unowned, before the constraint exists.</item>
/// <item>Record the rollout job, then propose <see cref="SchemaOp.AddForeignKey"/> in <c>WriteOnly</c>,
/// which also gives the new index its owner. From the ack on, every node enforces both sides.</item>
/// <item>The coordinator runs the validation pass and makes the constraint <c>Public</c>. On an orphan
/// it removes the constraint and its owned index and fails the statement with
/// <see cref="CamusDBErrorCodes.ForeignKeyViolation"/>.</item>
/// </list>
///
/// <para>A standalone node runs the same steps: it is the only member of its schema group, so each
/// proposal costs a single-node commit and the ack gate passes at once. Its index build uses the
/// standalone backfill, as <c>CREATE INDEX</c> does there.</para>
///
/// <para><b>A failure after step 3 is not rolled back.</b> A lost leadership leaves the constraint
/// enforced in <c>WriteOnly</c> with its job recorded, and the next leader's resume validates and
/// publishes it, or removes it. A failure before step 3 removes the index that step 2 built.</para>
///
/// <para><b>DROP</b> removes the constraint in one step (<c>SetElementState</c> to <c>Absent</c>, which
/// also clears the index's owner), then drops the index the constraint owned, by the name read before
/// the first step. The other order is refused: the DROP INDEX guard protects a backing index while
/// its constraint exists.</para>
/// </summary>
internal sealed partial class SchemaDdlService
{
    /// <summary>
    /// Runs an <c>ALTER TABLE</c> constraint statement on this node, after the caller decided not to
    /// forward it. The ticket API and the SQL dispatcher both call it, so the foreign-key routing is
    /// the same for both.
    /// </summary>
    internal async Task<bool> AlterConstraintLocalAsync(DatabaseDescriptor database, TableDescriptor table, AlterConstraintTicket ticket)
    {
        if (ticket.Operation == AlterConstraintOperation.AddForeignKey)
            return await AddForeignKeyAsync(database, ticket).ConfigureAwait(false);

        if (ticket.Operation == AlterConstraintOperation.DropConstraint && NamesOnlyAForeignKey(table.Schema, ticket.ConstraintName))
            return await DropForeignKeyAsync(database, ticket).ConfigureAwait(false);

        return await tableConstraintAlterer.Alter(catalogs, context.TableOpener, database, table, ticket, context.IsClusterMode).ConfigureAwait(false);
    }

    /// <summary>
    /// True when <paramref name="name"/> is a foreign key of <paramref name="table"/> and not a CHECK
    /// or named NOT NULL constraint. Those two keep their resolution order and their own path.
    /// </summary>
    private static bool NamesOnlyAForeignKey(TableSchema table, string name)
    {
        if (table.CheckConstraints?.Exists(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase)) == true)
            return false;

        if (table.Columns?.Exists(c => string.Equals(c.NotNullConstraintName, name, StringComparison.Ordinal)) == true)
            return false;

        return table.ForeignKeys?.Exists(fk => string.Equals(fk.Name, name, StringComparison.OrdinalIgnoreCase)) == true;
    }

    private async Task<bool> AddForeignKeyAsync(DatabaseDescriptor database, AlterConstraintTicket ticket)
    {
        ForeignKeyInfo info = ticket.ForeignKey
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "A foreign key definition is required for ADD FOREIGN KEY");

        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            // Opened inside the gate: a drop and re-create of the same name between an open outside
            // and the proposal would resolve the constraint against a table that is gone.
            TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
            string tableName = table.Name;

            ForeignKeyAlterPlan plan;

            await database.Schema.AcquireLockAsync().ConfigureAwait(false);
            try
            {
                plan = ForeignKeyDefinitionBuilder.ResolveForAlter(database.Schema, table.Schema, info, options);

                // Checked here as well as at apply time, so a cycle is refused before an index is built
                // for it. Only the parent id and the names are read; the backing index does not matter.
                ForeignKeyDefinitionRules.RequireNoCycle(database.Schema, table.Schema, plan.ToSchema(plan.BackingIndexId ?? ""));
            }
            finally
            {
                database.Schema.ReleaseLock();
            }

            await RequireNoOtherJobNamedAsync(database, table.Id, info.Name).ConfigureAwait(false);

            string backingIndexId;
            string? claimedIndexId = null;

            if (plan.OwnedIndexName is { } ownedIndexName)
            {
                backingIndexId = await BuildOwnedIndexAsync(database, table, plan, ownedIndexName).ConfigureAwait(false);
                claimedIndexId = backingIndexId;
            }
            else
            {
                backingIndexId = plan.BackingIndexId!;
            }

            ForeignKeySchema foreignKey = plan.ToSchema(backingIndexId);

            // Recorded before the constraint exists, so no crash can leave a WriteOnly constraint without
            // a job to finish it. A job whose constraint never appeared is removed by the next resume.
            await catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
            {
                TableName = tableName,
                TableId = table.Id,
                ElementName = foreignKey.Name,
                TargetState = SchemaElementState.Public,
                ElementKind = SchemaElementKind.ForeignKey,
            }).ConfigureAwait(false);

            try
            {
                await catalogs.ReplicateAddForeignKeyAsync(database, tableName, table.Id, foreignKey, claimedIndexId).ConfigureAwait(false);
            }
            catch
            {
                // The proposal can fail after its entry committed, for example when leadership moves
                // during the wait for the acks. Then the constraint exists in WriteOnly and its job must
                // stay for the next leader. Only a constraint that did not land is cleaned up here.
                bool landed = database.Schema.Tables.TryGetValue(tableName, out TableSchema? current)
                    && current.ForeignKeys?.Exists(fk => string.Equals(fk.Id, foreignKey.Id, StringComparison.Ordinal)) == true;

                if (!landed)
                {
                    await DeleteForeignKeyJobAsync(database, table.Id, foreignKey.Name).ConfigureAwait(false);

                    if (plan.OwnedIndexName is { } builtIndexName)
                        await DropIndexAfterFailureAsync(database, tableName, builtIndexName).ConfigureAwait(false);
                }

                throw;
            }

            database.Cache?.InvalidateByTableId(database.Id, table.Id);

            SchemaChangeCoordinator coordinator = new(catalogs, context.Logger)
            {
                ForeignKeyValidationAsync = async (db, childName, constraintName) =>
                {
                    if (TestInterceptBeforeForeignKeyValidation is { } intercept)
                        await intercept().ConfigureAwait(false);

                    await ValidateForeignKeyRowsAsync(db, childName, constraintName).ConfigureAwait(false);
                },
                DropIndexAsync = (db, childName, indexName) => DropIndexWithinGateAsync(db, childName, indexName)
            };

            await coordinator.RunJobAsync(
                database,
                new SchemaChangeJob(database.Name, tableName, table.Id, foreignKey.Name, SchemaElementState.Public, SchemaElementKind.ForeignKey)
            ).ConfigureAwait(false);

            if (context.Logger.IsEnabled(LogLevel.Information))
                context.Logger.LogInformation("Foreign key '{Constraint}' added to table '{Table}'", foreignKey.Name, tableName);

            return true;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Test-only hook, invoked by <c>ADD CONSTRAINT ... FOREIGN KEY</c> after every node acked the
    /// constraint in <c>WriteOnly</c> and before the validation pass. A test uses it to write rows or to
    /// fail the statement at exactly that point. Null in production; a test clears it after use.
    /// </summary>
    internal Func<Task>? TestInterceptBeforeForeignKeyValidation;

    /// <summary>
    /// A rollout job is keyed by table id and element name, and a foreign key uses its constraint name.
    /// A column or index job of the same name would share the key, and one job would overwrite the
    /// other. A job of a foreign key of the same name is left from an earlier attempt whose constraint
    /// is gone; it is safe to replace.
    /// </summary>
    private async Task RequireNoOtherJobNamedAsync(DatabaseDescriptor database, string tableId, string constraintName)
    {
        List<PersistedCoordinatorJob> jobs = await catalogs.LoadCoordinatorJobsAsync(database).ConfigureAwait(false);

        foreach (PersistedCoordinatorJob job in jobs)
        {
            if (job.ElementKind != SchemaElementKind.ForeignKey
                && string.Equals(job.TableId, tableId, StringComparison.Ordinal)
                && string.Equals(job.ElementName, constraintName, StringComparison.OrdinalIgnoreCase))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Cannot add foreign key '{constraintName}' to table '{job.TableName}': a change to the {job.ElementKind.ToString().ToLowerInvariant()} '{job.ElementName}' is still in progress. Retry when it finishes, or use another constraint name");
        }
    }

    /// <summary>
    /// Builds and publishes the index <paramref name="indexName"/> over the constraint's columns, in
    /// constraint order, ascending, and returns its <see cref="TableIndexSchema.KvId"/>. The index is
    /// unowned until the <see cref="SchemaOp.AddForeignKey"/> delta claims it. Each mode uses the build it
    /// uses for <c>CREATE INDEX</c>, so both backfill loops serve foreign keys too.
    /// </summary>
    private async Task<string> BuildOwnedIndexAsync(DatabaseDescriptor database, TableDescriptor table, ForeignKeyAlterPlan plan, string indexName)
    {
        string tableName = table.Name;

        if (context.IsClusterMode)
        {
            IndexBuildInfo indexInfo = new(
                ObjectIdGenerator.Generate().ToString(),
                indexName,
                [.. plan.ChildColumnIds],
                [.. plan.ChildColumnNames],
                IndexType.Multi);

            await StageIndexWithinGateAsync(database, table.Id, tableName, indexInfo).ConfigureAwait(false);
        }
        else
        {
            ColumnIndexInfo[] columns = new ColumnIndexInfo[plan.ChildColumnNames.Length];
            for (int i = 0; i < columns.Length; i++)
                columns[i] = new ColumnIndexInfo(plan.ChildColumnNames[i], OrderType.Ascending);

            AlterIndexTicket indexTicket = new(database.Name, tableName, indexName, columns, AlterIndexOperation.AddIndex);

            await RunIndexDdlWithinGateAsync(
                database, table, indexTicket, compensateOnAbort: true,
                tx => tableIndexAlterer.Alter(queryExecutor, database, table, indexTicket, tx)
            ).ConfigureAwait(false);
        }

        TableIndexSchema? built = null;

        if (database.Schema.Tables.TryGetValue(tableName, out TableSchema? tableSchema))
            built = tableSchema.Indexes?.Find(ix => string.Equals(ix.Name, indexName, StringComparison.OrdinalIgnoreCase));

        if (built is null || built.State != SchemaElementState.Public)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"The index '{indexName}' that foreign key '{plan.Name}' needs was not published on table '{tableName}'");

        return built.KvId;
    }

    private async Task<bool> DropForeignKeyAsync(DatabaseDescriptor database, AlterConstraintTicket ticket)
    {
        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
            string tableName = table.Name;

            ForeignKeySchema foreignKey = table.Schema.ForeignKeys?.Find(fk => string.Equals(fk.Name, ticket.ConstraintName, StringComparison.OrdinalIgnoreCase))
                ?? throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Constraint '{ticket.ConstraintName}' does not exist on table '{tableName}'");

            // Read before the removal: the same delta clears the index's owner, and afterwards nothing
            // tells the index the engine built apart from one the user made.
            string? ownedIndexName = SchemaChangeCoordinator.FindOwnedIndexName(database.Schema, tableName, foreignKey.Name);

            await catalogs.ReplicateElementStateAsync(
                database, tableName, foreignKey.Name, SchemaElementState.Absent, SchemaElementKind.ForeignKey
            ).ConfigureAwait(false);

            // A constraint dropped during its rollout leaves a job behind. A resume would find the
            // constraint Absent and delete it anyway; this only saves it the work.
            await DeleteForeignKeyJobAsync(database, table.Id, foreignKey.Name).ConfigureAwait(false);

            if (ownedIndexName is not null)
                await DropIndexAfterFailureAsync(database, tableName, ownedIndexName).ConfigureAwait(false);

            database.Cache?.InvalidateByTableId(database.Id, table.Id);

            if (context.Logger.IsEnabled(LogLevel.Information))
                context.Logger.LogInformation("Foreign key '{Constraint}' dropped from table '{Table}'", foreignKey.Name, tableName);

            return true;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
            await FireDeferredStepDownIfRequestedAsync(database).ConfigureAwait(false);
        }
    }

    private async Task DeleteForeignKeyJobAsync(DatabaseDescriptor database, string tableId, string constraintName)
    {
        try
        {
            await catalogs.DeleteCoordinatorJobAsync(database, tableId, constraintName).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            context.Logger.LogWarning(
                ex,
                "Failed to delete the rollout job of foreign key {ConstraintName} in database {DatabaseName}; the next resume removes it",
                constraintName,
                database.Name);
        }
    }

    /// <summary>
    /// Best effort. The statement has either failed already, and its own error must reach the caller, or
    /// removed the constraint, which is the part that matters. An index left behind is an ordinary index:
    /// it still serves queries, and the ticket API can drop it.
    /// </summary>
    private async Task DropIndexAfterFailureAsync(DatabaseDescriptor database, string tableName, string indexName)
    {
        try
        {
            await DropIndexWithinGateAsync(database, tableName, indexName).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            context.Logger.LogWarning(
                ex,
                "Failed to drop index {IndexName} of table {TableName}, which a foreign key no longer uses",
                indexName,
                tableName);
        }
    }
}
