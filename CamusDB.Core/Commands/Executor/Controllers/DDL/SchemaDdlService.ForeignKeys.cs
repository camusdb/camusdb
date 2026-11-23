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
using CamusDB.Core.Catalogs.Transactional;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// <c>ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY</c> and <c>DROP CONSTRAINT</c> of a foreign key.
///
/// <para><b>ADD is a staged rollout, in this order:</b></para>
/// <list type="number">
/// <item>Resolve the constraint against the live schema, under the DDL gate, entered for the child
/// table.</item>
/// <item>When no index can serve the parent-side probe, build <c>~fk_{name}</c> with the staged index
/// build and its backfill, and publish it, unowned, before the constraint exists.</item>
/// <item>Record the rollout job, then propose <see cref="SchemaOp.AddForeignKey"/> in <c>WriteOnly</c>,
/// which also gives the new index its owner. From the ack on, every node enforces both sides.</item>
/// <item>The coordinator runs the validation pass and makes the constraint <c>Public</c>. On an orphan
/// it removes the constraint and its owned index and fails the statement with
/// <see cref="CamusDBErrorCodes.ForeignKeyViolation"/>.</item>
/// </list>
///
/// <para>The index build of step 2 and the validation pass of step 4 run with the DDL gate released
/// and the child table held, like every staged change (<see cref="SchemaChangeCoordinator.RunJobAsync"/>):
/// DDL on other tables of the database proceeds meanwhile, DDL on the child table waits.</para>
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

        if (TransactionSchemaScope.OverlayFor(database) is { } overlay
            && !overlay.CreatedTable(table.Id)
            && ReadsRowsToValidate(table.Schema, ticket))
            return await StageRowConstraintOnSharedTableAsync(database, overlay, table, ticket).ConfigureAwait(false);

        if (ticket.Operation == AlterConstraintOperation.DropConstraint && NamesOnlyAForeignKey(table.Schema, ticket.ConstraintName))
            return await DropForeignKeyAsync(database, ticket).ConfigureAwait(false);

        return await tableConstraintAlterer.Alter(catalogs, context.TableOpener, database, table, ticket, context.IsClusterMode).ConfigureAwait(false);
    }

    /// <summary>
    /// True for an ADD CHECK, and for a SET NOT NULL on a column that exists and is nullable: the two
    /// constraint statements that must read every row of the table before the constraint holds.
    /// </summary>
    private static bool ReadsRowsToValidate(TableSchema table, AlterConstraintTicket ticket) => ticket.Operation switch
    {
        AlterConstraintOperation.AddCheck => true,
        AlterConstraintOperation.SetNotNull => table.Columns?.Find(c => string.Equals(c.Name, ticket.ColumnName, StringComparison.OrdinalIgnoreCase)) is { NotNull: false },
        _ => false,
    };

    /// <summary>
    /// ADD CHECK, or SET NOT NULL on a nullable column, inside a transaction with a private schema, on a
    /// table the transaction did not create: the cluster form runs on the shared table, owned by the
    /// transaction (<see cref="TableConstraintAlterer.StageRowConstraintAsync"/>). The constraint is enforced
    /// for every transaction from the statement on, and the existing rows are proven at the statement. No
    /// step joins the change set; the commit makes the constraint permanent, and a rollback removes it
    /// (<see cref="RunOwnedStagedChangeAsync"/>). The check reads with no lock, so locks the transaction
    /// holds on the table's rows do not refuse it.
    /// </summary>
    private Task<bool> StageRowConstraintOnSharedTableAsync(
        DatabaseDescriptor database, TransactionSchemaOverlay overlay, TableDescriptor table, AlterConstraintTicket ticket)
    {
        string statement = ticket.Operation == AlterConstraintOperation.AddCheck
            ? "ALTER TABLE ... ADD CONSTRAINT ... CHECK"
            : "ALTER TABLE ... ALTER COLUMN ... SET NOT NULL";

        return RunOwnedStagedChangeAsync(database, overlay, table, statement, async () =>
        {
            (string tableId, string tableName, string constraintName) = await tableConstraintAlterer.StageRowConstraintAsync(
                catalogs, context.TableOpener, database, ticket, overlay.ChangeSetId).ConfigureAwait(false);

            return new OwnedStagedElement(tableId, tableName, 1, [constraintName], []);
        }, locksRows: false);
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

        if (TransactionSchemaScope.OverlayFor(database) is { } overlay)
            return await AddForeignKeyInOwnerTransactionAsync(database, overlay, ticket, info).ConfigureAwait(false);

        await database.DdlGate.EnterAsync(ticket.TableName, context.Logger).ConfigureAwait(false);
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
                new SchemaChangeJob(database.Name, tableName, table.Id, foreignKey.Name, SchemaElementState.Public, SchemaElementKind.ForeignKey),
                gateHeldByCaller: true
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
    /// <c>ADD ... FOREIGN KEY</c> inside a transaction with a private schema, on a child table the
    /// transaction created. No other transaction can see that table before the commit, so the rollout runs
    /// inside the transaction, with no job record and no coordinator:
    /// <list type="number">
    /// <item>Build the backing index over the transaction's own rows, when the constraint needs one
    /// (<see cref="IndexBackfiller.BuildInTransactionAsync"/>).</item>
    /// <item>Add the constraint in <c>WriteOnly</c> to the change set. From here on the private schema
    /// enforces it on the transaction's own writes.</item>
    /// <item>Check every row of the child through the transaction with the checker an INSERT uses
    /// (<see cref="ForeignKeyStatementChecker"/>). It takes the shared rendezvous lock on each parent key
    /// it finds, so a concurrent DELETE of that parent waits for the transaction to end, as it does for a
    /// child row the transaction inserts. A missing parent fails the statement with
    /// <see cref="CamusDBErrorCodes.ForeignKeyViolation"/>.</item>
    /// <item>Move the constraint to <c>Public</c> in the change set, so the publish makes it visible
    /// checked.</item>
    /// </list>
    /// The publish fences the parent table as well as the child (<see cref="ChangeSetDeltaApplier"/>), so a
    /// DELETE planned without the constraint cannot remove a parent in the window before the publish. A
    /// child table that other transactions write takes the staged form instead
    /// (<see cref="StageForeignKeyOnSharedTableAsync"/>).
    /// </summary>
    private async Task<bool> AddForeignKeyInOwnerTransactionAsync(
        DatabaseDescriptor database, TransactionSchemaOverlay overlay, AlterConstraintTicket ticket, ForeignKeyInfo info)
    {
        // Opened with the private schema: the form follows the transaction's own view of the table.
        TableDescriptor target = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        if (!overlay.CreatedTable(target.Id))
            return await StageForeignKeyOnSharedTableAsync(database, overlay, target, info).ConfigureAwait(false);

        KvTransaction tx = overlay.Owner;
        string tableName;
        string constraintName;

        await database.DdlGate.EnterAsync(ticket.TableName, context.Logger).ConfigureAwait(false);
        try
        {
            TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
            TransactionalDdlRules.RequireOwnTable(overlay, table, "ALTER TABLE ... ADD FOREIGN KEY");
            tableName = table.Name;

            ForeignKeyAlterPlan plan;

            await database.Schema.AcquireLockAsync().ConfigureAwait(false);
            try
            {
                plan = ForeignKeyDefinitionBuilder.ResolveForAlter(database.Schema, table.Schema, info, options);
                ForeignKeyDefinitionRules.RequireNoCycle(database.Schema, table.Schema, plan.ToSchema(plan.BackingIndexId ?? ""));
            }
            finally
            {
                database.Schema.ReleaseLock();
            }

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
            constraintName = foreignKey.Name;

            await catalogs.ReplicateAddForeignKeyAsync(database, tableName, table.Id, foreignKey, claimedIndexId).ConfigureAwait(false);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        TableDescriptor child = await context.TableOpener.Open(database, tableName).ConfigureAwait(false);
        ForeignKeyStatementChecker checker = await ForeignKeyStatementChecker.ForChildWritesAsync(database, child, context.TableOpener, tx).ConfigureAwait(false);

        QueryTicket rows = new(
            txnState: tx,
            databaseName: database.Name,
            tableName: child.Name,
            index: null,
            projection: null,
            filters: null,
            where: null,
            orderBy: null,
            limit: null,
            offset: null,
            parameters: null
        );

        await foreach (QueryResultRow row in queryExecutor.Query(database, child, rows).ConfigureAwait(false))
            checker.AddChildRow(new Dictionary<string, ColumnValue>(row.Row, StringComparer.OrdinalIgnoreCase));

        await checker.CompleteAsync(tx).ConfigureAwait(false);

        await database.DdlGate.EnterAsync(tableName, context.Logger).ConfigureAwait(false);
        try
        {
            await catalogs.ReplicateElementStateAsync(
                database, tableName, constraintName, SchemaElementState.Public, SchemaElementKind.ForeignKey
            ).ConfigureAwait(false);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        if (context.Logger.IsEnabled(LogLevel.Information))
            context.Logger.LogInformation("Foreign key '{Constraint}' added to table '{Table}' inside a transaction", constraintName, tableName);

        return true;
    }

    /// <summary>
    /// <c>ADD ... FOREIGN KEY</c> inside a transaction with a private schema, on a child table the
    /// transaction did not create: the online rollout of <see cref="AddForeignKeyAsync"/> runs on the shared
    /// tables, owned by the transaction (<see cref="RunOwnedStagedChangeAsync"/>):
    /// <list type="number">
    /// <item>When no index can serve the parent-side probe, build <c>~fk_{name}</c> with the staged index
    /// build, owned by the transaction, and make it <c>Public</c> at once: the constraint rules require a
    /// readable backing index. An index changes no query result, so other transactions may read it before
    /// the commit.</item>
    /// <item>Record the constraint's job with its owner, and add the constraint in <c>WriteOnly</c>. From the
    /// acknowledgement on, every node enforces both sides, for every transaction.</item>
    /// <item>The coordinator checks the child rows (<see cref="ForeignKeyValidationPass"/>) and stops before
    /// <c>Public</c>. An orphan removes the constraint and its index and fails the statement with
    /// <see cref="CamusDBErrorCodes.ForeignKeyViolation"/>.</item>
    /// <item>The step of the constraint to <c>Public</c> joins the change set. The index's job record is
    /// owned too, so a rollback removes the index after the constraint.</item>
    /// </list>
    /// The check reads with no lock, so locks of the transaction on the child rows matter only to the index
    /// build of the first step (<see cref="TransactionalDdlRules.RequireNoRowLocks"/>). The transaction must
    /// not have created, changed or written the parent table
    /// (<see cref="TransactionalDdlRules.RequireStageableParent"/>), because the steps and the check run on
    /// the shared tables, outside the transaction.
    /// </summary>
    private async Task<bool> StageForeignKeyOnSharedTableAsync(
        DatabaseDescriptor database, TransactionSchemaOverlay overlay, TableDescriptor child, ForeignKeyInfo info)
    {
        const string statement = "ALTER TABLE ... ADD FOREIGN KEY";

        TableDescriptor parent = await context.TableOpener.Open(database, info.ReferencedTable).ConfigureAwait(false);
        TransactionalDdlRules.RequireStageableParent(overlay, parent, statement);

        return await RunOwnedStagedChangeAsync(database, overlay, child, statement, async () =>
        {
            await database.DdlGate.EnterAsync(child.Name, context.Logger).ConfigureAwait(false);
            try
            {
                TableDescriptor table = await context.TableOpener.Open(database, child.Name).ConfigureAwait(false);
                string tableName = table.Name;

                ForeignKeyAlterPlan plan;

                await database.Schema.AcquireLockAsync().ConfigureAwait(false);
                try
                {
                    plan = ForeignKeyDefinitionBuilder.ResolveForAlter(database.Schema, table.Schema, info, options);
                    ForeignKeyDefinitionRules.RequireNoCycle(database.Schema, table.Schema, plan.ToSchema(plan.BackingIndexId ?? ""));
                }
                finally
                {
                    database.Schema.ReleaseLock();
                }

                // Only the plan tells whether the statement builds an index, and nothing is staged yet.
                if (plan.OwnedIndexName is not null)
                    TransactionalDdlRules.RequireNoRowLocks(overlay, table, statement);

                await RequireNoOtherJobNamedAsync(database, table.Id, info.Name).ConfigureAwait(false);

                List<string> ownedElements = new(2);
                int steps = 0;
                string backingIndexId;
                string? claimedIndexId = null;

                if (plan.OwnedIndexName is { } ownedIndexName)
                {
                    IndexBuildInfo indexInfo = new(
                        ObjectIdGenerator.Generate().ToString(),
                        ownedIndexName,
                        [.. plan.ChildColumnIds],
                        [.. plan.ChildColumnNames],
                        IndexType.Multi);

                    steps += await StageIndexWithinGateAsync(database, table.Id, tableName, indexInfo, overlay.ChangeSetId).ConfigureAwait(false);
                    ownedElements.Add(ownedIndexName);

                    await catalogs.ReplicateElementStateAsync(
                        database, tableName, ownedIndexName, SchemaElementState.Public, SchemaElementKind.Index).ConfigureAwait(false);
                    steps++;

                    backingIndexId = indexInfo.IndexId;
                    claimedIndexId = backingIndexId;
                }
                else
                {
                    backingIndexId = plan.BackingIndexId!;
                }

                ForeignKeySchema foreignKey = plan.ToSchema(backingIndexId);

                // Recorded before the constraint exists, so the rollback and the resume find it.
                await catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
                {
                    TableName = tableName,
                    TableId = table.Id,
                    ElementName = foreignKey.Name,
                    TargetState = SchemaElementState.Public,
                    ElementKind = SchemaElementKind.ForeignKey,
                    OwnerChangeSetId = overlay.ChangeSetId,
                }).ConfigureAwait(false);
                ownedElements.Add(foreignKey.Name);

                await catalogs.ReplicateAddForeignKeyAsync(database, tableName, table.Id, foreignKey, claimedIndexId).ConfigureAwait(false);
                steps++;

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
                    new SchemaChangeJob(database.Name, tableName, table.Id, foreignKey.Name, SchemaElementState.Public, SchemaElementKind.ForeignKey, overlay.ChangeSetId),
                    gateHeldByCaller: true
                ).ConfigureAwait(false);

                steps += coordinator.StepsProposed;

                if (context.Logger.IsEnabled(LogLevel.Information))
                    context.Logger.LogInformation("Foreign key '{Constraint}' staged on table '{Table}' inside a transaction", foreignKey.Name, tableName);

                return new OwnedStagedElement(table.Id, tableName, steps, ownedElements, [(foreignKey.Name, SchemaElementKind.ForeignKey)]);
            }
            finally
            {
                database.SchemaDdlSemaphore.Release();
            }
        }, locksRows: false).ConfigureAwait(false);
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

        // Inside a transaction with a private schema the table is the transaction's own, and the index
        // is built in the transaction by the local build.
        if (context.IsClusterMode && TransactionSchemaScope.OverlayFor(database) is null)
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
        await database.DdlGate.EnterAsync(ticket.TableName, context.Logger).ConfigureAwait(false);
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
