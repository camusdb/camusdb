
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Transactional;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Executes ALTER TABLE constraint DDL operations: ADD CONSTRAINT CHECK, DROP CONSTRAINT,
/// ALTER COLUMN SET NOT NULL, ALTER COLUMN DROP NOT NULL and ALTER COLUMN SET STORAGE.
///
/// <para><b>ADD CHECK and SET NOT NULL enforce first, then read the existing rows.</b> The read takes
/// no lock, so a write that commits after the read passed its row is not seen. With the constraint
/// enforced before the read, and every write planned without it settled (the write-shape fence), each
/// such write was checked by its own statement. The opposite order lets a short autocommit UPDATE commit
/// a violating value between the read and the start of enforcement. If a row breaks the constraint, the
/// constraint is removed again and the statement fails. See <see cref="RowConstraintValidationPass"/>.
/// </para>
///
/// <para><b>Standalone</b>: the constraint is enforced in memory, the fence waits for the commits in
/// flight, the rows are read, and only then is the schema persisted. A crash before the persist leaves
/// no constraint, which is correct, because none was proven.</para>
///
/// <para><b>Cluster</b>: a coordinator job (<see cref="SchemaElementKind.Check"/> or
/// <see cref="SchemaElementKind.NotNull"/>) is recorded before the constraint replicates, and
/// <see cref="SchemaChangeCoordinator.ValidateRowConstraintAsync"/> waits for every live node, reads the
/// rows and deletes the job. A statement that fails for any reason, not only a violation, removes the
/// constraint again, so a failed ALTER leaves nothing enforced and can simply run again. A new leader
/// that finds the job validates again, so a constraint that is enforced but was never proven cannot
/// stay after a crash.</para>
///
/// <para>Both enter the database's DDL gate for their table, as a foreign key ADD does, so two ALTERs
/// of one database cannot interleave their checks and their replication. In a cluster the read of the
/// rows runs with the gate released and the table held (<see cref="SchemaChangeCoordinator.ValidateRowConstraintAsync"/>),
/// so DDL on other tables proceeds while the rows are read; a standalone node reads under the gate.</para>
///
/// <para><b>Inside a transaction with a private schema</b>, ADD CHECK and SET NOT NULL take one of two
/// forms. On a table the transaction created, the constraint joins the transaction's change set, and the
/// transaction's own rows are read through it and checked; no other transaction can see such a table
/// before the commit. On any other table the cluster form above runs, owned by the transaction
/// (<see cref="StageRowConstraintAsync"/>): the constraint is enforced for every transaction from the
/// statement on, the rows are proven at the statement, the commit makes it permanent, and a rollback
/// removes it. A SET NOT NULL on a column that is already NOT NULL only takes the new name, in the change
/// set, on any table.</para>
///
/// <para><b>DROP CONSTRAINT</b>: resolves the name against <see cref="TableSchema.CheckConstraints"/>
/// AND each column's <see cref="TableColumnSchema.NotNullConstraintName"/>. Immediate — no
/// existing-row scan required, because an early stop of enforcement is always safe.</para>
///
/// <para><b>DROP NOT NULL</b>: unconditional; replicates and clears the NOT NULL flag and constraint name.
/// </para>
///
/// <para>Foreign keys do not come here. <c>SchemaDdlService.AlterConstraintLocalAsync</c> routes
/// <c>ADD ... FOREIGN KEY</c>, and a <c>DROP CONSTRAINT</c> that names only a foreign key, to the staged
/// rollout, which needs the index build and the coordinator.</para>
/// </summary>
internal sealed class TableConstraintAlterer
{
    private readonly ILogger<ICamusDB> logger;

    /// <summary>
    /// Test-only hook, invoked by ADD CHECK and SET NOT NULL after the constraint is enforced and the
    /// earlier writers settled, and before the existing rows are read. A test writes rows at exactly that
    /// point. Null in production; a test clears it after use.
    /// </summary>
    internal Func<Task>? TestInterceptBeforeRowValidation;

    public TableConstraintAlterer(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    public async Task<bool> Alter(
        CatalogsManager catalogs,
        TableOpener tableOpener,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterConstraintTicket ticket,
        bool isClusterMode)
    {
        return ticket.Operation switch
        {
            AlterConstraintOperation.AddCheck => await AddCheck(catalogs, tableOpener, database, table, ticket, isClusterMode).ConfigureAwait(false),
            AlterConstraintOperation.DropConstraint => await DropConstraint(catalogs, database, table, ticket, isClusterMode).ConfigureAwait(false),
            AlterConstraintOperation.SetNotNull => await SetNotNull(catalogs, tableOpener, database, table, ticket, isClusterMode).ConfigureAwait(false),
            AlterConstraintOperation.DropNotNull => await DropNotNull(catalogs, database, table, ticket, isClusterMode).ConfigureAwait(false),
            AlterConstraintOperation.SetStorage => await SetStorage(catalogs, database, table, ticket, isClusterMode).ConfigureAwait(false),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown alter constraint operation '{ticket.Operation}'")
        };
    }

    private async Task<bool> AddCheck(
        CatalogsManager catalogs,
        TableOpener tableOpener,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterConstraintTicket ticket,
        bool isClusterMode)
    {
        // CHECK, named NOT NULL and foreign keys share one name space (ConstraintNameRules). Checked
        // here so a taken name is refused before any work, and again under the gate and the schema lock.
        ConstraintNameRules.RequireUnused(table.Schema, ticket.ConstraintName);

        SQLParser.NodeAst parsedCondition = SQLParser.SQLParserProcessor.ParseCondition(ticket.Expression!);

        if (TransactionSchemaScope.OverlayFor(database) is { } overlay)
        {
            await AddCheckInOwnerTransactionAsync(catalogs, tableOpener, database, overlay, table, ticket, parsedCondition).ConfigureAwait(false);
            return true;
        }

        await database.DdlGate.EnterAsync(ticket.TableName, logger).ConfigureAwait(false);
        try
        {
            if (isClusterMode)
                await AddCheckClusterAsync(catalogs, tableOpener, database, ticket).ConfigureAwait(false);
            else
                await AddCheckStandaloneAsync(catalogs, database, table, ticket, parsedCondition).ConfigureAwait(false);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        if (logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Information))
            logger.LogInformation("Check constraint '{Constraint}' added to table '{Table}'", ticket.ConstraintName, ticket.TableName);
        return true;
    }

    /// <summary>
    /// ADD CHECK inside a transaction with a private schema, on a table the transaction created: the
    /// constraint joins the change set, then the transaction's own rows are read through it and checked.
    /// No fence runs and no job is recorded, because no other transaction can see the table before the
    /// commit. The rows the transaction writes later are checked by their own statements. A violating row
    /// fails the statement, which leaves the transaction to roll back.
    /// </summary>
    private async Task AddCheckInOwnerTransactionAsync(
        CatalogsManager catalogs,
        TableOpener tableOpener,
        DatabaseDescriptor database,
        TransactionSchemaOverlay overlay,
        TableDescriptor table,
        AlterConstraintTicket ticket,
        SQLParser.NodeAst parsedCondition)
    {
        TransactionalDdlRules.RequireOwnTable(overlay, table, "ALTER TABLE ... ADD CONSTRAINT ... CHECK");

        string tableName;

        await database.DdlGate.EnterAsync(ticket.TableName, logger).ConfigureAwait(false);
        try
        {
            TableDescriptor gated = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
            ConstraintNameRules.RequireUnused(gated.Schema, ticket.ConstraintName);

            await catalogs.ReplicateAddCheckConstraintAsync(
                database, gated.Name, ticket.ConstraintName, ticket.Expression!, ticket.ReferencedColumns ?? [])
                .ConfigureAwait(false);

            tableName = gated.Name;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        TableDescriptor constrained = await tableOpener.Open(database, tableName).ConfigureAwait(false);
        await RowConstraintValidationPass.ScanForCheckViolationAsync(
            database, constrained, ticket.ConstraintName, parsedCondition, overlay.Owner).ConfigureAwait(false);
    }

    /// <summary>
    /// ADD CHECK on one node: enforce in memory, fence and wait for the commits in flight, read the
    /// rows, then persist. Every failure before the commit restores the previous list, so the node never
    /// enforces a constraint that is not durable or not proven.
    /// </summary>
    private async Task AddCheckStandaloneAsync(
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterConstraintTicket ticket,
        SQLParser.NodeAst parsedCondition)
    {
        List<CheckConstraintSchema>? previousChecks = null;
        bool mutated = false;
        KvTransaction? tx = null;

        try
        {
            await database.Schema.AcquireLockAsync().ConfigureAwait(false);
            try
            {
                ConstraintNameRules.RequireUnused(table.Schema, ticket.ConstraintName);

                previousChecks = table.Schema.CheckConstraints;

                CheckConstraintSchema check = new()
                {
                    Name = ticket.ConstraintName,
                    Expression = ticket.Expression!,
                    ReferencedColumns = ticket.ReferencedColumns ?? [],
                    ParsedCondition = parsedCondition,
                };

                // A new list, not an Add: a statement on another thread can be iterating the old one.
                table.Schema.CheckConstraints = previousChecks is null ? [check] : [.. previousChecks, check];
                mutated = true;
            }
            finally
            {
                database.Schema.ReleaseLock();
            }

            // Every statement that starts now checks the constraint. The fence refuses the later commit
            // of a transaction that wrote the table before, and waits for one whose commit is in flight.
            await database.FenceWritersAndWaitAsync(table.Schema, database.Kahuna.SchemaAckWaitTimeout).ConfigureAwait(false);

            if (TestInterceptBeforeRowValidation is { } intercept)
                await intercept().ConfigureAwait(false);

            await RowConstraintValidationPass.ScanForCheckViolationAsync(database, table, ticket.ConstraintName, parsedCondition).ConfigureAwait(false);

            tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);

            await catalogs.PersistSchemaTableAsync(database, table.Schema, tx).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
            mutated = false;
        }
        finally
        {
            if (mutated)
                await RevertChecksAsync(database, table, previousChecks).ConfigureAwait(false);

            if (tx is not null)
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// ADD CHECK in a cluster: record the job, replicate the constraint (every node enforces it from its
    /// apply on), then let the coordinator wait for every node, read the rows and delete the job, or
    /// remove the constraint on any failure.
    /// </summary>
    private async Task AddCheckClusterAsync(
        CatalogsManager catalogs,
        TableOpener tableOpener,
        DatabaseDescriptor database,
        AlterConstraintTicket ticket)
    {
        // Opened inside the gate: a drop and re-create of the same name between an open outside and the
        // proposal would put the job on a table that is gone.
        TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
        string tableName = table.Name;
        string constraintName = ticket.ConstraintName;

        ConstraintNameRules.RequireUnused(table.Schema, constraintName);
        await RequireNoOtherJobNamedAsync(catalogs, database, table.Id, tableName, constraintName).ConfigureAwait(false);

        await PersistRowConstraintJobAsync(catalogs, database, table.Id, tableName, constraintName, SchemaElementKind.Check).ConfigureAwait(false);

        try
        {
            await catalogs.ReplicateAddCheckConstraintAsync(
                database, tableName, constraintName, ticket.Expression!, ticket.ReferencedColumns ?? [])
                .ConfigureAwait(false);
        }
        catch
        {
            await TakeBackAfterReplicationFailureAsync(catalogs, database, table.Id, tableName, constraintName, SchemaElementKind.Check).ConfigureAwait(false);
            throw;
        }

        await ValidateThroughCoordinatorAsync(catalogs, tableOpener, database, table.Id, tableName, constraintName, SchemaElementKind.Check).ConfigureAwait(false);
    }

    private async Task<bool> DropConstraint(
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterConstraintTicket ticket,
        bool isClusterMode)
    {
        // Resolve the constraint name: first against CHECK constraints, then against named NOT NULL.
        bool isCheckConstraint = table.Schema.CheckConstraints?.Any(c =>
            string.Equals(c.Name, ticket.ConstraintName, StringComparison.OrdinalIgnoreCase)) == true;

        TableColumnSchema? notNullColumn = null;
        if (!isCheckConstraint && table.Schema.Columns is not null)
        {
            foreach (TableColumnSchema col in table.Schema.Columns)
            {
                if (string.Equals(col.NotNullConstraintName, ticket.ConstraintName, StringComparison.Ordinal))
                {
                    notNullColumn = col;
                    break;
                }
            }
        }

        if (!isCheckConstraint && notNullColumn is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Constraint '{ticket.ConstraintName}' does not exist on table '{table.Name}'");

        if (notNullColumn is not null)
        {
            // Delegate to DropNotNull using the resolved column name.
            AlterConstraintTicket notNullTicket = new(
                databaseName: ticket.DatabaseName,
                tableName: ticket.TableName,
                constraintName: ticket.ConstraintName,
                expression: null,
                referencedColumns: null,
                operation: AlterConstraintOperation.DropNotNull,
                columnName: notNullColumn.Name
            );
            return await DropNotNull(catalogs, database, table, notNullTicket, isClusterMode).ConfigureAwait(false);
        }

        if (isClusterMode)
        {
            await catalogs.ReplicateDropCheckConstraintAsync(
                database, ticket.TableName, ticket.ConstraintName)
                .ConfigureAwait(false);
        }
        else
        {
            KvTransaction tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);
            List<CheckConstraintSchema>? previousChecks = null;
            bool mutated = false;
            try
            {
                await database.Schema.AcquireLockAsync().ConfigureAwait(false);
                try
                {
                    previousChecks = table.Schema.CheckConstraints;
                    if (previousChecks is not null)
                        table.Schema.CheckConstraints = ConstraintDeltaApplier.WithoutCheck(previousChecks, ticket.ConstraintName);
                    mutated = true;
                }
                finally
                {
                    database.Schema.ReleaseLock();
                }

                await catalogs.PersistSchemaTableAsync(database, table.Schema, tx).ConfigureAwait(false);
                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
                mutated = false;
            }
            finally
            {
                if (mutated)
                    await RevertChecksAsync(database, table, previousChecks).ConfigureAwait(false);
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            }
        }

        if (logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Information))
            logger.LogInformation("Constraint '{Constraint}' dropped from table '{Table}'", ticket.ConstraintName, ticket.TableName);
        return true;
    }

    /// <summary>
    /// <c>ALTER TABLE ... ALTER COLUMN ... SET STORAGE</c>. Replaces the column with a copy carrying the
    /// new strategy and persists or replicates the schema; it never reads or rewrites a stored row.
    /// That is deliberate: an implicit table rewrite hidden inside an ALTER would make a metadata change
    /// take time proportional to the table. Existing rows keep their stored form until a write touches
    /// them or <c>ALTER TABLE ... REWRITE STORAGE</c> converts them.
    /// </summary>
    private async Task<bool> SetStorage(
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterConstraintTicket ticket,
        bool isClusterMode)
    {
        string columnName = ticket.ColumnName!;
        ColumnStorageStrategy storage = ticket.Storage!.Value;

        TableColumnSchema? column = table.Schema.Columns?.FirstOrDefault(c =>
            string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
        if (column is null)
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownColumn,
                $"Column '{columnName}' does not exist on table '{table.Name}'");

        // Checked here too, so the single-node path refuses before it opens a transaction.
        ConstraintDeltaApplier.WithStorage(column, storage);

        if (isClusterMode)
        {
            await catalogs.ReplicateSetColumnStorageAsync(database, ticket.TableName, columnName, storage).ConfigureAwait(false);
        }
        else
        {
            KvTransaction tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);
            int revertIdx = -1;
            TableColumnSchema? revertColumn = null;
            try
            {
                await database.Schema.AcquireLockAsync().ConfigureAwait(false);
                try
                {
                    List<TableColumnSchema> columns = table.Schema.Columns!;
                    int idx = columns.FindIndex(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
                    if (idx < 0)
                        throw new CamusDBException(
                            CamusDBErrorCodes.UnknownColumn,
                            $"Column '{columnName}' does not exist on table '{table.Name}'");

                    TableColumnSchema old = columns[idx];
                    revertIdx = idx;
                    revertColumn = old;
                    ConstraintDeltaApplier.ReplaceColumn(table.Schema, idx, ConstraintDeltaApplier.WithStorage(old, storage));
                }
                finally
                {
                    database.Schema.ReleaseLock();
                }

                await catalogs.PersistSchemaTableAsync(database, table.Schema, tx).ConfigureAwait(false);
                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
                revertColumn = null;
            }
            finally
            {
                if (revertColumn is not null)
                    await RevertColumnAsync(database, table, revertIdx, revertColumn).ConfigureAwait(false);
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            }
        }

        if (logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Information))
            logger.LogInformation("Storage of column '{Column}' of table '{Table}' set to {Storage}", columnName, ticket.TableName, storage);
        return true;
    }

    private async Task<bool> SetNotNull(
        CatalogsManager catalogs,
        TableOpener tableOpener,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterConstraintTicket ticket,
        bool isClusterMode)
    {
        string columnName = ticket.ColumnName!;

        // Verify column exists.
        TableColumnSchema? column = table.Schema.Columns?.FirstOrDefault(c =>
            string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
        if (column is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Column '{columnName}' does not exist on table '{table.Name}'");

        // Auto-name: {table}_{col}_not_null. The generated name must not be one that a CHECK or a
        // foreign key already uses (ConstraintNameRules); checked again under the gate and the lock.
        string constraintName = $"{table.Name}_{columnName}_not_null";
        ConstraintNameRules.RequireUnusedForNotNull(table.Schema, constraintName, column.Id);

        if (TransactionSchemaScope.OverlayFor(database) is { } overlay)
        {
            await SetNotNullInOwnerTransactionAsync(catalogs, tableOpener, database, overlay, table, columnName, constraintName).ConfigureAwait(false);
            return true;
        }

        await database.DdlGate.EnterAsync(ticket.TableName, logger).ConfigureAwait(false);
        try
        {
            if (isClusterMode)
                await SetNotNullClusterAsync(catalogs, tableOpener, database, ticket.TableName, columnName, constraintName).ConfigureAwait(false);
            else
                await SetNotNullStandaloneAsync(catalogs, database, table, columnName, constraintName).ConfigureAwait(false);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        if (logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Information))
            logger.LogInformation("NOT NULL set on column '{Column}' of table '{Table}'", columnName, ticket.TableName);
        return true;
    }

    /// <summary>
    /// SET NOT NULL inside a transaction with a private schema, on a table the transaction created: the
    /// flag joins the change set, then the transaction's own rows are read through it. See
    /// <see cref="AddCheckInOwnerTransactionAsync"/> for why no fence or job is needed. A column that is
    /// already NOT NULL only takes the new name, which joins the change set on any table.
    /// </summary>
    private async Task SetNotNullInOwnerTransactionAsync(
        CatalogsManager catalogs,
        TableOpener tableOpener,
        DatabaseDescriptor database,
        TransactionSchemaOverlay overlay,
        TableDescriptor table,
        string columnName,
        string constraintName)
    {
        string tableName;
        bool wasNotNull;

        await database.DdlGate.EnterAsync(table.Name, logger).ConfigureAwait(false);
        try
        {
            TableDescriptor gated = await tableOpener.Open(database, table.Name).ConfigureAwait(false);

            TableColumnSchema column = gated.Schema.Columns?.Find(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase))
                ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Column '{columnName}' does not exist on table '{gated.Name}'");

            ConstraintNameRules.RequireUnusedForNotNull(gated.Schema, constraintName, column.Id);
            wasNotNull = column.NotNull;

            // A rename of an existing NOT NULL reads no row, so it joins the change set on any table. The
            // staged form handles a nullable column of a shared table before this method is reached.
            if (!wasNotNull)
                TransactionalDdlRules.RequireOwnTable(overlay, gated, "ALTER TABLE ... ALTER COLUMN ... SET NOT NULL");

            await catalogs.ReplicateSetColumnNotNullAsync(database, gated.Name, columnName, notNull: true, constraintName).ConfigureAwait(false);

            tableName = gated.Name;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        if (wasNotNull)
            return;

        TableDescriptor constrained = await tableOpener.Open(database, tableName).ConfigureAwait(false);
        await RowConstraintValidationPass.ScanForNullAsync(database, constrained, columnName, overlay.Owner).ConfigureAwait(false);
    }

    /// <summary>
    /// SET NOT NULL on one node: enforce in memory, fence and wait for the commits in flight, read the
    /// rows, then persist. A column that is already NOT NULL only takes the new name: its rows are
    /// already proven, so there is nothing to read. Every failure before the commit restores the column.
    /// </summary>
    private async Task SetNotNullStandaloneAsync(
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        TableDescriptor table,
        string columnName,
        string constraintName)
    {
        int revertIdx = -1;
        TableColumnSchema? revertColumn = null;
        bool wasNotNull = false;
        KvTransaction? tx = null;

        try
        {
            await database.Schema.AcquireLockAsync().ConfigureAwait(false);
            try
            {
                List<TableColumnSchema> columns = table.Schema.Columns!;
                int idx = columns.FindIndex(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
                if (idx < 0)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Column '{columnName}' does not exist on table '{table.Name}'");

                TableColumnSchema old = columns[idx];
                ConstraintNameRules.RequireUnusedForNotNull(table.Schema, constraintName, old.Id);
                revertIdx = idx;
                revertColumn = old;
                wasNotNull = old.NotNull;
                ConstraintDeltaApplier.ReplaceColumn(table.Schema, idx, ConstraintDeltaApplier.WithNotNull(old, notNull: true, constraintName));
            }
            finally
            {
                database.Schema.ReleaseLock();
            }

            if (!wasNotNull)
            {
                // Every statement that starts now refuses a NULL. The fence refuses the later commit of
                // a transaction that wrote the table before, and waits for one whose commit is in flight.
                await database.FenceWritersAndWaitAsync(table.Schema, database.Kahuna.SchemaAckWaitTimeout).ConfigureAwait(false);

                if (TestInterceptBeforeRowValidation is { } intercept)
                    await intercept().ConfigureAwait(false);

                await RowConstraintValidationPass.ScanForNullAsync(database, table, columnName).ConfigureAwait(false);
            }

            tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);

            await catalogs.PersistSchemaTableAsync(database, table.Schema, tx).ConfigureAwait(false);
            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
            revertColumn = null;
        }
        finally
        {
            if (revertColumn is not null)
                await RevertColumnAsync(database, table, revertIdx, revertColumn).ConfigureAwait(false);

            if (tx is not null)
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// SET NOT NULL in a cluster. A column that is already NOT NULL only takes the new name. Otherwise
    /// the job is recorded, the flag replicates (every node refuses a NULL from its apply on), and the
    /// coordinator waits for every node, reads the rows and deletes the job, or makes the column
    /// nullable again on any failure.
    /// </summary>
    private async Task SetNotNullClusterAsync(
        CatalogsManager catalogs,
        TableOpener tableOpener,
        DatabaseDescriptor database,
        string ticketTableName,
        string columnName,
        string constraintName)
    {
        TableDescriptor table = await tableOpener.Open(database, ticketTableName).ConfigureAwait(false);
        string tableName = table.Name;

        TableColumnSchema column = table.Schema.Columns?.Find(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase))
            ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Column '{columnName}' does not exist on table '{tableName}'");

        ConstraintNameRules.RequireUnusedForNotNull(table.Schema, constraintName, column.Id);

        if (column.NotNull)
        {
            await catalogs.ReplicateSetColumnNotNullAsync(database, tableName, columnName, notNull: true, constraintName).ConfigureAwait(false);
            return;
        }

        await RequireNoOtherJobNamedAsync(catalogs, database, table.Id, tableName, constraintName).ConfigureAwait(false);

        await PersistRowConstraintJobAsync(catalogs, database, table.Id, tableName, constraintName, SchemaElementKind.NotNull).ConfigureAwait(false);

        try
        {
            await catalogs.ReplicateSetColumnNotNullAsync(database, tableName, columnName, notNull: true, constraintName).ConfigureAwait(false);
        }
        catch
        {
            await TakeBackAfterReplicationFailureAsync(catalogs, database, table.Id, tableName, constraintName, SchemaElementKind.NotNull).ConfigureAwait(false);
            throw;
        }

        await ValidateThroughCoordinatorAsync(catalogs, tableOpener, database, table.Id, tableName, constraintName, SchemaElementKind.NotNull).ConfigureAwait(false);
    }

    /// <summary>
    /// ADD CHECK, or SET NOT NULL on a nullable column, inside a transaction with a private schema, on a
    /// table other transactions write: the cluster form of the statement, owned by the change set
    /// <paramref name="ownerChangeSetId"/>, on both single-node and cluster databases (both keep the schema
    /// in a schema log). The job is recorded with its owner, the constraint replicates to the shared schema,
    /// and every node enforces it from its apply on, for every transaction. The coordinator then waits for
    /// the earlier writers and reads the rows (<see cref="SchemaChangeCoordinator.ValidateRowConstraintAsync"/>).
    /// A row that breaks the constraint takes it back and fails the statement. After a read that passed the
    /// job record stays: the owner's commit deletes it, and its rollback removes the constraint.
    /// <para>Runs on the shared schema; the caller clears the private one. Returns the table's id and name
    /// and the constraint name, which is the element name of the job record. The schema moves by one
    /// version.</para>
    /// </summary>
    internal async Task<(string TableId, string TableName, string ConstraintName)> StageRowConstraintAsync(
        CatalogsManager catalogs,
        TableOpener tableOpener,
        DatabaseDescriptor database,
        AlterConstraintTicket ticket,
        string ownerChangeSetId)
    {
        bool check = ticket.Operation == AlterConstraintOperation.AddCheck;
        SchemaElementKind kind = check ? SchemaElementKind.Check : SchemaElementKind.NotNull;

        // Parsed before any work, so a bad expression is refused with nothing recorded.
        if (check)
            SQLParser.SQLParserProcessor.ParseCondition(ticket.Expression!);

        await database.DdlGate.EnterAsync(ticket.TableName, logger).ConfigureAwait(false);
        try
        {
            TableDescriptor table = await tableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
            string tableName = table.Name;
            string constraintName;
            string? columnName = null;

            if (check)
            {
                constraintName = ticket.ConstraintName;
                ConstraintNameRules.RequireUnused(table.Schema, constraintName);
            }
            else
            {
                columnName = ticket.ColumnName!;

                TableColumnSchema column = table.Schema.Columns?.Find(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase))
                    ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Column '{columnName}' does not exist on table '{tableName}'");

                if (column.NotNull)
                    throw new CamusDBException(
                        CamusDBErrorCodes.ConcurrentSchemaChange,
                        $"Column '{columnName}' of table '{tableName}' became NOT NULL while the statement started; roll back and retry");

                constraintName = $"{tableName}_{columnName}_not_null";
                ConstraintNameRules.RequireUnusedForNotNull(table.Schema, constraintName, column.Id);
            }

            await RequireNoOtherJobNamedAsync(catalogs, database, table.Id, tableName, constraintName).ConfigureAwait(false);
            await PersistRowConstraintJobAsync(catalogs, database, table.Id, tableName, constraintName, kind, ownerChangeSetId).ConfigureAwait(false);

            try
            {
                if (check)
                    await catalogs.ReplicateAddCheckConstraintAsync(
                        database, tableName, constraintName, ticket.Expression!, ticket.ReferencedColumns ?? []).ConfigureAwait(false);
                else
                    await catalogs.ReplicateSetColumnNotNullAsync(database, tableName, columnName!, notNull: true, constraintName).ConfigureAwait(false);
            }
            catch
            {
                await TakeBackAfterReplicationFailureAsync(catalogs, database, table.Id, tableName, constraintName, kind).ConfigureAwait(false);
                throw;
            }

            await ValidateThroughCoordinatorAsync(catalogs, tableOpener, database, table.Id, tableName, constraintName, kind, ownerChangeSetId).ConfigureAwait(false);

            if (logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Information))
                logger.LogInformation("Constraint '{Constraint}' staged on table '{Table}' inside a transaction", constraintName, tableName);

            return (table.Id, tableName, constraintName);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }
    }

    /// <summary>
    /// Runs <see cref="SchemaChangeCoordinator.ValidateRowConstraintAsync"/> for a constraint this node
    /// just replicated, with the test hook in front of the read. A job of an explicit transaction
    /// (<paramref name="ownerChangeSetId"/>) keeps its record after a read that passed.
    /// </summary>
    private async Task ValidateThroughCoordinatorAsync(
        CatalogsManager catalogs,
        TableOpener tableOpener,
        DatabaseDescriptor database,
        string tableId,
        string tableName,
        string constraintName,
        SchemaElementKind kind,
        string? ownerChangeSetId = null)
    {
        database.Cache?.InvalidateByTableId(database.Id, tableId);

        SchemaChangeCoordinator coordinator = new(catalogs, logger)
        {
            RowConstraintValidationAsync = async (db, t, k, name) =>
            {
                if (TestInterceptBeforeRowValidation is { } intercept)
                    await intercept().ConfigureAwait(false);

                await RowConstraintValidationPass.ValidateAsync(db, tableOpener, t, k, name).ConfigureAwait(false);
            }
        };

        // The caller holds the gate; the coordinator releases it while the rows are read and holds it
        // again on return.
        await coordinator.ValidateRowConstraintAsync(
            database,
            new SchemaChangeJob(database.Name, tableName, tableId, constraintName, SchemaElementState.Public, kind, ownerChangeSetId),
            gateHeldByCaller: true
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Records the job before the constraint replicates, so no crash can leave an enforced constraint
    /// without a job to validate it. A job whose constraint never appeared is removed by the next resume.
    /// </summary>
    private static Task PersistRowConstraintJobAsync(
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        string tableId,
        string tableName,
        string constraintName,
        SchemaElementKind kind,
        string? ownerChangeSetId = null) =>
        catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
        {
            TableName = tableName,
            TableId = tableId,
            ElementName = constraintName,
            TargetState = SchemaElementState.Public,
            ElementKind = kind,
            OwnerChangeSetId = ownerChangeSetId,
        });

    /// <summary>
    /// A job is keyed by table id and element name. A column, index or foreign-key job of the same name
    /// is a change still in progress, and one job would overwrite the other. A CHECK or NOT NULL job of
    /// the same name is left from an earlier attempt whose constraint is gone (its name was found unused),
    /// so it is safe to replace.
    /// </summary>
    private static async Task RequireNoOtherJobNamedAsync(
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        string tableId,
        string tableName,
        string constraintName)
    {
        List<PersistedCoordinatorJob> jobs = await catalogs.LoadCoordinatorJobsAsync(database).ConfigureAwait(false);

        foreach (PersistedCoordinatorJob job in jobs)
        {
            if (job.ElementKind is not (SchemaElementKind.Check or SchemaElementKind.NotNull)
                && string.Equals(job.TableId, tableId, StringComparison.Ordinal)
                && string.Equals(job.ElementName, constraintName, StringComparison.OrdinalIgnoreCase))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Cannot add constraint '{constraintName}' to table '{tableName}': a change to the {job.ElementKind.ToString().ToLowerInvariant()} '{job.ElementName}' is still in progress. Retry when it finishes");
        }
    }

    /// <summary>
    /// The replication of the constraint failed. The entry can still have committed, with only the wait
    /// for the acknowledgements failing; then the constraint is enforced, and the statement reports an
    /// error. Leaving it for the next leader would keep it enforced and shown as valid, maybe for a long
    /// time, and a retry of the same statement would find its name taken. So the constraint is removed if
    /// it landed, and the job is deleted (<see cref="SchemaChangeCoordinator.TakeBackRowConstraintAsync"/>,
    /// which keeps the job only when the removal also fails).
    /// </summary>
    private Task TakeBackAfterReplicationFailureAsync(
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        string tableId,
        string tableName,
        string constraintName,
        SchemaElementKind kind) =>
        new SchemaChangeCoordinator(catalogs, logger).TakeBackRowConstraintAsync(
            database,
            new SchemaChangeJob(database.Name, tableName, tableId, constraintName, SchemaElementState.Public, kind));

    private async Task<bool> DropNotNull(
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterConstraintTicket ticket,
        bool isClusterMode)
    {
        string columnName = ticket.ColumnName!;

        TableColumnSchema? column = table.Schema.Columns?.FirstOrDefault(c =>
            string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
        if (column is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Column '{columnName}' does not exist on table '{table.Name}'");

        PrimaryKeyNotNullRule.RejectDropNotNullOnPrimaryKey(table, columnName);

        if (isClusterMode)
        {
            await catalogs.ReplicateSetColumnNotNullAsync(
                database, ticket.TableName, columnName, notNull: false, constraintName: null)
                .ConfigureAwait(false);
        }
        else
        {
            KvTransaction tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite
            ).ConfigureAwait(false);
            int revertIdx = -1;
            TableColumnSchema? revertColumn = null;
            try
            {
                await database.Schema.AcquireLockAsync().ConfigureAwait(false);
                try
                {
                    List<TableColumnSchema> columns = table.Schema.Columns!;
                    int idx = columns.FindIndex(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
                    TableColumnSchema old = columns[idx];
                    revertIdx = idx;
                    revertColumn = old;
                    ConstraintDeltaApplier.ReplaceColumn(table.Schema, idx, ConstraintDeltaApplier.WithNotNull(old, notNull: false, constraintName: null));
                }
                finally
                {
                    database.Schema.ReleaseLock();
                }

                await catalogs.PersistSchemaTableAsync(database, table.Schema, tx).ConfigureAwait(false);
                await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
                revertColumn = null;
            }
            finally
            {
                if (revertColumn is not null)
                    await RevertColumnAsync(database, table, revertIdx, revertColumn).ConfigureAwait(false);
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            }
        }

        if (logger.IsEnabled(Microsoft.Extensions.Logging.LogLevel.Information))
            logger.LogInformation("NOT NULL dropped from column '{Column}' of table '{Table}'", columnName, ticket.TableName);
        return true;
    }

    /// <summary>
    /// Restores <see cref="TableSchema.CheckConstraints"/> to a pre-mutation snapshot after a failed
    /// standalone persist/commit, so the in-memory schema does not stay ahead of what is durable.
    /// </summary>
    private static async Task RevertChecksAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        List<CheckConstraintSchema>? previousChecks)
    {
        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            table.Schema.CheckConstraints = previousChecks;
        }
        finally
        {
            database.Schema.ReleaseLock();
        }
    }

    /// <summary>
    /// Restores a single column to its pre-mutation value after a failed standalone persist/commit,
    /// undoing an in-memory SET/DROP NOT NULL that never became durable.
    /// </summary>
    private static async Task RevertColumnAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        int index,
        TableColumnSchema previousColumn)
    {
        await database.Schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            if (table.Schema.Columns is { } columns && index >= 0 && index < columns.Count)
                ConstraintDeltaApplier.ReplaceColumn(table.Schema, index, previousColumn);
        }
        finally
        {
            database.Schema.ReleaseLock();
        }
    }
}
