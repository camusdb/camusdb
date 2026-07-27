
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Executes ALTER TABLE constraint DDL operations: ADD CONSTRAINT CHECK, DROP CONSTRAINT,
/// ALTER COLUMN SET NOT NULL, and ALTER COLUMN DROP NOT NULL.
///
/// <b>ADD CHECK</b>: validates the expression against all existing rows before committing
/// so the constraint is true of the table at the moment it is added. Replicates via
/// <see cref="CatalogsManager.ReplicateAddCheckConstraintAsync"/>.
///
/// <b>DROP CONSTRAINT</b>: resolves the name against <see cref="TableSchema.CheckConstraints"/>
/// AND each column's <see cref="TableColumnSchema.NotNullConstraintName"/>. Immediate — no
/// existing-row scan required.
///
/// <b>SET NOT NULL</b>: scans all existing rows; rejects if any row has NULL in the target column.
/// Assigns an auto-name <c>{table}_{col}_not_null</c> for the constraint. Replicates via
/// <see cref="CatalogsManager.ReplicateSetColumnNotNullAsync"/>.
///
/// <b>DROP NOT NULL</b>: unconditional; replicates and clears the NOT NULL flag and constraint name.
/// </summary>
internal sealed class TableConstraintAlterer
{
    private readonly ILogger<ICamusDB> logger;

    public TableConstraintAlterer(ILogger<ICamusDB> logger)
    {
        this.logger = logger;
    }

    public async Task<bool> Alter(
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterConstraintTicket ticket,
        bool isClusterMode)
    {
        return ticket.Operation switch
        {
            AlterConstraintOperation.AddCheck => await AddCheck(catalogs, database, table, ticket, isClusterMode).ConfigureAwait(false),
            AlterConstraintOperation.DropConstraint => await DropConstraint(catalogs, database, table, ticket, isClusterMode).ConfigureAwait(false),
            AlterConstraintOperation.SetNotNull => await SetNotNull(catalogs, database, table, ticket, isClusterMode).ConfigureAwait(false),
            AlterConstraintOperation.DropNotNull => await DropNotNull(catalogs, database, table, ticket, isClusterMode).ConfigureAwait(false),
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown alter constraint operation '{ticket.Operation}'")
        };
    }

    private async Task<bool> AddCheck(
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        TableDescriptor table,
        AlterConstraintTicket ticket,
        bool isClusterMode)
    {
        // Duplicate constraint name check.
        if (table.Schema.CheckConstraints?.Any(c =>
                string.Equals(c.Name, ticket.ConstraintName, StringComparison.OrdinalIgnoreCase)) == true)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Constraint '{ticket.ConstraintName}' already exists on table '{table.Name}'");

        // Parse the condition once for the existence scan.
        var parsedCondition = SQLParser.SQLParserProcessor.ParseCondition(ticket.Expression!);

        // Validate all existing rows against the new constraint before committing it.
        // Any failing row causes the ALTER to be rejected (Postgres semantics; NOT VALID is deferred).
        await ScanAndValidateExistingRowsAsync(database, table, ticket.ConstraintName, parsedCondition).ConfigureAwait(false);

        if (isClusterMode)
        {
            await catalogs.ReplicateAddCheckConstraintAsync(
                database, ticket.TableName, ticket.ConstraintName, ticket.Expression!, ticket.ReferencedColumns ?? [])
                .ConfigureAwait(false);
        }
        else
        {
            // Standalone: apply the constraint directly inside a DDL transaction.
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
                    // Snapshot for revert: persist serializes the in-memory schema, so the mutation
                    // must precede persist. If persist/commit then fails we must undo it, or the node
                    // enforces a constraint that never became durable.
                    previousChecks = table.Schema.CheckConstraints is null ? null : [.. table.Schema.CheckConstraints];
                    CheckConstraintSchema check = new()
                    {
                        Name = ticket.ConstraintName,
                        Expression = ticket.Expression!,
                        ReferencedColumns = ticket.ReferencedColumns ?? [],
                        ParsedCondition = parsedCondition,
                    };
                    table.Schema.CheckConstraints ??= [];
                    table.Schema.CheckConstraints.RemoveAll(c => string.Equals(c.Name, ticket.ConstraintName, StringComparison.OrdinalIgnoreCase));
                    table.Schema.CheckConstraints.Add(check);
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
            logger.LogInformation("Check constraint '{Constraint}' added to table '{Table}'", ticket.ConstraintName, ticket.TableName);
        return true;
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
                    previousChecks = table.Schema.CheckConstraints is null ? null : [.. table.Schema.CheckConstraints];
                    table.Schema.CheckConstraints?.RemoveAll(c => string.Equals(c.Name, ticket.ConstraintName, StringComparison.OrdinalIgnoreCase));
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

    private async Task<bool> SetNotNull(
        CatalogsManager catalogs,
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

        // Scan existing rows — reject if any row has NULL in this column.
        await ScanAndValidateNotNullAsync(database, table, columnName).ConfigureAwait(false);

        // Auto-name: {table}_{col}_not_null
        string constraintName = $"{table.Name}_{columnName}_not_null";

        if (isClusterMode)
        {
            await catalogs.ReplicateSetColumnNotNullAsync(
                database, ticket.TableName, columnName, notNull: true, constraintName)
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
                    columns[idx] = new TableColumnSchema(
                        id: old.Id,
                        name: old.Name,
                        type: old.Type,
                        notNull: true,
                        defaultValue: old.DefaultValue,
                        state: old.State,
                        maxLength: old.MaxLength,
                        arrayElementType: old.ArrayElementType,
                        defaultFunction: old.DefaultFunction,
                        notNullConstraintName: constraintName,
                        comment: old.Comment
                    );
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
            logger.LogInformation("NOT NULL set on column '{Column}' of table '{Table}'", columnName, ticket.TableName);
        return true;
    }

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
                    columns[idx] = new TableColumnSchema(
                        id: old.Id,
                        name: old.Name,
                        type: old.Type,
                        notNull: false,
                        defaultValue: old.DefaultValue,
                        state: old.State,
                        maxLength: old.MaxLength,
                        arrayElementType: old.ArrayElementType,
                        defaultFunction: old.DefaultFunction,
                        notNullConstraintName: null,
                        comment: old.Comment
                    );
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
                columns[index] = previousColumn;
        }
        finally
        {
            database.Schema.ReleaseLock();
        }
    }

    /// <summary>
    /// Scans all existing rows and verifies that no row has NULL in <paramref name="columnName"/>.
    /// Throws <see cref="CamusDBErrorCodes.InvalidInput"/> if any row would violate the new
    /// NOT NULL constraint, aborting the ALTER before any schema change is committed.
    /// </summary>
    private static async Task ScanAndValidateNotNullAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        string columnName)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly
        ).ConfigureAwait(false);
        try
        {
            await foreach ((Util.ObjectIds.ObjectIdValue rowId, ReadOnlyMemory<byte> data)
                in table.Store.ScanRows(tx, afterRowId: null, trackReadSet: true).ConfigureAwait(false))
            {
                Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                    table.Schema,
                    tx.TransactionId,
                    rowId,
                    data,
                    visibilitySchemaVersion: table.Schema.Version
                ).ConfigureAwait(false);

                if (!row.TryGetValue(columnName, out ColumnValue? cv) || cv is null || cv.Type == ColumnType.Null)
                    throw new CamusDBException(
                        CamusDBErrorCodes.NotNullViolation,
                        $"column \"{columnName}\" of table \"{table.Name}\" contains null values");
            }
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Scans all existing rows and evaluates the new CHECK constraint against each one.
    /// Throws <see cref="CamusDBErrorCodes.CheckConstraintViolation"/> if any row fails,
    /// aborting the ALTER before any schema change is committed.
    /// </summary>
    private static async Task ScanAndValidateExistingRowsAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        string constraintName,
        SQLParser.NodeAst parsedCondition)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly
        ).ConfigureAwait(false);
        try
        {
            await foreach ((Util.ObjectIds.ObjectIdValue rowId, ReadOnlyMemory<byte> data)
                in table.Store.ScanRows(tx, afterRowId: null, trackReadSet: true).ConfigureAwait(false))
            {
                Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                    table.Schema,
                    tx.TransactionId,
                    rowId,
                    data,
                    visibilitySchemaVersion: table.Schema.Version
                ).ConfigureAwait(false);

                bool? result = CheckEvaluator.Evaluate(parsedCondition, row);
                if (result == false)
                    throw new CamusDBException(
                        CamusDBErrorCodes.CheckConstraintViolation,
                        $"existing row violates check constraint \"{constraintName}\"");
            }
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }
}
