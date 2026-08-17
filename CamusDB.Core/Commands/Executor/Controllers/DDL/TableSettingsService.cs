
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Applies <c>ALTER TABLE … SET (…)</c> and <c>ALTER TABLE … RESET (…)</c> table storage parameters.
///
/// <para><b>These are version-neutral by design.</b> The settings ride the table blob and do not bump
/// <see cref="TableSchema.Version"/>, because they change no column layout and so cannot invalidate a
/// row encoded under an older version. They still replicate through the schema log, so SET and RESET
/// are one delta with opposite directions — they must not diverge in ordering, locking, or cache
/// invalidation, which is why both live here rather than beside their respective callers.</para>
///
/// <para><b>Validation runs against the merged result, inside the semaphore.</b> Setting only a batch
/// size on a table that already has an expiration column must still be checked against that column,
/// so the incoming fragment alone is never enough. And opening the table before taking
/// <c>SchemaDdlSemaphore</c> would be a check-then-act: a concurrent RENAME or DROP COLUMN could
/// retire the column between the check and the write, leaving a TTL configuration pointing at nothing
/// — a dangling setting that fails only later, in a background sweep.</para>
/// </summary>
internal sealed class TableSettingsService
{
    private readonly ExecutorContext context;

    /// <summary>Configuration for this engine; injected, never ambient. See <see cref="ApplyOptions"/>.</summary>
    private CamusDBOptions options;

    private readonly CatalogsManager catalogs;

    internal TableSettingsService(ExecutorContext context, CamusDBOptions options, CatalogsManager catalogs)
    {
        this.context = context;
        this.options = options;
        this.catalogs = catalogs;
    }

    /// <summary>
    /// Swaps in a newly published configuration snapshot. Each statement pins the field once, so an
    /// in-flight statement keeps the snapshot it started with.
    /// </summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Applies <c>ALTER TABLE t SET (key = value)</c> table storage parameters version-neutrally (rides
    /// the table blob, no <see cref="TableSchema.Version"/> bump). Replicates in cluster mode (schema
    /// leader only); applies directly in standalone mode. Public so a ticket caller can invoke it
    /// without the SQL path.
    /// </summary>
    internal async Task<ExecuteDDLSQLResult> AlterTableSettings(AlterTableSettingsTicket ticket)
    {
        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();
        return await AlterTableSettings(database, ticket).ConfigureAwait(false);
    }

    internal async Task<ExecuteDDLSQLResult> AlterTableSettings(DatabaseDescriptor database, AlterTableSettingsTicket ticket)
    {
        // Canonical validation for every entry point (SQL and direct ticket): unknown key, malformed
        // value, empty set, or duplicate key are rejected here, and keys are lowercased.
        Dictionary<string, string> settings = CamusDB.Core.Catalogs.Models.TableSettings.Canonicalize([.. ticket.Settings]);

        string tableId;

        // Serialize with all other schema changes for this database, exactly like the established DDL
        // paths: the version capture, proposal (cluster), and blob rewrite must not race another DDL.
        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            // Open the table and validate INSIDE the semaphore. Validating first would be a
            // check-then-act against every other DDL: a concurrent RENAME or DROP COLUMN could retire
            // the column between the check and the write, leaving ttl_expiration_expression pointing at
            // nothing — a dangling configuration that fails only in a background sweep.
            TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);

            // Validate the MERGED result, not the incoming keys alone. Setting only a batch size on a
            // table that already has an expiration column must still be checked against that column, and
            // the bounds below depend on the table's current index count either way.
            Dictionary<string, string> merged = MergeSettings(table.Schema.Settings, settings, removed: null);
            ValidateTtlSettingsAgainstSchema(table, merged);

            // The engine-owned marker is derived, never user-supplied: it exists exactly when an
            // expiration column does, so it is written and cleared alongside it rather than tracked
            // separately where the two could disagree.
            ApplyDerivedTtlMarker(settings, merged);

            // Settings DDL replicates through the schema log, which only the schema leader may propose.
            // There is no HTTP forwarder for it yet, so a non-leader rejects with a clear error and the
            // client retargets the leader (the in-process path issues it on the leader). A standalone
            // node is the only member of its own schema group, so the check is a cluster concern only.
            if (context.IsClusterMode &&
                !await database.Kahuna.AmISchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false))
            {
                string leader = await database.Kahuna.WaitForSchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false);
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"ALTER TABLE SET must be executed by schema leader '{leader}' for database '{database.Name}'");
            }

            await catalogs.ReplicateSetTableSettingsAsync(database, ticket.TableName, settings).ConfigureAwait(false);

            tableId = table.Id;
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        database.Cache?.InvalidateByTableId(database.Id, tableId);
        return new ExecuteDDLSQLResult(database, true);
    }

    /// <summary>
    /// Applies <c>ALTER TABLE t RESET (key, ...)</c>, removing table storage parameters so each falls
    /// back to its engine default. Version-neutral and replicated exactly like the SET path — the two
    /// are one schema delta with opposite directions, so they must not diverge in ordering, locking, or
    /// cache invalidation. Public so a ticket caller can invoke it without the SQL path.
    /// </summary>
    internal async Task<ExecuteDDLSQLResult> AlterTableResetSettings(AlterTableResetSettingsTicket ticket)
    {
        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();
        return await AlterTableResetSettings(database, ticket).ConfigureAwait(false);
    }

    internal async Task<ExecuteDDLSQLResult> AlterTableResetSettings(DatabaseDescriptor database, AlterTableResetSettingsTicket ticket)
    {
        HashSet<string> keys = CamusDB.Core.Catalogs.Models.TableSettings.CanonicalizeResetKeys([.. ticket.Keys]);

        string tableId;

        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            // Opened inside the semaphore for the same reason as the SET path: the table must not change
            // between the decision and the write.
            TableDescriptor table = await context.TableOpener.Open(database, ticket.TableName).ConfigureAwait(false);
            tableId = table.Id;

            // Clearing the expiration column clears the derived marker with it, so the two can never
            // disagree about whether TTL is configured.
            if (keys.Contains(CamusDB.Core.Catalogs.Models.TableSettings.TtlExpirationExpressionKey))
                keys.Add(CamusDB.Core.Catalogs.Models.TableSettings.TtlKey);

            if (context.IsClusterMode &&
                !await database.Kahuna.AmISchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false))
            {
                string leader = await database.Kahuna.WaitForSchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false);
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"ALTER TABLE RESET must be executed by schema leader '{leader}' for database '{database.Name}'");
            }

            await catalogs.ReplicateSetTableSettingsAsync(
                database, ticket.TableName, new Dictionary<string, string>(StringComparer.Ordinal), keys).ConfigureAwait(false);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }

        database.Cache?.InvalidateByTableId(database.Id, tableId);
        return new ExecuteDDLSQLResult(database, true);
    }

    /// <summary>
    /// Rejects a delete batch size this table cannot actually execute.
    ///
    /// <para>Each expired row costs one mutation for the row plus one for every index entry it carries,
    /// so a table with several indexes multiplies the batch several-fold against
    /// <see cref="CamusDBOptions.MaxMutationsPerTransaction"/>. Checked here, at configuration time,
    /// because the alternative is a sweep whose every delete transaction aborts — visible only as a
    /// table that mysteriously stops expiring, with the cause buried in a background log.</para>
    ///
    /// <para>Deliberately conservative: it uses the table's current writable index count, and a later
    /// <c>CREATE INDEX</c> can still push a previously-valid batch over the line. Sizing to a worst case
    /// nobody has yet configured would reject reasonable settings today to guard against a schema that
    /// may never exist.</para>
    /// </summary>
    private void ValidateTtlBatchBudget(TableDescriptor table, IReadOnlyDictionary<string, string> settings)
    {
        if (!settings.TryGetValue(CamusDB.Core.Catalogs.Models.TableSettings.TtlDeleteBatchSizeKey, out string? raw) ||
            !int.TryParse(raw, System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out int batchSize))
            return;

        int indexCount = 0;
        foreach (KeyValuePair<string, TableIndexSchema> kv in table.Indexes)
        {
            if (SchemaElementStateRules.IsWritableIndex(table.Schema, kv.Value))
                indexCount++;
        }

        long mutationsPerRow = 1L + indexCount;
        long required = (long)batchSize * mutationsPerRow;

        if (required > options.MaxMutationsPerTransaction)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Table setting 'ttl_delete_batch_size' of {batchSize} needs {required} mutations per transaction " +
                $"({mutationsPerRow} per row across {indexCount} index(es)), exceeding the limit of " +
                $"{options.MaxMutationsPerTransaction}; lower it to at most " +
                $"{options.MaxMutationsPerTransaction / mutationsPerRow}");
    }

    /// <summary>
    /// Produces the settings bag a table would have after applying a SET and/or RESET, so validation can
    /// run against the end state rather than the incoming fragment.
    /// </summary>
    private static Dictionary<string, string> MergeSettings(
        IReadOnlyDictionary<string, string>? current,
        IReadOnlyDictionary<string, string>? added,
        IReadOnlyCollection<string>? removed)
    {
        Dictionary<string, string> merged = current is null
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            : new Dictionary<string, string>(current, StringComparer.Ordinal);

        if (added is not null)
        {
            foreach (KeyValuePair<string, string> kv in added)
                merged[kv.Key] = kv.Value;
        }

        if (removed is not null)
        {
            foreach (string key in removed)
                merged.Remove(key);
        }

        return merged;
    }

    /// <summary>
    /// Adds the derived <c>ttl</c> marker to a pending SET when the merged result configures an
    /// expiration column. Mirrors CockroachDB's engine-set <c>ttl</c> parameter: it records that TTL is
    /// active without the user being able to assert it independently, which is what keeps the marker
    /// from ever contradicting the configuration it describes.
    /// </summary>
    private static void ApplyDerivedTtlMarker(
        Dictionary<string, string> pending, IReadOnlyDictionary<string, string> merged)
    {
        if (merged.ContainsKey(CamusDB.Core.Catalogs.Models.TableSettings.TtlExpirationExpressionKey))
            pending[CamusDB.Core.Catalogs.Models.TableSettings.TtlKey] = "true";
    }

    /// <summary>
    /// Rejects a TTL configuration that cannot work against this table's schema. Split from
    /// <c>TableSettings.Canonicalize</c> because it needs the live table, which that context-free
    /// validator does not have.
    ///
    /// <para>The expiry column must exist and hold an instant (<c>DateTime</c>/<c>Date</c>) or an epoch
    /// in milliseconds (<c>Integer64</c>). It may not be part of the primary key: the sweep ranges over
    /// the primary keyspace to divide work, and expiring rows by the value it partitions on has no use
    /// case and surprising interactions.</para>
    /// </summary>
    private void ValidateTtlSettingsAgainstSchema(TableDescriptor table, IReadOnlyDictionary<string, string> settings)
    {
        ValidateTtlBatchBudget(table, settings);

        if (!settings.TryGetValue(CamusDB.Core.Catalogs.Models.TableSettings.TtlExpirationExpressionKey, out string? columnName))
            return;

        TableColumnSchema? column = null;
        foreach (TableColumnSchema candidate in table.Schema.Columns ?? [])
        {
            if (string.Equals(candidate.Name, columnName, StringComparison.OrdinalIgnoreCase))
            {
                column = candidate;
                break;
            }
        }

        if (column is null)
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownColumn,
                $"Table setting 'ttl_expiration_expression' names column '{columnName}', which does not exist in table '{table.Name}'");

        if (column.Type is not (ColumnType.DateTime or ColumnType.Date or ColumnType.Integer64))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Table setting 'ttl_expiration_expression' requires a DateTime, Date or Integer64 column; " +
                $"column '{column.Name}' is {column.Type}");

        if (table.Indexes.TryGetValue(CamusDBConstants.PrimaryKeyInternalName, out TableIndexSchema? primaryKey) &&
            primaryKey.Columns is not null)
        {
            foreach (string pkColumn in primaryKey.Columns)
            {
                if (string.Equals(pkColumn, column.Name, StringComparison.OrdinalIgnoreCase))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Table setting 'ttl_expiration_expression' cannot name primary key column '{column.Name}'");
            }
        }
    }
}
