
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Serializer;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Replication;

/// <summary>
/// Builds the <see cref="SchemaChangeLogEntry"/> for every DDL operation the engine can propose.
/// Gathering them here makes the set of proposable deltas readable in one place; they used to be
/// interleaved with the code that replicates them.
///
/// <para><b>Everything an entry needs is resolved here and frozen into the payload.</b> A follower
/// applying the entry — including one replaying it from the log long afterwards — must reach the
/// same result without reading anything else. Anything left to be looked up at apply time is a
/// value two nodes can disagree about.</para>
///
/// <para><b>The table id arrives from the caller and is carried verbatim.</b> The proposer allocates
/// it from the persistent sequence before building the entry, so every node applies the same id. A
/// follower must never generate one: two nodes generating independently would each be internally
/// consistent and mutually wrong.</para>
///
/// <para><b>A CREATE TABLE folds its inline indexes and CHECK constraints into the one delta.</b>
/// Creating a table is therefore exactly one schema version, not one per constraint — which matters
/// because each version costs a replication round-trip and an ack gate.</para>
///
/// <para>This class is pure: it builds an entry and returns it. It replicates nothing and persists
/// nothing.</para>
/// </summary>
internal static class SchemaChangeEntryFactory
{
    internal static SchemaChangeLogEntry CreateTableEntry(DatabaseDescriptor database, CreateTableTicket ticket, KvTransaction tx, string tableId)
    {
        SchemaColumnPayload[] columns = [.. ticket.Columns.Select(column =>
        {
            SchemaColumnPayload payload = SchemaColumnPayload.FromColumnInfo(column);
            payload.Id = ObjectIdGenerator.Generate().ToString();
            return payload;
        })];

        // Fold inline PRIMARY KEY / UNIQUE / INDEX constraints into this single CreateTable delta so
        // creating a table is exactly one schema version. The table is empty, so each index is born
        // at Public with nothing to backfill. Index ids and column ids are generated here and carried
        // in the payload so every node applies identical definitions.
        TableIndexSchema[]? indexes = BuildInlineIndexes(ticket, columns, database.Options);

        // Fold CHECK constraints declared in the CREATE TABLE into the same single delta so the
        // table is created with its constraints already in place.
        CheckConstraintSchema[]? checkConstraints = BuildInlineCheckConstraints(ticket);

        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = SchemaOp.CreateTable,
            Payload = Serializator.Serialize(new SchemaCreateTablePayload
            {
                TableId = tableId,
                TableName = ticket.TableName,
                Columns = columns,
                Indexes = indexes,
                CheckConstraints = checkConstraints,
                Comment = ticket.Comment,
                Kind = ticket.Kind,
                ViewDefinition = ticket.ViewDefinition,
                // Never true at creation: even WITH DATA creates the relation empty and then refreshes
                // it, so a refresh that fails leaves a materialized view that admits it holds nothing
                // rather than one that claims data it never received.
                IsPopulated = false,
                Settings = ticket.Settings is null
                    ? null
                    : new Dictionary<string, string>(ticket.Settings, StringComparer.Ordinal)
            })
        };
    }

    internal static SchemaChangeLogEntry AlterTableEntry(DatabaseDescriptor database, AlterColumnTicket ticket, KvTransaction tx)
    {
        if (ticket.Operation == AlterTableOperation.RenameColumn)
        {
            return new()
            {
                Ts = tx.TransactionId,
                Database = database.Id,
                FromVersion = database.Schema.SchemaVersion,
                ToVersion = database.Schema.SchemaVersion + 1,
                Op = SchemaOp.RenameColumn,
                Payload = Serializator.Serialize(new SchemaRenamePayload
                {
                    TableName = ticket.TableName,
                    Kind = SchemaRenameKind.Column,
                    ElementName = ticket.Column.Name,
                    NewName = ticket.NewName ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "NewName is required for RenameColumn")
                })
            };
        }

        SchemaOp op = ticket.Operation switch
        {
            AlterTableOperation.AddColumn => SchemaOp.AddColumn,
            AlterTableOperation.DropColumn => SchemaOp.DropColumn,
            _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown alter table operation '{ticket.Operation}'")
        };

        SchemaColumnPayload column = SchemaColumnPayload.FromColumnInfo(ticket.Column);
        if (op == SchemaOp.AddColumn)
            column.Id = ObjectIdGenerator.Generate().ToString();

        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = op,
            Payload = Serializator.Serialize(new SchemaAlterColumnPayload
            {
                TableName = ticket.TableName,
                Column = column
            })
        };
    }

    internal static SchemaChangeLogEntry DropTableEntry(DatabaseDescriptor database, string tableName, KvTransaction tx, bool deferred)
    {
        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = SchemaOp.DropTable,
            Payload = Serializator.Serialize(new SchemaDropTablePayload { TableName = tableName, Deferred = deferred })
        };
    }

    internal static SchemaChangeLogEntry RenameTableEntry(
        DatabaseDescriptor database,
        RenameTableTicket ticket,
        KvTransaction tx,
        Dictionary<string, ViewDefinition>? dependentViews)
    {
        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = SchemaOp.RenameTable,
            Payload = Serializator.Serialize(new SchemaRenamePayload
            {
                TableName = ticket.TableName,
                Kind = SchemaRenameKind.Table,
                NewName = ticket.NewName,
                DependentViewDefinitions = dependentViews
            })
        };
    }

    internal static SchemaChangeLogEntry RelinkTableEntry(DatabaseDescriptor database, OrphanTableRecord orphan, string newName, KvTransaction tx)
    {
        TableSchema src = orphan.Schema;

        SchemaColumnPayload[] columns = [.. (src.Columns ?? []).Select(c => new SchemaColumnPayload
        {
            Id = c.Id,
            Name = c.Name,
            Type = c.Type,
            NotNull = c.NotNull,
            DefaultValue = c.DefaultValue,
            DefaultFunction = c.DefaultFunction,
            State = c.State,
            MaxLength = c.MaxLength,
            ArrayElementType = c.ArrayElementType,
            NotNullConstraintName = c.NotNullConstraintName,
            Comment = c.Comment,
        })];

        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = SchemaOp.RelinkTable,
            Payload = Serializator.Serialize(new SchemaRelinkTablePayload
            {
                // Preserve the original id so the reattached table's store reads the retained rows/indexes.
                TableId = orphan.TableId,
                TableName = newName,
                // Preserve the schema version so rows keep decoding under the version they were written.
                Version = src.Version,
                Columns = columns,
                Indexes = src.Indexes is { Count: > 0 } ? [.. src.Indexes] : null,
                CheckConstraints = src.CheckConstraints is { Count: > 0 } ? [.. src.CheckConstraints] : null,
                // Preserve table settings (e.g. the auto-analyze opt-out) across deferred drop + relink.
                Settings = src.Settings is { Count: > 0 } ? new Dictionary<string, string>(src.Settings, StringComparer.Ordinal) : null,
                // Preserve the table comment too, so a relinked table comes back documented.
                Comment = src.Comment,
                // A materialized view must come back as one. Without these it would relink as an
                // ordinary table: writable, no longer refreshable, and — because a refreshed
                // materialized view's rows live under a key-space that is not its id — pointed at an
                // empty key-space, so it would also read as empty while its rows sat untouched.
                StorageId = src.StorageId,
                Kind = src.Kind,
                ViewDefinition = src.ViewDefinition,
                IsPopulated = src.IsPopulated,
                RefreshedAt = src.RefreshedAt,
            })
        };
    }

    internal static SchemaChangeLogEntry AddIndexEntry(
        DatabaseDescriptor database,
        AlterIndexTicket ticket,
        TableDescriptor table,
        KvTransaction tx
    )
    {
        // The completed index lives in table.Schema.Indexes (written by TableIndexAdder).
        TableIndexSchema? indexSchema = table.Schema.Indexes?.FirstOrDefault(ix => string.Equals(ix.Name, ticket.IndexName, StringComparison.OrdinalIgnoreCase))
            ?? throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Index '{ticket.IndexName}' not found in table schema after local apply — cannot build replication entry"
            );

        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = SchemaOp.AddIndex,
            Payload = Serializator.Serialize(new SchemaIndexPayload
            {
                TableName = ticket.TableName,
                IndexName = ticket.IndexName,
                Index = indexSchema
            })
        };
    }

    internal static SchemaChangeLogEntry DropIndexEntry(
        DatabaseDescriptor database,
        AlterIndexTicket ticket,
        KvTransaction tx
    )
    {
        return new()
        {
            Ts = tx.TransactionId,
            Database = database.Id,
            FromVersion = database.Schema.SchemaVersion,
            ToVersion = database.Schema.SchemaVersion + 1,
            Op = SchemaOp.DropIndex,
            Payload = Serializator.Serialize(new SchemaIndexPayload
            {
                TableName = ticket.TableName,
                IndexName = ticket.IndexName
            })
        };
    }

    /// <summary>
    /// Translates a CREATE TABLE ticket's inline constraints into fully-resolved index definitions
    /// (Public state, generated index id, column ids resolved against <paramref name="columns"/>).
    /// Mirrors the validation the standalone AddIndex path performs (column existence, duplicate
    /// index name) since those constraints no longer flow through it.
    /// </summary>
    internal static TableIndexSchema[]? BuildInlineIndexes(CreateTableTicket ticket, SchemaColumnPayload[] columns, CamusDBOptions options)
    {
        if (ticket.Constraints.Length == 0)
            return null;

        Dictionary<string, string> columnIdByName = new(columns.Length, StringComparer.Ordinal);
        foreach (SchemaColumnPayload column in columns)
            columnIdByName[column.Name] = column.Id!;

        List<TableIndexSchema> indexes = new(ticket.Constraints.Length);
        HashSet<string> seenNames = new(StringComparer.Ordinal);

        foreach (ConstraintInfo constraint in ticket.Constraints)
        {
            if (!seenNames.Add(constraint.Name))
            {
                string msg = constraint.Type == ConstraintType.PrimaryKey
                    ? $"Primary key already exists on table '{ticket.TableName}'"
                    : $"Index '{constraint.Name}' already exists on table '{ticket.TableName}'";
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, msg);
            }

            IndexType indexType = constraint.Type switch
            {
                ConstraintType.PrimaryKey => IndexType.Unique,
                ConstraintType.IndexUnique => IndexType.Unique,
                ConstraintType.IndexMulti => IndexType.Multi,
                _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Unknown constraint: " + constraint.Type)
            };

            // Combined key+include column ceiling (mirrors the standalone/cluster add path). A covering
            // index duplicates every included value into each entry, so its column count is bounded.
            int maxIndexColumns = options.MaxIndexColumns;
            if (maxIndexColumns > 0)
            {
                int totalColumns = constraint.Columns.Length + constraint.IncludeColumns.Length;
                if (totalColumns > maxIndexColumns)
                    throw new CamusDBException(
                        CamusDBErrorCodes.SchemaLimitExceeded,
                        $"Index '{constraint.Name}' spans {totalColumns} columns ({constraint.Columns.Length} key + {constraint.IncludeColumns.Length} INCLUDE), exceeding the maximum of {maxIndexColumns}");
            }

            IndexColumnOrder.RejectDescendingOnUnsupportedType(
                constraint.Columns,
                constraint.Name,
                name =>
                {
                    foreach (SchemaColumnPayload column in columns)
                        if (string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase))
                            return column.Type;
                    return null;
                });

            string[] columnIds = new string[constraint.Columns.Length];
            for (int i = 0; i < constraint.Columns.Length; i++)
            {
                string columnName = constraint.Columns[i].Name;
                if (!columnIdByName.TryGetValue(columnName, out string? columnId))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Column '{columnName}' does not exist on table '{ticket.TableName}'");
                columnIds[i] = columnId;
            }

            string[]? includeColumnIds = ResolveInlineIncludeColumnIds(ticket, constraint, columnIdByName);

            indexes.Add(new TableIndexSchema(
                ObjectIdGenerator.Generate().ToString(),
                constraint.Name,
                columnIds,
                indexType,
                SchemaElementState.Public,
                startOffset: null,
                columnDirections: IndexColumnOrder.Extract(constraint.Columns),
                includeColumnIds: includeColumnIds,
                comment: constraint.Comment
            ));
        }

        return [.. indexes];
    }

    /// <summary>
    /// Resolves and validates the stored/payload (INCLUDE) columns of an inline covering-index
    /// constraint declared in CREATE TABLE: each must exist, must not duplicate another include column,
    /// and must not also be a key column of the same index. Returns their column ids in declared order,
    /// or null when the constraint has no INCLUDE clause. (Column public-state is not checked here
    /// because every column in a fresh CREATE TABLE is public.)
    /// </summary>
    internal static string[]? ResolveInlineIncludeColumnIds(
        CreateTableTicket ticket,
        ConstraintInfo constraint,
        Dictionary<string, string> columnIdByName)
    {
        if (constraint.IncludeColumns.Length == 0)
            return null;

        HashSet<string> keyColumns = new(StringComparer.Ordinal);
        foreach (ColumnIndexInfo keyColumn in constraint.Columns)
            keyColumns.Add(keyColumn.Name);

        HashSet<string> seen = new(StringComparer.Ordinal);
        string[] includeColumnIds = new string[constraint.IncludeColumns.Length];

        for (int i = 0; i < constraint.IncludeColumns.Length; i++)
        {
            string includeName = constraint.IncludeColumns[i];

            if (!seen.Add(includeName))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Duplicate INCLUDE column '{includeName}' on index '{constraint.Name}'");

            if (keyColumns.Contains(includeName))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{includeName}' is already indexed as a key column of index '{constraint.Name}'");

            if (!columnIdByName.TryGetValue(includeName, out string? columnId))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"INCLUDE column '{includeName}' does not exist on table '{ticket.TableName}'");

            includeColumnIds[i] = columnId;
        }

        return includeColumnIds;
    }

    /// <summary>
    /// Converts the <see cref="CheckConstraintInfo"/> array on the ticket into a
    /// <see cref="CheckConstraintSchema"/> array suitable for inclusion in the
    /// <see cref="SchemaCreateTablePayload"/>. Returns null when the ticket has no check constraints
    /// (backward-compatible: absent in old payloads → treated as no checks).
    /// The <c>ParsedCondition</c> field is not included; it is rebuilt at apply time.
    /// </summary>
    internal static CheckConstraintSchema[]? BuildInlineCheckConstraints(CreateTableTicket ticket)
    {
        if (ticket.CheckConstraints.Length == 0)
            return null;

        return [.. ticket.CheckConstraints.Select(cc => new CheckConstraintSchema
        {
            Name = cc.Name,
            Expression = cc.Expression,
            ReferencedColumns = cc.ReferencedColumns
        })];
    }

    /// <summary>
    /// Builds the column payload for a staged <c>AddColumn</c> delta: every field the ticket carries,
    /// plus a fresh column id and the caller's starting <see cref="SchemaElementState"/>. The staged
    /// cluster path and the single-node path must produce identical columns, so both derive the
    /// payload from <see cref="SchemaColumnPayload.FromColumnInfo"/> rather than listing fields.
    /// </summary>
    internal static SchemaColumnPayload SchemaColumnPayloadWithState(ColumnInfo column, SchemaElementState initialState)
    {
        SchemaColumnPayload payload = SchemaColumnPayload.FromColumnInfo(column);
        payload.Id = ObjectIdGenerator.Generate().ToString();
        payload.State = initialState;
        return payload;
    }
}
