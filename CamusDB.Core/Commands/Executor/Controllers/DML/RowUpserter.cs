
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// Executes <c>INSERT … ON CONFLICT … DO NOTHING</c>, <c>INSERT … ON CONFLICT … DO UPDATE</c> and
/// <c>UPSERT INTO</c> over rows that the INSERT path already shaped (defaults and coercion).
///
/// <para><b>Find each conflict before writing, never "insert, then catch the duplicate".</b> A unique
/// entry is written with a conditional put, and a duplicate fails the batch with
/// <see cref="CamusDBErrorCodes.DuplicateUniqueKeyValue"/> after other keys may already be staged.
/// There are no savepoints, so that error ends the transaction. Each chunk of proposed rows therefore
/// takes an exclusive lock on its arbiter keys first, then reads them
/// (<see cref="Storage.Kv.KvTableStore.LockAndLookupUniqueRowIdsForWriteAsync"/>). While the lock is
/// held no other transaction can write those keys, so the answer stays true until the statement's own
/// write: a concurrent plain INSERT of the key waits and then fails at the lock-wait deadline, and a
/// concurrent upsert of the key meets the lock under wait-die.</para>
///
/// <para><b>Order per chunk:</b> fence and probe each arbiter index, split the rows into an insert set
/// and a conflict set, update the conflict set through
/// <see cref="RowUpdater.UpdateLocatedRowsAsync"/> (lock, read, WHERE, SET, BEFORE UPDATE, checks,
/// write), then insert the insert set through <see cref="RowInserter.InsertPreparedRowsAsync"/>. A
/// conflicting row that a concurrent delete removed before the row lock moves to the insert set; its
/// key is still fenced, so the insert cannot meet a duplicate.</para>
///
/// <para><b>One stored row, one change per statement.</b> With DO UPDATE, two proposed rows that
/// resolve to the same stored row, or to the same new key, fail with
/// <see cref="CamusDBErrorCodes.InvalidInput"/>: the result would depend on row order (PostgreSQL
/// error 21000). With DO NOTHING the second row is skipped, because it conflicts with the row the
/// first one wrote. Inside a chunk this is tracked by encoded arbiter key, across chunks by row id:
/// a later chunk's probe finds the earlier chunk's rows, because the probe sees the transaction's own
/// writes.</para>
///
/// <para><b>Triggers and foreign keys.</b> BEFORE INSERT runs for every proposed row before the
/// conflict check, because a trigger can change the key. An inserted row then runs as for INSERT, an
/// updated row as for UPDATE (BEFORE UPDATE sees the stored row as OLD). Both foreign-key checkers are
/// built before the first trigger and completed after the last write, before the AFTER triggers.</para>
/// </summary>
internal sealed class RowUpserter
{
    private readonly RowInserter rowInserter;

    private readonly RowUpdater rowUpdater;

    internal RowUpserter(RowInserter rowInserter, RowUpdater rowUpdater)
    {
        ArgumentNullException.ThrowIfNull(rowInserter);
        ArgumentNullException.ThrowIfNull(rowUpdater);

        this.rowInserter = rowInserter;
        this.rowUpdater = rowUpdater;
    }

    /// <summary>How many proposed rows the statement inserted, and how many stored rows it updated.</summary>
    internal readonly record struct UpsertCounts(int Inserted, int Updated)
    {
        /// <summary>The statement's row count: every inserted and every updated row; a skipped row counts nothing.</summary>
        public int Affected => Inserted + Updated;
    }

    /// <summary>
    /// Runs the clause over the ticket's rows, in order, in chunks of
    /// <see cref="CamusDBOptions.SpillEffectiveThreshold"/> rows.
    /// </summary>
    /// <param name="parameters">The statement's parameters, for the SET expressions and the WHERE.</param>
    /// <param name="returnedRows">
    /// Receives one (row id, stored values) pair for each inserted row and each updated row (its new
    /// image), in the order of the proposed rows. Null when the statement returns no rows.
    /// </param>
    public async Task<UpsertCounts> Upsert(
        DatabaseDescriptor database,
        TableDescriptor table,
        InsertTicket ticket,
        OnConflictClause clause,
        Dictionary<string, ColumnValue>? parameters,
        List<QueryResultRow>? returnedRows,
        CancellationToken cancellationToken = default)
    {
        MaterializedViewAccessGuard.RequireWritable(table);

        KvTransaction tx = ticket.TxnState;

        List<TableIndexSchema> arbiters = ResolveArbiters(table, clause);

        // Checked before any trigger runs or any key is locked: a statement that can update must hold
        // UPDATE as well as the INSERT the table open already checked.
        if (clause.Updates)
            RequireUpdatePrivilege(database, table);

        Dictionary<string, NodeAst>? setValues = clause.Action == OnConflictAction.Upsert
            ? BuildUpsertAssignments(table, clause, arbiters[0])
            : clause.SetValues;

        Extensions.StatementTriggers? updateTriggers = null;
        ForeignKeyStatementChecker updateKeys = ForeignKeyStatementChecker.None;

        if (setValues is not null)
        {
            updateTriggers = rowUpdater.CreateStatementTriggers(database, table, tx);

            // The structural checks of UPDATE: every assigned column exists and is writable, no
            // primary-key column is assigned, and a literal NULL into a NOT NULL column is refused
            // unless a BEFORE UPDATE trigger may repair it.
            RowUpdater.Validate(
                table,
                new UpdateTicket(tx, ticket.DatabaseName, ticket.TableName, null, setValues, clause.Where, null, parameters),
                checkNotNull: updateTriggers is not { HasBefore: true });

            ValidateQualifiers(table, ticket.TableName, setValues, clause.Where);

            updateKeys = await rowUpdater.CreateStatementCheckerAsync(
                database, table, tx, setValues, assignsEveryColumn: updateTriggers is { HasBefore: true }).ConfigureAwait(false);
        }

        Extensions.StatementTriggers? insertTriggers = rowInserter.CreateStatementTriggers(database, table, tx);

        // Before any BEFORE trigger runs: the checker opens and pins the parent tables, and an open can
        // throw SchemaCatchingUp, which the dispatcher retries in this same transaction.
        ForeignKeyStatementChecker insertKeys = await rowInserter.CreateStatementCheckerAsync(database, table, tx).ConfigureAwait(false);

        // BEFORE INSERT runs for every proposed row, before the conflict check, because it can change
        // the key. The rows are copied first: a transport keeps the request's rows for a retry.
        List<Dictionary<string, ColumnValue>> proposed = ticket.Values;

        if (insertTriggers is { HasBefore: true })
        {
            proposed = new(ticket.Values.Count);

            foreach (Dictionary<string, ColumnValue> values in ticket.Values)
            {
                Dictionary<string, ColumnValue> row = new(values, values.Comparer);
                await insertTriggers.FireBeforeAsync(null, row).ConfigureAwait(false);
                proposed.Add(row);
            }
        }

        // The proposed row is judged as an INSERT would judge it, before the conflict check, as in
        // PostgreSQL: a row that breaks NOT NULL or CHECK fails even when its key is taken.
        RowInserter.Validate(table, new InsertTicket(tx, ticket.DatabaseName, ticket.TableName, proposed));

        // The rows the current chunk's ComputeNewRow reads as EXCLUDED, by conflict-set position.
        List<Dictionary<string, ColumnValue>> excludedRows = new();

        RowUpdater.LocatedRowUpdate? update = setValues is null ? null : new()
        {
            Database = database,
            Table = table,
            Transaction = tx,
            AssignedColumns = new HashSet<string>(setValues.Keys, StringComparer.OrdinalIgnoreCase),
            // The SET expressions and the WHERE can read any stored column, so every column is decoded.
            EvaluatedColumns = null,
            ForeignKeys = updateKeys,
            Triggers = updateTriggers,
            ComputeNewRow = (position, storedRow) =>
                new ValueTask<Dictionary<string, ColumnValue>?>(
                    ComputeUpdatedRow(table, ticket.TableName, storedRow, excludedRows[position], setValues, clause.Where, parameters))
        };

        // Rows this statement inserted or updated, for the duplicate rule across chunks.
        HashSet<ObjectIdValue> writtenRowIds = new();

        // Encoded arbiter keys the current chunk's rows claimed, for the duplicate rule inside a chunk.
        HashSet<string> chunkKeys = new(StringComparer.Ordinal);

        int chunkSize = Math.Max(1, database.Options.SpillEffectiveThreshold);
        int inserted = 0;
        int updated = 0;

        for (int offset = 0; offset < proposed.Count; offset += chunkSize)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int count = Math.Min(chunkSize, proposed.Count - offset);

            (ObjectIdValue?[] conflicts, List<string>?[] rowKeys) = await ProbeArbitersAsync(
                table, tx, arbiters, proposed, offset, count, cancellationToken).ConfigureAwait(false);

            // Split, in proposed order.
            chunkKeys.Clear();
            excludedRows.Clear();

            List<int> insertPositions = new(count);
            List<int>? updatePositions = null;
            List<ObjectIdValue>? updateRowIds = null;

            for (int i = 0; i < count; i++)
            {
                ObjectIdValue? found = conflicts[i];

                if (ClaimsAny(chunkKeys, rowKeys[i]) || (found is { } foundId && writtenRowIds.Contains(foundId)))
                {
                    if (update is not null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInput,
                            "ON CONFLICT DO UPDATE cannot affect a row a second time: two proposed rows of the statement have the same conflict key");

                    // DO NOTHING: the row conflicts with one this statement wrote.
                    continue;
                }

                if (found is { } conflictRowId)
                {
                    // DO NOTHING, or an UPSERT with no column to assign: the stored row stays as it is.
                    if (update is null)
                        continue;

                    (updatePositions ??= new()).Add(i);
                    (updateRowIds ??= new()).Add(conflictRowId);
                    excludedRows.Add(proposed[offset + i]);
                    writtenRowIds.Add(conflictRowId);
                }
                else
                {
                    insertPositions.Add(i);
                }

                Claim(chunkKeys, rowKeys[i]);
            }

            // The conflict set first: a row a concurrent delete removed before its lock moves to the
            // insert set, in proposed order.
            Dictionary<string, ColumnValue>?[]? newImages = null;

            if (updateRowIds is not null)
            {
                newImages = returnedRows is not null ? new Dictionary<string, ColumnValue>?[updateRowIds.Count] : null;
                bool[] missing = new bool[updateRowIds.Count];

                updated += await rowUpdater.UpdateLocatedRowsAsync(update!, updateRowIds, newImages, missing).ConfigureAwait(false);

                for (int k = 0; k < missing.Length; k++)
                {
                    if (!missing[k])
                        continue;

                    writtenRowIds.Remove(updateRowIds[k]);
                    InsertSorted(insertPositions, updatePositions![k]);
                }
            }

            List<QueryResultRow>? insertedRows = null;

            if (insertPositions.Count > 0)
            {
                List<Dictionary<string, ColumnValue>> rows = new(insertPositions.Count);
                foreach (int position in insertPositions)
                    rows.Add(proposed[offset + position]);

                // Always collected: the new row ids feed the duplicate rule of the later chunks.
                insertedRows = new(rows.Count);

                inserted += await rowInserter.InsertPreparedRowsAsync(
                    database, table, new InsertTicket(tx, ticket.DatabaseName, ticket.TableName, rows),
                    insertKeys, insertedRows, insertTriggers).ConfigureAwait(false);

                foreach (QueryResultRow row in insertedRows)
                    writtenRowIds.Add(row.RowId);
            }

            if (returnedRows is not null)
                AppendReturnedRows(returnedRows, count, insertPositions, insertedRows, updatePositions, updateRowIds, newImages);
        }

        // After the statement's last write, never before: see ForeignKeyStatementChecker.
        if (insertKeys.HasWork && inserted > 0)
            await insertKeys.CompleteAsync(tx, cancellationToken).ConfigureAwait(false);

        if (updateKeys.HasWork && updated > 0)
            await updateKeys.CompleteAsync(tx, cancellationToken).ConfigureAwait(false);

        // After the last write and the constraint checks: an AFTER trigger sees only a statement that
        // is going to stand.
        if (insertTriggers is not null)
            await insertTriggers.FireAfterAsync().ConfigureAwait(false);

        if (updateTriggers is not null)
            await updateTriggers.FireAfterAsync().ConfigureAwait(false);

        return new UpsertCounts(inserted, updated);
    }

    /// <summary>
    /// Fences and probes every arbiter index for one chunk. Answers, per chunk position, the first
    /// stored row an arbiter found (null for none) and the encoded keys the row holds across the
    /// arbiters (null for a row whose every arbiter key has a NULL column). A row with a NULL in an
    /// arbiter's key has no entry in that index, so it can never conflict there and is not locked.
    /// </summary>
    private static async Task<(ObjectIdValue?[] Conflicts, List<string>?[] RowKeys)> ProbeArbitersAsync(
        TableDescriptor table,
        KvTransaction tx,
        List<TableIndexSchema> arbiters,
        List<Dictionary<string, ColumnValue>> proposed,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        ObjectIdValue?[] conflicts = new ObjectIdValue?[count];
        List<string>?[] rowKeys = new List<string>?[count];

        List<CompositeColumnValue> keys = new(count);
        List<int> positions = new(count);

        foreach (TableIndexSchema arbiter in arbiters)
        {
            keys.Clear();
            positions.Clear();

            for (int i = 0; i < count; i++)
            {
                if (BuildKey(proposed[offset + i], arbiter.Columns) is not { } key)
                    continue;

                keys.Add(key);
                positions.Add(i);
            }

            if (keys.Count == 0)
                continue;

            (ObjectIdValue?[] rowIds, string?[] kvKeys) = await table.Store
                .LockAndLookupUniqueRowIdsForWriteAsync(tx, arbiter.KvId, keys, cancellationToken).ConfigureAwait(false);

            for (int k = 0; k < positions.Count; k++)
            {
                int i = positions[k];

                if (kvKeys[k] is { } kvKey)
                    (rowKeys[i] ??= new(arbiters.Count)).Add(kvKey);

                if (conflicts[i] is null && rowIds[k] is { } rowId)
                    conflicts[i] = rowId;
            }
        }

        return (conflicts, rowKeys);
    }

    /// <summary>The arbiter key of a proposed row, or null when a key column is absent or NULL.</summary>
    private static CompositeColumnValue? BuildKey(Dictionary<string, ColumnValue> row, string[] columns)
    {
        ColumnValue[] values = new ColumnValue[columns.Length];

        for (int i = 0; i < columns.Length; i++)
        {
            if (!row.TryGetValue(columns[i], out ColumnValue? value) || value.Type == ColumnType.Null)
                return null;

            values[i] = value;
        }

        return new CompositeColumnValue(values);
    }

    private static bool ClaimsAny(HashSet<string> claimed, List<string>? keys)
    {
        if (keys is null)
            return false;

        foreach (string key in keys)
        {
            if (claimed.Contains(key))
                return true;
        }

        return false;
    }

    private static void Claim(HashSet<string> claimed, List<string>? keys)
    {
        if (keys is null)
            return;

        foreach (string key in keys)
            claimed.Add(key);
    }

    private static void InsertSorted(List<int> positions, int position)
    {
        int index = positions.BinarySearch(position);
        positions.Insert(index < 0 ? ~index : index, position);
    }

    /// <summary>
    /// Adds the chunk's written rows to the RETURNING buffer in the order of the proposed rows: an
    /// inserted row with its stored values, an updated row with its new image. A skipped row adds
    /// nothing.
    /// </summary>
    private static void AppendReturnedRows(
        List<QueryResultRow> returnedRows,
        int count,
        List<int> insertPositions,
        List<QueryResultRow>? insertedRows,
        List<int>? updatePositions,
        List<ObjectIdValue>? updateRowIds,
        Dictionary<string, ColumnValue>?[]? newImages)
    {
        int nextInsert = 0;
        int nextUpdate = 0;

        // A position can be in both lists: a conflicting row whose stored row was gone at the lock has
        // no new image and was inserted instead. Both cursors advance past it.
        for (int i = 0; i < count; i++)
        {
            if (updatePositions is not null && nextUpdate < updatePositions.Count && updatePositions[nextUpdate] == i)
            {
                if (newImages![nextUpdate] is { } newRow)
                    returnedRows.Add(new QueryResultRow(updateRowIds![nextUpdate], newRow));

                nextUpdate++;
            }

            if (nextInsert < insertPositions.Count && insertPositions[nextInsert] == i)
            {
                returnedRows.Add(insertedRows![nextInsert]);
                nextInsert++;
            }
        }
    }

    /// <summary>
    /// Evaluates the DO UPDATE WHERE and the SET list for one conflicting row, over the stored row
    /// (bare names and <c>table.column</c>) and the proposed row (<c>excluded.column</c>). Returns null
    /// when the WHERE is not true, which leaves the row unchanged and uncounted. Every SET expression
    /// reads the stored row as locked and read, never a value another assignment of the same list
    /// produced.
    /// </summary>
    private static Dictionary<string, ColumnValue>? ComputeUpdatedRow(
        TableDescriptor table,
        string tableName,
        Dictionary<string, ColumnValue> storedRow,
        Dictionary<string, ColumnValue> excludedRow,
        Dictionary<string, NodeAst> setValues,
        NodeAst? where,
        Dictionary<string, ColumnValue>? parameters)
    {
        List<TableColumnSchema> columns = table.Schema.Columns!;
        Dictionary<string, ColumnValue> merged = new(columns.Count * 3, StringComparer.OrdinalIgnoreCase);

        foreach (TableColumnSchema column in columns)
        {
            ColumnValue stored = storedRow.TryGetValue(column.Name, out ColumnValue? s) ? s : ColumnValue.Null;
            ColumnValue proposed = excludedRow.TryGetValue(column.Name, out ColumnValue? p) ? p : ColumnValue.Null;

            merged[column.Name] = stored;
            merged[string.Concat(tableName, ".", column.Name)] = stored;
            merged[string.Concat(OnConflictClause.ExcludedQualifier, ".", column.Name)] = proposed;
        }

        if (where is not null)
        {
            ColumnValue condition = SqlExecutor.EvalExpr(where, merged, parameters);

            if (condition.Type is not ColumnType.Bool and not ColumnType.Null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"The WHERE of ON CONFLICT DO UPDATE must be a boolean condition, not {condition.Type}");

            if (condition.Type != ColumnType.Bool || !condition.BoolValue)
                return null;
        }

        Dictionary<string, ColumnValue> newRow = new(storedRow, StringComparer.OrdinalIgnoreCase);

        foreach (KeyValuePair<string, NodeAst> assignment in setValues)
            newRow[assignment.Key] = SqlExecutor.EvalExpr(assignment.Value, merged, parameters);

        return newRow;
    }

    /// <summary>
    /// The unique indexes that decide a conflict. A column target matches the unique indexes whose
    /// column set equals it, in any order; a constraint target names one unique index; no target (DO
    /// NOTHING only) makes every unique index an arbiter; UPSERT uses the primary key. Only an index that
    /// is fully built can arbitrate: one still filling has no entry for many existing rows.
    /// </summary>
    private static List<TableIndexSchema> ResolveArbiters(TableDescriptor table, OnConflictClause clause)
    {
        Dictionary<string, TableIndexSchema> indexes = table.Indexes;
        List<TableIndexSchema> arbiters = new();

        if (clause.Action == OnConflictAction.Upsert)
        {
            if (!indexes.TryGetValue(CamusDBConstants.PrimaryKeyInternalName, out TableIndexSchema? primaryKey))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Table '{table.Name}' has no primary key for UPSERT");

            arbiters.Add(primaryKey);
            return arbiters;
        }

        if (clause.ConstraintName is { } constraintName)
        {
            foreach (TableIndexSchema index in indexes.Values)
            {
                if (!string.Equals(index.Name, constraintName, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (index.Type != IndexType.Unique)
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Constraint '{constraintName}' of table '{table.Name}' is not a unique constraint, so it cannot decide an ON CONFLICT");

                RequireBuilt(table, index);
                arbiters.Add(index);
                return arbiters;
            }

            // The primary key's internal name cannot be written in SQL, and the name a CREATE TABLE
            // declares for it is not stored, so it answers to PostgreSQL's default name instead.
            if (string.Equals(constraintName, string.Concat(table.Name, "_pkey"), StringComparison.OrdinalIgnoreCase) &&
                indexes.TryGetValue(CamusDBConstants.PrimaryKeyInternalName, out TableIndexSchema? namedPrimaryKey))
            {
                arbiters.Add(namedPrimaryKey);
                return arbiters;
            }

            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Table '{table.Name}' has no unique constraint named '{constraintName}'");
        }

        if (clause.TargetColumns is { } targetColumns)
        {
            HashSet<string> target = new(targetColumns, StringComparer.OrdinalIgnoreCase);
            TableIndexSchema? unbuilt = null;

            foreach (TableIndexSchema index in indexes.Values)
            {
                if (index.Type != IndexType.Unique || index.Columns.Length != target.Count || !target.SetEquals(index.Columns))
                    continue;

                if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                {
                    unbuilt = index;
                    continue;
                }

                arbiters.Add(index);
            }

            if (arbiters.Count > 0)
                return arbiters;

            if (unbuilt is not null)
                RequireBuilt(table, unbuilt);

            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"No unique index of table '{table.Name}' matches the ON CONFLICT columns ({string.Join(", ", targetColumns)})");
        }

        foreach (TableIndexSchema index in indexes.Values)
        {
            if (index.Type == IndexType.Unique && SchemaElementStateRules.IsReadableIndex(table.Schema, index))
                arbiters.Add(index);
        }

        return arbiters;
    }

    private static void RequireBuilt(TableDescriptor table, TableIndexSchema index)
    {
        if (!SchemaElementStateRules.IsReadableIndex(table.Schema, index))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Unique index '{index.Name}' of table '{table.Name}' is still being built, so it cannot decide an ON CONFLICT yet");
    }

    /// <summary>
    /// The SET list of an UPSERT: <c>c = excluded.c</c> for every listed column (every column when the
    /// statement lists none) that is writable and not part of the primary key. Null when no column is
    /// left, so a taken key leaves the stored row as it is.
    /// </summary>
    private static Dictionary<string, NodeAst>? BuildUpsertAssignments(TableDescriptor table, OnConflictClause clause, TableIndexSchema primaryKey)
    {
        HashSet<string> keyColumns = new(primaryKey.Columns, StringComparer.OrdinalIgnoreCase);
        Dictionary<string, NodeAst> setValues = new(StringComparer.OrdinalIgnoreCase);

        IEnumerable<string> listed = clause.UpsertColumns ?? ColumnNames(table);

        foreach (string name in listed)
        {
            if (keyColumns.Contains(name) || setValues.ContainsKey(name))
                continue;

            TableColumnSchema? column = FindColumn(table, name);
            if (column is null || !SchemaElementStateRules.IsWritable(column))
                continue;

            setValues[column.Name] = new NodeAst(
                NodeType.Identifier, null, null, null, null, null, null, null,
                string.Concat(OnConflictClause.ExcludedQualifier, ".", column.Name));
        }

        return setValues.Count == 0 ? null : setValues;
    }

    private static IEnumerable<string> ColumnNames(TableDescriptor table)
    {
        foreach (TableColumnSchema column in table.Schema.Columns!)
            yield return column.Name;
    }

    private static TableColumnSchema? FindColumn(TableDescriptor table, string name)
    {
        foreach (TableColumnSchema column in table.Schema.Columns!)
        {
            if (string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase))
                return column;
        }

        return null;
    }

    /// <summary>
    /// Refuses a qualified reference in the SET list or the WHERE that names anything other than the
    /// target table or <c>excluded</c>, or a column the table does not have. Checked once, before any
    /// row is written, so the error does not depend on whether a conflict happens.
    /// </summary>
    private static void ValidateQualifiers(TableDescriptor table, string tableName, Dictionary<string, NodeAst> setValues, NodeAst? where)
    {
        List<string> references = new();

        foreach (NodeAst expression in setValues.Values)
            QueryExpressionWalker.CollectColumnReferences(expression, references);

        if (where is not null)
            QueryExpressionWalker.CollectColumnReferences(where, references);

        foreach (string reference in references)
        {
            int dot = reference.IndexOf('.');
            if (dot < 0)
                continue;

            string qualifier = reference[..dot];
            string column = reference[(dot + 1)..];

            if (!string.Equals(qualifier, OnConflictClause.ExcludedQualifier, StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(qualifier, tableName, StringComparison.OrdinalIgnoreCase))
                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownColumn,
                    $"Unknown column '{reference}': ON CONFLICT DO UPDATE can read {tableName}.<column> and excluded.<column> only");

            if (FindColumn(table, column) is null)
                throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Unknown column '{reference}'");
        }
    }

    /// <summary>
    /// DO UPDATE and UPSERT need UPDATE on the table as well as INSERT. Only one privilege rides the
    /// ambient scope that the table open checks, and the statement maps to INSERT, so UPDATE is checked
    /// here, on its own: two privileges granted in two statements live in two grant records, and one
    /// combined mask would match neither. An absent scope is an internal caller or a forwarded
    /// statement that a follower already authorized, as for TRUNCATE.
    /// </summary>
    private static void RequireUpdatePrivilege(DatabaseDescriptor database, TableDescriptor table)
    {
        if (!database.Options.AuthenticationEnabled)
            return;

        AuthorizationScope scope = AuthorizationContext.Current;
        if (scope.Principal is not { } principal || scope.RequiredPrivilege is null)
            return;

        if (!principal.HasPrivilege(Privilege.Update, database.Id, table.Id))
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege,
                $"Missing UPDATE privilege on table '{database.Name}.{table.Name}': ON CONFLICT DO UPDATE and UPSERT need INSERT and UPDATE");
    }
}
