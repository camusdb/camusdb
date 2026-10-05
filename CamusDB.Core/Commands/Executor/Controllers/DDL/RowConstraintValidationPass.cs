/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Proves that every existing row of a table satisfies a CHECK constraint or a column's NOT NULL
/// constraint, for <c>ALTER TABLE ... ADD CONSTRAINT ... CHECK</c> and
/// <c>ALTER TABLE ... ALTER COLUMN ... SET NOT NULL</c>.
///
/// <para><b>The constraint must be enforced before this pass reads a row.</b> The read takes no lock,
/// so a write that commits after the read passed its row is not seen. Such a write is safe only when
/// the write itself was checked. So the caller first makes every new statement check the constraint,
/// then waits until each write planned without it has committed or can never commit (the write-shape
/// fence), and only then calls this pass. In the other order, a short autocommit UPDATE can commit a
/// violating value between the read and the start of enforcement, and the constraint is then published
/// over a row that breaks it.</para>
///
/// <para><b>A violation found here is reported, not repaired.</b> The caller removes the constraint
/// again. A refused write during the pass is the cost of that order: the constraint was enforced for a
/// short time, then removed.</para>
///
/// <para>The read uses one Read Committed read-only transaction over the row key space. A row that a
/// concurrent statement changes after the read saw a violating value still fails the pass. That is a
/// refusal on the safe side: the statement can be run again.</para>
/// </summary>
internal static class RowConstraintValidationPass
{
    /// <summary>
    /// Opens <paramref name="tableName"/> and validates the constraint <paramref name="constraintName"/>
    /// of <paramref name="kind"/> against every row. Returns without a read when the constraint no
    /// longer exists: a concurrent DROP removed it, and there is nothing left to prove. A leader that
    /// resumes an interrupted job calls this too.
    /// </summary>
    internal static async Task ValidateAsync(
        DatabaseDescriptor database,
        TableOpener tableOpener,
        string tableName,
        SchemaElementKind kind,
        string constraintName)
    {
        TableDescriptor table = await tableOpener.Open(database, tableName).ConfigureAwait(false);

        if (kind == SchemaElementKind.Check)
        {
            CheckConstraintSchema? check = table.Schema.CheckConstraints?.Find(
                c => string.Equals(c.Name, constraintName, StringComparison.OrdinalIgnoreCase));

            if (check is null)
                return;

            SQLParser.NodeAst condition = check.ParsedCondition ?? SQLParser.SQLParserProcessor.ParseCondition(check.Expression);

            await ScanForCheckViolationAsync(database, table, check.Name, condition).ConfigureAwait(false);
            return;
        }

        if (kind == SchemaElementKind.NotNull)
        {
            TableColumnSchema? column = FindNotNullColumn(table.Schema, constraintName);
            if (column is null)
                return;

            await ScanForNullAsync(database, table, column.Name).ConfigureAwait(false);
            return;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"'{kind}' is not a row constraint; constraint '{constraintName}' on table '{tableName}' cannot be validated by a row scan");
    }

    /// <summary>
    /// The column whose enforced NOT NULL constraint is named <paramref name="constraintName"/>, or null
    /// when no column holds it. The name is the only identity a NOT NULL constraint has.
    /// </summary>
    internal static TableColumnSchema? FindNotNullColumn(TableSchema table, string constraintName) =>
        table.Columns?.Find(c => c.NotNull && string.Equals(c.NotNullConstraintName, constraintName, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// Scans every row and throws <see cref="CamusDBErrorCodes.NotNullViolation"/> on the first row
    /// whose <paramref name="columnName"/> is NULL. Only that column is decoded. A row written before
    /// the column existed decodes to the column's default, which is what a full decode gives.
    /// </summary>
    internal static async Task ScanForNullAsync(DatabaseDescriptor database, TableDescriptor table, string columnName)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly
        ).ConfigureAwait(false);
        try
        {
            HashSet<string> requiredColumns = new(StringComparer.OrdinalIgnoreCase) { columnName };
            RowEncoder.DictionaryDecodeState decodeState = new();

            await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data)
                in table.Store.ScanRows(tx, afterRowId: null).ConfigureAwait(false))
            {
                Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                    table.Schema,
                    tx.TransactionId,
                    rowId,
                    data,
                    requiredColumns: requiredColumns,
                    visibilitySchemaVersion: table.Schema.Version,
                    decodeState: decodeState
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
    /// Scans every row and throws <see cref="CamusDBErrorCodes.CheckConstraintViolation"/> on the first
    /// row for which <paramref name="condition"/> is FALSE. A NULL result passes, as it does for DML.
    /// </summary>
    internal static async Task ScanForCheckViolationAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        string constraintName,
        SQLParser.NodeAst condition)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly
        ).ConfigureAwait(false);
        try
        {
            // The condition can reference any column, so every column is decoded, but the decode
            // plan is shared across the scan.
            RowEncoder.DictionaryDecodeState decodeState = new();

            await foreach ((ObjectIdValue rowId, ReadOnlyMemory<byte> data)
                in table.Store.ScanRows(tx, afterRowId: null).ConfigureAwait(false))
            {
                Dictionary<string, ColumnValue> row = await RowEncoder.DecodeWritableAsync(
                    table.Schema,
                    tx.TransactionId,
                    rowId,
                    data,
                    visibilitySchemaVersion: table.Schema.Version,
                    decodeState: decodeState
                ).ConfigureAwait(false);

                if (CheckEvaluator.Evaluate(condition, row) == false)
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
