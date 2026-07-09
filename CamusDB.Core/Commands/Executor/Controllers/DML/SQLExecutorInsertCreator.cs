
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal sealed class SQLExecutorInsertCreator : SQLExecutorBaseCreator
{
    // Shared empty row context for evaluating VALUES expressions: INSERT literals/params/functions
    // never reference row columns, and EvalExpr treats the row as read-only, so one instance is
    // reused across every value cell instead of allocating an empty dictionary per cell.
    private static readonly Dictionary<string, ColumnValue> EmptyRow = new();

    internal async Task<InsertTicket> CreateInsertTicket(
        CommandExecutor commandExecutor,
        DatabaseDescriptor database,
        ExecuteSQLTicket ticket,
        NodeAst ast
    )
    {
        if (ast.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing table name");

        string tableName = ast.leftAst.yytext!;

        // If the fields are not provided, we consult them from the latest version of the schema.
        TableDescriptor table = await commandExecutor.OpenTable(new(database.Name, tableName)).ConfigureAwait(false);

        List<string> fields = new();

        if (ast.rightAst is null)
        {
            foreach (TableColumnSchema column in table.Schema.Columns!)
                fields.Add(column.Name);
        }
        else
        {
            GetIdentifierList(ast.rightAst, fields);

            // Reject a repeated column name in the explicit target list. Case sensitivity mirrors
            // the schema dictionary (StringComparer.Ordinal) so (a, A) are treated as distinct.
            HashSet<string> seen = new(fields.Count, StringComparer.Ordinal);
            foreach (string field in fields)
            {
                if (!seen.Add(field))
                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"Column '{field}' specified more than once in INSERT column list");
            }
        }

        if (ast.extendedOne is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing or empty values list");

        // Precompute per-field schema metadata once for the whole statement.
        // Index i corresponds to fields[i]: the schema entry for coercion, and the default value.
        List<TableColumnSchema> schemaColumns = table.Schema.Columns!;
        Dictionary<string, TableColumnSchema> colSchemaByName = new(schemaColumns.Count, StringComparer.Ordinal);
        foreach (TableColumnSchema col in schemaColumns)
            colSchemaByName[col.Name] = col;

        (TableColumnSchema? Schema, ColumnValue Default)[] fieldMeta = new (TableColumnSchema?, ColumnValue)[fields.Count];
        for (int i = 0; i < fields.Count; i++)
        {
            colSchemaByName.TryGetValue(fields[i], out TableColumnSchema? colDef);
            ColumnValue defaultVal = colDef?.DefaultValue ?? ColumnValue.Null;
            fieldMeta[i] = (colDef, defaultVal);
        }

        // Columns that carry defaults but were not listed in the INSERT field list.
        List<(string Name, ColumnValue Default)> extraDefaults = new();
        foreach (TableColumnSchema col in schemaColumns)
        {
            if (col.DefaultValue is not null && !fields.Contains(col.Name))
                extraDefaults.Add((col.Name, col.DefaultValue));
        }

        // Single-pass: build each row dictionary directly from the AST, applying coercion
        // and defaults in the same traversal — no List<List<ColumnValue?>> intermediate.
        List<Dictionary<string, ColumnValue>> batchValues = new();
        FillBatchDicts(ast.extendedOne, ticket.Parameters, fields, fieldMeta, extraDefaults, batchValues);

        return new InsertTicket(
            txnState: ticket.TxnState,
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            values: batchValues
        );
    }

    /// <summary>
    /// Recursively walks the batch-list AST and for each VALUES row builds a
    /// <see cref="Dictionary{TKey,TValue}"/> directly — no intermediate
    /// <c>List&lt;List&lt;ColumnValue?&gt;&gt;</c> is allocated. Coercion to the declared column
    /// type and default substitution are applied in the same pass.
    /// </summary>
    private static void FillBatchDicts(
        NodeAst batchListAst,
        Dictionary<string, ColumnValue>? parameters,
        List<string> fields,
        (TableColumnSchema? Schema, ColumnValue Default)[] fieldMeta,
        List<(string Name, ColumnValue Default)> extraDefaults,
        List<Dictionary<string, ColumnValue>> batchValues)
    {
        if (batchListAst.nodeType == NodeType.InsertBatchList)
        {
            if (batchListAst.leftAst is not null)
                FillBatchDicts(batchListAst.leftAst, parameters, fields, fieldMeta, extraDefaults, batchValues);
            if (batchListAst.rightAst is not null)
                FillBatchDicts(batchListAst.rightAst, parameters, fields, fieldMeta, extraDefaults, batchValues);
            return;
        }

        // Flatten the ExprList tree into a fixed-size buffer, one slot per field.
        ColumnValue?[] slots = new ColumnValue?[fields.Count];
        int filled = 0;
        FillSlots(batchListAst, parameters, slots, ref filled);

        if (filled != fields.Count)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"The number of fields is not equal to the number of values. Fields={fields.Count} != Values={filled} Position={batchValues.Count}"
            );

        Dictionary<string, ColumnValue> row = new(fields.Count + extraDefaults.Count);

        for (int i = 0; i < fields.Count; i++)
        {
            ColumnValue val = slots[i] ?? fieldMeta[i].Default;
            if (fieldMeta[i].Schema is { } schema)
                val = CastScalarFunctions.CoerceToColumnType(val, schema);
            row[fields[i]] = val;
        }

        // Add columns that have defaults but were not part of the INSERT field list.
        foreach ((string name, ColumnValue def) in extraDefaults)
        {
            if (!row.ContainsKey(name))
                row[name] = def;
        }

        batchValues.Add(row);
    }

    /// <summary>
    /// Traverses an ExprList tree in left-then-right order and writes each evaluated leaf value
    /// into <paramref name="slots"/>. A DEFAULT leaf writes <c>null</c> to signal "use schema default."
    /// </summary>
    private static void FillSlots(
        NodeAst node,
        Dictionary<string, ColumnValue>? parameters,
        ColumnValue?[] slots,
        ref int filled)
    {
        if (node.nodeType == NodeType.ExprList)
        {
            if (node.leftAst is not null)
                FillSlots(node.leftAst, parameters, slots, ref filled);
            if (node.rightAst is not null)
                FillSlots(node.rightAst, parameters, slots, ref filled);
            return;
        }

        if (filled >= slots.Length)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Too many values in VALUES row (expected {slots.Length})"
            );

        // ExprDefault leaves a null slot; the caller substitutes the schema default.
        slots[filled++] = node.nodeType == NodeType.ExprDefault
            ? null
            : EvalExpr(node, EmptyRow, parameters);
    }
}