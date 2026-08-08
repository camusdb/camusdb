
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
        //
        // Resolve against the descriptor we were handed, not by name. Re-resolving through the
        // registry using database.Name reintroduces whatever name that cached descriptor was created
        // with — after a RENAME DATABASE that is the *old* name, which the registry no longer knows,
        // so every INSERT failed with "Database '<old>' does not exist" until the node restarted.
        // It also saves a redundant registry lookup and use-handle on the INSERT hot path.
        TableDescriptor table = await commandExecutor
            .OpenTableWithDescriptor(database, new(ticket.DatabaseName, tableName))
            .ConfigureAwait(false);

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

        // Per-field schema metadata and the default-bearing columns the field list omits are resolved
        // once for the whole statement; the shaper is shared with the INSERT … SELECT path so both
        // forms coerce and default identically.
        InsertRowShaper shaper = InsertRowShaper.Create(table.Schema, fields);

        // Single-pass: build each row dictionary directly from the AST, applying coercion
        // and defaults in the same traversal — no List<List<ColumnValue?>> intermediate.
        List<Dictionary<string, ColumnValue>> batchValues = new();
        FillBatchDicts(ast.extendedOne, ticket.Parameters, shaper, batchValues);

        return new InsertTicket(
            txnState: ticket.TxnState,
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            values: batchValues
        );
    }

    /// <summary>
    /// Builds the ticket for <c>INSERT INTO t [(c1, …)] SELECT …</c>.
    ///
    /// <para>No schema is consulted here, unlike <see cref="CreateInsertTicket"/>: the values come
    /// from a query rather than literals, so there is nothing to coerce yet, and even the target
    /// column list cannot be defaulted until the table is opened by the controller. That keeps
    /// ticket creation free of table access on this path.</para>
    /// </summary>
    internal InsertSelectTicket CreateInsertSelectTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing table name");

        if (ast.extendedOne is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing source query");

        List<string>? targetColumns = null;

        if (ast.rightAst is not null)
        {
            targetColumns = new();
            GetIdentifierList(ast.rightAst, targetColumns);
        }

        return new InsertSelectTicket(
            txnState: ticket.TxnState,
            databaseName: ticket.DatabaseName,
            tableName: ast.leftAst.yytext!,
            targetColumns: targetColumns,
            sourceSelect: ast.extendedOne,
            parameters: ticket.Parameters
        );
    }

    /// <summary>
    /// Recursively walks the batch-list AST and for each VALUES row builds a
    /// <see cref="Dictionary{TKey,TValue}"/> directly — no intermediate
    /// <c>List&lt;List&lt;ColumnValue?&gt;&gt;</c> is allocated. Coercion to the declared column
    /// type and default substitution are applied in the same pass by
    /// <see cref="InsertRowShaper.ShapeRow"/>.
    /// </summary>
    private static void FillBatchDicts(
        NodeAst batchListAst,
        Dictionary<string, ColumnValue>? parameters,
        InsertRowShaper shaper,
        List<Dictionary<string, ColumnValue>> batchValues)
    {
        if (batchListAst.nodeType == NodeType.InsertBatchList)
        {
            if (batchListAst.leftAst is not null)
                FillBatchDicts(batchListAst.leftAst, parameters, shaper, batchValues);
            if (batchListAst.rightAst is not null)
                FillBatchDicts(batchListAst.rightAst, parameters, shaper, batchValues);
            return;
        }

        // Flatten the ExprList tree into a fixed-size buffer, one slot per field.
        ColumnValue?[] slots = new ColumnValue?[shaper.FieldCount];
        int filled = 0;
        FillSlots(batchListAst, parameters, slots, ref filled);

        if (filled != shaper.FieldCount)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"The number of fields is not equal to the number of values. Fields={shaper.FieldCount} != Values={filled} Position={batchValues.Count}"
            );

        batchValues.Add(shaper.ShapeRow(slots));
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