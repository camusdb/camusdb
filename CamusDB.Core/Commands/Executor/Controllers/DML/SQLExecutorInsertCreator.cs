
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
    private readonly SequenceStatementBinder sequenceBinder;

    internal SQLExecutorInsertCreator(SequenceStatementBinder sequenceBinder)
    {
        ArgumentNullException.ThrowIfNull(sequenceBinder);

        this.sequenceBinder = sequenceBinder;
    }

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
        // GENERATED ALWAYS means the column's values can only come from its sequence, so a
        // statement that names it — including the implicit all-columns form — is refused rather
        // than having its value silently replaced.
        SequenceDefaults.RequireNoValueForAlwaysIdentity(table.Schema, fields);

        InsertRowShaper shaper = InsertRowShaper.Create(table.Schema, fields);

        // Every value this statement will draw from a sequence is reserved here, in one call per
        // sequence, before the first row is shaped: the row count is known from the VALUES list and
        // shaping is synchronous, so the alternative is one network round trip per row. Reserving
        // before the mutation starts also means a sequence failure cannot fail the transaction
        // late, after rows have been written.
        ExecuteSQLTicket bound = await sequenceBinder.BindAsync(
            database,
            ticket,
            ast,
            allowedRegion: ast.extendedOne,
            rowCount: CountValueRows(ast.extendedOne),
            defaultSequenceIds: shaper.DefaultSequenceIds,
            statementKind: "INSERT",
            ticket.CancellationToken).ConfigureAwait(false);

        // Single-pass: build each row dictionary directly from the AST, applying coercion
        // and defaults in the same traversal — no List<List<ColumnValue?>> intermediate.
        List<Dictionary<string, ColumnValue>> batchValues = new();
        FillBatchDicts(ast.extendedOne, bound.Parameters, shaper, batchValues);

        // Every row is shaped by now, so the values the statement actually drew are known — which
        // is not the same as the values it reserved when a VALUES expression skipped a branch.
        SequenceStatementBinder.RecordDrawnValues(bound.Parameters, ticket.TxnState);

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
    /// Walks the batch-list AST and for each VALUES row builds a
    /// <see cref="Dictionary{TKey,TValue}"/> directly — no intermediate
    /// <c>List&lt;List&lt;ColumnValue?&gt;&gt;</c> is allocated. Coercion to the declared column
    /// type and default substitution are applied in the same pass by
    /// <see cref="InsertRowShaper.ShapeRow"/>.
    ///
    /// <para>The walk uses an explicit stack rather than recursion. The grammar builds the batch
    /// list left-deep (<c>insert_batch_list : insert_batch_list ',' insert_values</c>), so a
    /// recursive walk reaches one frame per row and a large VALUES list overflows the stack of the
    /// thread serving the request — a process-ending failure that no <c>catch</c> can absorb.
    /// It is reachable well inside what the server admits: a statement is refused only once its
    /// mutations exceed <see cref="CamusDBOptions.MaxMutationsPerTransaction"/>, and that check
    /// runs later, in the write path, long after this walk has already built every row.</para>
    ///
    /// <para>Children are pushed right-then-left so they pop left-to-right. That keeps row order —
    /// and therefore the evaluation order of volatile values and function defaults, and the
    /// <c>Position</c> in an arity error — identical to the order the rows appear in the SQL
    /// text.</para>
    /// </summary>
    private static void FillBatchDicts(
        NodeAst batchListAst,
        Dictionary<string, ColumnValue>? parameters,
        InsertRowShaper shaper,
        List<Dictionary<string, ColumnValue>> batchValues)
    {
        Stack<NodeAst> pending = new();
        pending.Push(batchListAst);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (node.nodeType == NodeType.InsertBatchList)
            {
                if (node.rightAst is not null)
                    pending.Push(node.rightAst);
                if (node.leftAst is not null)
                    pending.Push(node.leftAst);
                continue;
            }

            AddRow(node, parameters, shaper, batchValues);
        }
    }

    /// <summary>
    /// Shapes one VALUES row and appends it to <paramref name="batchValues"/>. The per-row
    /// <c>ExprList</c> walk stays recursive: its depth is the number of columns in the row, which
    /// the table schema bounds, not the number of rows in the statement.
    /// </summary>
    private static void AddRow(
        NodeAst rowAst,
        Dictionary<string, ColumnValue>? parameters,
        InsertRowShaper shaper,
        List<Dictionary<string, ColumnValue>> batchValues)
    {
        // Flatten the ExprList tree into a fixed-size buffer, one slot per field.
        ColumnValue?[] slots = new ColumnValue?[shaper.FieldCount];
        int filled = 0;
        FillSlots(rowAst, parameters, slots, ref filled);

        if (filled != shaper.FieldCount)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"The number of fields is not equal to the number of values. Fields={shaper.FieldCount} != Values={filled} Position={batchValues.Count}"
            );

        batchValues.Add(shaper.ShapeRow(slots, parameters));
    }

    /// <summary>
    /// Counts the rows a VALUES list holds, so the statement can reserve exactly that many sequence
    /// values before it shapes any of them.
    /// </summary>
    /// <remarks>
    /// Iterative for the reason <see cref="FillBatchDicts"/> is: the batch list is left-deep, so a
    /// recursive count reaches one frame per row and a large VALUES list overflows the stack of the
    /// thread serving the request — a process-ending failure no catch can absorb.
    /// </remarks>
    private static int CountValueRows(NodeAst batchListAst)
    {
        int rows = 0;
        Stack<NodeAst> pending = new();
        pending.Push(batchListAst);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (node.nodeType == NodeType.InsertBatchList)
            {
                if (node.rightAst is not null)
                    pending.Push(node.rightAst);
                if (node.leftAst is not null)
                    pending.Push(node.leftAst);
                continue;
            }

            rows++;
        }

        return rows;
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