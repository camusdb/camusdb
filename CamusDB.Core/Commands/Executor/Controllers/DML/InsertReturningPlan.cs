/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// The bound RETURNING list of one <c>INSERT … RETURNING</c> statement: its output schema and the
/// query ticket that projects the inserted rows through the ordinary SELECT projector.
///
/// <para><b>Why a SELECT binding.</b> A RETURNING list is a select list over the target table — the
/// same items, aliases, <c>*</c> and scalar expressions. Binding it as <c>SELECT &lt;list&gt; FROM
/// &lt;target&gt;</c> reuses the binder's name validation, the schema builder's type inference and the
/// projector's evaluation, so RETURNING and SELECT cannot disagree about what an expression means or
/// what a column is called. The bind runs under a SELECT requirement, so the per-table privilege
/// check demands SELECT on the target: the list reads stored values, defaults and sequence draws the
/// caller could not otherwise see.</para>
///
/// <para><b>What the list may not hold.</b> No aggregate (one output row belongs to one inserted row),
/// no subquery (the projector evaluates rows synchronously and the bind pipeline's subquery stages
/// are not run here), and no sequence call (a draw would change a sequence for each returned row, and
/// the statement's sequence reservation does not cover the list). Each is refused before anything is
/// written: the first two with <see cref="CamusDBErrorCodes.InvalidInput"/>, a sequence call with
/// <see cref="CamusDBErrorCodes.SequenceCallNotAllowedHere"/>.</para>
///
/// <para><b>Input rows.</b> The inserter reports each row as the values dictionary it encoded. That
/// dictionary holds only the columns the statement or a default supplied, so
/// <see cref="ProjectAsync"/> widens each row to every readable column of the table, in schema order,
/// with NULL for an absent one — the shape a scan of the table would produce, which is what
/// <c>RETURNING *</c> and the schema builder's <c>*</c> expansion both assume.</para>
/// </summary>
internal sealed class InsertReturningPlan
{
    private static readonly QueryProjector Projector = new();

    private readonly QueryTicket projectionTicket;

    private readonly string[] inputColumns;

    private readonly RowLayout inputLayout;

    /// <summary>The output columns, in RETURNING-list order, with <c>*</c> expanded.</summary>
    public IReadOnlyList<DerivedColumnSchema> Columns { get; }

    private InsertReturningPlan(QueryTicket projectionTicket, IReadOnlyList<DerivedColumnSchema> columns, TableSchema schema)
    {
        this.projectionTicket = projectionTicket;
        Columns = columns;

        List<string> readable = new(schema.Columns?.Count ?? 0);
        foreach (TableColumnSchema column in schema.Columns ?? [])
        {
            if (SchemaElementStateRules.IsReadable(column))
                readable.Add(column.Name);
        }

        inputColumns = readable.ToArray();
        inputLayout = RowLayout.ForColumns(inputColumns);
    }

    /// <summary>
    /// The RETURNING list of an INSERT statement, or null when the statement is not an INSERT or has
    /// no RETURNING list. The grammar puts the list in <see cref="NodeAst.extendedTwo"/> for all four
    /// INSERT forms.
    /// </summary>
    internal static NodeAst? GetReturningList(NodeAst ast) =>
        StatementScope.IsWriteReturningRows(ast) ? ast.extendedTwo : null;

    /// <summary>True when <paramref name="ast"/> is an INSERT that returns rows.</summary>
    internal static bool HasReturning(NodeAst ast) => StatementScope.IsWriteReturningRows(ast);

    /// <summary>
    /// Refuses the items a RETURNING list may not hold (see the class summary). Runs on the AST
    /// alone, before the statement reserves sequence values or writes anything.
    /// </summary>
    internal static void Validate(NodeAst returningList)
    {
        foreach (NodeAst item in FlattenItems(returningList))
        {
            NodeAst expression = QueryExpressionClassifier.UnwrapAlias(item);

            if (QueryExpressionClassifier.IsAggregateProjection(expression)
                || QueryExpressionClassifier.IsCompoundAggregateProjection(expression))
            {
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Aggregate functions are not allowed in RETURNING");
            }

            if (QueryTicketAdapter.ContainsSubquery(expression))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "Subqueries are not allowed in RETURNING");

            if (SequenceStatementBinder.ContainsSequenceCall(expression))
                throw new CamusDBException(
                    CamusDBErrorCodes.SequenceCallNotAllowedHere,
                    "Sequence functions are not allowed in RETURNING");
        }
    }

    /// <summary>
    /// Binds <paramref name="returningList"/> as a select list over the table the INSERT targets.
    /// See the class summary for why this is a SELECT binding and why it demands SELECT.
    /// </summary>
    /// <param name="targetAst">The INSERT's target table node, used as the FROM clause.</param>
    internal static async Task<InsertReturningPlan> BindAsync(
        SelectQueryCreator selectQueryCreator,
        QueryBinder queryBinder,
        DatabaseDescriptor database,
        TableDescriptor table,
        NodeAst targetAst,
        NodeAst returningList,
        ExecuteSQLTicket ticket)
    {
        Validate(returningList);

        NodeAst selectAst = new(NodeType.Select, returningList, targetAst, null, null, null, null, null, null);
        SelectQuery query = selectQueryCreator.CreateSelectQuery(selectAst);

        BoundSelectQuery bound;

        // The binder opens the target through the per-table chokepoint, which checks the ambient
        // requirement. Narrowed here, it checks SELECT on the target instead of the statement's INSERT.
        using (AuthorizationContext.WithRequiredPrivilege(Privilege.Select))
            bound = await queryBinder.BindAsync(database, query).ConfigureAwait(false);

        QueryTicket projectionTicket = QueryTicketAdapter.ToQueryTicket(bound, ticket);
        IReadOnlyList<DerivedColumnSchema> columns = DerivedTableSchemaBuilder.Build(query, bound);

        return new InsertReturningPlan(projectionTicket, columns, table.Schema);
    }

    /// <summary>
    /// Projects the inserted rows through the RETURNING list, in insert order. The rows are all in
    /// memory already, so the result is a list rather than a cursor: a transport sends it only after
    /// the statement and its transaction completed.
    /// </summary>
    internal async Task<List<QueryResultRow>> ProjectAsync(List<QueryResultRow> insertedRows)
    {
        List<QueryResultRow> output = new(insertedRows.Count);

        if (insertedRows.Count == 0)
            return output;

        List<QueryResultRow> inputRows = new(insertedRows.Count);

        foreach (QueryResultRow inserted in insertedRows)
        {
            ColumnValue[] values = new ColumnValue[inputColumns.Length];

            for (int i = 0; i < inputColumns.Length; i++)
                values[i] = inserted.Row.TryGetValue(inputColumns[i], out ColumnValue? value) ? value : ColumnValue.Null;

            inputRows.Add(new QueryResultRow(inserted.RowId, new QueryRow(inserted.RowId, inputLayout, values)));
        }

        await foreach (QueryResultRow projected in Projector
            .ProjectResultset(projectionTicket, QueryResultStream.FromRows(inputRows))
            .ConfigureAwait(false))
        {
            output.Add(projected);
        }

        return output;
    }

    /// <summary>
    /// The items of a select list in written order. Iterative, because the list is a left-deep chain
    /// whose depth equals its length.
    /// </summary>
    private static List<NodeAst> FlattenItems(NodeAst list)
    {
        List<NodeAst> items = new();
        Stack<NodeAst> pending = new();
        pending.Push(list);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (node.nodeType != NodeType.IdentifierList)
            {
                items.Add(node);
                continue;
            }

            // Right first, so the left subtree (the earlier items) is popped and emitted first.
            if (node.rightAst is not null)
                pending.Push(node.rightAst);

            if (node.leftAst is not null)
                pending.Push(node.leftAst);
        }

        return items;
    }
}
