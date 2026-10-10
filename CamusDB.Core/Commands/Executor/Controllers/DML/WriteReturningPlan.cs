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
/// The bound RETURNING list of one <c>INSERT</c>, <c>UPDATE</c> or <c>DELETE</c> statement: its output
/// schema and the query ticket that projects the written rows through the ordinary SELECT projector.
///
/// <para><b>Why a SELECT binding.</b> A RETURNING list is a select list over the target table — the
/// same items, aliases, <c>*</c> and scalar expressions. Binding it as <c>SELECT &lt;list&gt; FROM
/// &lt;target&gt;</c> reuses the binder's name validation, the schema builder's type inference and the
/// projector's evaluation, so RETURNING and SELECT cannot disagree about what an expression means or
/// what a column is called. The bind runs under a SELECT requirement, so the per-table privilege
/// check demands SELECT on the target: the list reads stored values, defaults and sequence draws the
/// caller could not otherwise see.</para>
///
/// <para><b>What the list may not hold.</b> No aggregate (one output row belongs to one written row),
/// no subquery (the projector evaluates rows synchronously and the bind pipeline's subquery stages
/// are not run here), and no sequence call (a draw would change a sequence for each returned row, and
/// the statement's sequence reservation does not cover the list). Each is refused before anything is
/// written: the first two with <see cref="CamusDBErrorCodes.InvalidInput"/>, a sequence call with
/// <see cref="CamusDBErrorCodes.SequenceCallNotAllowedHere"/>.</para>
///
/// <para><b>Input rows.</b> Each input row is a values dictionary: the row an INSERT encoded, the new
/// image an UPDATE wrote, or the row a DELETE removed. An UPDATE returns the new image only. The
/// dictionary can hold fewer columns than the table: an INSERT holds only the columns the statement or
/// a default supplied, and an UPDATE or a DELETE decodes only the columns it needs. So
/// <see cref="ProjectAsync"/> reshapes each row into the input layout of the projection, with NULL for
/// an absent column. For <c>RETURNING *</c> that layout is every readable column of the table, in schema
/// order — the shape a full scan produces, which the schema builder's <c>*</c> expansion assumes. For
/// any other list it is only <see cref="RequiredColumns"/>, in schema order — the shape a narrowed
/// SELECT scan produces — so a narrow list over a wide table copies a few cells per row, not one per
/// table column. An UPDATE or a DELETE must decode <see cref="RequiredColumns"/>, or a column the list
/// reads comes back NULL with no error.</para>
/// </summary>
internal sealed class WriteReturningPlan
{
    private static readonly QueryProjector Projector = new();

    private readonly QueryTicket projectionTicket;

    private readonly string[] inputColumns;

    private readonly RowLayout inputLayout;

    /// <summary>The output columns, in RETURNING-list order, with <c>*</c> expanded.</summary>
    public IReadOnlyList<DerivedColumnSchema> Columns { get; }

    /// <summary>
    /// The schema-cased columns the list reads, or null when it reads every column (a <c>*</c>, or a
    /// name that does not match a column of the current schema exactly). An UPDATE or a DELETE adds
    /// these to the columns it decodes from each written row. Computed by the same analysis a SELECT
    /// scan uses to narrow its decode, so a qualified name resolves as it does in a SELECT.
    /// </summary>
    public IReadOnlySet<string>? RequiredColumns { get; }

    private WriteReturningPlan(QueryTicket projectionTicket, IReadOnlyList<DerivedColumnSchema> columns, TableSchema schema)
    {
        this.projectionTicket = projectionTicket;
        Columns = columns;

        RequiredColumns = RequiredColumnAnalyzer.ComputeSingleTable(projectionTicket) is { } referenced
            ? MutationRowRecheck.ToSchemaColumns(schema, referenced)
            : null;

        // The input layout: every readable column for a list that reads every column, otherwise only
        // the columns the list reads. Both keep schema order, as a scan's layout does.
        List<string> input = new(RequiredColumns?.Count ?? schema.Columns?.Count ?? 0);
        foreach (TableColumnSchema column in schema.Columns ?? [])
        {
            if (SchemaElementStateRules.IsReadable(column) && (RequiredColumns is null || RequiredColumns.Contains(column.Name)))
                input.Add(column.Name);
        }

        inputColumns = input.ToArray();
        inputLayout = RowLayout.ForColumns(inputColumns);
    }

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
    /// Binds <paramref name="returningList"/> as a select list over the table the statement writes.
    /// See the class summary for why this is a SELECT binding and why it demands SELECT.
    /// </summary>
    /// <param name="targetAst">The statement's target table node, used as the FROM clause.</param>
    internal static async Task<WriteReturningPlan> BindAsync(
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
        // requirement. Narrowed here, it checks SELECT on the target instead of the statement's own
        // INSERT, UPDATE or DELETE privilege.
        using (AuthorizationContext.WithRequiredPrivilege(Privilege.Select))
            bound = await queryBinder.BindAsync(database, query).ConfigureAwait(false);

        QueryTicket projectionTicket = QueryTicketAdapter.ToQueryTicket(bound, ticket);
        IReadOnlyList<DerivedColumnSchema> columns = DerivedTableSchemaBuilder.Build(query, bound);

        return new WriteReturningPlan(projectionTicket, columns, table.Schema);
    }

    /// <summary>
    /// Projects the written rows through the RETURNING list, in the given order. The rows are all in
    /// memory already, so the result is a list rather than a cursor: a transport sends it only after
    /// the statement and its transaction completed.
    /// </summary>
    internal async Task<List<QueryResultRow>> ProjectAsync(List<QueryResultRow> writtenRows)
    {
        List<QueryResultRow> output = new(writtenRows.Count);
        await ProjectIntoAsync(writtenRows, output).ConfigureAwait(false);
        return output;
    }

    /// <summary>Projects <paramref name="writtenRows"/> and appends the output rows to <paramref name="output"/>.</summary>
    private async Task ProjectIntoAsync(List<QueryResultRow> writtenRows, List<QueryResultRow> output)
    {
        if (writtenRows.Count == 0)
            return;

        List<QueryResultRow> inputRows = new(writtenRows.Count);

        foreach (QueryResultRow written in writtenRows)
        {
            ColumnValue[] values = new ColumnValue[inputColumns.Length];

            for (int i = 0; i < inputColumns.Length; i++)
                values[i] = written.Row.TryGetValue(inputColumns[i], out ColumnValue? value) ? value : ColumnValue.Null;

            inputRows.Add(new QueryResultRow(written.RowId, new QueryRow(written.RowId, inputLayout, values)));
        }

        await foreach (QueryResultRow projected in Projector
            .ProjectResultset(projectionTicket, QueryResultStream.FromRows(inputRows))
            .ConfigureAwait(false))
        {
            output.Add(projected);
        }
    }

    /// <summary>
    /// Starts the output buffer of one UPDATE or DELETE attempt. See <see cref="ReturningRowCollector"/>.
    /// </summary>
    internal ReturningRowCollector CreateCollector() => new(this);

    /// <summary>
    /// The output buffer of one UPDATE or DELETE attempt. The write path hands it each chunk of rows
    /// right after the chunk's batch write succeeded, and the collector projects the chunk at once:
    /// the buffer then keeps only the output columns, not the decoded rows, so a statement that
    /// returns one small column from wide rows holds that column alone until the commit.
    ///
    /// <para>One collector belongs to one attempt. A retried attempt gets a new one, so it never
    /// returns a row from the attempt that failed. A chunk whose write recorded a retryable abort is
    /// never handed over, and a statement that recorded one returns no rows at all.</para>
    /// </summary>
    internal sealed class ReturningRowCollector
    {
        private readonly WriteReturningPlan plan;

        private readonly List<QueryResultRow> rows = new();

        internal ReturningRowCollector(WriteReturningPlan plan) => this.plan = plan;

        /// <summary>The columns the write path must decode from each written row; see <see cref="WriteReturningPlan.RequiredColumns"/>.</summary>
        public IReadOnlySet<string>? RequiredColumns => plan.RequiredColumns;

        /// <summary>The projected output rows, in write order.</summary>
        public List<QueryResultRow> Rows => rows;

        /// <summary>Projects one written chunk and appends its output rows.</summary>
        public Task AddChunkAsync(List<QueryResultRow> writtenRows) => plan.ProjectIntoAsync(writtenRows, rows);
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
