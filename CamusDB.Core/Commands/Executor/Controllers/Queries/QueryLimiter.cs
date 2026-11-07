
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QueryLimiter
{
    /// <summary>
    /// The error for a LIMIT or OFFSET parameter that is not an integer. The grammar takes only an
    /// integer literal or a placeholder here, so the value is what the client bound, and this is client
    /// input, not an engine fault. A float is refused, not rounded: GoogleSQL requires an integer here,
    /// and the row count must not depend on a conversion the statement did not ask for.
    /// </summary>
    internal static CamusDBException NotAnInteger(string clause, ColumnValue value) =>
        new(CamusDBErrorCodes.InvalidInput, $"{clause} must be an integer, got {value.Type}");

    /// <summary>
    /// The error for a negative LIMIT or OFFSET, with the message of PostgreSQL. A negative count used
    /// to give an answer: <c>LIMIT -1</c> returned no rows, <c>OFFSET -1</c> skipped none, and
    /// <c>LIMIT 2 OFFSET -1</c> returned one row. PostgreSQL refuses the statement.
    /// </summary>
    internal static CamusDBException Negative(string clause) =>
        new(CamusDBErrorCodes.InvalidInput, $"{clause} must not be negative");

    /// <summary>
    /// Reads the value of a LIMIT or OFFSET clause: an integer literal or a parameter. Every place that
    /// reads one of these clauses calls this, so a value that is not an integer, or that is negative,
    /// fails the statement the same way wherever the clause is read.
    /// </summary>
    internal static long ReadRowCount(NodeAst node, Dictionary<string, ColumnValue>? parameters, string clause)
    {
        ColumnValue value = SqlExecutor.EvalExpr(node, new Dictionary<string, ColumnValue>(), parameters);
        if (value.Type != ColumnType.Integer64)
            throw NotAnInteger(clause, value);

        if (value.LongValue < 0)
            throw Negative(clause);

        return value.LongValue;
    }

    /// <summary>
    /// Refuses a negative integer literal in the LIMIT or OFFSET of <paramref name="query"/>. The binder
    /// calls this for every SELECT it binds, so the statement fails before any row is read, also when
    /// the clause sits in a subquery that no row reaches. A parameter has no value at bind;
    /// <see cref="ReadRowCount"/> refuses it when the clause is read.
    /// </summary>
    internal static void RefuseNegativeLiteral(SelectQuery query)
    {
        RefuseNegativeLiteral(query.Limit, "LIMIT");
        RefuseNegativeLiteral(query.Offset, "OFFSET");
    }

    private static void RefuseNegativeLiteral(NodeAst? node, string clause)
    {
        // A literal too large for INT64 is left to the evaluator, which reports the overflow.
        if (node is { nodeType: NodeType.Integer }
            && long.TryParse(node.yytext, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long value)
            && value < 0)
            throw Negative(clause);
    }

    /// <summary>
    /// Applies LIMIT and/or OFFSET to the cursor. Either clause may be present alone:
    /// a missing OFFSET skips nothing and a missing LIMIT streams every remaining row
    /// (offset-only queries like <c>SELECT … OFFSET 20</c> are valid SQL).
    /// </summary>
    internal IAsyncEnumerable<QueryResultRow> LimitResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.Limit is null && ticket.Offset is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid internal limit context");

        return LimitOffsetResultset(ticket, dataCursor);
    }

    private static async IAsyncEnumerable<QueryResultRow> LimitOffsetResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        long? limit = null;
        long offset = 0;

        if (ticket.Limit is not null)
            limit = ReadRowCount(ticket.Limit, ticket.Parameters, "LIMIT");

        if (ticket.Offset is not null)
            offset = ReadRowCount(ticket.Offset, ticket.Parameters, "OFFSET");

        // The input is lazy, so every row pulled from it is work done upstream — for a join, finding
        // one more match can mean reading much of its right side. The loop therefore stops as soon as
        // it has yielded the last row it needs, and LIMIT 0 pulls nothing, instead of pulling one row
        // past the limit only to discard it.
        if (limit == 0)
            yield break;

        // The position after the last row to return. It saturates: a LIMIT near long.MaxValue plus an
        // OFFSET must not wrap to a negative end, which would stop the query after its first row.
        long end = limit is null || limit.Value > long.MaxValue - offset
            ? long.MaxValue
            : offset + limit.Value;

        long count = 0;

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            if (count >= offset)
                yield return resultRow;

            count++;

            if (count >= end)
                yield break;
        }
    }
}