
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QuerySorter
{
    internal async IAsyncEnumerable<QueryResultRow> SortResultset(
        QueryTicket ticket,
        IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.OrderBy is null || ticket.OrderBy.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid internal sort context");

        List<QueryResultRow> rows = new();
        await foreach (QueryResultRow row in dataCursor.ConfigureAwait(false))
            rows.Add(row);

        ValidateSortKeys(ticket.OrderBy, rows);
        rows.Sort(new QueryResultRowOrderComparer(ticket.OrderBy));

        foreach (QueryResultRow row in rows)
            yield return row;
    }

    private static void ValidateSortKeys(IReadOnlyList<QueryOrderBy> orderBy, IReadOnlyList<QueryResultRow> rows)
    {
        foreach (QueryResultRow row in rows)
        {
            foreach (QueryOrderBy clause in orderBy)
                _ = GetSortValue(row, clause.ColumnName);
        }
    }

    private static ColumnValue GetSortValue(QueryResultRow row, string columnName)
    {
        // Joins key rows by the qualified name ("u.position") — try that first.
        if (row.Row.TryGetValue(columnName, out ColumnValue? value))
            return value;

        // Single-table scan rows are keyed by the bare column name, so an alias-qualified
        // ORDER BY column ("u.position") must fall back to its bare form ("position").
        int dot = columnName.LastIndexOf('.');
        if (dot >= 0 && dot < columnName.Length - 1
            && row.Row.TryGetValue(columnName[(dot + 1)..], out value))
            return value;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"Sort column '{columnName}' is missing from result row");
    }

    private sealed class QueryResultRowOrderComparer : IComparer<QueryResultRow>
    {
        private readonly IReadOnlyList<QueryOrderBy> orderBy;

        public QueryResultRowOrderComparer(IReadOnlyList<QueryOrderBy> orderBy)
        {
            this.orderBy = orderBy;
        }

        public int Compare(QueryResultRow left, QueryResultRow right)
        {
            foreach (QueryOrderBy clause in orderBy)
            {
                ColumnValue leftValue = GetSortValue(left, clause.ColumnName);
                ColumnValue rightValue = GetSortValue(right, clause.ColumnName);
                int comparison = leftValue.CompareTo(rightValue);

                if (comparison == 0)
                    continue;

                return clause.Type == OrderType.Ascending ? comparison : -comparison;
            }

            return 0;
        }
    }
}
