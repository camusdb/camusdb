
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Comparers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal sealed class QuerySorter
{
    // @todo rewrite this method to support any level of sorting
    internal async IAsyncEnumerable<QueryResultRow> SortResultset(QueryTicket ticket, IAsyncEnumerable<QueryResultRow> dataCursor)
    {
        if (ticket.OrderBy is null || ticket.OrderBy.Count == 0)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid internal sort context");

        if (ticket.OrderBy.Count > 2)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "High number of order clauses is not supported");

        string firstSortColumn = ticket.OrderBy[0].ColumnName;
        string secondSortColumn = ticket.OrderBy.Count > 1 ? ticket.OrderBy[1].ColumnName : "id"; // @todo many tables won't have an id column

        SortedDictionary<ColumnValue, SortedDictionary<ColumnValue, List<QueryResultRow>>> sortedRows;

        if (ticket.OrderBy[0].Type == OrderType.Ascending)
            sortedRows = new();
        else
            sortedRows = new(new DescendingComparer<ColumnValue>());

        await foreach (QueryResultRow resultRow in dataCursor)
        {
            Dictionary<string, ColumnValue> row = resultRow.Row;

            if (!row.TryGetValue(firstSortColumn, out ColumnValue? firstSortColumnValue))
                continue;

            if (!row.TryGetValue(secondSortColumn, out ColumnValue? secondSortColumnValue))
                continue;

            if (sortedRows.TryGetValue(firstSortColumnValue, out SortedDictionary<ColumnValue, List<QueryResultRow>>? existingSortGroup))
            {
                if (existingSortGroup.TryGetValue(secondSortColumnValue, out List<QueryResultRow>? innerSortGroup))
                    innerSortGroup.Add(resultRow);
                else
                    existingSortGroup.Add(secondSortColumnValue, new() { resultRow });
            }
            else
            {
                SortedDictionary<ColumnValue, List<QueryResultRow>> secondSortGroup;
                
                if (ticket.OrderBy.Count == 1 || ticket.OrderBy[1].Type == OrderType.Ascending)
                    secondSortGroup = new()
                    {
                        { secondSortColumnValue, new() { resultRow } }
                    };
                else
                    secondSortGroup = new(new DescendingComparer<ColumnValue>())
                    {
                        { secondSortColumnValue, new() { resultRow } }
                    };

                sortedRows.Add(firstSortColumnValue, secondSortGroup);
            }
        }

        foreach (KeyValuePair<ColumnValue, SortedDictionary<ColumnValue, List<QueryResultRow>>> sortedGroup in sortedRows)
        {
            foreach (KeyValuePair<ColumnValue, List<QueryResultRow>> secondSortGroup in sortedGroup.Value)
            {
                foreach (QueryResultRow sortedRow in secondSortGroup.Value)
                    yield return sortedRow;
            }
        }
    }
}
