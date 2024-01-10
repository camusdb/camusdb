
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Models;

public sealed class QueryPlan
{
	public List<QueryPlanStep> Steps { get; } = new();

	public DatabaseDescriptor Database { get; }

	public TableDescriptor Table { get; }

	public QueryTicket Ticket { get; }

	public IAsyncEnumerable<QueryResultRow>? DataCursor { get; set; }

    public QueryPlan(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
	{
		Database = database;
		Table = table;
		Ticket = ticket;
	}

	public void AddStep(QueryPlanStep step)
	{
		Steps.Add(step);
	}    
}
