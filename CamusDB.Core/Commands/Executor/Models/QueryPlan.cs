
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Models;

public enum QueryPlanStepType
{
	QueryFromIndex,
	QueryFromTableIndex,
	SortBy
}

public readonly struct QueryPlanStep
{
	public QueryPlanStepType Type { get; }	

	public QueryPlanStep(QueryPlanStepType type)
	{
		Type = type;		
	}
}

public sealed class QueryPlan
{
	public List<QueryPlanStep> Steps { get; } = new();

	public DatabaseDescriptor Database { get; }

	public TableDescriptor Table { get; }

	public QueryTicket Ticket { get; }

	public IAsyncEnumerable<Dictionary<string, ColumnValue>>? DataCursor { get; set; }

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
