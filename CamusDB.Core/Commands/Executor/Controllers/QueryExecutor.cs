
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Trees;
using CamusDB.Core.BufferPool;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class QueryExecutor
{
    private readonly RowDeserializer rowDeserializer = new();

    private readonly QueryPlanner queryPlanner = new();

    private readonly QueryFilterer queryFilterer = new();

    private readonly QuerySorter querySorter = new();

    public async Task<IAsyncEnumerable<QueryResultRow>> Query(
        DatabaseDescriptor database,
        TableDescriptor table,
        QueryTicket ticket,
        bool noLocking
    )
    {
        QueryPlan plan = queryPlanner.GetPlan(database, table, ticket);

        if (noLocking)
            return ExecuteQueryPlanInternal(plan);

        return await ExecuteQueryPlanWithLocks(plan);
    }

    private async Task<IAsyncEnumerable<QueryResultRow>> ExecuteQueryPlanWithLocks(QueryPlan plan)
    {
        using IDisposable readerLock = await plan.Table.ReaderWriterLock.ReaderLockAsync();

        return ExecuteQueryPlanInternal(plan);
    }

    private IAsyncEnumerable<QueryResultRow> ExecuteQueryPlanInternal(QueryPlan plan)
    {
        foreach (QueryPlanStep step in plan.Steps)
        {
            Console.WriteLine("Executing step {0}", step.Type);

            switch (step.Type)
            {
                case QueryPlanStepType.QueryFromIndex:
                    plan.DataCursor = QueryUsingIndex(plan.Database, plan.Table, plan.Ticket);
                    break;

                case QueryPlanStepType.QueryFromTableIndex:
                    plan.DataCursor = QueryUsingTableIndex(plan.Database, plan.Table, plan.Ticket);
                    break;

                case QueryPlanStepType.SortBy:
                    if (plan.DataCursor is null)
                        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Data cursor is null");

                    plan.DataCursor = querySorter.SortResultset(plan.Ticket, plan.DataCursor);
                    break;

                default:
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown query plan step: " + step.Type);
            }
        }

        if (plan.DataCursor is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Data cursor is null");

        return plan.DataCursor;
    }    

    public async IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryById(DatabaseDescriptor database, TableDescriptor table, QueryByIdTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace;

        using IDisposable readerLock = await table.ReaderWriterLock.ReaderLockAsync();

        if (!table.Indexes.TryGetValue(CamusDBConfig.PrimaryKeyInternalName, out TableIndexSchema? index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Table doesn't have a primary key index"
            );
        }

        if (index.UniqueRows is null)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Table doesn't have a primary key index"
            );
        }

        ColumnValue columnId = new(ColumnType.Id, ticket.Id);

        BTreeTuple? pageOffset = await index.UniqueRows.Get(0, columnId);

        if (pageOffset is null)
        {
            Console.WriteLine("Index Pk={0} does not exist", ticket.Id);
            yield break;
        }

        byte[] data = await tablespace.GetDataFromPage(pageOffset.SlotTwo);
        if (data.Length == 0)
        {
            Console.WriteLine("Index RowId={0} has an empty page data", ticket.Id);
            yield break;
        }

        Console.WriteLine("Got row id {0} from page data {1}", pageOffset.SlotOne, pageOffset.SlotTwo);

        yield return rowDeserializer.Deserialize(table.Schema, data);
    }

    private async IAsyncEnumerable<QueryResultRow> QueryUsingTableIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace;

        await foreach (BTreeEntry<ObjectIdValue, ObjectIdValue> entry in table.Rows.EntriesTraverse())
        {
            if (entry.Value.IsNull())
            {
                Console.WriteLine("Index RowId={0} has no page offset value", entry.Key);
                continue;
            }

            byte[] data = await tablespace.GetDataFromPage(entry.Value);
            if (data.Length == 0)
            {
                Console.WriteLine("Index RowId={0} has an empty page data", entry.Key);
                continue;
            }

            Dictionary<string, ColumnValue> row = rowDeserializer.Deserialize(table.Schema, data);

            if (ticket.Filters is not null && ticket.Filters.Count > 0)
            {
                if (queryFilterer.MeetFilters(ticket.Filters, row))
                    yield return new(new(entry.Key, entry.Value), row);
            }
            else
            {
                if (ticket.Where is not null)
                {
                    if (queryFilterer.MeetWhere(ticket.Where, row))
                        yield return new(new(entry.Key, entry.Value), row);
                }
                else
                    yield return new(new(entry.Key, entry.Value), row);
            }
        }
    }

    private async IAsyncEnumerable<QueryResultRow> QueryUsingUniqueIndex(DatabaseDescriptor database, TableDescriptor table, BTree<ColumnValue, BTreeTuple?> index, QueryTicket ticket)
    {
        BufferPoolHandler tablespace = database.TableSpace;

        await foreach (BTreeEntry<ColumnValue, BTreeTuple?> entry in index.EntriesTraverse())
        {
            if (entry.Value is null)
            {
                Console.WriteLine("Index RowId={0} has no page offset value", entry.Key);
                continue;
            }

            byte[] data = await tablespace.GetDataFromPage(entry.Value.SlotOne);
            if (data.Length == 0)
            {
                Console.WriteLine("Index RowId={0} has an empty page data", entry.Key);
                continue;
            }

            Dictionary<string, ColumnValue> row = rowDeserializer.Deserialize(table.Schema, data);

            if (ticket.Filters is not null && ticket.Filters.Count > 0)
            {
                if (queryFilterer.MeetFilters(ticket.Filters, row))
                    yield return new(entry.Value, row);
            }
            else
            {
                if (ticket.Where is not null)
                {
                    if (queryFilterer.MeetWhere(ticket.Where, row))
                        yield return new(entry.Value, row);
                }
                else
                    yield return new(entry.Value, row);
            }
        }
    }    

    private async IAsyncEnumerable<QueryResultRow> QueryUsingMultiIndex(DatabaseDescriptor database, TableDescriptor table, BTreeMulti<ColumnValue> index)
    {
        BufferPoolHandler tablespace = database.TableSpace;

        foreach (BTreeMultiEntry<ColumnValue> entry in index.EntriesTraverse())
        {
            //Console.WriteLine("MultiTree={0} Key={0} PageOffset={1}", index.Id, entry.Key, entry.Value!.Size());

            await foreach (BTreeEntry<ObjectIdValue, ObjectIdValue> subEntry in entry.Value!.EntriesTraverse())
            {
                //Console.WriteLine(" > Index Key={0} PageOffset={1}", subEntry.Key, subEntry.Value);

                if (subEntry.Value.IsNull())
                {
                    Console.WriteLine("Index RowId={0} has no page offset value", subEntry.Key);
                    continue;
                }

                byte[] data = await tablespace.GetDataFromPage(subEntry.Value);
                if (data.Length == 0)
                {
                    Console.WriteLine("Index RowId={0} has an empty page data", subEntry.Key);
                    continue;
                }

                yield return new(new(subEntry.Key, subEntry.Value), rowDeserializer.Deserialize(table.Schema, data));
            }
        }
    }

    private IAsyncEnumerable<QueryResultRow> QueryUsingIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        if (!table.Indexes.TryGetValue(ticket.IndexName!, out TableIndexSchema? index))
        {
            throw new CamusDBException(
                CamusDBErrorCodes.UnknownKey,
                "Key '" + ticket.IndexName! + "' doesn't exist in table '" + table.Name + "'"
            );
        }

        if (index.Type == IndexType.Unique)
            return QueryUsingUniqueIndex(database, table, index.UniqueRows!, ticket);

        return QueryUsingMultiIndex(database, table, index.MultiRows!);
    }
}
