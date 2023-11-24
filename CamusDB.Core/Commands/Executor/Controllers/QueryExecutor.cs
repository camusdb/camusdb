
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
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class QueryExecutor
{
    private readonly RowDeserializer rowDeserializer = new();

    public async Task<IAsyncEnumerable<Dictionary<string, ColumnValue>>> Query(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
    {
        using IDisposable readerLock = await table.ReaderWriterLock.ReaderLockAsync();

        if (string.IsNullOrEmpty(ticket.IndexName))
            return QueryUsingTableIndex(database, table, ticket);

        return QueryUsingIndex(database, table, ticket);
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

        BTreeTuple? pageOffset = await index.UniqueRows.Get(columnId);

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

    private async IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryUsingTableIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
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
                if (MeetFilters(ticket.Filters, row))
                    yield return row;
            }
            else
            {
                if (ticket.Where is not null)
                {
                    if (MeetWhere(ticket.Where, row))
                        yield return row;
                }
                else
                    yield return row;
            }
        }
    }

    private async IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryUsingUniqueIndex(DatabaseDescriptor database, TableDescriptor table, BTree<ColumnValue, BTreeTuple?> index, QueryTicket ticket)
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
                if (MeetFilters(ticket.Filters, row))
                    yield return row;
            }
            else
            {
                if (ticket.Where is not null)
                {
                    if (MeetWhere(ticket.Where, row))
                        yield return row;
                }
                else
                    yield return row;
            }
        }
    }

    private bool MeetWhere(NodeAst where, Dictionary<string, ColumnValue> row)
    {
        ColumnValue evaluatedExpr = EvalExpr(where, row);

        switch (evaluatedExpr.Type)
        {
            case ColumnType.Null:
                return false;

            case ColumnType.Bool:
                //Console.WriteLine(evaluatedExpr.Value);
                return evaluatedExpr.Value == "True" || evaluatedExpr.Value == "true";

            case ColumnType.Float:
                if (float.TryParse(evaluatedExpr.Value, out float res))
                {
                    if (res != 0)
                        return true;
                }
                return false;

            case ColumnType.Integer64:
                if (long.TryParse(evaluatedExpr.Value, out long res2))
                {
                    if (res2 != 0)
                        return true;
                }
                return false;
        }

        return false;
    }

    private ColumnValue EvalExpr(NodeAst expr, Dictionary<string, ColumnValue> row)
    {
        switch (expr.nodeType)
        {
            case NodeType.Number:
                return new ColumnValue(ColumnType.Integer64, expr.yytext!);

            case NodeType.String:
                return new ColumnValue(ColumnType.String, expr.yytext!.Trim('"'));

            case NodeType.Identifier:

                if (row.TryGetValue(expr.yytext!, out ColumnValue? columnValue))
                    return columnValue;

                throw new Exception("Not found column: " + expr.yytext!);

            case NodeType.ExprEquals:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, (leftValue.CompareTo(rightValue) == 0).ToString());
                }

            case NodeType.ExprNotEquals:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, (leftValue.CompareTo(rightValue) != 0).ToString());
                }

            default:
                Console.WriteLine("ERROR {0}", expr.nodeType);
                break;
        }

        return new ColumnValue(ColumnType.Null, "");
    }

    private static bool MeetFilters(List<QueryFilter> filters, Dictionary<string, ColumnValue> row)
    {        
        foreach (QueryFilter filter in filters)
        {
            if (string.IsNullOrEmpty(filter.ColumnName))
            {
                Console.WriteLine("Found empty or null column name in filters");
                return false;
            }

            if (!row.TryGetValue(filter.ColumnName, out ColumnValue? value))
                return false;

            switch (filter.Op)
            {
                case "=":
                    if (value.Value != filter.Value.Value)
                        return false;
                    break;

                case "!=":
                    if (value.Value == filter.Value.Value)
                        return false;
                    break;

                default:
                    Console.WriteLine("Unknown operator");
                    break;
            }
        }

        return true;
    }

    private async IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryUsingMultiIndex(DatabaseDescriptor database, TableDescriptor table, BTreeMulti<ColumnValue> index)
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

                yield return rowDeserializer.Deserialize(table.Schema, data);
            }
        }
    }

    private IAsyncEnumerable<Dictionary<string, ColumnValue>> QueryUsingIndex(DatabaseDescriptor database, TableDescriptor table, QueryTicket ticket)
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
