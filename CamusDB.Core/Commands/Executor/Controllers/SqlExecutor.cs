﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

internal sealed class SqlExecutor
{
    public SqlExecutor()
    {

    }

    public QueryTicket CreateQueryTicket(ExecuteSQLTicket ticket)
    {
        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql);

        switch (ast.nodeType)
        {
            case NodeType.Select:

                string tableName = ast.rightAst!.yytext!;

                return new(ticket.DatabaseName, tableName, null, null, ast.extendedOne, GetQueryClause(ast));

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown query AST stmt: " + ast.nodeType);
        }
    }

    private static List<QueryOrderBy>? GetQueryClause(NodeAst ast)
    {
        if (ast.extendedTwo is null)
            return null;

        List<QueryOrderBy> orderClauses = new();

        foreach (string orderByColumn in GetIdentifierList(ast.extendedTwo))
            orderClauses.Add(new QueryOrderBy(orderByColumn, QueryOrderByType.Ascending));

        return orderClauses;
    }

    internal UpdateTicket CreateUpdateTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing columns list to update");

        Dictionary<string, ColumnValue> values = new();

        List<(string, ColumnValue)> updateItemList = GetUpdateItemList(ast.rightAst);

        foreach ((string, ColumnValue) x in updateItemList)
            values[x.Item1] = x.Item2;

        return new(ticket.DatabaseName, tableName, values, null);
    }

    private static List<(string, ColumnValue)> GetUpdateItemList(NodeAst updateItemList)
    {
        if (updateItemList.nodeType == NodeType.UpdateItem)
            return new() { (updateItemList.leftAst!.yytext ?? "", EvalExpr(updateItemList.rightAst!, new())) };

        if (updateItemList.nodeType == NodeType.UpdateList)
        {
            List<(string, ColumnValue)> allUpdateItems = new();

            if (updateItemList.leftAst is not null)
                allUpdateItems.AddRange(GetUpdateItemList(updateItemList.leftAst));

            if (updateItemList.rightAst is not null)
                allUpdateItems.AddRange(GetUpdateItemList(updateItemList.rightAst));

            return allUpdateItems;
        }        

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid update values list");
    }    

    private static List<string> GetIdentifierList(NodeAst orderByAst)
    {
        if (orderByAst.nodeType == NodeType.Identifier)
            return new() { orderByAst.yytext ?? "" };

        if (orderByAst.nodeType == NodeType.IdentifierList)
        {
            List<string> allIdentifiers = new();

            if (orderByAst.leftAst is not null)
                allIdentifiers.AddRange(GetIdentifierList(orderByAst.leftAst));

            if (orderByAst.rightAst is not null)
                allIdentifiers.AddRange(GetIdentifierList(orderByAst.rightAst));

            return allIdentifiers;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid order by clause");
    }

    private static ColumnValue EvalExpr(NodeAst expr, Dictionary<string, ColumnValue> row)
    {
        switch (expr.nodeType)
        {
            case NodeType.Number:
                return new ColumnValue(ColumnType.Integer64, expr.yytext!);

            case NodeType.String:
                return new ColumnValue(ColumnType.String, expr.yytext!.Trim('"'));

            case NodeType.Bool:
                return new ColumnValue(ColumnType.Bool, expr.yytext!);

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

            case NodeType.ExprLessThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, (leftValue.CompareTo(rightValue) < 0).ToString());
                }

            case NodeType.ExprGreaterThan:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, (leftValue.CompareTo(rightValue) > 0).ToString());
                }

            case NodeType.ExprOr:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, (leftValue.Value.ToLowerInvariant() == "true" || rightValue.Value.ToLowerInvariant() == "true").ToString());
                }

            case NodeType.ExprAnd:
                {
                    ColumnValue leftValue = EvalExpr(expr.leftAst!, row);
                    ColumnValue rightValue = EvalExpr(expr.rightAst!, row);

                    return new ColumnValue(ColumnType.Bool, (leftValue.Value.ToLowerInvariant() == "true" && rightValue.Value.ToLowerInvariant() == "true").ToString());
                }

            default:
                Console.WriteLine("ERROR {0}", expr.nodeType);
                break;
        }

        return new ColumnValue(ColumnType.Null, "");
    }
}
