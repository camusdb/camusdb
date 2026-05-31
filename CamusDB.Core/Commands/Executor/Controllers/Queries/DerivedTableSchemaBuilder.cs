
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal static class DerivedTableSchemaBuilder
{
    public static IReadOnlyList<DerivedColumnSchema> Build(SelectQuery query, BoundSelectQuery innerBound)
    {
        List<DerivedColumnSchema> columns = new(query.Projections.Count);
        QueryRowNameResolver innerResolver = new(innerBound.Sources, innerBound.DerivedSources);

        for (int i = 0; i < query.Projections.Count; i++)
        {
            ProjectionItem projection = query.Projections[i];
            string name = QueryProjectionResolver.GetOutputNameFromProjectionExpression(projection.Expression, i);
            ColumnType type = InferType(projection.Expression, innerBound, innerResolver);
            columns.Add(new DerivedColumnSchema(name, type));
        }

        return columns;
    }

    private static ColumnType InferType(
        NodeAst expression,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver)
    {
        NodeAst target = QueryExpressionClassifier.UnwrapAlias(expression);

        if (target.nodeType == NodeType.Identifier && target.yytext is not null)
        {
            string lookupKey = innerResolver.ResolveRowLookupKey(target.yytext);
            return ResolveColumnType(innerBound, lookupKey, target.yytext);
        }

        if (target.nodeType == NodeType.ExprFuncCall)
            return InferAggregateType(target, innerBound, innerResolver);

        return ColumnType.String;
    }

    private static ColumnType InferAggregateType(
        NodeAst funcCall,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver)
    {
        string funcName = funcCall.leftAst!.yytext!.ToLowerInvariant();

        return funcName switch
        {
            "count" => ColumnType.Integer64,
            "sum" or "avg" => InferAggregateArgumentType(funcCall, innerBound, innerResolver, fallback: ColumnType.Float64),
            "min" or "max" => InferAggregateArgumentType(funcCall, innerBound, innerResolver, fallback: ColumnType.String),
            _ => ColumnType.String,
        };
    }

    private static ColumnType InferAggregateArgumentType(
        NodeAst funcCall,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver,
        ColumnType fallback)
    {
        if (funcCall.rightAst is null)
            return fallback;

        NodeAst argument = funcCall.rightAst.nodeType == NodeType.ExprArgumentList
            ? funcCall.rightAst.leftAst ?? funcCall.rightAst
            : funcCall.rightAst;

        if (argument.nodeType == NodeType.ExprAllFields)
            return ColumnType.Integer64;

        return InferType(argument, innerBound, innerResolver);
    }

    private static ColumnType ResolveColumnType(BoundSelectQuery innerBound, string lookupKey, string originalIdentifier)
    {
        foreach (BoundTableSource source in innerBound.Sources)
        {
            foreach (TableColumnSchema column in source.Table.Schema.Columns ?? [])
            {
                if (column.Name == lookupKey)
                    return column.Type;
            }
        }

        foreach (BoundDerivedTableSource source in innerBound.DerivedSources)
        {
            if (source.HasColumn(lookupKey))
                return source.GetColumnType(lookupKey);

            if (TrySplitQualified(originalIdentifier, out _, out string bareName) && source.HasColumn(bareName))
                return source.GetColumnType(bareName);
        }

        return ColumnType.String;
    }

    private static bool TrySplitQualified(string identifier, out string alias, out string columnName)
    {
        int dotIndex = identifier.IndexOf('.');

        if (dotIndex <= 0 || dotIndex >= identifier.Length - 1)
        {
            alias = "";
            columnName = identifier;
            return false;
        }

        alias = identifier[..dotIndex];
        columnName = identifier[(dotIndex + 1)..];
        return true;
    }
}
