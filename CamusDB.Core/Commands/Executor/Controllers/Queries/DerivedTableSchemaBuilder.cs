
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
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

        // Compound aggregate check is hoisted before nodeType dispatch so arithmetic-topped
        // compounds (ExprAdd, ExprDiv, …) are also handled — not just ExprFuncCall-wrapped ones.
        if (QueryExpressionClassifier.IsCompoundAggregateProjection(target))
            return InferCompoundAggregateType(target, innerBound, innerResolver);

        if (target.nodeType == NodeType.Identifier && target.yytext is not null)
        {
            string lookupKey = innerResolver.ResolveRowLookupKey(target.yytext);
            return ResolveColumnType(innerBound, lookupKey, target.yytext);
        }

        if (target.nodeType == NodeType.ExprFuncCall)
        {
            if (QueryExpressionClassifier.IsAggregateProjection(target))
                return InferAggregateType(target, innerBound, innerResolver);

            string funcName = target.leftAst!.yytext!.ToLowerInvariant();

            if (ScalarFunctionEvaluator.IsRegisteredScalarFunction(funcName))
                return ScalarFunctionEvaluator.InferReturnType(funcName, []);

            return ColumnType.String;
        }

        if (target.nodeType == NodeType.ExprCast)
            return CastScalarFunctions.InferCastReturnType(target.rightAst!);

        return ColumnType.String;
    }

    /// <summary>
    /// Infers the return type of a compound aggregate expression (aggregate nested inside a
    /// non-aggregate node). Two shapes are handled:
    /// <list type="bullet">
    ///   <item>Scalar function wrapper such as <c>COALESCE(SUM(x), 0)</c>: collects the inferred
    ///     type of each argument and forwards them to
    ///     <see cref="ScalarFunctionEvaluator.InferReturnType"/> so the outer function returns
    ///     the real type instead of <see cref="ColumnType.Null"/>.</item>
    ///   <item>Arithmetic binary op such as <c>SUM(a)+1</c> or <c>SUM(a)/SUM(b)</c>: infers
    ///     both operand types and returns the wider numeric type.</item>
    /// </list>
    /// </summary>
    private static ColumnType InferCompoundAggregateType(
        NodeAst target,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver)
    {
        if (target.nodeType == NodeType.ExprFuncCall)
        {
            string funcName = target.leftAst!.yytext!.ToLowerInvariant();
            if (ScalarFunctionEvaluator.IsRegisteredScalarFunction(funcName))
            {
                ColumnType[] argTypes = InferArgListTypes(target.rightAst, innerBound, innerResolver);
                return ScalarFunctionEvaluator.InferReturnType(funcName, argTypes);
            }

            return ColumnType.String;
        }

        // Arithmetic binary op: return the wider numeric type of both operands.
        ColumnType left  = target.leftAst  is not null ? InferArgType(target.leftAst,  innerBound, innerResolver) : ColumnType.Null;
        ColumnType right = target.rightAst is not null ? InferArgType(target.rightAst, innerBound, innerResolver) : ColumnType.Null;
        return WiderNumericType(left, right);
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

    /// <summary>
    /// Collects the inferred <see cref="ColumnType"/> for each argument in a function's argument
    /// list. Used to pass real argument types to <see cref="ScalarFunctionEvaluator.InferReturnType"/>
    /// for compound-aggregate expressions so the outer function (e.g. COALESCE) does not receive an
    /// empty list and fall back to <see cref="ColumnType.Null"/>.
    /// </summary>
    private static ColumnType[] InferArgListTypes(
        NodeAst? argList,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver)
    {
        if (argList is null)
            return [];

        List<ColumnType> types = new();
        CollectArgTypes(argList, innerBound, innerResolver, types);
        return types.ToArray();
    }

    private static void CollectArgTypes(
        NodeAst argNode,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver,
        List<ColumnType> types)
    {
        if (argNode.nodeType == NodeType.ExprArgumentList)
        {
            if (argNode.leftAst is not null)
                CollectArgTypes(argNode.leftAst, innerBound, innerResolver, types);

            if (argNode.rightAst is not null)
                CollectArgTypes(argNode.rightAst, innerBound, innerResolver, types);

            return;
        }

        types.Add(InferArgType(argNode, innerBound, innerResolver));
    }

    /// <summary>
    /// Infers the <see cref="ColumnType"/> of a single expression node when it appears as a
    /// function argument or arithmetic operand. Fast-paths for literals and identifiers avoid
    /// re-entering the compound-aggregate logic; the default case delegates to
    /// <see cref="InferType"/> so nested compounds (e.g. COALESCE inside another COALESCE)
    /// and bare aggregate calls are resolved correctly.
    /// </summary>
    private static ColumnType InferArgType(
        NodeAst arg,
        BoundSelectQuery innerBound,
        QueryRowNameResolver innerResolver)
    {
        return arg.nodeType switch
        {
            NodeType.Integer    => ColumnType.Integer64,
            NodeType.Float      => ColumnType.Float64,
            NodeType.String     => ColumnType.String,
            NodeType.Bool       => ColumnType.Bool,
            NodeType.Null       => ColumnType.Null,
            NodeType.Identifier => arg.yytext is not null
                ? ResolveColumnType(innerBound, innerResolver.ResolveRowLookupKey(arg.yytext), arg.yytext)
                : ColumnType.String,
            // All other nodes (bare aggregates, compounds, scalar funcs, casts) go through
            // InferType so type inference is consistent and recursive.
            _ => InferType(arg, innerBound, innerResolver),
        };
    }

    /// <summary>
    /// Returns the wider of two types for arithmetic inference: Float64 &gt; Float32 &gt;
    /// Integer64. Falls back to String for non-numeric or mismatched operands.
    /// </summary>
    private static ColumnType WiderNumericType(ColumnType a, ColumnType b)
    {
        if (a == ColumnType.Float64 || b == ColumnType.Float64) return ColumnType.Float64;
        if (a == ColumnType.Float32 || b == ColumnType.Float32) return ColumnType.Float32;
        if (a == ColumnType.Integer64 || b == ColumnType.Integer64) return ColumnType.Integer64;
        return ColumnType.String;
    }

    private static ColumnType ResolveColumnType(BoundSelectQuery innerBound, string lookupKey, string originalIdentifier)
    {
        foreach (BoundTableSource source in innerBound.Sources)
        {
            foreach (TableColumnSchema column in source.Table.Schema.Columns ?? [])
            {
                if (column.Name == lookupKey && SchemaElementStateRules.IsReadable(column))
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
