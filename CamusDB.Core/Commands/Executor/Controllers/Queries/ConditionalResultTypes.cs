/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Gives CASE, COALESCE, IFNULL (and its alias NVL) and IF one result type by
/// <see cref="ResultTypeRule"/>, and refuses one whose inputs have no common type before any row is
/// read. These are the <em>conditional</em> expressions: each returns one of several inputs.
///
/// <para><b>Two moments.</b></para>
/// <list type="number">
/// <item><b>When a statement is bound</b> (<see cref="Check"/>), every conditional expression in it
///   is typed from the statement: literals, casts, column references through a
///   <see cref="ColumnTypeLookup"/>, arithmetic over those, and nested conditionals. A pair with no
///   common type is <see cref="CamusDBErrorCodes.InvalidInput"/>, even when the branch never runs and
///   over an empty table, as in GoogleSQL. The result type is recorded on the binding's
///   <see cref="QueryRowNameResolver"/> when there is one.</item>
/// <item><b>When the expression is evaluated</b> (<see cref="Apply"/>), the value it returns is
///   combined with the recorded type, or, without a record, with the types of its literal inputs, and
///   converted to the result (<see cref="ResultTypeRule.Widen"/>). An input whose type the statement
///   does not show (a parameter, a function call) is checked here, against the inputs whose types are
///   known.</item>
/// </list>
///
/// <para><b>Why the binding holds the result type, and not the node.</b> The parse cache shares one
/// tree between every execution of one SQL text, also on another database or after an ALTER, where a
/// column has another type. A type that depends on the schema therefore cannot be stored on the node.
/// The literal-only type (<see cref="LiteralResultType"/>) depends on the text alone, so it is cached
/// per node.</para>
///
/// <para><b>Unknown is not String.</b> The column metadata builder
/// (<see cref="DerivedTableSchemaBuilder"/>) types an expression it cannot type as String. A refusal
/// built on that would refuse correct statements, so the typer here returns null for unknown, and an
/// unknown input is left to the check at evaluation. A derived-table column declared String is read
/// as unknown for the same reason: String is that builder's fallback.</para>
///
/// <para>The walk does not enter a subquery. A subquery binds through the bind pipeline with its own
/// sources, and its names would resolve against the wrong columns here.</para>
/// </summary>
internal static class ConditionalResultTypes
{
    /// <summary>
    /// The static type of a column reference, or null when the name is not a column whose type is
    /// known (an output alias, an outer reference, a derived column declared only as a hint).
    /// </summary>
    internal delegate ExpressionType? ColumnTypeLookup(string identifier);

    // The literal-only result type of a conditional node. It depends on the SQL text alone, so it is
    // safe to share between executions of a cached parse tree. Boxed so a "no known type" result can be
    // cached too.
    private static readonly ConditionalWeakTable<NodeAst, StrongBox<ExpressionType?>> LiteralTypes = new();

    /// <summary>
    /// True for the node shapes this class types: a CASE (simple, searched, or an IF, which the parser
    /// builds as a searched CASE with one WHEN), and a call to COALESCE, IFNULL or NVL.
    /// </summary>
    internal static bool IsConditional(NodeAst node) =>
        node.nodeType == NodeType.ExprCase || IsCoalesceCall(node);

    /// <summary>The construct name an error uses for <paramref name="node"/>.</summary>
    internal static string ConstructName(NodeAst node) =>
        node.nodeType == NodeType.ExprCase
            ? (IsIf(node) ? "IF" : "CASE")
            : node.leftAst!.yytext!.ToUpperInvariant();

    /// <summary>True for a CASE node that the parser built from <c>IF(cond, a, b)</c>.</summary>
    internal static bool IsIf(NodeAst node) =>
        node.nodeType == NodeType.ExprCase && string.Equals(node.yytext, "IF", StringComparison.Ordinal);

    private static bool IsCoalesceCall(NodeAst node) =>
        node.nodeType == NodeType.ExprFuncCall
        && node.leftAst?.yytext is { } name
        && (name.Equals("coalesce", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ifnull", StringComparison.OrdinalIgnoreCase)
            || name.Equals("nvl", StringComparison.OrdinalIgnoreCase));

    // ── Bind time ────────────────────────────────────────────────────────────

    /// <summary>
    /// Checks every conditional expression in a bound SELECT: its projections, WHERE, GROUP BY,
    /// HAVING, ORDER BY and each JOIN ON. Records each known result type on
    /// <see cref="BoundSelectQuery.RowNames"/>. In GROUP BY, HAVING and ORDER BY a name that is also
    /// an output alias is read as unknown, because there it may mean the alias. In the select list,
    /// WHERE and JOIN ON a name always means a column.
    /// </summary>
    internal static void CheckQuery(BoundSelectQuery bound)
    {
        SelectQuery query = bound.Query;
        ColumnTypeLookup columns = LookupFor(bound, excludeOutputAliases: false);
        ColumnTypeLookup aliasAware = LookupFor(bound, excludeOutputAliases: true);
        QueryRowNameResolver recorder = bound.RowNames;

        foreach (ProjectionItem projection in query.Projections)
            Check(projection.Expression, columns, recorder);

        if (query.Where is not null)
            Check(query.Where.Expression, columns, recorder);

        CheckJoinPredicates(query.Source, columns, recorder);

        if (query.GroupBy is not null)
            foreach (NodeAst expression in query.GroupBy)
                Check(expression, aliasAware, recorder);

        if (query.Having is not null)
            Check(query.Having.Expression, aliasAware, recorder);

        if (query.OrderBy is not null)
            foreach (OrderByItem item in query.OrderBy)
                Check(item.Expression, aliasAware, recorder);
    }

    private static void CheckJoinPredicates(QuerySource source, ColumnTypeLookup lookup, QueryRowNameResolver recorder)
    {
        if (source is not JoinSource join)
            return;

        CheckJoinPredicates(join.Left, lookup, recorder);
        CheckJoinPredicates(join.Right, lookup, recorder);
        Check(join.OnPredicate, lookup, recorder);
    }

    /// <summary>
    /// Checks every conditional expression in <paramref name="expression"/> and records each known
    /// result type on <paramref name="recorder"/> when it is given. Throws
    /// <see cref="CamusDBErrorCodes.InvalidInput"/> for the first one whose known inputs have no
    /// common type.
    /// </summary>
    internal static void Check(NodeAst? expression, ColumnTypeLookup? lookup, QueryRowNameResolver? recorder)
    {
        if (expression is null || IsSubquery(expression.nodeType))
            return;

        // The depth of a statement is capped right after parsing (StatementDepthGuard), so this
        // recursion is bounded.
        Check(expression.leftAst, lookup, recorder);
        Check(expression.rightAst, lookup, recorder);
        Check(expression.extendedOne, lookup, recorder);
        Check(expression.extendedTwo, lookup, recorder);
        Check(expression.extendedThree, lookup, recorder);
        Check(expression.extendedFour, lookup, recorder);
        Check(expression.extendedFive, lookup, recorder);
        Check(expression.extendedSix, lookup, recorder);
        Check(expression.extendedSeven, lookup, recorder);

        if (!IsConditional(expression))
            return;

        if (Combine(expression, lookup, throwOnConflict: true, out _) is { } resultType)
            recorder?.RecordConditionalResultType(expression, resultType);
    }

    /// <summary>
    /// A lookup over the sources of <paramref name="bound"/>: a base-table column gives its declared
    /// type (with its element type), and a derived-table column gives its declared type unless that is
    /// the String fallback. With <paramref name="excludeOutputAliases"/>, a name that is also an output
    /// alias of the projection list gives null.
    /// </summary>
    internal static ColumnTypeLookup LookupFor(BoundSelectQuery bound, bool excludeOutputAliases)
    {
        HashSet<string>? outputNames = null;

        if (excludeOutputAliases)
        {
            foreach (ProjectionItem projection in bound.Query.Projections)
            {
                if (projection.OutputName is { } name && projection.Expression.nodeType == NodeType.ExprAlias)
                    (outputNames ??= new(StringComparer.OrdinalIgnoreCase)).Add(name);
            }
        }

        return identifier =>
        {
            if (outputNames is not null && outputNames.Contains(identifier))
                return null;

            string lookupKey;
            try
            {
                lookupKey = bound.RowNames.ResolveRowLookupKey(identifier);
            }
            catch (CamusDBException)
            {
                // An outer reference of a correlated subquery, or a name only another scope knows.
                return null;
            }

            foreach (BoundTableSource source in bound.Sources)
            {
                foreach (TableColumnSchema column in source.Table.Schema.Columns ?? [])
                {
                    if (!SchemaElementStateRules.IsReadable(column))
                        continue;

                    if (lookupKey == column.Name || lookupKey == QueryRowNameResolver.FormatQualifiedKey(source.Alias, column.Name))
                        return TypeOf(column);
                }
            }

            int dot = identifier.IndexOf('.');
            string bareName = dot > 0 ? identifier[(dot + 1)..] : identifier;

            foreach (BoundDerivedTableSource source in bound.DerivedSources)
            {
                foreach (DerivedColumnSchema column in source.Columns)
                {
                    if (string.Equals(column.Name, lookupKey, StringComparison.OrdinalIgnoreCase)
                        || string.Equals(column.Name, bareName, StringComparison.OrdinalIgnoreCase))
                        return column.Type == ColumnType.String
                            ? null
                            : new ExpressionType(column.Type, column.ElementType ?? ColumnType.Null);
                }
            }

            return null;
        };
    }

    /// <summary>A lookup over the columns of one table, for UPDATE and DELETE.</summary>
    internal static ColumnTypeLookup LookupFor(TableSchema schema)
    {
        return identifier =>
        {
            int dot = identifier.IndexOf('.');
            string bareName = dot > 0 ? identifier[(dot + 1)..] : identifier;

            foreach (TableColumnSchema column in schema.Columns ?? [])
            {
                if (SchemaElementStateRules.IsReadable(column)
                    && string.Equals(column.Name, bareName, StringComparison.OrdinalIgnoreCase))
                    return TypeOf(column);
            }

            return null;
        };
    }

    private static ExpressionType TypeOf(TableColumnSchema column) =>
        new(column.Type, column.Type == ColumnType.Array ? column.ArrayElementType ?? ColumnType.Null : ColumnType.Null);

    // ── Evaluation time ──────────────────────────────────────────────────────

    /// <summary>
    /// Converts <paramref name="value"/>, the value <paramref name="node"/> returns for one row, to the
    /// node's result type. The type is the one the binding recorded, or else the literal-only type
    /// (<see cref="LiteralResultType"/>). Throws when the value's type has no common type with it: the
    /// value came from an input whose type only its value shows.
    /// </summary>
    internal static ColumnValue Apply(NodeAst node, ColumnValue value, QueryRowNameResolver? resolver)
    {
        if (value.Type == ColumnType.Null)
            return value;

        ExpressionType? known = null;

        if (resolver is not null && resolver.TryGetConditionalResultType(node, out ExpressionType recorded))
            known = recorded;
        else
            known = LiteralResultType(node);

        if (known is not { } resultType)
            return value;

        // A string value here has a type only its value shows (a literal, or a parameter), so it may
        // take a date or datetime result type and is parsed, as a literal is.
        ExpressionType valueType = ExpressionType.Of(value) with { IsStringLiteral = value.Type == ColumnType.String };

        if (!ResultTypeRule.TryCombine(resultType, valueType, out ExpressionType combined))
            throw ResultTypeRule.NoCommonType(ConstructName(node), resultType, valueType);

        return ResultTypeRule.Widen(value, combined.Type);
    }

    /// <summary>
    /// The result type of <paramref name="node"/> from its inputs whose types the SQL text alone shows
    /// (literals, casts, nested conditionals over those), or null when none does. Throws when two of
    /// them have no common type, so a statement that no binding checked still fails whenever the
    /// expression runs. Cached per node, because the text decides it.
    /// </summary>
    internal static ExpressionType? LiteralResultType(NodeAst node)
    {
        if (LiteralTypes.TryGetValue(node, out StrongBox<ExpressionType?>? cached))
            return cached.Value;

        ExpressionType? type = Combine(node, lookup: null, throwOnConflict: true, out _);
        LiteralTypes.AddOrUpdate(node, new StrongBox<ExpressionType?>(type));
        return type;
    }

    // ── The typer ────────────────────────────────────────────────────────────

    /// <summary>
    /// Combines the known types of the result inputs of <paramref name="node"/>. Returns null when no
    /// input type is known. <paramref name="complete"/> is true when every input type is known. With
    /// <paramref name="throwOnConflict"/>, a pair with no common type throws; without it, the result is
    /// null.
    /// </summary>
    private static ExpressionType? Combine(NodeAst node, ColumnTypeLookup? lookup, bool throwOnConflict, out bool complete)
    {
        ExpressionType? result = null;
        bool allKnown = true;
        bool conflict = false;

        void Fold(NodeAst? input)
        {
            if (input is null || conflict)
                return;

            if (TypeOf(input, lookup) is not { } type)
            {
                allKnown = false;
                return;
            }

            if (result is not { } current)
            {
                result = type;
                return;
            }

            if (ResultTypeRule.TryCombine(current, type, out ExpressionType combined))
            {
                result = combined;
                return;
            }

            if (throwOnConflict)
                throw ResultTypeRule.NoCommonType(ConstructName(node), current, type);

            conflict = true;
        }

        if (node.nodeType == NodeType.ExprCase)
        {
            if (node.rightAst is not null)
                foreach (NodeAst clause in SQLExecutorBaseCreator.EnumerateWhenClauses(node.rightAst))
                    Fold(clause.rightAst);

            // An omitted ELSE is a NULL result, which combines with every type.
            Fold(node.extendedOne);
        }
        else
        {
            foreach (NodeAst argument in Arguments(node.rightAst))
                Fold(argument);
        }

        complete = allKnown && !conflict;

        // A string literal that takes a date or datetime result type is parsed now, so a malformed
        // literal fails before any row is read, as in GoogleSQL.
        if (throwOnConflict && !conflict && result is { } final && TemporalComparison.IsTemporal(final.Type))
            ParseStringLiteralInputs(node, final.Type);

        return conflict ? null : result;
    }

    private static void ParseStringLiteralInputs(NodeAst node, ColumnType target)
    {
        void Parse(NodeAst? input)
        {
            if (input is not null && QueryExpressionClassifier.UnwrapAlias(input) is { nodeType: NodeType.String, yytext: { } raw })
                _ = ResultTypeRule.ParseTemporal(SQLExecutorBaseCreator.UnquoteStringLiteral(raw), target);
        }

        if (node.nodeType == NodeType.ExprCase)
        {
            if (node.rightAst is not null)
                foreach (NodeAst clause in SQLExecutorBaseCreator.EnumerateWhenClauses(node.rightAst))
                    Parse(clause.rightAst);

            Parse(node.extendedOne);
        }
        else
        {
            foreach (NodeAst argument in Arguments(node.rightAst))
                Parse(argument);
        }
    }

    /// <summary>
    /// The static type of <paramref name="expression"/>, or null when the statement does not show it.
    /// </summary>
    private static ExpressionType? TypeOf(NodeAst expression, ColumnTypeLookup? lookup)
    {
        NodeAst target = QueryExpressionClassifier.UnwrapAlias(expression);

        switch (target.nodeType)
        {
            case NodeType.Integer: return new(ColumnType.Integer64);
            case NodeType.Float: return new(ColumnType.Float64);
            case NodeType.String: return ExpressionType.StringLiteral;
            case NodeType.Bool: return new(ColumnType.Bool);
            case NodeType.Null: return ExpressionType.Null;
            case NodeType.BytesLiteral: return new(ColumnType.Bytes);
            case NodeType.ObjectIdLiteral: return new(ColumnType.Id);

            case NodeType.Identifier:
                return target.yytext is not null && lookup is not null ? lookup(target.yytext) : null;

            case NodeType.ExprCast when target.rightAst is not null:
                try
                {
                    ColumnType castType = CastScalarFunctions.InferCastReturnType(target.rightAst);
                    return castType == ColumnType.Array ? null : new ExpressionType(castType);
                }
                catch (CamusDBException)
                {
                    return null;
                }

            case NodeType.ArrayLiteral:
                return new ExpressionType(ColumnType.Array, ArrayLiteralElementType(target.leftAst, lookup) ?? ColumnType.Null);

            case NodeType.ExprSubscript:
                return TypeOf(target.leftAst!, lookup) is { Type: ColumnType.Array, ElementType: not ColumnType.Null } array
                    ? new ExpressionType(array.ElementType)
                    : null;

            case NodeType.ExprNegate:
                return TypeOf(target.leftAst!, lookup) is { } operand && MixedNumericComparison.IsNumeric(operand.Type)
                    ? operand
                    : null;

            case NodeType.ExprAdd or NodeType.ExprSub or NodeType.ExprMult or NodeType.ExprDiv
                when target.leftAst is not null && target.rightAst is not null:
                return TypeOf(target.leftAst, lookup) is { } left
                       && TypeOf(target.rightAst, lookup) is { } right
                       && MixedNumericComparison.ArithmeticResultType(left.Type, right.Type) is { } arithmetic
                    ? new ExpressionType(arithmetic)
                    : null;

            // A nested conditional is not a literal, even when all its inputs are, as in GoogleSQL.
            case NodeType.ExprCase:
                return Combine(target, lookup, throwOnConflict: false, out bool caseComplete) is { } caseType && caseComplete
                    ? caseType with { IsStringLiteral = false }
                    : null;

            case NodeType.ExprFuncCall when IsCoalesceCall(target):
                return Combine(target, lookup, throwOnConflict: false, out bool callComplete) is { } callType && callComplete
                    ? callType with { IsStringLiteral = false }
                    : null;
        }

        return DerivedTableSchemaBuilder.IsBooleanResult(target.nodeType) ? new ExpressionType(ColumnType.Bool) : null;
    }

    /// <summary>
    /// The element type of an <c>ARRAY[…]</c> literal: the type of its first element that is not a NULL
    /// literal, as evaluation decides it, or null when that element's type is not known.
    /// </summary>
    private static ColumnType? ArrayLiteralElementType(NodeAst? list, ColumnTypeLookup? lookup)
    {
        if (list is null)
            return null;

        if (list.nodeType == NodeType.ExprList)
        {
            ColumnType? left = ArrayLiteralElementType(list.leftAst, lookup);
            if (left is not null)
                return left == ColumnType.Null ? ArrayLiteralElementType(list.rightAst, lookup) : left;

            // The first element is unknown, so the element type is unknown too.
            return null;
        }

        if (list.nodeType == NodeType.Null)
            return ColumnType.Null;

        return TypeOf(list, lookup) is { } element && element.Type != ColumnType.Array ? element.Type : null;
    }

    /// <summary>The arguments of a function call, in order (an in-order walk of the argument list nodes).</summary>
    internal static List<NodeAst> Arguments(NodeAst? argumentList)
    {
        List<NodeAst> arguments = new(2);
        CollectArguments(argumentList, arguments);
        return arguments;
    }

    private static void CollectArguments(NodeAst? node, List<NodeAst> arguments)
    {
        if (node is null)
            return;

        if (node.nodeType != NodeType.ExprArgumentList)
        {
            arguments.Add(node);
            return;
        }

        CollectArguments(node.leftAst, arguments);
        CollectArguments(node.rightAst, arguments);
    }

    private static bool IsSubquery(NodeType nodeType) => nodeType is
        NodeType.Select or
        NodeType.ExprScalarSubquery or
        NodeType.ExprInSubquery or
        NodeType.ExprNotInSubquery or
        NodeType.ExprExistsSubquery or
        NodeType.ExprExistsCorrelated;
}
