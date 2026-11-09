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
///   <see cref="ColumnTypeLookup"/>, arithmetic over those, nested conditionals, and a call of a
///   function whose argument types are known, by its declared type
///   (<see cref="ScalarFunctionEvaluator.DeclaredResultType"/>), as in PostgreSQL. A pair with no
///   common type is <see cref="CamusDBErrorCodes.InvalidInput"/>, even when the branch never runs and
///   over an empty table, as in GoogleSQL. The result type is recorded on the binding's
///   <see cref="QueryRowNameResolver"/> when there is one.</item>
/// <item><b>When the expression is evaluated</b> (<see cref="Apply"/>), the value it returns is
///   combined with the recorded type, or, without a record, with the types of its literal inputs, and
///   converted to the result (<see cref="ResultTypeRule.Widen"/>). An input whose type the statement
///   does not show (a parameter, a call with such an argument, a function whose type comes from its
///   value) is checked here, against the inputs whose types are known.</item>
/// </list>
///
/// <para><b>Why a call is typed here and not for an operator.</b> A later argument of COALESCE, or a
/// branch of CASE that does not run, is never evaluated, so without its declared type a statement
/// such as <c>CASE WHEN c THEN 1 ELSE upper(s) END</c> passed in every row where <c>c</c> was true,
/// and one column could hold an INT64 in one row and a FLOAT64 in the next
/// (<c>COALESCE(n, sqrt(4.0))</c>). An operator over a call runs the call first, so the per-row check
/// already sees its value; <see cref="TypeOf"/> types a call for its other callers only when they ask
/// for it.</para>
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
///
/// <para><b>The one bind-time walk.</b> <see cref="Check"/> visits every expression position of a
/// statement once, so it also applies <see cref="StaticOperandRules"/> to each node: a function name
/// and its argument count, and each operator whose operand types the statement shows. Each node
/// receives the types of its operands from the visit of its children, so a long operator chain costs
/// one pass and not one pass per level. A statement error therefore does not depend on the rows, a
/// LIMIT, or a dead branch.</para>
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

    internal static bool IsCoalesceCall(NodeAst node) =>
        node.nodeType == NodeType.ExprFuncCall
        && node.leftAst?.yytext is { } name
        && (name.Equals("coalesce", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ifnull", StringComparison.OrdinalIgnoreCase)
            || name.Equals("nvl", StringComparison.OrdinalIgnoreCase));

    // ── Bind time ────────────────────────────────────────────────────────────

    /// <summary>
    /// Checks every expression of a bound SELECT: its projections, WHERE, GROUP BY, HAVING, ORDER BY
    /// and each JOIN ON. WHERE, HAVING and ON must also be boolean. Records each known result type on
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
            CheckCondition(query.Where.Expression, columns, recorder, "WHERE");

        CheckJoinPredicates(query.Source, columns, recorder);

        if (query.GroupBy is not null)
            foreach (NodeAst expression in query.GroupBy)
                Check(expression, aliasAware, recorder);

        if (query.Having is not null)
            CheckCondition(query.Having.Expression, aliasAware, recorder, "HAVING");

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
        CheckCondition(join.OnPredicate, lookup, recorder, "ON");
    }

    /// <summary>
    /// Checks every node of <paramref name="expression"/>: <see cref="StaticOperandRules"/>, and the
    /// result type of each conditional expression, which it records on <paramref name="recorder"/> when
    /// that is given. Throws for the first node the statement alone shows to be wrong.
    /// </summary>
    /// <param name="expression">The expression, or null when the statement has none at this position.</param>
    /// <param name="lookup">The column types of this scope, or null when the scope has no columns.</param>
    /// <param name="recorder">Where a result type goes, or null when no binding holds it.</param>
    /// <param name="allowAggregates">False where no aggregate can run (UPDATE, DELETE, VALUES). There an
    /// aggregate name resolves as a scalar function, and fails as it does when a row reaches it.</param>
    internal static void Check(NodeAst? expression, ColumnTypeLookup? lookup, QueryRowNameResolver? recorder, bool allowAggregates = true) =>
        Visit(expression, lookup, recorder, allowAggregates);

    /// <summary>
    /// <see cref="Check"/> for the condition of a clause. The condition must also be a boolean or NULL,
    /// so a known type of any other kind is <see cref="CamusDBErrorCodes.InvalidInput"/>. A condition
    /// whose type only its value shows (a parameter, a function call) is checked per row
    /// (<see cref="QueryFilterer.ToPredicateResult"/>).
    /// </summary>
    /// <param name="clause">The clause name an error uses: WHERE, HAVING or ON.</param>
    internal static void CheckCondition(NodeAst? condition, ColumnTypeLookup? lookup, QueryRowNameResolver? recorder, string clause, bool allowAggregates = true)
    {
        if (Visit(condition, lookup, recorder, allowAggregates) is { Type: not (ColumnType.Bool or ColumnType.Null) } type)
            throw StaticOperandRules.ConditionNotBoolean(clause, type.ToString());
    }

    /// <summary>
    /// <see cref="Check"/> for a value that is written to <paramref name="column"/>. The value must also
    /// have a type that the column accepts (<see cref="StaticOperandRules.RequireAssignable"/>).
    /// </summary>
    internal static void CheckAssignment(NodeAst value, TableColumnSchema column, ColumnTypeLookup? lookup)
    {
        if (Visit(value, lookup, recorder: null, allowAggregates: false) is { } type)
            StaticOperandRules.RequireAssignable(value, type, column);
    }

    /// <summary>
    /// The static type of each output column of <paramref name="bound"/>, for a statement that writes
    /// them, or null when the select list does not line up with <paramref name="columns"/>. A
    /// <c>SELECT *</c> reads each column through the lookup, so a derived column declared as the String
    /// fallback is unknown here too. An entry is null when the statement does not show that type.
    /// </summary>
    internal static IReadOnlyList<ExpressionType?>? OutputTypes(BoundSelectQuery bound, IReadOnlyList<DerivedColumnSchema> columns)
    {
        IReadOnlyList<ProjectionItem> projections = bound.Query.Projections;
        ColumnTypeLookup lookup = LookupFor(bound, excludeOutputAliases: false);
        ExpressionType?[] types = new ExpressionType?[columns.Count];

        if (projections is [{ Expression.nodeType: NodeType.ExprAllFields }])
        {
            for (int i = 0; i < types.Length; i++)
                types[i] = lookup(columns[i].RowKey);

            return types;
        }

        if (projections.Count != types.Length)
            return null;

        for (int i = 0; i < types.Length; i++)
            types[i] = TypeOf(projections[i].Expression, lookup);

        return types;
    }

    /// <summary>
    /// Visits <paramref name="node"/> and its children, applies the rules with the types its children
    /// returned, and returns its own static type, or null when the statement does not show it. The
    /// depth of a statement is capped right after parsing (<see cref="StatementDepthGuard"/>), so this
    /// recursion is bounded, and the rules live in other methods so that its frame stays small.
    /// </summary>
    private static ExpressionType? Visit(NodeAst? node, ColumnTypeLookup? lookup, QueryRowNameResolver? recorder, bool allowAggregates)
    {
        if (node is null)
            return null;

        // The tested value of an IN subquery is an expression of this scope. A correlated IN keeps its node to
        // the end, and the type of its body's column is recorded on it, so the comparison is checked here, as
        // the membership test of an uncorrelated IN is.
        if (node.nodeType is NodeType.ExprInSubquery or NodeType.ExprNotInSubquery)
            StaticOperandRules.CheckSubqueryMembership(node, Visit(node.leftAst, lookup, recorder, allowAggregates));

        if (IsSubquery(node.nodeType))
            return FromOperands(node, null, null, lookup);

        if (node.nodeType == NodeType.ExprAlias)
            return Visit(node.leftAst, lookup, recorder, allowAggregates);

        // A SELECT replaces each window call it allows with a result column when it is created, so one
        // that remains sits where SQL allows none. Its function is not a scalar call, so the walk stops here.
        if (node.nodeType == NodeType.ExprWindowCall)
            throw SQLExecutorBaseCreator.UnevaluableExpression(NodeType.ExprWindowCall);

        // The left child of a call is the function name, and the right child of a cast is the target
        // type. Neither is an expression.
        ExpressionType? left = node.nodeType == NodeType.ExprFuncCall ? null : Visit(node.leftAst, lookup, recorder, allowAggregates);
        ExpressionType? right = node.nodeType == NodeType.ExprCast ? null : Visit(node.rightAst, lookup, recorder, allowAggregates);
        ExpressionType? one = Visit(node.extendedOne, lookup, recorder, allowAggregates);
        ExpressionType? two = Visit(node.extendedTwo, lookup, recorder, allowAggregates);

        Visit(node.extendedThree, lookup, recorder, allowAggregates);
        Visit(node.extendedFour, lookup, recorder, allowAggregates);
        Visit(node.extendedFive, lookup, recorder, allowAggregates);
        Visit(node.extendedSix, lookup, recorder, allowAggregates);
        Visit(node.extendedSeven, lookup, recorder, allowAggregates);

        StaticOperandRules.Check(node, left, right, one, two, lookup, allowAggregates);

        if (!IsConditional(node))
            return FromOperands(node, left, right, lookup);

        ExpressionType? resultType = Combine(node, lookup, throwOnConflict: true, out bool complete, builtInOnly: false);

        if (resultType is { } known)
            recorder?.RecordConditionalResultType(node, known);

        // A nested conditional is not a literal, even when all its inputs are, as in GoogleSQL.
        return complete && resultType is { } whole ? whole with { IsStringLiteral = false } : null;
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
            string? qualifier = dot > 0 ? identifier[..dot] : null;

            foreach (BoundDerivedTableSource source in bound.DerivedSources)
            {
                // Two derived tables can each have a column of one name, so a qualified name reads
                // the column of the table it names and no other.
                if (qualifier is not null && !string.Equals(source.Alias, qualifier, StringComparison.OrdinalIgnoreCase))
                    continue;

                DerivedColumnSchema? keyed = source.FindColumnByLookupKey(lookupKey);

                foreach (DerivedColumnSchema column in source.Columns)
                {
                    if (ReferenceEquals(column, keyed)
                        || (keyed is null && string.Equals(column.Name, bareName, StringComparison.OrdinalIgnoreCase)))
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

    /// <summary>A lookup over the columns a CREATE TABLE declares, for its CHECK constraints.</summary>
    internal static ColumnTypeLookup LookupFor(IReadOnlyList<ColumnInfo> columns)
    {
        return identifier =>
        {
            foreach (ColumnInfo column in columns)
            {
                if (string.Equals(column.Name, identifier, StringComparison.OrdinalIgnoreCase))
                    return new ExpressionType(column.Type, column.Type == ColumnType.Array ? column.ArrayElementType ?? ColumnType.Null : ColumnType.Null);
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
    /// (literals, casts, calls of built-in functions over those, nested conditionals), or null when
    /// none does. Throws when two of
    /// them have no common type, so a statement that no binding checked still fails whenever the
    /// expression runs. Cached per node, because the text decides it.
    /// </summary>
    internal static ExpressionType? LiteralResultType(NodeAst node)
    {
        if (LiteralTypes.TryGetValue(node, out StrongBox<ExpressionType?>? cached))
            return cached.Value;

        // Built-in calls only: this type is cached on a parse tree that every database shares, and an
        // extension function belongs to one database.
        ExpressionType? type = Combine(node, lookup: null, throwOnConflict: true, out _, builtInOnly: true);
        LiteralTypes.AddOrUpdate(node, new StrongBox<ExpressionType?>(type));
        return type;
    }

    // ── The typer ────────────────────────────────────────────────────────────

    /// <summary>
    /// Combines the known types of the result inputs of <paramref name="node"/>. Returns null when no
    /// input type is known. <paramref name="complete"/> is true when every input type is known. With
    /// <paramref name="throwOnConflict"/>, a pair with no common type throws; without it, the result is
    /// null. An input that is a function call has its declared type; with
    /// <paramref name="builtInOnly"/>, only a call of a built-in function does.
    /// </summary>
    private static ExpressionType? Combine(NodeAst node, ColumnTypeLookup? lookup, bool throwOnConflict, out bool complete, bool builtInOnly)
    {
        CallTyping calls = builtInOnly ? CallTyping.BuiltIn : CallTyping.All;
        ExpressionType? result = null;
        bool allKnown = true;
        bool conflict = false;

        void Fold(NodeAst? input)
        {
            if (input is null || conflict)
                return;

            if (TypeOf(input, lookup, calls) is not { } type)
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

    /// <summary>Which function calls <see cref="TypeOf"/> gives a type.</summary>
    internal enum CallTyping
    {
        /// <summary>No call: its type is the job of the per-row check.</summary>
        None,

        /// <summary>A call of a built-in function, by its declared type.</summary>
        BuiltIn,

        /// <summary>A call of a built-in function or of an extension function, by its declared type.</summary>
        All,
    }

    /// <summary>
    /// The static type of <paramref name="expression"/>, or null when the statement does not show it.
    /// A function call has a type only with <paramref name="calls"/>, and only when the type of each of
    /// its arguments is known.
    /// </summary>
    internal static ExpressionType? TypeOf(NodeAst expression, ColumnTypeLookup? lookup, CallTyping calls = CallTyping.None)
    {
        NodeAst target = QueryExpressionClassifier.UnwrapAlias(expression);

        ExpressionType? left = null;
        ExpressionType? right = null;

        switch (target.nodeType)
        {
            case NodeType.ExprSubscript or NodeType.ExprNegate or NodeType.ExprBitNot:
                left = TypeOf(target.leftAst!, lookup, calls);
                break;

            case NodeType.ExprAdd or NodeType.ExprSub or NodeType.ExprMult or NodeType.ExprDiv
                or NodeType.ExprConcat or NodeType.ExprBitAnd or NodeType.ExprBitOr or NodeType.ExprBitXor
                or NodeType.ExprShiftLeft or NodeType.ExprShiftRight
                when target.leftAst is not null && target.rightAst is not null:
                left = TypeOf(target.leftAst, lookup, calls);
                right = TypeOf(target.rightAst, lookup, calls);
                break;
        }

        return FromOperands(target, left, right, lookup, calls);
    }

    /// <summary>
    /// The static type of <paramref name="target"/> from the types of its left and right operands, the
    /// one table both <see cref="TypeOf"/> and <see cref="Visit"/> read. A pair the evaluator refuses
    /// gives null here, because the refusal is the job of <see cref="StaticOperandRules"/>.
    /// </summary>
    private static ExpressionType? FromOperands(NodeAst target, ExpressionType? left, ExpressionType? right, ColumnTypeLookup? lookup, CallTyping calls = CallTyping.None)
    {
        switch (target.nodeType)
        {
            case NodeType.Integer: return new(ColumnType.Integer64);
            case NodeType.Float: return new(ColumnType.Float64);
            case NodeType.String: return ExpressionType.StringLiteral;
            case NodeType.Bool: return new(ColumnType.Bool);
            case NodeType.Null: return SubqueryResultTypes.TypeOf(target) ?? ExpressionType.Null;

            // A statement parameter has no static type. The placeholder that stands for an outer column in
            // a correlated subquery has the type of that column, see CorrelatedSubqueryBody.
            case NodeType.Placeholder: return SubqueryResultTypes.TypeOf(target);

            // A correlated scalar subquery keeps its node, with the type of its column, see ExistsSubqueryPreparer.
            case NodeType.ExprScalarSubquery: return SubqueryResultTypes.TypeOf(target);
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
                return new ExpressionType(ColumnType.Array, ArrayLiteralElementType(target.leftAst, lookup, calls) ?? ColumnType.Null);

            case NodeType.ExprSubscript:
                return left is { Type: ColumnType.Array, ElementType: not ColumnType.Null } array
                    ? new ExpressionType(array.ElementType)
                    : null;

            case NodeType.ExprNegate:
                return left is { } operand && MixedNumericComparison.IsNumeric(operand.Type)
                    ? operand with { IsStringLiteral = false }
                    : null;

            case NodeType.ExprAdd or NodeType.ExprSub or NodeType.ExprMult or NodeType.ExprDiv:
                return left is { } addend && right is { } other
                       && MixedNumericComparison.ArithmeticResultType(addend.Type, other.Type) is { } arithmetic
                    ? new ExpressionType(arithmetic)
                    : null;

            case NodeType.ExprConcat or NodeType.ExprBitAnd or NodeType.ExprBitOr or NodeType.ExprBitXor
                or NodeType.ExprShiftLeft or NodeType.ExprShiftRight:
                return left is { } first && right is { } second
                       && ConcatAndBitwiseOperators.ResultType(target.nodeType, first.Type, second.Type) is { } combined
                    ? new ExpressionType(combined)
                    : null;

            case NodeType.ExprBitNot:
                return left is { } complement
                       && ConcatAndBitwiseOperators.ResultType(NodeType.ExprBitNot, complement.Type, ColumnType.Null) is { } bitNot
                    ? new ExpressionType(bitNot)
                    : null;

            // A nested conditional is not a literal, even when all its inputs are, as in GoogleSQL.
            case NodeType.ExprCase:
                return Combine(target, lookup, throwOnConflict: false, out bool caseComplete, calls == CallTyping.BuiltIn) is { } caseType && caseComplete
                    ? caseType with { IsStringLiteral = false }
                    : null;

            case NodeType.ExprFuncCall when IsCoalesceCall(target):
                return Combine(target, lookup, throwOnConflict: false, out bool callComplete, calls == CallTyping.BuiltIn) is { } callType && callComplete
                    ? callType with { IsStringLiteral = false }
                    : null;

            case NodeType.ExprFuncCall when calls != CallTyping.None:
                return DeclaredCallType(target, lookup, calls);
        }

        return DerivedTableSchemaBuilder.IsBooleanResult(target.nodeType) ? new ExpressionType(ColumnType.Bool) : null;
    }

    /// <summary>
    /// The declared type of the call <paramref name="call"/> when the type of each of its arguments is
    /// known, or null otherwise. A string literal argument reads as STRING, as the call receives it.
    /// </summary>
    private static ExpressionType? DeclaredCallType(NodeAst call, ColumnTypeLookup? lookup, CallTyping calls)
    {
        if (call.leftAst?.yytext is not { } name)
            return null;

        List<NodeAst> arguments = Arguments(call.rightAst);
        ColumnType[] argumentTypes = arguments.Count == 0 ? [] : new ColumnType[arguments.Count];

        for (int i = 0; i < arguments.Count; i++)
        {
            if (TypeOf(arguments[i], lookup, calls) is not { } argument)
                return null;

            argumentTypes[i] = argument.Type;
        }

        return ScalarFunctionEvaluator.DeclaredResultType(name.ToLowerInvariant(), argumentTypes, builtInOnly: calls == CallTyping.BuiltIn);
    }

    /// <summary>
    /// The element type of an <c>ARRAY[…]</c> literal: the type of its first element that is not a NULL
    /// literal, as evaluation decides it, or null when that element's type is not known.
    /// </summary>
    private static ColumnType? ArrayLiteralElementType(NodeAst? list, ColumnTypeLookup? lookup, CallTyping calls = CallTyping.None)
    {
        if (list is null)
            return null;

        if (list.nodeType == NodeType.ExprList)
        {
            ColumnType? left = ArrayLiteralElementType(list.leftAst, lookup, calls);
            if (left is not null)
                return left == ColumnType.Null ? ArrayLiteralElementType(list.rightAst, lookup, calls) : left;

            // The first element is unknown, so the element type is unknown too.
            return null;
        }

        if (list.nodeType == NodeType.Null)
            return ColumnType.Null;

        return TypeOf(list, lookup, calls) is { } element && element.Type != ColumnType.Array ? element.Type : null;
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
