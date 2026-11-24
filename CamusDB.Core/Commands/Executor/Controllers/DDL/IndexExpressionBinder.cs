/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// One column as <see cref="IndexExpressionBinder"/> sees it. The binder serves a table that exists
/// (<see cref="TableColumnSchema"/>) and a table that a CREATE TABLE is about to create (its column
/// payloads), so it reads columns through this shape.
/// </summary>
internal readonly record struct IndexExpressionColumn(string Id, string Name, ColumnType Type, ColumnType? ArrayElementType, bool IsPublic)
{
    public static IndexExpressionColumn From(TableColumnSchema column) => new(
        column.Id,
        column.Name,
        column.Type,
        column.ArrayElementType,
        SchemaElementStateRules.IsReadable(column) && SchemaElementStateRules.IsWritable(column));
}

/// <summary>
/// The key of an index that a statement asks for, resolved against its table: one entry per key
/// position. At a column position the id and name are the column's. At an expression position the id
/// is <see cref="IndexKeyExpression.ColumnIdMarker"/>, the name is the hidden key name, and
/// <see cref="KeyExpressions"/> and <see cref="Expressions"/> hold the stored and the bound form.
/// </summary>
internal sealed record ResolvedIndexKey(
    string[] ColumnIds,
    string[] ColumnNames,
    IndexKeyExpression[]? KeyExpressions,
    BoundIndexExpression?[]? Expressions);

/// <summary>
/// Validates, stores and binds the expression key parts of an expression index
/// (<c>CREATE INDEX i ON t ((lower(email)))</c>).
///
/// <para><b>Three forms of one expression.</b> The statement carries SQL text with column names.
/// <see cref="Prepare"/> validates it against the table and produces the stored form, an
/// <see cref="IndexKeyExpression"/> whose text names each column by its id token
/// (<see cref="StoredColumnRef"/>), so a RENAME COLUMN never changes it. <see cref="Bind"/> turns the
/// stored form back into a <see cref="BoundIndexExpression"/> with the current names, which the write
/// paths evaluate and the planner matches. Every form is produced by one renderer, so a text that
/// <see cref="Prepare"/> accepted parses again to the same expression.</para>
///
/// <para><b>What an expression may hold.</b> The value of the expression is stored in every entry, so
/// it must be the same for the same row each time it runs. Only literals, public columns of the table,
/// operators, CASE, CAST to a type without a size, and calls of the built-in functions on
/// <see cref="ScalarFunctionRegistry.ImmutableFunctionNames"/> are accepted. Everything else is
/// <see cref="CamusDBErrorCodes.InvalidIndexExpression"/>: a subquery, an aggregate, a window call, a
/// parameter, DEFAULT, a column of another relation, an extension or inline function, and a function
/// that reads the clock, the session, a sequence or a random source, and a call that passes a time zone
/// argument. The result type must be known
/// before a row is read and must have an ordered key encoding.</para>
///
/// <para>Every walk here is iterative: the grammar is left-recursive, so a long operator chain is a
/// deep tree.</para>
/// </summary>
internal static class IndexExpressionBinder
{
    /// <summary>
    /// The prefix of a hidden key name. <c>#</c> cannot start or appear in an SQL identifier, so no
    /// column, alias or parameter can have this name.
    /// </summary>
    internal const string KeyNamePrefix = "#expr:";

    /// <summary>True when <paramref name="name"/> is the hidden key name of an expression key part.</summary>
    internal static bool IsKeyName(string? name) =>
        name is not null && name.StartsWith(KeyNamePrefix, StringComparison.Ordinal);

    /// <summary>
    /// The SQL text a statement carries for an expression key part: the expression rendered with the
    /// names the user wrote and function names in lower case. Refuses a construct an index cannot hold
    /// before the table is open, so the error does not wait for the schema leader.
    /// </summary>
    internal static string RenderForTicket(NodeAst expression)
    {
        RequireIndexableShape(expression, column: null);

        // Each name is quoted, so a column whose name is a keyword parses again on the leader.
        Dictionary<NodeAst, NodeAst> quoted = new(ReferenceEqualityComparer.Instance);
        foreach (NodeAst identifier in ColumnReferences(expression))
            quoted[identifier] = Identifier(QuoteName(identifier.yytext!));

        return Renderer.Instance.Render(LowerFunctionNames(NodeAstWalk.Replace(expression, quoted)));
    }

    /// <summary>Each part of a possibly qualified name in backquotes: <c>`t`.`email`</c>.</summary>
    private static string QuoteName(string name) =>
        string.Join('.', name.Split('.').Select(static part => "`" + part + "`"));

    /// <summary>
    /// Resolves the key of the index that <paramref name="ticket"/> adds against
    /// <paramref name="columns"/>. Column positions are checked by the callers, which report a missing
    /// or non-public column with their own messages; this method only reads their ids.
    /// </summary>
    internal static ResolvedIndexKey Resolve(
        string tableName,
        string indexName,
        ColumnIndexInfo[] key,
        IReadOnlyList<IndexExpressionColumn> columns)
    {
        string[] columnIds = new string[key.Length];
        string[] columnNames = new string[key.Length];
        List<IndexKeyExpression>? stored = null;
        BoundIndexExpression?[]? bound = null;

        for (int i = 0; i < key.Length; i++)
        {
            ColumnIndexInfo part = key[i];

            if (part.Expression is null)
            {
                IndexExpressionColumn? column = FindByName(columns, part.Name);
                columnIds[i] = column?.Id ?? throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column '{part.Name}' does not exist on table '{tableName}'");
                columnNames[i] = column.Value.Name;
                continue;
            }

            (IndexKeyExpression keyExpression, BoundIndexExpression boundExpression) = Prepare(tableName, indexName, i, part.Expression, columns);
            (stored ??= []).Add(keyExpression);
            (bound ??= new BoundIndexExpression?[key.Length])[i] = boundExpression;
            columnIds[i] = IndexKeyExpression.ColumnIdMarker;
            columnNames[i] = boundExpression.KeyName;
        }

        return new ResolvedIndexKey(columnIds, columnNames, stored?.ToArray(), bound);
    }

    /// <summary>
    /// Validates one expression key part against <paramref name="columns"/> and returns its stored
    /// and bound forms. See the class summary for what is refused.
    /// </summary>
    internal static (IndexKeyExpression Stored, BoundIndexExpression Bound) Prepare(
        string tableName,
        string indexName,
        int position,
        string expressionSql,
        IReadOnlyList<IndexExpressionColumn> columns)
    {
        NodeAst expression = SQLParserProcessor.ParseCondition(expressionSql);

        // A column is resolved by name, with or without the table's own name as qualifier.
        Dictionary<NodeAst, NodeAst> toTokens = new(ReferenceEqualityComparer.Instance);
        List<string> columnIds = [];

        RequireIndexableShape(expression, identifier =>
        {
            string bare = StripTableQualifier(identifier.yytext!, tableName)
                ?? throw Refused(indexName, $"column '{identifier.yytext}' is not a column of table '{tableName}'");

            IndexExpressionColumn column = FindByName(columns, bare)
                ?? throw Refused(indexName, $"column '{bare}' does not exist on table '{tableName}'");

            if (!column.IsPublic)
                throw Refused(indexName, $"column '{column.Name}' is not public on table '{tableName}'");

            if (!columnIds.Contains(column.Id, StringComparer.Ordinal))
                columnIds.Add(column.Id);

            toTokens[identifier] = Identifier(StoredColumnRef.Format(column.Id));
        });

        ColumnType type = ResultType(expression, indexName, columns, tableName);

        NodeAst storedAst = LowerFunctionNames(NodeAstWalk.Replace(expression, toTokens));
        IndexKeyExpression stored = new(position, Renderer.Instance.Render(storedAst), type, [.. columnIds]);

        return (stored, Bind(stored, columns, tableName));
    }

    /// <summary>
    /// Binds a stored expression key part to the current names of <paramref name="columns"/>. Called
    /// when a table is opened and when an index job resumes. A token whose column is gone means the
    /// schema is corrupt: DROP COLUMN refuses a column an index expression reads. The bound tree is
    /// typed from the column types (<see cref="OwnedExpressionTypes"/>) before any path can evaluate it.
    /// </summary>
    internal static BoundIndexExpression Bind(IndexKeyExpression stored, IReadOnlyList<IndexExpressionColumn> columns, string? tableName)
    {
        NodeAst parsed = SQLParserProcessor.ParseCondition(stored.Sql);

        Dictionary<NodeAst, NodeAst> toNames = new(ReferenceEqualityComparer.Instance);
        Dictionary<NodeAst, NodeAst> toQuotedNames = new(ReferenceEqualityComparer.Instance);
        List<string> referenced = [];

        foreach (NodeAst identifier in ColumnReferences(parsed))
        {
            if (!StoredColumnRef.TryGetColumnId(identifier.yytext, out string columnId))
                throw new CamusDBException(
                    CamusDBErrorCodes.SystemSpaceCorrupt,
                    $"Index expression '{stored.Sql}' of table '{tableName}' names '{identifier.yytext}' instead of a column id");

            IndexExpressionColumn column = FindById(columns, columnId)
                ?? throw new CamusDBException(
                    CamusDBErrorCodes.SystemSpaceCorrupt,
                    $"Index expression '{stored.Sql}' of table '{tableName}' reads column id '{columnId}', which the table does not have");

            toNames[identifier] = Identifier(column.Name);

            // The display text quotes each name, as SHOW CREATE TABLE quotes columns, so a column
            // whose name is a keyword still parses again.
            toQuotedNames[identifier] = Identifier(QuoteName(column.Name));

            if (!referenced.Contains(column.Name, StringComparer.OrdinalIgnoreCase))
                referenced.Add(column.Name);
        }

        NodeAst named = NodeAstWalk.Replace(parsed, toNames);

        // The tree belongs to this index alone, so its CASE, COALESCE and ARRAY[...] nodes keep the types
        // the column types give. Every row's key then has the value a SELECT computes for the same
        // expression, also in a row where a NULL hides a column's type.
        OwnedExpressionTypes.Type(named, identifier => FindByName(columns, identifier) is { } column
            ? new ExpressionType(column.Type, column.ArrayElementType ?? ColumnType.Null)
            : null);

        return new BoundIndexExpression(
            KeyNameOf(stored.Sql, stored.Type),
            named,
            Renderer.Instance.Render(NodeAstWalk.Replace(parsed, toQuotedNames)),
            Canonical(named, static name => name)!,
            stored.Type,
            [.. referenced]);
    }

    /// <summary>
    /// Builds the in-memory key of a persisted index: the name at each position (a column name, or the
    /// hidden key name of an expression) and the bound expressions. <paramref name="resolveColumnName"/>
    /// maps a column id to its current name, or null when the table has no such column.
    /// </summary>
    internal static (string[] Names, BoundIndexExpression?[]? Expressions) BindKey(
        string[] columnIds,
        IndexKeyExpression[]? keyExpressions,
        IReadOnlyList<IndexExpressionColumn> columns,
        string? tableName,
        Func<string, string?> resolveColumnName)
    {
        string[] names = new string[columnIds.Length];
        BoundIndexExpression?[]? expressions = null;

        for (int i = 0; i < columnIds.Length; i++)
            names[i] = resolveColumnName(columnIds[i])!;

        if (keyExpressions is { Length: > 0 })
        {
            expressions = new BoundIndexExpression?[columnIds.Length];

            foreach (IndexKeyExpression keyExpression in keyExpressions)
            {
                if (keyExpression.Position < 0 || keyExpression.Position >= columnIds.Length)
                    throw new CamusDBException(
                        CamusDBErrorCodes.SystemSpaceCorrupt,
                        $"Index expression '{keyExpression.Sql}' of table '{tableName}' has key position {keyExpression.Position} outside its index");

                BoundIndexExpression bound = Bind(keyExpression, columns, tableName);
                expressions[keyExpression.Position] = bound;
                names[keyExpression.Position] = bound.KeyName;
            }
        }

        return (names, expressions);
    }

    /// <summary>The columns of a table as the binder reads them.</summary>
    internal static IReadOnlyList<IndexExpressionColumn> ColumnsOf(IReadOnlyList<TableColumnSchema>? columns)
    {
        if (columns is null || columns.Count == 0)
            return [];

        IndexExpressionColumn[] result = new IndexExpressionColumn[columns.Count];
        for (int i = 0; i < columns.Count; i++)
            result[i] = IndexExpressionColumn.From(columns[i]);

        return result;
    }

    /// <summary>
    /// The text two spellings of one expression share: column references become
    /// <paramref name="resolveColumn"/> of their bare name, in lower case, and function names are in
    /// lower case. Returns null when <paramref name="resolveColumn"/> answers null for a reference,
    /// which a caller uses to say "this expression reads something other than one table's columns".
    /// Parentheses never reach the AST, so they cannot change the text.
    /// </summary>
    internal static string? Canonical(NodeAst expression, Func<string, string?> resolveColumn)
    {
        Dictionary<NodeAst, NodeAst> replacements = new(ReferenceEqualityComparer.Instance);

        foreach (NodeAst identifier in ColumnReferences(expression))
        {
            string? name = resolveColumn(identifier.yytext!);
            if (name is null)
                return null;

            replacements[identifier] = Identifier(name.ToLowerInvariant());
        }

        try
        {
            return Renderer.Instance.Render(LowerFunctionNames(NodeAstWalk.Replace(expression, replacements)));
        }
        catch (CamusDBException)
        {
            // A construct the renderer cannot print (a subquery, a window call) is never an index
            // expression, so it matches none.
            return null;
        }
    }

    /// <summary>
    /// The hidden key name of an expression, from its stored text and type. The text names columns by
    /// id, so two indexes over one expression share the name, and a rename does not change it. The type
    /// is part of the name, so the planner, which types a key name through the first index that has it,
    /// never decodes one index with the type of another. The text is hashed because the planner keys
    /// names case-insensitively, and two expressions can differ only in the case of a string literal.
    /// </summary>
    internal static string KeyNameOf(string storedSql, ColumnType type)
    {
        // FNV-1a over the UTF-16 code units: stable across processes and builds, unlike string.GetHashCode.
        ulong hash = 14695981039346656037UL;
        foreach (char c in storedSql)
        {
            hash ^= c;
            hash *= 1099511628211UL;
        }

        hash ^= (ulong)type;
        hash *= 1099511628211UL;

        return KeyNamePrefix + hash.ToString("X16", System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The column references of <paramref name="expression"/>: every identifier except a function
    /// name and a CAST target type.
    /// </summary>
    internal static List<NodeAst> ColumnReferences(NodeAst expression)
    {
        List<NodeAst> identifiers = [];
        Stack<NodeAst> pending = new();
        pending.Push(expression);

        while (pending.TryPop(out NodeAst? node))
        {
            switch (node.nodeType)
            {
                case NodeType.Identifier:
                    identifiers.Add(node);
                    continue;

                case NodeType.ExprFuncCall:
                    if (node.rightAst is not null)
                        pending.Push(node.rightAst);
                    continue;

                case NodeType.ExprCast:
                    if (node.leftAst is not null)
                        pending.Push(node.leftAst);
                    continue;
            }

            PushChildren(pending, node);
        }

        return identifiers;
    }

    /// <summary>
    /// Refuses a node an index expression cannot hold. <paramref name="column"/> receives each column
    /// reference; null when the caller only checks the shape (the ticket builder, which has no table).
    /// </summary>
    private static void RequireIndexableShape(NodeAst expression, Action<NodeAst>? column)
    {
        Stack<NodeAst> pending = new();
        pending.Push(expression);

        while (pending.TryPop(out NodeAst? node))
        {
            switch (node.nodeType)
            {
                case NodeType.Identifier:
                    column?.Invoke(node);
                    continue;

                case NodeType.ExprFuncCall:
                    RequireImmutableCall(node);
                    if (node.rightAst is not null)
                        pending.Push(node.rightAst);
                    continue;

                case NodeType.ExprCast:
                    if (node.leftAst is not null)
                        pending.Push(node.leftAst);
                    continue;

                case NodeType.ExprScalarSubquery:
                case NodeType.ExprInSubquery:
                case NodeType.ExprNotInSubquery:
                case NodeType.ExprExistsSubquery:
                case NodeType.ExprExistsCorrelated:
                case NodeType.ExprQuantifiedComparison:
                case NodeType.Select:
                    throw Refused(null, "a subquery cannot be part of an index expression");

                case NodeType.ExprWindowCall:
                    throw Refused(null, "a window function cannot be part of an index expression");

                case NodeType.Placeholder:
                    throw Refused(null, $"a parameter ({node.yytext}) cannot be part of an index expression");

                case NodeType.ExprDefault:
                    throw Refused(null, "DEFAULT cannot be part of an index expression");

                case NodeType.ExprAllFields:
                    throw Refused(null, "* cannot be part of an index expression");
            }

            if (!AllowedNodes.Contains(node.nodeType))
                throw Refused(null, $"the construct {node.nodeType} cannot be part of an index expression");

            PushChildren(pending, node);
        }
    }

    private static void RequireImmutableCall(NodeAst call)
    {
        string name = call.leftAst?.yytext ?? "";

        if (call.rightAst?.nodeType == NodeType.ExprAllFields || AggregateFunctionCatalog.IsAggregate(name))
            throw Refused(null, $"the aggregate function '{name}' cannot be part of an index expression");

        // A dotted name calls a function of an installed extension.
        if (name.Contains('.'))
            throw Refused(null, $"the extension function '{name}' cannot be part of an index expression: only immutable built-in functions can");

        if (!ScalarFunctionEvaluator.TryResolveBuiltIn(name, out ScalarFunctionDescriptor descriptor))
            throw Refused(null, $"the function '{name}' cannot be part of an index expression: only immutable built-in functions can");

        if (!ScalarFunctionRegistry.IsImmutable(descriptor))
            throw Refused(null, $"the function '{name}' cannot be part of an index expression: its result can change for the same arguments");

        // The zone rules come with the build, so a later build can compute another key for the same row.
        if (ScalarFunctionEvaluator.IsTimeZoneDependentCall(call))
            throw Refused(null, $"the function '{name}' with a time zone argument cannot be part of an index expression: its result depends on the time zone rules of the build");
    }

    /// <summary>
    /// The result type of the expression: known before a row is read, and with an ordered key encoding.
    /// The operand rules run first, so an expression that no row could evaluate is refused here too.
    /// </summary>
    private static ColumnType ResultType(NodeAst expression, string indexName, IReadOnlyList<IndexExpressionColumn> columns, string tableName)
    {
        ConditionalResultTypes.ColumnTypeLookup lookup = identifier =>
        {
            string? bare = StripTableQualifier(identifier, tableName);
            if (bare is null)
                return null;

            IndexExpressionColumn? column = FindByName(columns, bare);
            return column is { } c ? new ExpressionType(c.Type, c.ArrayElementType ?? ColumnType.Null) : null;
        };

        ConditionalResultTypes.Check(expression, lookup, recorder: null, allowAggregates: false);

        ExpressionType? type = ConditionalResultTypes.TypeOf(expression, lookup, ConditionalResultTypes.CallTyping.BuiltIn);

        if (type is null || type.Value.Type == ColumnType.Null)
            throw Refused(indexName, "the type of the expression is not known before a row is read; add a CAST to the type the index should hold");

        ColumnType result = type.Value.Type;

        if (!HasOrderedKeyEncoding(result))
            throw Refused(indexName, $"an expression of type {result} has no ordered index key; cast it to a type that has one (for jsonb, use ->> to index a text field)");

        return result;
    }

    /// <summary>True for a type <c>KeyEncoder</c> encodes in an order-preserving key.</summary>
    internal static bool HasOrderedKeyEncoding(ColumnType type) => type is
        ColumnType.Id or ColumnType.Integer64 or ColumnType.String or ColumnType.Bool or
        ColumnType.Float64 or ColumnType.Float32 or ColumnType.Bytes or ColumnType.Date or
        ColumnType.DateTime or ColumnType.Uuid or ColumnType.Numeric;

    private static readonly HashSet<NodeType> AllowedNodes =
    [
        NodeType.Integer, NodeType.Float, NodeType.String, NodeType.Bool, NodeType.Null,
        NodeType.BytesLiteral, NodeType.ObjectIdLiteral, NodeType.ArrayLiteral,
        NodeType.ExprList, NodeType.ExprArgumentList,
        NodeType.ExprEquals, NodeType.ExprNotEquals, NodeType.ExprLessThan, NodeType.ExprGreaterThan,
        NodeType.ExprGreaterEqualsThan, NodeType.ExprLessEqualsThan, NodeType.ExprBetween,
        NodeType.ExprOr, NodeType.ExprAnd, NodeType.ExprNot,
        NodeType.ExprAdd, NodeType.ExprSub, NodeType.ExprMult, NodeType.ExprDiv, NodeType.ExprNegate,
        NodeType.ExprConcat, NodeType.ExprBitAnd, NodeType.ExprBitOr, NodeType.ExprBitXor, NodeType.ExprBitNot,
        NodeType.ExprShiftLeft, NodeType.ExprShiftRight,
        NodeType.ExprCase, NodeType.ExprCaseWhen, NodeType.ExprCaseWhenList,
        NodeType.ExprLike, NodeType.ExprILike,
        NodeType.ExprRegexMatch, NodeType.ExprRegexMatchCi, NodeType.ExprRegexNotMatch, NodeType.ExprRegexNotMatchCi,
        NodeType.ExprIsNull, NodeType.ExprIsNotNull, NodeType.ExprIsTrue, NodeType.ExprIsNotTrue,
        NodeType.ExprIsFalse, NodeType.ExprIsNotFalse,
        NodeType.ExprInMembership, NodeType.ExprNotInMembership,
        NodeType.ExprSubscript,
    ];

    private static NodeAst LowerFunctionNames(NodeAst expression)
    {
        Dictionary<NodeAst, NodeAst> replacements = new(ReferenceEqualityComparer.Instance);
        Stack<NodeAst> pending = new();
        pending.Push(expression);

        while (pending.TryPop(out NodeAst? node))
        {
            if (node.nodeType == NodeType.ExprFuncCall
                && node.leftAst?.yytext is { } name
                && !string.Equals(name, name.ToLowerInvariant(), StringComparison.Ordinal))
                replacements[node.leftAst] = Identifier(name.ToLowerInvariant());

            PushChildren(pending, node);
        }

        return NodeAstWalk.Replace(expression, replacements);
    }

    /// <summary>
    /// The column name in <paramref name="identifier"/> without a qualifier, or null when the
    /// qualifier names another relation. With no table name every qualifier is accepted and dropped.
    /// </summary>
    private static string? StripTableQualifier(string identifier, string? tableName)
    {
        int dot = identifier.LastIndexOf('.');
        if (dot < 0)
            return identifier;

        if (tableName is not null && !string.Equals(identifier[..dot], tableName, StringComparison.OrdinalIgnoreCase))
            return null;

        return identifier[(dot + 1)..];
    }

    private static IndexExpressionColumn? FindByName(IReadOnlyList<IndexExpressionColumn> columns, string name)
    {
        foreach (IndexExpressionColumn column in columns)
        {
            if (string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase))
                return column;
        }

        return null;
    }

    private static IndexExpressionColumn? FindById(IReadOnlyList<IndexExpressionColumn> columns, string id)
    {
        foreach (IndexExpressionColumn column in columns)
        {
            if (string.Equals(column.Id, id, StringComparison.Ordinal))
                return column;
        }

        return null;
    }

    private static NodeAst Identifier(string name) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    private static void PushChildren(Stack<NodeAst> pending, NodeAst node)
    {
        if (node.leftAst is not null) pending.Push(node.leftAst);
        if (node.rightAst is not null) pending.Push(node.rightAst);
        if (node.extendedOne is not null) pending.Push(node.extendedOne);
        if (node.extendedTwo is not null) pending.Push(node.extendedTwo);
        if (node.extendedThree is not null) pending.Push(node.extendedThree);
        if (node.extendedFour is not null) pending.Push(node.extendedFour);
        if (node.extendedFive is not null) pending.Push(node.extendedFive);
        if (node.extendedSix is not null) pending.Push(node.extendedSix);
        if (node.extendedSeven is not null) pending.Push(node.extendedSeven);
    }

    internal static CamusDBException Refused(string? indexName, string reason) => new(
        CamusDBErrorCodes.InvalidIndexExpression,
        indexName is null
            ? $"Invalid index expression: {reason}"
            : $"Invalid expression in index '{indexName}': {reason}");

    /// <summary>
    /// The renderer for index expressions. A node it cannot print is a construct no index expression
    /// holds, so it reports <see cref="CamusDBErrorCodes.InvalidIndexExpression"/>.
    /// </summary>
    private sealed class Renderer : SqlAstRenderer
    {
        public static readonly Renderer Instance = new();

        protected override CamusDBException Unsupported(NodeAst expr) =>
            Refused(null, $"the construct {expr.nodeType} cannot be part of an index expression");
    }
}
