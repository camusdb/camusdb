/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// What one view contributes to a write through it: the relation it reads, the alias its body reads it
/// by, its <c>WHERE</c>, and, for each of its output columns, the expression in its body — which is a
/// plain column of the relation below for a column that can be written.
/// </summary>
internal sealed class UpdatableViewLevel
{
    public required string ViewName { get; init; }

    public required ViewSchema View { get; init; }

    /// <summary>The relation the body reads: a table, or another view.</summary>
    public required string BaseName { get; init; }

    /// <summary>The name the body reads <see cref="BaseName"/> by. Every expression below uses it.</summary>
    public required string BaseAlias { get; init; }

    /// <summary>The body's <c>WHERE</c>, or null.</summary>
    public NodeAst? Where { get; init; }

    /// <summary>Each output column's expression in the body, keyed by the view's column name.</summary>
    public required Dictionary<string, NodeAst> Expressions { get; init; }

    /// <summary>
    /// The writable output columns, mapped to the column of <see cref="BaseName"/> each one is. A
    /// column absent here is computed, and can be read through the view but not written.
    /// </summary>
    public required Dictionary<string, string> BaseColumns { get; init; }
}

/// <summary>
/// Decides whether a view can be written through, and what it contributes when it can. PostgreSQL's
/// auto-updatability rules: CamusDB has no rules and no <c>INSTEAD OF</c> triggers, so a view these rules
/// refuse is read-only for good, and this is the only gate.
/// </summary>
/// <remarks>
/// <para>A view is auto-updatable when its body reads exactly one relation — a table, or a view that is
/// itself auto-updatable — and has no aggregate, window function, <c>DISTINCT</c>, <c>GROUP BY</c>,
/// <c>HAVING</c>, <c>LIMIT</c>, <c>OFFSET</c> or set operation. An output column that is a plain column
/// of that relation can be written; any other output column is computed and can only be read, as in
/// PostgreSQL.</para>
///
/// <para><b>One rule stricter than PostgreSQL:</b> the body's <c>WHERE</c> may not hold a subquery. The
/// <c>WHERE</c> is merged into the write, which runs as the caller, so a subquery there would read its
/// tables as the caller rather than as the view's owner — a boundary nobody writing the view would
/// expect.</para>
///
/// <para>Materialized views take no writes at all, whatever their shape.</para>
/// </remarks>
internal static class ViewUpdatability
{
    internal static UpdatableViewLevel Analyze(Schema schema, string viewName, ViewSchema view, Func<string, NodeAst> parse)
    {
        ViewDefinition definition = view.Definition
            ?? throw new CamusDBException(CamusDBErrorCodes.SystemSpaceCorrupt, $"View '{viewName}' has no stored definition");

        NodeAst body = parse(definition.Sql);
        if (StoredBodyBinder.MayContainReferences(definition.Sql))
            body = StoredBodyBinder.ResolveStoredForm(schema, body, viewName);

        if (body.nodeType != NodeType.Select || body.rightAst?.nodeType == NodeType.SetOperationReference)
            throw NotUpdatable(viewName, "its body is a set operation (UNION, INTERSECT or EXCEPT)");

        if (body.rightAst is null)
            throw NotUpdatable(viewName, "its body reads no relation");

        if (body.yytext is not null)
            throw NotUpdatable(viewName, "its body uses DISTINCT");

        if (body.extendedFive is not null)
            throw NotUpdatable(viewName, "its body uses GROUP BY");

        if (body.extendedSix is not null)
            throw NotUpdatable(viewName, "its body uses HAVING");

        if (body.extendedThree is not null || body.extendedFour is not null)
            throw NotUpdatable(viewName, "its body uses LIMIT or OFFSET");

        (string baseName, string baseAlias) = body.rightAst.nodeType switch
        {
            NodeType.TableReference when body.rightAst.leftAst?.yytext is { } name
                => (name, body.rightAst.rightAst?.yytext ?? name),
            NodeType.Identifier when body.rightAst.yytext is { } name
                => (name, name),
            NodeType.Join or NodeType.CommaJoin or NodeType.CommaJoinTableList
                => throw NotUpdatable(viewName, "its body reads more than one relation"),
            NodeType.DerivedTableReference
                => throw NotUpdatable(viewName, "its body reads a subquery, a CTE or a function, not a table or a view"),
            _ => throw NotUpdatable(viewName, "its body does not read a single table or view"),
        };

        HashSet<string> baseColumnNames;

        if (schema.Views.TryGetValue(baseName, out ViewSchema? baseView))
        {
            baseColumnNames = new((baseView.Definition?.Columns ?? []).Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
        }
        else if (schema.Tables.TryGetValue(baseName, out TableSchema? baseTable))
        {
            if (baseTable.IsMaterializedView)
                throw NotUpdatable(viewName, $"its body reads materialized view '{baseName}', which cannot be written to");

            baseColumnNames = new((baseTable.Columns ?? []).Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
        }
        else
        {
            throw new CamusDBException(
                CamusDBErrorCodes.ViewDoesntExist,
                $"Relation '{baseName}' that view '{viewName}' reads does not exist");
        }

        if (body.extendedOne is { } where && ContainsSubquery(where))
            throw NotUpdatable(viewName, "the WHERE of its body has a subquery");

        List<NodeAst> items = [];
        Flatten(body.leftAst, items);

        List<ViewColumnSchema> columns = definition.Columns ?? [];
        if (items.Count != columns.Count)
            throw NotUpdatable(viewName, "its stored body does not match its column list");

        Dictionary<string, NodeAst> expressions = new(StringComparer.OrdinalIgnoreCase);
        Dictionary<string, string> baseColumns = new(StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < items.Count; i++)
        {
            NodeAst expression = items[i].nodeType == NodeType.ExprAlias ? items[i].leftAst! : items[i];

            if (QueryExpressionClassifier.ContainsAggregate(expression))
                throw NotUpdatable(viewName, "its body computes aggregates");

            if (NodeAstWalk.Any(expression, static n => n.nodeType == NodeType.ExprWindowCall))
                throw NotUpdatable(viewName, "its body uses window functions");

            expressions[columns[i].Name] = expression;

            if (expression.nodeType == NodeType.Identifier && expression.yytext is { } text)
            {
                (string? qualifier, string column) = Split(text);

                if ((qualifier is null || string.Equals(qualifier, baseAlias, StringComparison.OrdinalIgnoreCase))
                    && baseColumnNames.Contains(column))
                    baseColumns[columns[i].Name] = column;
            }
        }

        return new UpdatableViewLevel
        {
            ViewName = viewName,
            View = view,
            BaseName = baseName,
            BaseAlias = baseAlias,
            Where = body.extendedOne,
            Expressions = expressions,
            BaseColumns = baseColumns,
        };
    }

    /// <summary>Splits <c>qualifier.column</c> at its last dot; a bare name has no qualifier.</summary>
    internal static (string? Qualifier, string Column) Split(string text)
    {
        int dot = text.LastIndexOf('.');
        return dot > 0 && dot < text.Length - 1 ? (text[..dot], text[(dot + 1)..]) : (null, text);
    }

    internal static bool ContainsSubquery(NodeAst ast)
        => NodeAstWalk.Any(ast, static n => n.nodeType is NodeType.ExprScalarSubquery or NodeType.ExprInSubquery
            or NodeType.ExprNotInSubquery or NodeType.ExprExistsSubquery or NodeType.ExprExistsCorrelated
            or NodeType.Select);

    internal static void Flatten(NodeAst? node, List<NodeAst> into)
    {
        if (node is null)
            return;

        if (node.nodeType == NodeType.IdentifierList)
        {
            Flatten(node.leftAst, into);
            Flatten(node.rightAst, into);
            return;
        }

        into.Add(node);
    }

    internal static CamusDBException NotUpdatable(string viewName, string reason)
        => new(
            CamusDBErrorCodes.ViewNotUpdatable,
            $"Cannot write through view '{viewName}': {reason}. Only a view whose body reads one table or " +
            "updatable view, with no aggregate, window function, DISTINCT, GROUP BY, HAVING, LIMIT, OFFSET, " +
            "set operation or subquery in its WHERE, can be written through; write to the table instead");
}

/// <summary>
/// The statement-scoped state of a write that came through a view: which table the rewritten statement
/// writes, and the <c>WITH CHECK OPTION</c> conditions every new row must meet.
/// </summary>
/// <remarks>
/// <para><b>Target authorization.</b> The caller was checked against the view, and each
/// view's owner against what that view reads, before the statement was rewritten — the owner's-rights
/// model a read through a view already follows. The rewritten statement then opens the base table,
/// which the caller may hold no grant on, so that one open — and only that one: a subquery the caller
/// wrote still reads its tables as the caller — skips the per-table check.</para>
///
/// <para><b>Row checks.</b> A row passes a check option only when the view's
/// condition is true for it — visible through the view — so a NULL fails, unlike a <c>CHECK</c>
/// constraint. It is evaluated on the complete new row, defaults applied, beside the table's own
/// <c>CHECK</c> constraints and inside the same transaction, before the row is written.</para>
/// </remarks>
internal sealed class ViewWriteContext
{
    private static readonly AsyncLocal<ViewWriteContext?> current = new();

    /// <summary>The write through a view the current flow is executing, or null.</summary>
    public static ViewWriteContext? Current
    {
        get => current.Value;
        set => current.Value = value;
    }

    public required string BaseTableName { get; init; }

    public required string BaseTableId { get; init; }

    /// <summary>The caller whose write this is; the base table is authorized to this principal only.</summary>
    public Principal? Caller { get; init; }

    /// <summary>
    /// True when an open of <paramref name="tableId"/> by <paramref name="principal"/> is this write
    /// reaching its base table, which the caller was authorized for through the view.
    /// </summary>
    /// <remarks>
    /// Only the caller's own principal: inside another view's body the principal is that view's owner,
    /// and its access to the table is its own to have or lack. And only because every reference to the
    /// base table in the caller's own statement — a subquery, an INSERT's source — was checked against
    /// the caller's grants before the statement was rewritten (see
    /// <see cref="UpdatableViewRewriter.RequireCallerMayReadDirectReferences"/>), so this cannot be used
    /// to read the table through a subquery.
    /// </remarks>
    public static bool AuthorizesOpen(string? tableId, Principal? principal)
        => Current is { } write
           && principal is not null
           && ReferenceEquals(principal, write.Caller)
           && string.Equals(write.BaseTableId, tableId, StringComparison.Ordinal);

    /// <summary>Each condition a new row must meet, with the view it comes from, in base-table column names.</summary>
    public required IReadOnlyList<(string ViewName, NodeAst Condition)> CheckOptions { get; init; }

    /// <summary>True when <paramref name="tableName"/> is the table this statement writes through the view.</summary>
    public bool IsTarget(string tableName) => string.Equals(BaseTableName, tableName, StringComparison.OrdinalIgnoreCase);

    /// <summary>Throws <see cref="CamusDBErrorCodes.ViewCheckOptionViolated"/> unless the row meets every check option.</summary>
    public void EnforceCheckOptions(TableDescriptor table, IReadOnlyDictionary<string, ColumnValue> row)
    {
        if (CheckOptions.Count == 0 || !string.Equals(table.Id, BaseTableId, StringComparison.Ordinal))
            return;

        foreach ((string viewName, NodeAst condition) in CheckOptions)
        {
            if (CheckEvaluator.Evaluate(condition, row) != true)
                throw new CamusDBException(
                    CamusDBErrorCodes.ViewCheckOptionViolated,
                    $"new row violates check option for view \"{viewName}\"");
        }
    }
}

/// <summary>
/// Rewrites an <c>INSERT</c>, <c>UPDATE</c> or <c>DELETE</c> whose target is a view into the same
/// statement on the table beneath it, one view at a time, so every behavior of a write to a table — the
/// caller's transaction, its locks, the mutation cap, constraints, triggers, statistics — applies
/// unchanged. There is deliberately no second write path.
/// </summary>
/// <remarks>
/// <para>For each view, from the one named down to the table: the view's column names in the statement
/// become the expressions of its body; a column written must be a plain column of the relation below;
/// and, for <c>UPDATE</c> and <c>DELETE</c>, the body's <c>WHERE</c> is ANDed into the statement's, so a
/// write reaches only rows the view shows.</para>
///
/// <para>A name the view does not expose is refused rather than passed through: left alone, it would
/// name a column of the base table the view hides, and the row count of the write would disclose it.</para>
/// </remarks>
internal static class UpdatableViewRewriter
{
    internal static bool TargetsView(Schema schema, NodeAst ast)
        => ast.nodeType is NodeType.Insert or NodeType.InsertSelect or NodeType.Update or NodeType.Delete
           && ast.leftAst?.yytext is { } target
           && schema.Views.ContainsKey(target);

    internal static async Task<(NodeAst Statement, ViewWriteContext Context)> RewriteAsync(
        DatabaseDescriptor database,
        NodeAst ast,
        Func<string, NodeAst> parse,
        Func<string, string, Task<Principal?>>? resolveOwner)
    {
        Schema schema = database.Schema;

        Privilege required = ast.nodeType switch
        {
            NodeType.Update => Privilege.Update,
            NodeType.Delete => Privilege.Delete,
            _ => Privilege.Insert,
        };

        string target = ast.leftAst!.yytext!;

        // The caller is checked on the view it named, before anything about the view is read, so a
        // caller with no grant learns nothing about its body.
        ViewAuthorization.Require(database, target, schema.Views[target], required);

        Principal? caller = AuthorizationContext.Current.Principal;
        bool deletes = ast.nodeType == NodeType.Delete;

        NodeAst statement = ast;
        string qualifier = StatementAlias(ast) ?? target;
        List<(string ViewName, NodeAst Condition)> checks = [];
        bool cascaded = false;
        int depth = 0;

        while (schema.Views.TryGetValue(target, out ViewSchema? view))
        {
            int maxDepth = database.Options.MaxViewExpansionDepth;
            if (maxDepth > 0 && ++depth > maxDepth)
                throw new CamusDBException(
                    CamusDBErrorCodes.ViewRecursionDetected,
                    $"Writing through view '{target}' exceeded the maximum nesting depth of {maxDepth}");

            UpdatableViewLevel level = ViewUpdatability.Analyze(schema, target, view, parse);

            await RequireOwnerMayWriteAsync(database, level, required, caller, resolveOwner).ConfigureAwait(false);

            // The conditions gathered so far name this view's columns; carry them down a level.
            for (int i = 0; i < checks.Count; i++)
                checks[i] = (checks[i].ViewName, RewriteColumns(checks[i].Condition, level, qualifier));

            // PostgreSQL's rule. CASCADED checks this view and every view beneath it, whatever their own
            // option; LOCAL checks this one, and beneath it only views that ask for a check themselves.
            CheckOptionKind option = view.Definition!.CheckOption;
            bool checksThisLevel = cascaded || option != CheckOptionKind.None;
            if (option == CheckOptionKind.Cascaded)
                cascaded = true;

            if (checksThisLevel && !deletes && level.Where is not null)
                checks.Add((level.ViewName, level.Where));

            statement = RewriteStatement(statement, level, qualifier);
            qualifier = level.BaseAlias;
            target = level.BaseName;
        }

        TableSchema baseTable = schema.Tables[target];

        RequireCallerMayReadDirectReferences(database, ast, baseTable, caller);

        // The table's row is keyed by bare column names, and an INSERT names its table without an alias.
        for (int i = 0; i < checks.Count; i++)
            checks[i] = (checks[i].ViewName, StripQualifier(checks[i].Condition, qualifier, inSubquery: false));

        statement = StripFinalQualifier(statement, qualifier);

        return (statement, new ViewWriteContext
        {
            BaseTableName = target,
            BaseTableId = baseTable.Id!,
            Caller = caller,
            CheckOptions = checks,
        });
    }

    /// <summary>
    /// Requires the caller's own SELECT grant on the base table for every place the caller's statement
    /// names it directly — a subquery in SET or WHERE, or the source of an INSERT. The write reaches the
    /// table on the view owner's authority, and that authority covers the write only.
    /// </summary>
    internal static void RequireCallerMayReadDirectReferences(DatabaseDescriptor database, NodeAst statement, TableSchema baseTable, Principal? caller)
    {
        if (!database.Options.AuthenticationEnabled || caller is null)
            return;

        bool names = NodeAstWalk.Any(statement, node =>
            node.nodeType is NodeType.TableReference or NodeType.Identifier
            && ReferenceEquals(node, statement.leftAst) is false
            && NamesRelation(node, baseTable.Name!));

        if (names && !caller.HasPrivilege(Privilege.Select, database.Id, baseTable.Id))
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege,
                $"Missing Select privilege on table '{database.Name}.{baseTable.Name}'");
    }

    /// <summary>True when <paramref name="node"/> is a FROM-position reference to <paramref name="relation"/>.</summary>
    private static bool NamesRelation(NodeAst node, string relation)
        => node.nodeType == NodeType.TableReference
            ? string.Equals(node.leftAst?.yytext, relation, StringComparison.OrdinalIgnoreCase)
            : false;

    /// <summary>
    /// The owner of each view must be allowed to make the write on what the view reads — the
    /// owner's-rights model a read through a view follows. A view stored without an owner was created
    /// with authentication off and is checked against the caller, as a read of it is.
    /// </summary>
    private static async Task RequireOwnerMayWriteAsync(
        DatabaseDescriptor database,
        UpdatableViewLevel level,
        Privilege required,
        Principal? caller,
        Func<string, string, Task<Principal?>>? resolveOwner)
    {
        if (!database.Options.AuthenticationEnabled || caller is null)
            return;

        Principal actor = caller;

        if (level.View.Definition?.OwnerId is { Length: > 0 } ownerId && resolveOwner is not null)
            actor = await resolveOwner(level.View.Definition.Owner ?? "", ownerId).ConfigureAwait(false)
                ?? throw new CamusDBException(
                    CamusDBErrorCodes.InsufficientPrivilege,
                    $"The owner of view '{level.ViewName}' no longer exists, so the view cannot be written through. " +
                    "Recreate it under a current user, or transfer it with ALTER VIEW ... OWNER TO.");

        string? baseId = database.Schema.Views.TryGetValue(level.BaseName, out ViewSchema? baseView)
            ? baseView.Id
            : database.Schema.Tables[level.BaseName].Id;

        // An UPDATE or DELETE also reads the rows it picks, so its owner must be able to read them too.
        bool reads = required is Privilege.Update or Privilege.Delete;

        if (!actor.HasPrivilege(required, database.Id, baseId) || (reads && !actor.HasPrivilege(Privilege.Select, database.Id, baseId)))
            throw new CamusDBException(
                CamusDBErrorCodes.InsufficientPrivilege,
                $"View '{level.ViewName}' cannot be written through: its owner '{actor.UserName}' lacks the " +
                $"{required}{(reads ? " and Select" : "")} privilege on '{database.Name}.{level.BaseName}'");
    }

    private static string? StatementAlias(NodeAst ast) => ast.nodeType switch
    {
        NodeType.Update => ast.extendedFive?.yytext,
        NodeType.Delete => ast.extendedFour?.yytext,
        _ => null,
    };

    private static NodeAst RewriteStatement(NodeAst statement, UpdatableViewLevel level, string qualifier)
    {
        switch (statement.nodeType)
        {
            case NodeType.Insert:
            case NodeType.InsertSelect:
                {
                    if (statement.extendedThree is not null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.FeatureNotSupported,
                            $"INSERT ... ON CONFLICT and UPSERT cannot target view '{level.ViewName}'; write to its table instead");

                    List<string> columns = [];
                    if (statement.rightAst is null)
                        columns.AddRange((level.View.Definition!.Columns ?? []).Select(c => c.Name));
                    else
                        foreach (NodeAst field in Items(statement.rightAst))
                            columns.Add(field.yytext ?? "");

                    List<NodeAst> baseFields = [.. columns.Select(column => Identifier(WritableColumn(level, column, "insert into")))];

                    return new NodeAst(
                        statement.nodeType,
                        Identifier(level.BaseName),
                        List(baseFields),
                        statement.extendedOne,
                        statement.extendedTwo is { } returning ? RewriteReturning(returning, level, qualifier) : null,
                        null,
                        statement.extendedFour,
                        statement.extendedFive,
                        statement.yytext,
                        statement.extendedSix,
                        statement.extendedSeven);
                }

            case NodeType.Update:
                {
                    if (statement.extendedFour is not null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.FeatureNotSupported,
                            $"UPDATE ... FROM cannot target view '{level.ViewName}'; write to its table instead");

                    List<NodeAst> assignments = [];
                    foreach (NodeAst item in UpdateItems(statement.rightAst))
                    {
                        (string? itemQualifier, string column) = ViewUpdatability.Split(item.leftAst?.yytext ?? "");
                        if (itemQualifier is not null && !string.Equals(itemQualifier, qualifier, StringComparison.OrdinalIgnoreCase))
                            throw UnknownColumn(level, item.leftAst!.yytext!);

                        assignments.Add(new NodeAst(
                            NodeType.UpdateItem,
                            Identifier(WritableColumn(level, column, "update")),
                            RewriteColumns(item.rightAst!, level, qualifier),
                            null, null, null, null, null, null));
                    }

                    return new NodeAst(
                        NodeType.Update,
                        Identifier(level.BaseName),
                        UpdateList(assignments),
                        And(level.Where, RewriteColumns(statement.extendedOne!, level, qualifier)),
                        statement.extendedTwo,
                        statement.extendedThree is { } returning ? RewriteReturning(returning, level, qualifier) : null,
                        null,
                        Identifier(level.BaseAlias),
                        statement.yytext,
                        statement.extendedSix,
                        statement.extendedSeven);
                }

            case NodeType.Delete:
                {
                    if (statement.extendedThree is not null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.FeatureNotSupported,
                            $"DELETE ... USING cannot target view '{level.ViewName}'; write to its table instead");

                    return new NodeAst(
                        NodeType.Delete,
                        Identifier(level.BaseName),
                        And(level.Where, RewriteColumns(statement.rightAst!, level, qualifier)),
                        statement.extendedOne,
                        statement.extendedTwo is { } returning ? RewriteReturning(returning, level, qualifier) : null,
                        null,
                        Identifier(level.BaseAlias),
                        statement.extendedFive,
                        statement.yytext,
                        statement.extendedSix,
                        statement.extendedSeven);
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"{statement.nodeType} is not a write");
        }
    }

    /// <summary>The column of the relation below that view column <paramref name="column"/> writes, or a refusal.</summary>
    private static string WritableColumn(UpdatableViewLevel level, string column, string verb)
    {
        if (level.BaseColumns.TryGetValue(column, out string? baseColumn))
            return baseColumn;

        if (level.Expressions.ContainsKey(column))
            throw new CamusDBException(
                CamusDBErrorCodes.ViewColumnNotUpdatable,
                $"Cannot {verb} column '{column}' of view '{level.ViewName}': it is computed, not a column of " +
                $"'{level.BaseName}', so there is nothing to write it to");

        throw UnknownColumn(level, column);
    }

    private static CamusDBException UnknownColumn(UpdatableViewLevel level, string column)
        => new(CamusDBErrorCodes.UnknownColumn, $"Column '{column}' does not exist in view '{level.ViewName}'");

    /// <summary>
    /// A RETURNING list over the view, in terms of the relation below. A column keeps the view's name for
    /// its output, and <c>*</c> is every column of the view.
    /// </summary>
    private static NodeAst RewriteReturning(NodeAst list, UpdatableViewLevel level, string qualifier)
    {
        List<NodeAst> items = [];

        foreach (NodeAst item in Items(list))
        {
            switch (item.nodeType)
            {
                case NodeType.ExprAllFields:
                    foreach (ViewColumnSchema column in level.View.Definition!.Columns ?? [])
                        items.Add(Alias(level.Expressions[column.Name], column.Name));
                    break;

                case NodeType.ExprAlias:
                    items.Add(Alias(RewriteColumns(item.leftAst!, level, qualifier), item.rightAst!.yytext!));
                    break;

                case NodeType.Identifier when item.yytext is { } text:
                    items.Add(Alias(RewriteColumns(item, level, qualifier), ViewUpdatability.Split(text).Column));
                    break;

                default:
                    items.Add(RewriteColumns(item, level, qualifier));
                    break;
            }
        }

        return List(items);
    }

    /// <summary>
    /// <paramref name="expression"/> with each reference to a column of the view replaced by that column's
    /// expression in the view's body. Outside a subquery a bare name is the view's; inside one only a
    /// name qualified by the view's alias is, since a bare name there belongs to the subquery's own FROM.
    /// </summary>
    internal static NodeAst RewriteColumns(NodeAst expression, UpdatableViewLevel level, string qualifier)
        => Map(expression, inSubquery: false, (node, inSubquery) =>
        {
            if (node.nodeType != NodeType.Identifier || node.yytext is not { } text)
                return null;

            (string? nodeQualifier, string column) = ViewUpdatability.Split(text);

            if (nodeQualifier is null)
            {
                if (inSubquery)
                    return node;
            }
            else if (!string.Equals(nodeQualifier, qualifier, StringComparison.OrdinalIgnoreCase))
            {
                if (inSubquery)
                    return node;

                throw new CamusDBException(
                    CamusDBErrorCodes.UnknownColumn,
                    $"Unknown relation '{nodeQualifier}' in a write through view '{level.ViewName}'");
            }

            return level.Expressions.TryGetValue(column, out NodeAst? replacement)
                ? replacement
                : throw UnknownColumn(level, column);
        });

    /// <summary><paramref name="expression"/> with <c>alias.column</c> written as <c>column</c>, outside subqueries.</summary>
    private static NodeAst StripQualifier(NodeAst expression, string alias, bool inSubquery)
        => Map(expression, inSubquery, (node, nested) =>
        {
            if (nested || node.nodeType != NodeType.Identifier || node.yytext is not { } text)
                return null;

            (string? nodeQualifier, string column) = ViewUpdatability.Split(text);
            return nodeQualifier is not null && string.Equals(nodeQualifier, alias, StringComparison.OrdinalIgnoreCase)
                ? Identifier(column)
                : node;
        });

    /// <summary>
    /// Rebuilds <paramref name="node"/> bottom-up with <paramref name="replace"/> applied to each node; a
    /// subtree <paramref name="replace"/> leaves alone is shared. The slots that hold a name
    /// rather than a column — a function's name, a cast's type, an alias — are never visited.
    /// </summary>
    private static NodeAst Map(NodeAst node, bool inSubquery, Func<NodeAst, bool, NodeAst?> replace)
    {
        if (replace(node, inSubquery) is { } replaced)
            return replaced;

        bool nested = inSubquery || node.nodeType == NodeType.Select;

        NodeAst? Child(NodeAst? child) => child is null ? null : Map(child, nested, replace);

        NodeAst? left = node.nodeType == NodeType.ExprFuncCall ? node.leftAst : Child(node.leftAst);
        NodeAst? right = node.nodeType is NodeType.ExprCast or NodeType.ExprAlias ? node.rightAst : Child(node.rightAst);
        NodeAst? one = Child(node.extendedOne);
        NodeAst? two = Child(node.extendedTwo);
        NodeAst? three = Child(node.extendedThree);
        NodeAst? four = Child(node.extendedFour);
        NodeAst? five = Child(node.extendedFive);
        NodeAst? six = Child(node.extendedSix);
        NodeAst? seven = Child(node.extendedSeven);

        if (ReferenceEquals(left, node.leftAst) && ReferenceEquals(right, node.rightAst)
            && ReferenceEquals(one, node.extendedOne) && ReferenceEquals(two, node.extendedTwo)
            && ReferenceEquals(three, node.extendedThree) && ReferenceEquals(four, node.extendedFour)
            && ReferenceEquals(five, node.extendedFive) && ReferenceEquals(six, node.extendedSix)
            && ReferenceEquals(seven, node.extendedSeven))
            return node;

        return new NodeAst(node.nodeType, left, right, one, two, three, four, five, node.yytext, six, seven);
    }

    /// <summary>
    /// Writes the table's columns bare in the statement's own expressions. An INSERT names its table with
    /// no alias at all; an UPDATE or DELETE keeps its alias for a correlated subquery, which reads the
    /// target through it, but its own expressions no longer depend on how the alias is resolved.
    /// </summary>
    private static NodeAst StripFinalQualifier(NodeAst statement, string alias)
    {
        NodeAst? Strip(NodeAst? node) => node is null ? null : StripQualifier(node, alias, inSubquery: false);

        return statement.nodeType switch
        {
            NodeType.Insert or NodeType.InsertSelect => new NodeAst(
                statement.nodeType, statement.leftAst, statement.rightAst, statement.extendedOne, Strip(statement.extendedTwo),
                statement.extendedThree, statement.extendedFour, statement.extendedFive, statement.yytext,
                statement.extendedSix, statement.extendedSeven),

            NodeType.Update => new NodeAst(
                NodeType.Update, statement.leftAst,
                UpdateList([.. UpdateItems(statement.rightAst).Select(item => new NodeAst(
                    NodeType.UpdateItem, item.leftAst, Strip(item.rightAst), null, null, null, null, null, null))]),
                Strip(statement.extendedOne), statement.extendedTwo, Strip(statement.extendedThree),
                statement.extendedFour, statement.extendedFive, statement.yytext, statement.extendedSix, statement.extendedSeven),

            NodeType.Delete => new NodeAst(
                NodeType.Delete, statement.leftAst, Strip(statement.rightAst), statement.extendedOne, Strip(statement.extendedTwo),
                statement.extendedThree, statement.extendedFour, statement.extendedFive, statement.yytext,
                statement.extendedSix, statement.extendedSeven),

            _ => statement,
        };
    }

    private static NodeAst Identifier(string name)
        => new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    private static NodeAst Alias(NodeAst expression, string name)
        => new(NodeType.ExprAlias, expression, Identifier(name), null, null, null, null, null, null);

    private static NodeAst And(NodeAst? left, NodeAst right)
        => left is null ? right : new NodeAst(NodeType.ExprAnd, left, right, null, null, null, null, null, null);

    private static List<NodeAst> Items(NodeAst list)
    {
        List<NodeAst> items = [];
        ViewUpdatability.Flatten(list, items);
        return items;
    }

    private static NodeAst List(IReadOnlyList<NodeAst> items)
    {
        NodeAst list = items[0];
        for (int i = 1; i < items.Count; i++)
            list = new NodeAst(NodeType.IdentifierList, list, items[i], null, null, null, null, null, null);
        return list;
    }

    private static IEnumerable<NodeAst> UpdateItems(NodeAst? node)
    {
        if (node is null)
            yield break;

        if (node.nodeType == NodeType.UpdateList)
        {
            foreach (NodeAst item in UpdateItems(node.leftAst))
                yield return item;
            foreach (NodeAst item in UpdateItems(node.rightAst))
                yield return item;
            yield break;
        }

        yield return node;
    }

    private static NodeAst UpdateList(IReadOnlyList<NodeAst> items)
    {
        NodeAst list = items[0];
        for (int i = 1; i < items.Count; i++)
            list = new NodeAst(NodeType.UpdateList, list, items[i], null, null, null, null, null, null);
        return list;
    }
}
