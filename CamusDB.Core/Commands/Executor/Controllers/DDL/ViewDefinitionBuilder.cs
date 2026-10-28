
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

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Turns a bound <c>CREATE VIEW</c> body into the <see cref="ViewDefinition"/> that gets stored:
/// the frozen column list, the normalized SQL text, and the resolved dependency ids.
///
/// <para>Every rejection here happens at <c>CREATE</c> time rather than at first <c>SELECT</c>,
/// which is both PostgreSQL's behavior and the only useful one — a view whose body is invalid is a
/// mistake the author can fix now, not an error some later reader should discover.</para>
/// </summary>
internal static class ViewDefinitionBuilder
{
    /// <summary>
    /// Builds the definition. <paramref name="derived"/> is the body's output schema and drives the
    /// column list; <paramref name="projections"/> is the expression list that produced it, and the
    /// two must come from the same bound query or they can disagree about arity.
    /// </summary>
    /// <param name="setOperation">
    /// The bound set operation when the body is <c>q1 UNION q2</c> (or INTERSECT, EXCEPT), which
    /// <see cref="SelectRowSource.SetOperation"/> carries; null for any other body. A set-operation
    /// body is normalized operand by operand (see <see cref="NormalizeSetOperationBody"/>).
    /// </param>
    public static ViewDefinition Build(
        Schema schema,
        NodeAst bodyAst,
        IReadOnlyList<NodeAst> projections,
        IReadOnlyList<DerivedColumnSchema> derived,
        NodeAst? columnAliasList,
        CheckOptionKind checkOption,
        string? owner,
        string? ownerId = null,
        BoundSetOperation? setOperation = null)
    {
        RejectTimeTravelBody(bodyAst);

        if (derived.Count == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "CREATE VIEW requires the body query to project at least one column");

        List<string> aliases = FlattenIdentifierList(columnAliasList);

        if (aliases.Count > 0 && aliases.Count != derived.Count)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"CREATE VIEW specifies {aliases.Count} column name(s) but the body query returns {derived.Count}");

        bool isSetOperation = SetOperationAst.IsSetOperationSelect(bodyAst);

        if (isSetOperation && setOperation is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "A set-operation view body reached the definition builder without its bound operands");

        // Expanding '*' into the body's actual output columns is what freezes the view's shape:
        // without it, adding a column to a base table would silently widen every SELECT * view over
        // it, changing what dependent views and clients see with no statement having been issued.
        // A set operation projects '*' over its operands, so it is the operands' '*' that is expanded,
        // and its output names are the projections of its leftmost operand.
        NodeAst normalizedBody = isSetOperation
            ? NormalizeSetOperationBody(bodyAst, setOperation!, derived)
            : ExpandStarProjections(bodyAst, derived);

        RejectUnfrozenSetOperationStars(normalizedBody);

        List<NodeAst> effectiveProjections = [];
        FlattenProjections(isSetOperation ? LeftmostOperand(normalizedBody).leftAst! : normalizedBody.leftAst!, effectiveProjections);

        if (effectiveProjections.Count != derived.Count)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"View body binding produced {derived.Count} column(s) for {effectiveProjections.Count} projection(s)");

        List<ViewColumnSchema> columns = new(derived.Count);

        for (int i = 0; i < derived.Count; i++)
        {
            string name = aliases.Count > 0 ? aliases[i] : RequireProjectionName(effectiveProjections[i], i);

            RejectEmptyName(name, i);
            RejectQualifiedName(name);

            columns.Add(new ViewColumnSchema
            {
                Name = name,
                Type = derived[i].Type,
                // Only a bare column reference is writable through an auto-updatable view; anything
                // computed has no base column to write back to, and a set operation has no single
                // base row a write could reach.
                SourceColumnName = isSetOperation ? null : TryGetSourceColumn(effectiveProjections[i]),
            });
        }

        RejectDuplicateNames(columns);

        // Rewriting each projection to carry its final name as an explicit alias is what makes the
        // stored body actually project the view's columns. Without it, a column-alias list (or a
        // qualified reference like o.id) would be recorded in Columns but absent from the SQL, so the
        // derived table the view expands to would expose the body's names and every reference to the
        // view's own column names would fail to resolve.
        NodeAst aliasedBody = isSetOperation
            ? ApplyLeftmostOutputNames(normalizedBody, effectiveProjections, columns)
            : ApplyOutputNames(normalizedBody, effectiveProjections, columns);

        // Computed from the body while it still names its relations, because that is what the
        // lookup below matches on. Binding first would leave nothing for it to resolve.
        (List<string> tableIds, List<string> viewIds) = ResolveDependencies(schema, aliasedBody);

        // Also computed from the name-form body, and for the same reason: the analyzer resolves a
        // column through the relation name or alias the FROM clause still carries.
        List<string> columnIds = ViewColumnDependencyAnalyzer.Collect(schema, aliasedBody);

        // The stored body refers to its relations by immutable id, so a rename is metadata-only and
        // no dependent definition has to be rewritten to keep working. Names come back at every
        // point a body is read or shown; see StoredBodyBinder.
        NodeAst boundBody = StoredBodyBinder.BindStoredForm(schema, aliasedBody);

        return new ViewDefinition
        {
            Sql = ViewBodyRenderer.RenderSelect(boundBody),
            Columns = columns,
            DependsOnTableIds = tableIds,
            DependsOnViewIds = viewIds,
            DependsOnColumnIds = columnIds,
            CheckOption = checkOption,
            Owner = owner,
            OwnerId = ownerId,
        };
    }

    /// <summary>
    /// Rebuilds the projection list so every item carries its final view column name as an explicit
    /// alias.
    /// </summary>
    /// <remarks>
    /// <b>Every</b> item, including one whose name already matches the column it selects. That looks
    /// redundant and is not: the body's columns are stored by immutable id, so a projection written
    /// <c>total</c> is stored as a token and would publish whatever that column is called at read
    /// time — silently renaming the view's own output the first time the base column is renamed. The
    /// alias is what pins the published name. It costs nothing to read, because the renderer omits an
    /// alias that merely repeats what it aliases.
    /// </remarks>
    private static NodeAst ApplyOutputNames(
        NodeAst select, List<NodeAst> projections, List<ViewColumnSchema> columns)
    {
        NodeAst? list = null;
        bool changed = false;

        for (int i = 0; i < projections.Count; i++)
        {
            NodeAst projection = projections[i];
            string finalName = columns[i].Name;

            bool alreadyNamed =
                projection.nodeType == NodeType.ExprAlias &&
                string.Equals(projection.rightAst?.yytext, finalName, StringComparison.Ordinal);

            NodeAst item;
            if (alreadyNamed)
            {
                item = projection;
            }
            else
            {
                // Re-aliasing an already-aliased projection replaces the alias rather than nesting,
                // so a column-alias list overriding an AS in the body produces one name, not two.
                NodeAst inner = projection.nodeType == NodeType.ExprAlias ? projection.leftAst! : projection;

                item = new NodeAst(
                    NodeType.ExprAlias,
                    inner,
                    new NodeAst(NodeType.Identifier, null, null, null, null, null, null, null, finalName),
                    null, null, null, null, null, null);

                changed = true;
            }

            list = list is null
                ? item
                : new NodeAst(NodeType.IdentifierList, list, item, null, null, null, null, null, null);
        }

        if (!changed)
            return select;

        return new NodeAst(
            NodeType.Select,
            list, select.rightAst, select.extendedOne, select.extendedTwo, select.extendedThree,
            select.extendedFour, select.extendedFive, select.yytext, select.extendedSix, select.extendedSeven);
    }

    /// <summary>
    /// The name a projection gives its output column, or a rejection when it gives none.
    /// </summary>
    /// <remarks>
    /// Derived from the projection node rather than from the bound column metadata on purpose: an
    /// unnamed expression is assigned its ordinal ("0", "1", …) as a positional key by the query
    /// pipeline, which is a usable key for a one-shot result set but not a name a view can publish —
    /// it would appear in SHOW COLUMNS, in dependent views, and in client column maps as something
    /// nothing can reference, and it would silently renumber if the projection list were reordered.
    /// PostgreSQL invents <c>?column?</c> here; refusing and asking for an alias costs the author one
    /// <c>AS</c> and produces a view that works.
    /// </remarks>
    private static string RequireProjectionName(NodeAst projection, int ordinal)
    {
        switch (projection.nodeType)
        {
            case NodeType.ExprAlias when projection.rightAst?.yytext is { Length: > 0 } alias:
                return alias;

            // A qualified reference (o.id) publishes the unqualified column name, as in PostgreSQL.
            case NodeType.Identifier when projection.yytext is { Length: > 0 } identifier:
                return StripQualifier(identifier)!;

            default:
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column {ordinal + 1} of the view body is an expression with no name; add an alias (AS <name>)");
        }
    }

    /// <summary>
    /// Refuses a body carrying <c>AS OF SYSTEM TIME</c>, at any nesting depth.
    ///
    /// <para>Neither form is storable. An absolute timestamp freezes the view to one instant forever,
    /// so it silently stops tracking its own tables. A relative one ('-2h') means something different
    /// at every reference, so two readers of the same view legitimately disagree about its contents.
    /// A user who wants a point-in-time copy wants <c>CREATE TABLE … AS SELECT … AS OF SYSTEM TIME</c>,
    /// which already exists and says what it does.</para>
    /// </summary>
    private static void RejectTimeTravelBody(NodeAst node)
    {
        if (node.nodeType == NodeType.Select && node.extendedSeven is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "A view body may not use AS OF SYSTEM TIME: an absolute timestamp would freeze the view " +
                "to one instant forever, and a relative one would mean something different at every " +
                "reference. Use CREATE TABLE ... AS SELECT ... AS OF SYSTEM TIME for a point-in-time copy.");

        foreach (NodeAst? child in new[]
                 {
                     node.leftAst, node.rightAst, node.extendedOne, node.extendedTwo,
                     node.extendedThree, node.extendedFour, node.extendedFive, node.extendedSix
                 })
        {
            if (child is not null)
                RejectTimeTravelBody(child);
        }
    }

    /// <summary>
    /// Normalizes a set-operation body before it is stored: every operand's <c>*</c> is expanded
    /// from that operand's bound columns, and every ORDER BY name on the leftmost path becomes a
    /// position.
    ///
    /// <para><b>Why positions.</b> The output names of a set operation are the names of its leftmost
    /// operand, and the stored body pins the view's names as aliases on that operand
    /// (<see cref="ApplyLeftmostOutputNames"/>). A column-alias list renames them, so
    /// <c>CREATE VIEW v (a) AS SELECT x FROM t UNION … ORDER BY x</c> would no longer find <c>x</c>.
    /// Each wrapper on the leftmost path has the same output names, so one name list serves all of
    /// them. A position means the same column under any name.</para>
    /// </summary>
    private static NodeAst NormalizeSetOperationBody(
        NodeAst wrapper,
        BoundSetOperation bound,
        IReadOnlyList<DerivedColumnSchema> outputColumns)
    {
        NodeAst setOperationAst = wrapper.rightAst!;

        NodeAst left = NormalizeOperand(setOperationAst.leftAst!, bound.Left, bound.LeftColumns, outputColumns);
        NodeAst right = NormalizeOperand(setOperationAst.rightAst!, bound.Right, bound.RightColumns, null);

        NodeAst? orderBy = wrapper.extendedTwo is null ? null : OrderByNamesToPositions(wrapper.extendedTwo, outputColumns);

        return new NodeAst(
            NodeType.Select,
            wrapper.leftAst,
            new NodeAst(NodeType.SetOperationReference, left, right, null, null, null, null, null, setOperationAst.yytext),
            wrapper.extendedOne, orderBy, wrapper.extendedThree, wrapper.extendedFour, wrapper.extendedFive,
            wrapper.yytext, wrapper.extendedSix, wrapper.extendedSeven);
    }

    /// <summary>
    /// One operand of <see cref="NormalizeSetOperationBody"/>. <paramref name="leftmostOutput"/> is the
    /// output schema when the operand is on the leftmost path, and null otherwise.
    /// </summary>
    private static NodeAst NormalizeOperand(
        NodeAst operand,
        BoundSelectQuery operandBound,
        IReadOnlyList<DerivedColumnSchema> operandColumns,
        IReadOnlyList<DerivedColumnSchema>? leftmostOutput)
    {
        if (SetOperationAst.IsSetOperationSelect(operand))
        {
            BoundSetOperation nested = operandBound.DerivedSources.Count == 1 && operandBound.DerivedSources[0].SetOperation is { } found
                ? found
                : throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    "A nested set operation in a view body has no bound operands");

            return NormalizeSetOperationBody(operand, nested, leftmostOutput ?? operandColumns);
        }

        NodeAst expanded = ExpandStarProjections(operand, operandColumns);

        // A parenthesized leftmost operand's own ORDER BY may name an output column by the name the
        // alias list replaces, so it moves to positions too.
        if (leftmostOutput is not null && expanded.extendedTwo is not null)
            return WithOrderBy(expanded, OrderByNamesToPositions(expanded.extendedTwo, leftmostOutput, projectionsOf: expanded));

        return expanded;
    }

    /// <summary>
    /// Returns <paramref name="orderBy"/> with each bare name that is an output column replaced by that
    /// column's position. For a leaf operand (<paramref name="projectionsOf"/> set), only a name that
    /// is an explicit alias of the operand moves: a bare column name there may be a base column that
    /// the operand does not project, and it sorts before projection, so it is left as it is.
    /// </summary>
    private static NodeAst OrderByNamesToPositions(
        NodeAst orderBy,
        IReadOnlyList<DerivedColumnSchema> outputColumns,
        NodeAst? projectionsOf = null)
    {
        HashSet<string>? aliases = null;

        if (projectionsOf is not null)
        {
            aliases = new(StringComparer.OrdinalIgnoreCase);
            List<NodeAst> projections = [];
            FlattenProjections(projectionsOf.leftAst!, projections);

            foreach (NodeAst projection in projections)
            {
                if (projection.nodeType == NodeType.ExprAlias && projection.rightAst?.yytext is { } alias)
                    aliases.Add(alias);
            }
        }

        switch (orderBy.nodeType)
        {
            case NodeType.IdentifierList:
                {
                    NodeAst l = OrderByNamesToPositions(orderBy.leftAst!, outputColumns, projectionsOf);
                    NodeAst r = OrderByNamesToPositions(orderBy.rightAst!, outputColumns, projectionsOf);
                    return ReferenceEquals(l, orderBy.leftAst) && ReferenceEquals(r, orderBy.rightAst)
                        ? orderBy
                        : new NodeAst(NodeType.IdentifierList, l, r, null, null, null, null, null, null);
                }

            case NodeType.SortAsc:
            case NodeType.SortDesc:
                {
                    NodeAst inner = OrderByNamesToPositions(orderBy.leftAst!, outputColumns, projectionsOf);
                    return ReferenceEquals(inner, orderBy.leftAst)
                        ? orderBy
                        : new NodeAst(orderBy.nodeType, inner, null, null, null, null, null, null, null);
                }

            case NodeType.Identifier when orderBy.yytext is { } name && (aliases is null || aliases.Contains(name)):
                for (int i = 0; i < outputColumns.Count; i++)
                {
                    if (string.Equals(outputColumns[i].Name, name, StringComparison.OrdinalIgnoreCase))
                        return NodeAst.FromLong(i + 1);
                }

                return orderBy;

            default:
                return orderBy;
        }
    }

    private static NodeAst WithOrderBy(NodeAst select, NodeAst orderBy) =>
        new(NodeType.Select, select.leftAst, select.rightAst, select.extendedOne, orderBy, select.extendedThree,
            select.extendedFour, select.extendedFive, select.yytext, select.extendedSix, select.extendedSeven);

    /// <summary>The leftmost leaf operand of a set-operation body: the one that names its columns.</summary>
    private static NodeAst LeftmostOperand(NodeAst wrapper)
    {
        NodeAst node = wrapper;

        while (SetOperationAst.IsSetOperationSelect(node))
            node = node.rightAst!.leftAst!;

        return node;
    }

    /// <summary>
    /// <see cref="ApplyOutputNames"/> for a set operation: the aliases go on the leftmost leaf
    /// operand, whose names are the output names, and every wrapper on the way is rebuilt.
    /// </summary>
    private static NodeAst ApplyLeftmostOutputNames(NodeAst node, List<NodeAst> projections, List<ViewColumnSchema> columns)
    {
        if (!SetOperationAst.IsSetOperationSelect(node))
            return ApplyOutputNames(node, projections, columns);

        NodeAst setOperationAst = node.rightAst!;
        NodeAst left = ApplyLeftmostOutputNames(setOperationAst.leftAst!, projections, columns);

        if (ReferenceEquals(left, setOperationAst.leftAst))
            return node;

        return new NodeAst(
            NodeType.Select,
            node.leftAst,
            new NodeAst(NodeType.SetOperationReference, left, setOperationAst.rightAst, null, null, null, null, null, setOperationAst.yytext),
            node.extendedOne, node.extendedTwo, node.extendedThree, node.extendedFour, node.extendedFive,
            node.yytext, node.extendedSix, node.extendedSeven);
    }

    /// <summary>
    /// Refuses a <c>*</c> in an operand of a set operation that the normalization above could not
    /// expand: one inside a derived table or a subquery of the body. The view would store the
    /// <c>*</c> as it is, and a column added to that operand's table would then give the two
    /// operands different column counts, so every read of the view would fail.
    /// </summary>
    private static void RejectUnfrozenSetOperationStars(NodeAst root)
    {
        Stack<NodeAst> pending = new();
        pending.Push(root);

        while (pending.Count > 0)
        {
            NodeAst node = pending.Pop();

            if (node.nodeType == NodeType.SetOperationReference)
            {
                foreach (NodeAst operand in new[] { node.leftAst!, node.rightAst! })
                {
                    if (SetOperationAst.IsSetOperationSelect(operand))
                        continue;

                    List<NodeAst> projections = [];
                    FlattenProjections(operand.leftAst!, projections);

                    if (projections.Any(p => p.nodeType == NodeType.ExprAllFields))
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInput,
                            "A view body may not use '*' in an operand of a UNION, INTERSECT or EXCEPT inside a " +
                            "derived table or a subquery; list the columns, so a column added to a table " +
                            "cannot give the operands different column counts");
                }
            }

            foreach (NodeAst? child in new[]
                     {
                         node.leftAst, node.rightAst, node.extendedOne, node.extendedTwo, node.extendedThree,
                         node.extendedFour, node.extendedFive, node.extendedSix, node.extendedSeven
                     })
            {
                if (child is not null)
                    pending.Push(child);
            }
        }
    }

    /// <summary>
    /// Replaces a <c>*</c> projection with the explicit output columns the body actually produces.
    /// Returns the original node untouched when there is no star, so the common case allocates
    /// nothing.
    /// </summary>
    private static NodeAst ExpandStarProjections(NodeAst select, IReadOnlyList<DerivedColumnSchema> derived)
    {
        List<NodeAst> projections = [];
        FlattenProjections(select.leftAst!, projections);

        if (!projections.Any(p => p.nodeType == NodeType.ExprAllFields))
            return select;

        if (projections.Count != 1)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "A view body may not mix '*' with other projections: the expansion order is not " +
                "stable across a base-table column add, so the view's frozen shape could not be honored");

        // Rebuilt left-nested, matching how select_field_list nests, so downstream walkers that
        // expect that shape (projection collection, renderer) see nothing unusual.
        NodeAst? list = null;
        foreach (DerivedColumnSchema column in derived)
        {
            NodeAst identifier = new(NodeType.Identifier, null, null, null, null, null, null, null, column.Name);
            list = list is null
                ? identifier
                : new NodeAst(NodeType.IdentifierList, list, identifier, null, null, null, null, null, null);
        }

        return new NodeAst(
            NodeType.Select,
            list, select.rightAst, select.extendedOne, select.extendedTwo, select.extendedThree,
            select.extendedFour, select.extendedFive, select.yytext, select.extendedSix, select.extendedSeven);
    }

    private static void FlattenProjections(NodeAst node, List<NodeAst> into)
    {
        if (node.nodeType == NodeType.IdentifierList)
        {
            if (node.leftAst is not null) FlattenProjections(node.leftAst, into);
            if (node.rightAst is not null) FlattenProjections(node.rightAst, into);
            return;
        }

        into.Add(node);
    }

    private static List<string> FlattenIdentifierList(NodeAst? node)
    {
        List<string> names = [];
        if (node is null)
            return names;

        Walk(node);
        return names;

        void Walk(NodeAst n)
        {
            if (n.nodeType == NodeType.IdentifierList)
            {
                if (n.leftAst is not null) Walk(n.leftAst);
                if (n.rightAst is not null) Walk(n.rightAst);
                return;
            }

            names.Add(n.yytext ?? "");
        }
    }

    /// <summary>
    /// The base column an output column is a direct reference to, or null when it is computed.
    /// An aliased column reference (<c>total AS t</c>) is still a direct reference — the alias
    /// renames the output, it does not compute anything.
    /// </summary>
    private static string? TryGetSourceColumn(NodeAst projection) => projection.nodeType switch
    {
        NodeType.Identifier => StripQualifier(projection.yytext),
        NodeType.ExprAlias when projection.leftAst?.nodeType == NodeType.Identifier
            => StripQualifier(projection.leftAst.yytext),
        _ => null,
    };

    private static string? StripQualifier(string? name)
    {
        if (name is null)
            return null;

        int dot = name.LastIndexOf('.');
        return dot < 0 ? name : name[(dot + 1)..];
    }

    private static void RejectEmptyName(string name, int ordinal)
    {
        if (!string.IsNullOrWhiteSpace(name))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"CREATE VIEW column name {ordinal + 1} is empty");
    }

    private static void RejectQualifiedName(string name)
    {
        if (!name.Contains('.'))
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"View column name '{name}' is qualified; add an alias (AS <name>) to give it a bare name");
    }

    private static void RejectDuplicateNames(List<ViewColumnSchema> columns)
    {
        HashSet<string> seen = new(StringComparer.OrdinalIgnoreCase);

        foreach (ViewColumnSchema column in columns)
        {
            if (!seen.Add(column.Name))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Column name '{column.Name}' is specified more than once");
        }
    }

    /// <summary>
    /// Walks the body's FROM clauses (including those of nested subqueries and derived tables) and
    /// resolves every referenced relation to its immutable id.
    /// </summary>
    /// <remarks>
    /// Ids, never names: a name is a moving target that a rename invalidates, and the whole point of
    /// recording dependencies is to survive renames and to answer "what breaks if I drop this".
    /// A name that resolves to nothing is left out rather than raising — the binder has already
    /// rejected genuinely unknown relations, so anything unresolved here is a construct that does not
    /// reference a stored relation at all.
    /// </remarks>
    private static (List<string> TableIds, List<string> ViewIds) ResolveDependencies(Schema schema, NodeAst body)
    {
        HashSet<string> tableIds = new(StringComparer.Ordinal);
        HashSet<string> viewIds = new(StringComparer.Ordinal);

        CollectRelationNames(body, name =>
        {
            if (schema.Tables.TryGetValue(name, out TableSchema? table) && table.Id is not null)
                tableIds.Add(table.Id);
            else if (schema.Views.TryGetValue(name, out ViewSchema? view) && view.Id is not null)
                viewIds.Add(view.Id);
        });

        return ([.. tableIds], [.. viewIds]);
    }

    /// <summary>
    /// Invokes <paramref name="onName"/> for every relation name appearing in a FROM clause anywhere
    /// in the tree, including inside scalar/EXISTS/IN subqueries and derived tables.
    /// </summary>
    internal static void CollectRelationNames(NodeAst node, Action<string> onName)
    {
        switch (node.nodeType)
        {
            case NodeType.Select:
                if (node.rightAst is not null)
                    CollectFromRelations(node.rightAst, onName);
                break;

            case NodeType.TableReference:
                if (node.leftAst?.yytext is not null)
                    onName(node.leftAst.yytext);
                break;
        }

        foreach (NodeAst? child in new[]
                 {
                     node.leftAst, node.rightAst, node.extendedOne, node.extendedTwo,
                     node.extendedThree, node.extendedFour, node.extendedFive, node.extendedSix
                 })
        {
            if (child is not null)
                CollectRelationNames(child, onName);
        }
    }

    private static void CollectFromRelations(NodeAst from, Action<string> onName)
    {
        switch (from.nodeType)
        {
            case NodeType.Identifier:
                if (from.yytext is not null)
                    onName(from.yytext);
                return;

            case NodeType.IdentifierWithOpts:
                if (from.leftAst?.yytext is not null)
                    onName(from.leftAst.yytext);
                return;

            case NodeType.TableReference:
                if (from.leftAst?.yytext is not null)
                    onName(from.leftAst.yytext);
                return;

            case NodeType.DerivedTableReference:
                // The subquery's own FROM is reached by the generic walk in CollectRelationNames.
                return;

            case NodeType.Join:
            case NodeType.CommaJoin:
            case NodeType.CommaJoinTableList:
                if (from.leftAst is not null) CollectFromRelations(from.leftAst, onName);
                if (from.rightAst is not null) CollectFromRelations(from.rightAst, onName);
                return;
        }
    }
}
