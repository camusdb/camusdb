/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// The parts of one <c>UPDATE … FROM</c> or <c>DELETE … USING</c> statement that
/// <see cref="JoinedDmlExecutor"/> runs: the SELECT of its read phase, and copies of its SET values and
/// of its re-checked predicate in which each column of another relation is a hidden placeholder.
///
/// <para><b>Which relation a name reads.</b> A name qualified with the target's alias (or with the
/// table name when the target has no alias) reads the target. A bare name reads the target when the
/// target has a column of that name; the read phase also selects that bare name, so the SELECT binder
/// refuses it as ambiguous when another relation has a column of the same name, as PostgreSQL does.
/// Every other name reads another relation, and the SELECT binder refuses it when no relation has it.
/// The decision is by name, so it needs no bound query, and the binder still checks every name.</para>
///
/// <para><b>The read phase.</b> The SELECT lists the primary-key columns of the target, then one
/// column for each distinct name of another relation, then one column for each SET value that holds a
/// subquery (computed there as a whole, at the statement snapshot), then the bare target names it
/// selects only for the ambiguity check. Its FROM is the target and the listed relations: a comma join
/// when the list is one relation or a comma list, so the planner moves the equalities of the WHERE into
/// join conditions as it does for any comma join; and an inner join whose ON holds the WHERE conjuncts
/// that name the target when the list is a chain of JOINs, because a chain is planned from its ON
/// conditions. The target is the last relation of a chain, so a RIGHT JOIN inside the list keeps its
/// own left operand. Both shapes return the rows of the cross product that pass the WHERE.</para>
///
/// <para>The parsed tree is never changed: it can be shared through the parser cache. Every rewrite
/// builds new nodes (<see cref="NodeAstWalk.Replace"/>).</para>
/// </summary>
internal sealed class JoinedDmlPlan
{
    /// <summary>The statement form, for errors: <c>UPDATE ... FROM</c>, <c>DELETE ... USING</c>, or <c>UPDATE</c> or <c>DELETE</c> with a target alias only.</summary>
    public string StatementName { get; }

    /// <summary>The target as a table reference with its alias, the FROM of the RETURNING bind.</summary>
    public NodeAst TargetReference { get; }

    /// <summary>The SELECT of the read phase; see the class summary for its columns.</summary>
    public NodeAst JoinSelect { get; }

    /// <summary>The primary-key columns of the target, in key order; the first output columns of <see cref="JoinSelect"/>.</summary>
    public string[] KeyColumns { get; }

    /// <summary>Each hidden placeholder name, and the output column of <see cref="JoinSelect"/> that gives its value.</summary>
    public IReadOnlyList<(string Name, int Ordinal)> Cells { get; }

    /// <summary>The SET values with hidden placeholders and bare target names, keyed by column; null for a DELETE.</summary>
    public Dictionary<string, NodeAst>? SetValues { get; }

    /// <summary>
    /// The WHERE conjuncts that name a target column and hold no subquery, with hidden placeholders and
    /// bare target names, combined with AND; null when there is none.
    /// </summary>
    public NodeAst? Recheck { get; }

    /// <summary>The schema-cased target columns <see cref="Recheck"/> reads; empty when there is no re-check.</summary>
    public IReadOnlySet<string> RecheckColumns { get; }

    /// <summary>The schema-cased target columns the SET values and <see cref="Recheck"/> read.</summary>
    public IReadOnlySet<string>? TargetColumnsRead { get; }

    /// <summary>Each hidden placeholder node and the output column whose static type it takes.</summary>
    private readonly List<(NodeAst Placeholder, int Ordinal)> typedPlaceholders;

    private JoinedDmlPlan(
        string statementName,
        NodeAst targetReference,
        NodeAst joinSelect,
        string[] keyColumns,
        List<(string, int)> cells,
        Dictionary<string, NodeAst>? setValues,
        NodeAst? recheck,
        IReadOnlySet<string> recheckColumns,
        IReadOnlySet<string>? targetColumnsRead,
        List<(NodeAst, int)> typedPlaceholders)
    {
        StatementName = statementName;
        TargetReference = targetReference;
        JoinSelect = joinSelect;
        KeyColumns = keyColumns;
        Cells = cells;
        SetValues = setValues;
        Recheck = recheck;
        RecheckColumns = recheckColumns;
        TargetColumnsRead = targetColumnsRead;
        this.typedPlaceholders = typedPlaceholders;
    }

    /// <summary>
    /// Gives each hidden placeholder the static type of the output column it stands for, once the read
    /// phase is bound. A type that the binding does not show leaves the placeholder untyped, as a
    /// statement parameter is.
    /// </summary>
    public void RememberTypes(IReadOnlyList<ExpressionType?>? staticTypes)
    {
        if (staticTypes is null)
            return;

        foreach ((NodeAst placeholder, int ordinal) in typedPlaceholders)
        {
            if (ordinal < staticTypes.Count)
                SubqueryResultTypes.Remember(placeholder, staticTypes[ordinal]);
        }
    }

    /// <summary>
    /// Builds the plan of <paramref name="ast"/>, an UPDATE (with <paramref name="setValues"/>) or a
    /// DELETE (without), against the current schema of <paramref name="table"/>.
    /// </summary>
    public static JoinedDmlPlan Build(
        TableDescriptor table,
        NodeAst ast,
        Dictionary<string, NodeAst>? setValues,
        NodeAst where,
        NodeAst? returningList,
        string statementName)
    {
        string tableName = ast.leftAst!.yytext!;
        string? alias = DmlJoinSlots.TargetAlias(ast)?.yytext;
        NodeAst? relations = DmlJoinSlots.RelationList(ast);

        Builder builder = new(table, alias ?? tableName);

        NodeAst targetReference = new(
            NodeType.TableReference,
            Identifier(tableName),
            alias is null ? null : Identifier(alias),
            null, null, null, null, null, null);

        if (relations is not null)
            RefuseSecondUnaliasedTarget(relations, tableName, alias, statementName);

        // The primary key comes first: the read phase reads the key of each joined row by position.
        // Only a public key: the write phase finds each joined row through it.
        if (table.ReadablePrimaryKey is not { } primaryKey)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Table '{table.Name}' has no primary key that a statement can read");

        string[] keyColumns = new string[primaryKey.Columns.Length];
        for (int i = 0; i < keyColumns.Length; i++)
        {
            keyColumns[i] = builder.SchemaName(primaryKey.Columns[i]) ?? primaryKey.Columns[i];
            builder.Project(Identifier(string.Concat(builder.Qualifier, ".", keyColumns[i])));
        }

        // The SET values: a value without a subquery is evaluated in the write phase, on the locked read;
        // a value with one is computed in the read phase as a whole.
        Dictionary<string, NodeAst>? rewrittenSet = null;
        if (setValues is not null)
        {
            rewrittenSet = new(setValues.Count, StringComparer.OrdinalIgnoreCase);

            foreach ((string column, NodeAst value) in setValues)
            {
                rewrittenSet[column] = RequiredColumnAnalyzer.ContainsSubqueryNode(value)
                    ? builder.ProjectWholeValue(value)
                    : builder.Rewrite(value, recheck: false);
            }
        }

        // The re-check: the conjuncts of the WHERE that name a target column and hold no subquery.
        List<NodeAst> conjuncts = [];
        PredicateAnalyzer.CollectAndConjuncts(where, conjuncts);

        List<NodeAst> recheckConjuncts = [];
        List<NodeAst> targetConjuncts = [];
        List<NodeAst> otherConjuncts = [];

        foreach (NodeAst conjunct in conjuncts)
        {
            bool namesTarget = !RequiredColumnAnalyzer.ContainsSubqueryNode(conjunct) && builder.NamesTarget(conjunct);

            if (namesTarget)
            {
                targetConjuncts.Add(conjunct);
                recheckConjuncts.Add(builder.Rewrite(conjunct, recheck: true));
            }
            else
            {
                otherConjuncts.Add(conjunct);
            }
        }

        // RETURNING reads the target only; its bare target names join the ambiguity check. Without a
        // relation list, * is the target's columns, as in PostgreSQL, so it is refused only with one.
        if (returningList is not null)
            builder.CheckReturning(returningList, statementName, refuseStar: relations is not null);

        NodeAst from;
        NodeAst? joinWhere;

        if (relations is null)
        {
            from = targetReference;
            joinWhere = where;
        }
        else if (relations.nodeType == NodeType.Join)
        {
            // A chain of JOINs: the target joins last, on the conjuncts that name it.
            from = targetConjuncts.Count > 0
                ? new NodeAst(NodeType.Join, relations, targetReference, PredicateAnalyzer.CombineConjuncts(targetConjuncts),
                    null, null, null, null, JoinAstKind.Write(JoinAstKind.Kind.Inner))
                : new NodeAst(NodeType.Join, relations, targetReference, null,
                    null, null, null, null, JoinAstKind.Write(JoinAstKind.Kind.Cross));
            joinWhere = otherConjuncts.Count > 0 ? PredicateAnalyzer.CombineConjuncts(otherConjuncts) : null;
        }
        else
        {
            from = new NodeAst(NodeType.CommaJoin, targetReference, CommaList(relations), null, null, null, null, null, null);
            joinWhere = where;
        }

        NodeAst joinSelect = new(NodeType.Select, builder.ProjectionList(), from, joinWhere, null, null, null, null, null, null);

        return new JoinedDmlPlan(
            statementName,
            targetReference,
            joinSelect,
            keyColumns,
            builder.Cells,
            rewrittenSet,
            recheckConjuncts.Count > 0 ? PredicateAnalyzer.CombineConjuncts(recheckConjuncts) : null,
            builder.RecheckColumns,
            builder.TargetColumnsRead,
            builder.TypedPlaceholders);
    }

    /// <summary>
    /// The listed relations as the right operand of a comma join: a comma list stays as it is, one
    /// relation is a list of one.
    /// </summary>
    private static NodeAst CommaList(NodeAst relations)
    {
        if (relations.nodeType != NodeType.CommaJoin)
            return relations;

        // CommaJoin(first, rest) lists first before rest; rebuild it as one left-deep list.
        List<NodeAst> items = [relations.leftAst!];
        Stack<NodeAst> pending = new();
        NodeAst node = relations.rightAst!;

        // The list is left-deep: CommaJoinTableList(earlier items, last item). Walk it without recursion.
        while (node.nodeType == NodeType.CommaJoinTableList)
        {
            pending.Push(node.rightAst!);
            node = node.leftAst!;
        }

        items.Add(node);
        while (pending.TryPop(out NodeAst? item))
            items.Add(item);

        NodeAst list = items[0];
        for (int i = 1; i < items.Count; i++)
            list = new NodeAst(NodeType.CommaJoinTableList, list, items[i], null, null, null, null, null, null);

        return list;
    }

    /// <summary>
    /// Refuses the target table named a second time in the relation list with no alias on either side, as
    /// PostgreSQL does: both would answer to one name. A self join gives one of them an alias.
    /// </summary>
    private static void RefuseSecondUnaliasedTarget(NodeAst relations, string tableName, string? alias, string statementName)
    {
        string targetName = alias ?? tableName;

        Stack<NodeAst> pending = new();
        pending.Push(relations);

        while (pending.TryPop(out NodeAst? node))
        {
            switch (node.nodeType)
            {
                case NodeType.TableReference:
                case NodeType.DerivedTableReference:
                case NodeType.TableFunctionReference:
                    {
                        // The name a relation answers to: its alias, or the table or function name.
                        string? name = node.nodeType switch
                        {
                            NodeType.TableReference => node.rightAst?.yytext ?? node.leftAst!.yytext,
                            NodeType.DerivedTableReference => node.rightAst?.yytext,
                            _ => node.extendedOne?.yytext ?? node.leftAst?.yytext,
                        };

                        if (string.Equals(name, targetName, StringComparison.OrdinalIgnoreCase))
                            throw new CamusDBException(
                                CamusDBErrorCodes.InvalidInput,
                                $"Table name '{name}' is specified more than once in {statementName}: give the target or the other relation an alias");
                        break;
                    }

                case NodeType.Join:
                case NodeType.CommaJoin:
                case NodeType.CommaJoinTableList:
                    if (node.leftAst is not null)
                        pending.Push(node.leftAst);
                    if (node.rightAst is not null)
                        pending.Push(node.rightAst);
                    break;
            }
        }
    }

    private static NodeAst Identifier(string name) =>
        new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    /// <summary>Collects the projection, the hidden placeholders and the target columns while the plan is built.</summary>
    private sealed class Builder
    {
        private readonly TableSchema schema;

        private readonly List<NodeAst> projections = [];

        /// <summary>The placeholder of each distinct name of another relation, compared without case.</summary>
        private readonly Dictionary<string, NodeAst> placeholderByName = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>The bare target names already selected for the ambiguity check.</summary>
        private readonly HashSet<string> checkedBareNames = new(StringComparer.OrdinalIgnoreCase);

        private readonly HashSet<string> targetColumnsRead = new(StringComparer.OrdinalIgnoreCase);

        private readonly HashSet<string> recheckColumns = new(StringComparer.OrdinalIgnoreCase);

        public Builder(TableDescriptor table, string qualifier)
        {
            schema = table.Schema;
            Qualifier = qualifier;
        }

        /// <summary>The name that qualifies a target column: the alias, or the table name without one.</summary>
        public string Qualifier { get; }

        public List<(string, int)> Cells { get; } = [];

        public List<(NodeAst, int)> TypedPlaceholders { get; } = [];

        public IReadOnlySet<string> TargetColumnsRead => targetColumnsRead;

        public IReadOnlySet<string> RecheckColumns => recheckColumns;

        /// <summary>Adds an output column to the read phase and returns its position.</summary>
        public int Project(NodeAst expression)
        {
            projections.Add(expression);
            return projections.Count - 1;
        }

        /// <summary>The projection list of the read phase, as the select-list node the grammar builds.</summary>
        public NodeAst ProjectionList()
        {
            NodeAst list = projections[0];
            for (int i = 1; i < projections.Count; i++)
                list = new NodeAst(NodeType.IdentifierList, list, projections[i], null, null, null, null, null, null);

            return list;
        }

        /// <summary>The schema spelling of a readable target column, or null when the target has none of that name.</summary>
        public string? SchemaName(string name)
        {
            foreach (TableColumnSchema column in schema.Columns!)
            {
                if (string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase) && SchemaElementStateRules.IsReadable(column))
                    return column.Name;
            }

            return null;
        }

        /// <summary>
        /// The target column a name reads, or null when it reads another relation. A name qualified with
        /// the target that the target does not have is an unknown column.
        /// </summary>
        public string? TargetColumnOf(string name)
        {
            int dot = name.LastIndexOf('.');

            if (dot < 0)
                return SchemaName(name);

            if (!string.Equals(name[..dot], Qualifier, StringComparison.OrdinalIgnoreCase))
                return null;

            return SchemaName(name[(dot + 1)..])
                ?? throw new CamusDBException(CamusDBErrorCodes.UnknownColumn, $"Unknown column '{name}'");
        }

        /// <summary>True when the expression names a column of the target.</summary>
        public bool NamesTarget(NodeAst expression)
        {
            List<NodeAst> references = [];
            QueryExpressionWalker.CollectColumnReferenceNodes(expression, references);

            foreach (NodeAst reference in references)
            {
                if (TargetColumnOf(reference.yytext!) is not null)
                    return true;
            }

            return false;
        }

        /// <summary>
        /// A copy of <paramref name="expression"/> (which holds no subquery) in which each target name is
        /// the bare column name and each name of another relation is its hidden placeholder.
        /// </summary>
        public NodeAst Rewrite(NodeAst expression, bool recheck)
        {
            List<NodeAst> references = [];
            QueryExpressionWalker.CollectColumnReferenceNodes(expression, references);

            Dictionary<NodeAst, NodeAst> replacements = new(ReferenceEqualityComparer.Instance);

            foreach (NodeAst reference in references)
            {
                string name = reference.yytext!;

                if (TargetColumnOf(name) is { } column)
                {
                    targetColumnsRead.Add(column);
                    if (recheck)
                        recheckColumns.Add(column);

                    if (name.IndexOf('.') < 0)
                        CheckBareName(name);

                    if (!string.Equals(name, column, StringComparison.Ordinal))
                        replacements[reference] = Identifier(column);

                    continue;
                }

                replacements[reference] = PlaceholderFor(name);
            }

            return NodeAstWalk.Replace(expression, replacements);
        }

        /// <summary>A SET value with a subquery: the read phase computes it, and the value is its placeholder.</summary>
        public NodeAst ProjectWholeValue(NodeAst value)
        {
            int ordinal = Project(value);
            string name = NextName();
            NodeAst placeholder = new(NodeType.Placeholder, null, null, null, null, null, null, null, name);

            Cells.Add((name, ordinal));
            TypedPlaceholders.Add((placeholder, ordinal));
            return placeholder;
        }

        /// <summary>
        /// Refuses a RETURNING item that reads another relation, and a bare <c>*</c>; selects each bare
        /// target name for the ambiguity check.
        /// </summary>
        public void CheckReturning(NodeAst returningList, string statementName, bool refuseStar)
        {
            Stack<NodeAst> pending = new();
            pending.Push(returningList);

            while (pending.TryPop(out NodeAst? item))
            {
                if (item.nodeType == NodeType.IdentifierList)
                {
                    if (item.rightAst is not null)
                        pending.Push(item.rightAst);
                    if (item.leftAst is not null)
                        pending.Push(item.leftAst);
                    continue;
                }

                NodeAst expression = QueryExpressionClassifier.UnwrapAlias(item);

                if (expression.nodeType == NodeType.ExprAllFields)
                {
                    if (!refuseStar)
                        continue;

                    throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInput,
                        $"RETURNING * is not supported in {statementName}: in PostgreSQL it also returns the columns of the other " +
                        "relations, which CamusDB does not. List the columns of the target");
                }

                List<NodeAst> references = [];
                QueryExpressionWalker.CollectColumnReferenceNodes(expression, references);

                foreach (NodeAst reference in references)
                {
                    string name = reference.yytext!;

                    if (TargetColumnOf(name) is null)
                        throw new CamusDBException(
                            CamusDBErrorCodes.InvalidInput,
                            $"RETURNING in {statementName} can read only columns of the target table, not '{name}'");

                    if (name.IndexOf('.') < 0)
                        CheckBareName(name);
                }
            }
        }

        /// <summary>Selects a bare target name once, so the SELECT binder refuses it when it is ambiguous.</summary>
        private void CheckBareName(string name)
        {
            if (checkedBareNames.Add(name))
                Project(Identifier(name));
        }

        /// <summary>The placeholder of a name of another relation: one for each distinct name, and one output column.</summary>
        private NodeAst PlaceholderFor(string name)
        {
            if (placeholderByName.TryGetValue(name, out NodeAst? existing))
                return existing;

            int ordinal = Project(Identifier(name));
            string hidden = NextName();
            NodeAst placeholder = new(NodeType.Placeholder, null, null, null, null, null, null, null, hidden);

            placeholderByName[name] = placeholder;
            Cells.Add((hidden, ordinal));
            TypedPlaceholders.Add((placeholder, ordinal));
            return placeholder;
        }

        private string NextName() =>
            JoinedDmlExecutor.ParameterPrefix + Cells.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);
    }
}
