/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// A correlated subquery that runs through the whole bind pipeline once for each outer row, so every
/// form of SELECT can be its body: a join, a derived table, no FROM, an aggregate, HAVING, OFFSET, a
/// window function, or another subquery.
///
/// <para><b>How the outer row reaches the body.</b> Each name of the body that resolves to the outer query
/// is replaced, in a copy of the body tree, by a hidden placeholder (<see cref="ParameterPrefix"/> and a
/// number). The lexer accepts only letters, digits and underscores after <c>@</c>, so no statement can name
/// such a placeholder. For each outer row, <see cref="ParametersFor"/> reads the outer values and adds them
/// to the statement parameters, and the body runs with those parameters. One placeholder stands for one
/// outer name, wherever the body writes it. A placeholder has the type of its outer column
/// (<see cref="SubqueryResultTypes.Remember"/>), so the static checks of the body see that type, as they
/// see the type of a column.</para>
///
/// <para><b>One bind or one bind for each row.</b> A bind runs each uncorrelated subquery of the body and
/// keeps its value. When no placeholder is inside a nested subquery, nothing that the bind runs depends on
/// the outer row: the body binds once here (<see cref="Bound"/>) and each row gets only a new query ticket.
/// Otherwise a nested subquery reads an outer value, so the body binds again for each row, and
/// <see cref="Bound"/> is null.</para>
///
/// <para><b>Errors do not depend on the rows.</b> This preparation always binds the body once, also when
/// no outer row reaches it: a real bind when it binds once, or a bind in the
/// <see cref="StoredDefinitionBind"/> flow, which runs no nested subquery and reads no row. So an unknown
/// name or a known type mismatch fails the statement over an empty outer table too.</para>
///
/// <para>An outer name inside a derived table or a set operation of the body is replaced too, see
/// <see cref="SubqueryScopeAnalyzer"/>. An outer name in an argument of a table function in FROM is not,
/// so such a statement fails with an error, not with a wrong answer.</para>
/// </summary>
internal sealed class CorrelatedSubqueryBody
{
    /// <summary>The start of each hidden placeholder name.</summary>
    internal const string ParameterPrefix = "@#outer";

    private readonly SubqueryQueryExecutor executor;

    private readonly bool isExistsBody;

    private readonly OuterValue[] values;

    private readonly IReadOnlyList<BoundTableSource> outerSources;

    private readonly IReadOnlyList<BoundDerivedTableSource> outerDerivedSources;

    private readonly QueryRowNameResolver outerRowNames;

    /// <summary>The body, with a hidden placeholder for each outer name.</summary>
    public NodeAst Select { get; }

    /// <summary>The body bound for all its runs, or null when it binds again for each row.</summary>
    public SelectBindResult? Bound { get; }

    /// <summary>One outer name and the placeholder that stands for it.</summary>
    private readonly record struct OuterValue(string Parameter, NodeAst Name);

    private CorrelatedSubqueryBody(
        SubqueryQueryExecutor executor,
        bool isExistsBody,
        NodeAst select,
        SelectBindResult? bound,
        OuterValue[] values,
        IReadOnlyList<BoundTableSource> outerSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        QueryRowNameResolver outerRowNames)
    {
        this.executor = executor;
        this.isExistsBody = isExistsBody;
        Select = select;
        Bound = bound;
        this.values = values;
        this.outerSources = outerSources;
        this.outerDerivedSources = outerDerivedSources;
        this.outerRowNames = outerRowNames;
    }

    /// <summary>
    /// Prepares <paramref name="select"/>, whose <paramref name="outerReferences"/> are the nodes that
    /// <see cref="SubqueryScopeAnalyzer.FindOuterReferencesAsync"/> returned for it. The bind runs under
    /// <paramref name="ticket"/>, with the statement parameters and no outer value: a stage that read an outer
    /// value at bind time would fail with an unknown placeholder, not use a wrong value.
    /// </summary>
    /// <param name="bodyQuery">The body as query creation made it, used only to type the outer names.</param>
    /// <param name="isExistsBody">True for the body of an EXISTS, whose output the pipeline can discard.</param>
    public static async Task<CorrelatedSubqueryBody> PrepareAsync(
        SubqueryQueryExecutor executor,
        DatabaseDescriptor database,
        NodeAst select,
        SelectQuery bodyQuery,
        IReadOnlyList<NodeAst> outerReferences,
        IReadOnlyList<BoundTableSource> outerSources,
        IReadOnlyList<BoundDerivedTableSource> outerDerivedSources,
        ExecuteSQLTicket ticket,
        bool isExistsBody)
    {
        QueryRowNameResolver outerRowNames = new(outerSources, outerDerivedSources);
        ConditionalResultTypes.ColumnTypeLookup outerTypes = ConditionalResultTypes.LookupFor(
            new BoundSelectQuery(bodyQuery, outerSources, outerRowNames, outerDerivedSources),
            excludeOutputAliases: false);

        Dictionary<string, NodeAst> placeholders = new(StringComparer.OrdinalIgnoreCase);
        Dictionary<NodeAst, NodeAst> replacements = new(ReferenceEqualityComparer.Instance);
        List<OuterValue> values = [];

        foreach (NodeAst reference in outerReferences)
        {
            string name = reference.yytext!;

            if (!placeholders.TryGetValue(name, out NodeAst? placeholder))
            {
                // Also fills the memo of the resolver, which each row then reads.
                outerRowNames.ValidateColumnReference(name);

                string parameter = ParameterPrefix + values.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);
                placeholder = SubqueryResultTypes.Remember(
                    new NodeAst(NodeType.Placeholder, null, null, null, null, null, null, null, parameter),
                    outerTypes(name));

                placeholders[name] = placeholder;
                values.Add(new OuterValue(parameter, reference));
            }

            replacements[reference] = placeholder;
        }

        NodeAst substituted = NodeAstWalk.Replace(select, replacements);
        SelectBindResult? bound = null;

        if (NestedSubqueryReadsOuterValue(substituted))
        {
            using (StoredDefinitionBind.Enter())
                await executor.BindForRunsAsync(database, substituted, ticket, isExistsBody).ConfigureAwait(false);
        }
        else
        {
            bound = await executor.BindForRunsAsync(database, substituted, ticket, isExistsBody).ConfigureAwait(false);
        }

        return new CorrelatedSubqueryBody(executor, isExistsBody, substituted, bound, [.. values], outerSources, outerDerivedSources, outerRowNames);
    }

    /// <summary>
    /// The rows of the body for <paramref name="outerRow"/>. A body that binds once gets a new query ticket
    /// for this row; any other body binds again, with this row's values. The body keeps the subquery
    /// executor that prepared it, because the operators that evaluate a filter are built before that
    /// executor exists.
    /// </summary>
    public IAsyncEnumerable<QueryResultRow> RunAsync(
        DatabaseDescriptor database,
        IReadOnlyDictionary<string, ColumnValue> outerRow,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? statementParameters,
        CancellationToken cancellationToken)
    {
        Dictionary<string, ColumnValue> parameters = ParametersFor(outerRow, statementParameters);

        if (Bound is { } bound)
            return executor.RunBoundAsync(database, bound, txnState, parameters, cancellationToken);

        return isExistsBody
            ? executor.ExecuteExistsSelectAsync(database, Select, txnState, parameters, cancellationToken)
            : executor.ExecuteSelectAsync(database, Select, txnState, parameters, cancellationToken);
    }

    /// <summary>
    /// The statement parameters and the outer values of <paramref name="outerRow"/>, under their hidden
    /// placeholder names. A new dictionary for each row: the statement parameters are not changed.
    /// </summary>
    private Dictionary<string, ColumnValue> ParametersFor(
        IReadOnlyDictionary<string, ColumnValue> outerRow,
        Dictionary<string, ColumnValue>? statementParameters)
    {
        Dictionary<string, ColumnValue> outerValues = CorrelatedRowMerger.MergeOuterForEvaluation(
            outerRow, outerSources, outerDerivedSources);

        Dictionary<string, ColumnValue> parameters = statementParameters is null
            ? new(values.Length)
            : new(statementParameters, statementParameters.Comparer);

        foreach (OuterValue value in values)
            parameters[value.Parameter] = SqlExecutor.EvalExpr(value.Name, outerValues, statementParameters, outerRowNames);

        return parameters;
    }

    /// <summary>
    /// True when a hidden placeholder is inside a nested subquery of <paramref name="select"/>. Then a
    /// bind runs a subquery that reads an outer value, and the body cannot bind once for all rows.
    /// </summary>
    private static bool NestedSubqueryReadsOuterValue(NodeAst select)
    {
        Stack<NodeAst> pending = new();
        pending.Push(select);

        while (pending.TryPop(out NodeAst? node))
        {
            NodeAst? nested = node.nodeType switch
            {
                NodeType.ExprScalarSubquery or NodeType.ExprExistsSubquery => node.leftAst,
                NodeType.ExprInSubquery or NodeType.ExprNotInSubquery => node.rightAst,
                _ => null,
            };

            if (nested is not null && NodeAstWalk.Any(nested, static n => IsHiddenPlaceholder(n)))
                return true;

            foreach (NodeAst? child in (ReadOnlySpan<NodeAst?>)[
                node.leftAst, node.rightAst,
                node.extendedOne, node.extendedTwo, node.extendedThree, node.extendedFour,
                node.extendedFive, node.extendedSix, node.extendedSeven])
            {
                if (child is not null && !ReferenceEquals(child, nested))
                    pending.Push(child);
            }
        }

        return false;
    }

    private static bool IsHiddenPlaceholder(NodeAst node) =>
        node.nodeType == NodeType.Placeholder
        && node.yytext is { } name
        && name.StartsWith(ParameterPrefix, StringComparison.Ordinal);
}
