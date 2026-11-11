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
/// such a placeholder. For each outer row, <see cref="Outer"/> reads the outer values, <see cref="ParametersFor"/>
/// adds them to the statement parameters, and the body runs with those parameters. The same values key the
/// result cache of the caller (<see cref="CorrelatedResultCache{T}"/>). One placeholder stands for one
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

    /// <summary>The hidden placeholder name of each outer value, in the order of <see cref="Outer"/>.</summary>
    private readonly string[] parameterNames;

    /// <summary>The body, with a hidden placeholder for each outer name.</summary>
    public NodeAst Select { get; }

    /// <summary>Reads the outer values of the body from an outer row; see <see cref="CorrelatedResultCache{T}"/>.</summary>
    public CorrelatedOuterValues Outer { get; }

    /// <summary>
    /// True when two runs with the same outer values give the same result, so a
    /// <see cref="CorrelatedResultCache{T}"/> can keep it: the body has no volatile function and reads no hidden
    /// value of an enclosing query level.
    /// </summary>
    public bool ResultsCanBeKept { get; }

    /// <summary>The body bound for all its runs, or null when it binds again for each row.</summary>
    public SelectBindResult? Bound { get; }

    /// <summary>
    /// The static type of the one output column of a body that is not an EXISTS body, or null when the
    /// statement does not show it. The statement checks compare it as they compare the column of an
    /// uncorrelated subquery, see <see cref="SubqueryResultTypes"/>.
    /// </summary>
    public Models.ExpressionType? OutputType { get; }

    private CorrelatedSubqueryBody(
        SubqueryQueryExecutor executor,
        bool isExistsBody,
        NodeAst select,
        SelectBindResult? bound,
        Models.ExpressionType? outputType,
        string[] parameterNames,
        CorrelatedOuterValues outer,
        bool resultsCanBeKept)
    {
        this.executor = executor;
        this.isExistsBody = isExistsBody;
        Select = select;
        Bound = bound;
        OutputType = outputType;
        this.parameterNames = parameterNames;
        Outer = outer;
        ResultsCanBeKept = resultsCanBeKept;
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

        // One value for each distinct outer name, in the order of the reader; the placeholder of value i is
        // named ParameterPrefix + i.
        CorrelatedOuterValues outer = CorrelatedOuterValues.Create(outerReferences, outerSources, outerDerivedSources, outerRowNames);

        Dictionary<string, NodeAst> placeholders = new(StringComparer.OrdinalIgnoreCase);
        Dictionary<NodeAst, NodeAst> replacements = new(ReferenceEqualityComparer.Instance);
        string[] parameterNames = new string[outer.Names.Count];

        for (int i = 0; i < outer.Names.Count; i++)
        {
            string name = outer.Names[i].yytext!;

            parameterNames[i] = ParameterPrefix + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            placeholders[name] = SubqueryResultTypes.Remember(
                new NodeAst(NodeType.Placeholder, null, null, null, null, null, null, null, parameterNames[i]),
                outerTypes(name));
        }

        foreach (NodeAst reference in outerReferences)
            replacements[reference] = placeholders[reference.yytext!];

        NodeAst substituted = NodeAstWalk.Replace(select, replacements);

        bool resultsCanBeKept = CorrelatedResultCache<bool>.CanKeepResultsOf(
            substituted, new HashSet<NodeAst>(placeholders.Values, ReferenceEqualityComparer.Instance));
        SelectBindResult checkedBind;
        SelectBindResult? bound = null;

        // A value body returns one column, as an uncorrelated subquery must. A * (also the * over a set
        // operation) shows its column count only once it is bound.
        if (!isExistsBody)
            SubqueryQueryExecutor.ValidateSingleColumnProjection(bodyQuery);

        if (NestedSubqueryReadsOuterValue(substituted))
        {
            using (StoredDefinitionBind.Enter())
                checkedBind = await executor.BindForRunsAsync(database, substituted, ticket, isExistsBody).ConfigureAwait(false);
        }
        else
        {
            checkedBind = await executor.BindForRunsAsync(database, substituted, ticket, isExistsBody).ConfigureAwait(false);
            bound = checkedBind;
        }

        Models.ExpressionType? outputType = null;

        if (!isExistsBody)
        {
            SubqueryQueryExecutor.ValidateSingleColumnOutput(checkedBind.Bound);

            outputType = SubqueryResultTypes.OutputTypeOf(checkedBind.Bound);
        }

        return new CorrelatedSubqueryBody(executor, isExistsBody, substituted, bound, outputType, parameterNames, outer, resultsCanBeKept);
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
        CancellationToken cancellationToken) =>
        RunAsync(database, Outer.For(outerRow, statementParameters), txnState, statementParameters, cancellationToken);

    /// <summary>
    /// The rows of the body for the outer values <paramref name="outerValues"/>, which <see cref="Outer"/> read
    /// from one outer row. A caller that keyed a <see cref="CorrelatedResultCache{T}"/> with them passes them
    /// here, so the row is read once.
    /// </summary>
    public IAsyncEnumerable<QueryResultRow> RunAsync(
        DatabaseDescriptor database,
        ColumnValue[] outerValues,
        KvTransaction txnState,
        Dictionary<string, ColumnValue>? statementParameters,
        CancellationToken cancellationToken)
    {
        CorrelatedRunProbe.CountRun();

        Dictionary<string, ColumnValue> parameters = ParametersFor(outerValues, statementParameters);

        if (Bound is { } bound)
            return executor.RunBoundAsync(database, bound, txnState, parameters, cancellationToken);

        return isExistsBody
            ? executor.ExecuteExistsSelectAsync(database, Select, txnState, parameters, cancellationToken)
            : executor.ExecuteSelectAsync(database, Select, txnState, parameters, cancellationToken);
    }

    /// <summary>
    /// The statement parameters and <paramref name="outerValues"/>, under their hidden placeholder names. A new
    /// dictionary for each row: the statement parameters are not changed.
    /// </summary>
    private Dictionary<string, ColumnValue> ParametersFor(
        ColumnValue[] outerValues,
        Dictionary<string, ColumnValue>? statementParameters)
    {
        Dictionary<string, ColumnValue> parameters = statementParameters is null
            ? new(parameterNames.Length)
            : new(statementParameters, statementParameters.Comparer);

        for (int i = 0; i < parameterNames.Length; i++)
            parameters[parameterNames[i]] = outerValues[i];

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
