/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The part of an UPDATE or DELETE predicate that the write phase evaluates again, against the row it
/// read after it locked that row.
///
/// <para><b>Why.</b> The locate scan that picks the rows to change reads them without a lock at Read
/// Committed. Another transaction can change a located row and commit before this statement locks it.
/// The write phase therefore locks first, reads the row again, and re-evaluates the predicate against
/// that read: a row that no longer matches is left alone, as PostgreSQL does when it re-checks an
/// updated row. Without the re-check, <c>UPDATE t SET v = 1 WHERE state = 'open'</c> would still change
/// a row that a concurrent commit had just closed.</para>
///
/// <para><b>What is re-checked.</b> Every conjunct of the WHERE clause that contains no subquery, and
/// every API filter. A conjunct that still holds a subquery (an <c>EXISTS</c>, which the DML subquery
/// rewrite leaves in place) keeps the answer the locate scan gave: it is a statement-level condition,
/// and dropping it from the re-check can only keep a row, never lose one. Evaluation goes through the
/// same filter the locate scan uses, so a string compared with a Uuid or Id column is coerced the same
/// way an index seek coerces it.</para>
///
/// <para><b>Decode set.</b> <see cref="Columns"/> names, in schema case, the columns the re-check reads.
/// It is null when a referenced name does not match a column of the current schema (a qualified name,
/// for example): the caller then decodes every column, so the re-check sees exactly what the locate
/// scan saw.</para>
/// </summary>
internal sealed class MutationRowRecheck
{
    private readonly NodeAst? predicate;

    /// <summary>The schema-cased columns the re-check reads, or null to decode every column.</summary>
    public IReadOnlySet<string>? Columns { get; }

    /// <summary>True when there is nothing to re-check: no WHERE clause and no API filter.</summary>
    public bool IsEmpty => predicate is null;

    private MutationRowRecheck(NodeAst? predicate, IReadOnlySet<string>? columns)
    {
        this.predicate = predicate;
        Columns = columns;
    }

    public static MutationRowRecheck Build(TableSchema schema, NodeAst? where, List<QueryFilter>? filters)
    {
        List<NodeAst> conjuncts = [];

        if (where is not null)
        {
            List<NodeAst> all = [];
            PredicateAnalyzer.CollectAndConjuncts(where, all);

            foreach (NodeAst conjunct in all)
            {
                if (!RequiredColumnAnalyzer.ContainsSubqueryNode(conjunct))
                    conjuncts.Add(conjunct);
            }
        }

        if (filters is not null)
        {
            foreach (QueryFilter filter in filters)
                conjuncts.Add(PredicateAnalyzer.BuildFilterConjunct(filter));
        }

        if (conjuncts.Count == 0)
            return new(null, new HashSet<string>(StringComparer.OrdinalIgnoreCase));

        HashSet<string> referenced = new(StringComparer.OrdinalIgnoreCase);
        foreach (NodeAst conjunct in conjuncts)
            QueryExpressionWalker.CollectColumnReferences(conjunct, referenced);

        return new(PredicateAnalyzer.CombineConjuncts(conjuncts), ToSchemaColumns(schema, referenced));
    }

    /// <summary>
    /// Maps referenced names to the schema's own spelling, or returns null when one of them is not a
    /// column of <paramref name="schema"/>. Null tells the caller to decode every column.
    /// </summary>
    internal static HashSet<string>? ToSchemaColumns(TableSchema schema, IReadOnlySet<string> referenced)
    {
        HashSet<string> columns = new(StringComparer.OrdinalIgnoreCase);

        foreach (string name in referenced)
        {
            TableColumnSchema? column = schema.Columns!.Find(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));
            if (column is null)
                return null;

            columns.Add(column.Name);
        }

        return columns;
    }

    /// <summary>
    /// True when <paramref name="row"/>, read under the write lock, still satisfies the predicate.
    /// <paramref name="locateTicket"/> is the ticket of the locate scan: it carries the statement's
    /// parameters.
    /// </summary>
    public ValueTask<bool> MatchesAsync(
        QueryExecutor queryExecutor,
        DatabaseDescriptor database,
        QueryTicket locateTicket,
        IReadOnlyDictionary<string, ColumnValue> row)
    {
        if (predicate is null)
            return new ValueTask<bool>(true);

        return queryExecutor.MeetMutationPredicateAsync(predicate, row, locateTicket, database);
    }
}
