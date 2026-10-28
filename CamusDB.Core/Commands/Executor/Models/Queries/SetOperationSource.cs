/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// <c>UNION</c>, <c>INTERSECT</c> or <c>EXCEPT</c> over two queries.
///
/// <para><b>Never a top-level source.</b> Like <see cref="TableFunctionSource"/>, this record only
/// ever sits as the <see cref="SelectQuery.Source"/> of the inner shell of a
/// <see cref="DerivedTableSource"/>, so <c>q1 UNION q2 ORDER BY 1</c> is planned as
/// <c>SELECT * FROM (q1 UNION q2) ORDER BY 1</c>. Every relation-kind consumer then treats the set
/// operation as one more derived table, and only the binder, the leaf scanner and the shape
/// computer need to know what is inside. Read it through <see cref="DerivedTableSource.SetOperation"/>.</para>
///
/// <para><b>The operands are complete queries.</b> Each one has its own sources, filter, grouping
/// and, when it was written in parentheses, its own ORDER BY and LIMIT. The binder binds each one
/// separately, so each one needs SELECT on its own tables; the output column names come from
/// <see cref="Left"/>. An operand never carries <c>AS OF SYSTEM TIME</c>: every operand reads in the
/// one snapshot of the statement.</para>
/// </summary>
/// <param name="Kind">The operator.</param>
/// <param name="All">True for the ALL form, which keeps duplicate rows.</param>
/// <param name="Left">The left operand. Its output names are the output names of the set operation.</param>
/// <param name="Right">The right operand.</param>
public sealed record SetOperationSource(
    SetOperationKind Kind,
    bool All,
    SelectQuery Left,
    SelectQuery Right) : QuerySource
{
    /// <summary>
    /// The alias of the derived table that holds a set operation. No query can name it, because the
    /// set operation has no alias in the statement; it exists because every derived table needs one.
    /// The asterisks keep it apart from any identifier a user can write without quotes.
    /// </summary>
    public const string DerivedAlias = "*set_operation*";

    /// <summary>The operator as SQL writes it, with ALL for the ALL form: <c>UNION ALL</c>, <c>EXCEPT</c>.</summary>
    public string OperatorText => Kind switch
    {
        SetOperationKind.Union => All ? "UNION ALL" : "UNION",
        SetOperationKind.Intersect => All ? "INTERSECT ALL" : "INTERSECT",
        _ => All ? "EXCEPT ALL" : "EXCEPT",
    };
}
