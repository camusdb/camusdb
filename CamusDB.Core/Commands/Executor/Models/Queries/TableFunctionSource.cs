
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A set-returning function used as a relation in <c>FROM</c>, such as
/// <c>generate_series(1, 10) AS i</c>.
///
/// <para><b>Never a top-level source.</b> This record only ever sits as the <see cref="SelectQuery.Source"/>
/// of the inner query of a <see cref="DerivedTableSource"/>: <c>FROM generate_series(1, 3) AS i</c> is
/// planned as the derived table <c>(SELECT i FROM generate_series(1, 3)) AS i</c>. That lets every
/// relation-kind consumer — join ordering, alias and column resolution, predicate pushdown, row-name
/// qualification — treat the function as one more derived table, and only the binder and the leaf
/// scanner need to know what is inside. Read it through <see cref="DerivedTableSource.Function"/>.</para>
///
/// <para><b>No storage.</b> The rows are computed, not read: the source opens no table, takes no lock,
/// and adds nothing to a transaction's read set, so it needs no privilege of its own.</para>
/// </summary>
/// <param name="FunctionName">The canonical (lower-case) name the function registry resolved.</param>
/// <param name="Arguments">
/// The argument expressions, in call order. The query creator admits only expressions that read no
/// column, so each one is evaluated once, with the statement's parameters, before the first row.
/// </param>
/// <param name="ColumnName">The name of the one output column, after the alias rule is applied.</param>
public sealed record TableFunctionSource(
    string FunctionName,
    IReadOnlyList<NodeAst> Arguments,
    string ColumnName) : QuerySource;
