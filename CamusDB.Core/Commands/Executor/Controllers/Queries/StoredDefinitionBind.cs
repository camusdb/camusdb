/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Marks the asynchronous flow in which <c>CREATE VIEW</c> or <c>CREATE MATERIALIZED VIEW</c> binds a
/// body that is stored and evaluated later, by the statements that read it.
///
/// <para>Such a bind must not depend on the rows. PostgreSQL accepts
/// <c>CREATE VIEW c AS SELECT (SELECT v / 0 FROM t) AS x</c> and fails the read, and the bind pipeline
/// would otherwise run each uncorrelated subquery of the body to fold its value in as a literal. In this
/// flow two things change:</para>
/// <list type="bullet">
/// <item><see cref="SubqueryQueryExecutor"/> binds a subquery, records its output type, and returns no
/// rows. A scalar subquery so becomes a NULL of its declared type, an <c>IN</c> list is empty, and an
/// <c>EXISTS</c> is false. A wrong name or type in a subquery still fails the statement, and the view
/// column type comes from the declaration, not from a value.</item>
/// <item><see cref="ConstantDivisionCheck"/> does not refuse a constant division. The statement that
/// reads the definition binds it again, outside this flow, and fails there.</item>
/// </list>
///
/// <para>The flow includes each subquery the body binds and each view the body expands. A
/// materialized view fills itself outside this flow, so its rows come from a real run.</para>
/// </summary>
internal static class StoredDefinitionBind
{
    private static readonly AsyncLocal<bool> Active = new();

    /// <summary>True inside <see cref="Enter"/>, in this asynchronous flow.</summary>
    internal static bool IsActive => Active.Value;

    /// <summary>Starts the flow until the scope ends.</summary>
    internal static Scope Enter() => new(Active.Value);

    /// <summary>Restores the earlier state of <see cref="Enter"/>.</summary>
    internal readonly struct Scope : IDisposable
    {
        private readonly bool previous;

        internal Scope(bool previous)
        {
            this.previous = previous;
            Active.Value = true;
        }

        public void Dispose() => Active.Value = previous;
    }
}
