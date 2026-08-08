
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// A bound, not-yet-drained row source for a statement that consumes a query — <c>INSERT … SELECT</c>
/// and <c>CREATE TABLE … AS SELECT</c>.
///
/// <para>It is disposable because a time-travel source owns a resource whose lifetime must span the
/// whole drain: a Kahuna snapshot-floor hold, which stops revision reclamation from advancing past
/// the requested timestamp while the copy is still reading at it. Releasing it early would let a long
/// copy read a partially reclaimed history and produce a silently incomplete table, so every caller
/// must dispose this only after the cursor is fully drained (or the statement has failed).</para>
/// </summary>
internal sealed class SelectRowSource : IAsyncDisposable
{
    private readonly Func<Task>? release;
    private int disposed;

    /// <summary>The source query's output columns, in projection order.</summary>
    public IReadOnlyList<DerivedColumnSchema> Columns { get; }

    /// <summary>The unexecuted row cursor. Nothing has been read until it is enumerated.</summary>
    public IAsyncEnumerable<QueryResultRow> Cursor { get; }

    /// <summary>
    /// The projection expressions that produced <see cref="Columns"/>, after subquery rewriting — the
    /// exact list the output schema was derived from, so a caller that inspects them cannot disagree
    /// with it.
    /// </summary>
    public IReadOnlyList<NodeAst> Projections { get; }

    /// <summary>
    /// True when the source reads at a historical snapshot (<c>AS OF SYSTEM TIME</c>) rather than at
    /// the statement's own transaction. Callers use it to report a copy that read nothing, which for
    /// a time-travel source may mean the requested history has already been reclaimed.
    /// </summary>
    public bool IsTimeTravel => release is not null;

    public SelectRowSource(
        IReadOnlyList<DerivedColumnSchema> columns,
        IAsyncEnumerable<QueryResultRow> cursor,
        IReadOnlyList<NodeAst> projections,
        Func<Task>? release = null)
    {
        Columns = columns;
        Cursor = cursor;
        Projections = projections;
        this.release = release;
    }

    /// <summary>
    /// Releases the snapshot hold, if any. Idempotent: a statement may dispose on both its success and
    /// its failure path, and releasing an already-released hold must not turn a real error into a
    /// confusing second one.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (release is null || Interlocked.Exchange(ref disposed, 1) == 1)
            return;

        await release().ConfigureAwait(false);
    }
}
