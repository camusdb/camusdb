
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Adapters that present already-materialized rows as the <see cref="IAsyncEnumerable{T}"/> cursor
/// every read path returns.
///
/// <para>Statements that produce their whole result in one step — <c>ANALYZE</c>, the introspection
/// statements, a FROM-less <c>SELECT</c> — still have to hand back the same cursor type a streaming
/// scan does, so a caller never has to ask which kind of statement it ran.</para>
/// </summary>
internal static class QueryResultStream
{
    /// <summary>Presents a materialized list of rows as an async cursor.</summary>
    internal static async IAsyncEnumerable<QueryResultRow> FromRows(IReadOnlyList<QueryResultRow> rows)
    {
        foreach (QueryResultRow row in rows)
            yield return row;

        await Task.CompletedTask;
    }

    /// <summary>Presents a single row as an async cursor.</summary>
    internal static async IAsyncEnumerable<QueryResultRow> FromRow(QueryResultRow row)
    {
        yield return row;

        await Task.CompletedTask;
    }

    /// <summary>An async cursor over no rows.</summary>
    internal static async IAsyncEnumerable<QueryResultRow> Empty()
    {
        await Task.CompletedTask;
        yield break;
    }
}
