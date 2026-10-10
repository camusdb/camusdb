
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Grpc.Client.Batching;

/// <summary>
/// The kind of a batched op, so the demultiplexer knows how many response messages to expect for a
/// <c>request_id</c> (a QUERY streams schema + rows + a terminator; everything else is one terminal).
/// </summary>
internal enum BatchOpKind
{
    Query,
    NonQuery,
    Start,
    Commit,
    Rollback,
    Prepare,
    Close,
}

/// <summary>
/// Materialized result of a batched QUERY: the ordered output-column schema followed by the positional
/// rows, plus the trailing causal token. Rows align to <see cref="Schema"/> by position.
/// </summary>
public sealed class QueryResult
{
    public ResultSchema Schema { get; }
    public IReadOnlyList<ResultRow> Rows { get; }
    public CausalToken Token { get; }

    /// <summary>
    /// Routing advice the reply carried, or null. Present only when the connection negotiated
    /// routing metadata; the connection has already learned from it, so this is diagnostic.
    /// </summary>
    public Routing.CamusRoutingAdvice? Routing { get; }

    public QueryResult(
        ResultSchema schema, IReadOnlyList<ResultRow> rows, CausalToken token,
        Routing.CamusRoutingAdvice? routing = null)
    {
        Schema  = schema;
        Rows    = rows;
        Token   = token;
        Routing = routing;
    }
}

/// <summary>
/// Result of a batched NON_QUERY: affected-row count plus the trailing causal token, and the rows of
/// an INSERT, UPDATE or DELETE with a RETURNING list.
///
/// <para>A statement that returns rows never makes a no-rows call fail: the count is here as always,
/// and the rows are available beside it. A client that wants only the count asks the server not to
/// send the rows (the <c>discardReturningRows</c> overloads), which leaves both RETURNING members
/// null.</para>
/// </summary>
public sealed class NonQueryResult
{
    public int AffectedRows { get; }
    public CausalToken Token { get; }

    /// <inheritdoc cref="QueryResult.Routing"/>
    public Routing.CamusRoutingAdvice? Routing { get; }

    /// <summary>
    /// The output columns of a RETURNING list. Null for a statement without RETURNING and
    /// for a call that asked for the count only. Not null with an empty <see cref="ReturningRows"/>
    /// when the statement wrote no rows.
    /// </summary>
    public ResultSchema? ReturningSchema { get; }

    /// <summary>
    /// The RETURNING rows, one per inserted row, positional against <see cref="ReturningSchema"/> —
    /// the same shape as <see cref="QueryResult.Rows"/>. Null exactly when
    /// <see cref="ReturningSchema"/> is null.
    /// </summary>
    public IReadOnlyList<ResultRow>? ReturningRows { get; }

    public NonQueryResult(
        int affectedRows,
        CausalToken token,
        Routing.CamusRoutingAdvice? routing = null,
        ResultSchema? returningSchema = null,
        IReadOnlyList<ResultRow>? returningRows = null)
    {
        AffectedRows    = affectedRows;
        Token           = token;
        Routing         = routing;
        ReturningSchema = returningSchema;
        ReturningRows   = returningSchema is null ? null : returningRows ?? [];
    }
}

/// <summary>
/// A Hybrid Logical Clock timestamp threaded through a session for read-your-writes. All three
/// components travel together — dropping <see cref="N"/> makes the token a lossy copy (see the client
/// protocol doc §4.2). <see cref="IsEmpty"/> is the zero token used before the first reply.
/// </summary>
public readonly struct CausalToken
{
    public int N { get; }
    public long L { get; }
    public long C { get; }

    public CausalToken(int n, long l, long c)
    {
        N = n;
        L = l;
        C = c;
    }

    public bool IsEmpty => N == 0 && L == 0 && C == 0;

    public static CausalToken Empty => default;
}

/// <summary>
/// Raised when a batched op fails. <see cref="Code"/> is the <c>CADBxxxx</c> domain code carried in-band
/// as a <c>BatchError</c>, so a caller can apply the retry taxonomy (see the client protocol doc §8)
/// without parsing the message.
/// </summary>
public sealed class CamusGrpcException : Exception
{
    public string Code { get; }

    public CamusGrpcException(string code, string message) : base(message)
    {
        Code = code;
    }
}
