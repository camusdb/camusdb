
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

    public QueryResult(ResultSchema schema, IReadOnlyList<ResultRow> rows, CausalToken token)
    {
        Schema = schema;
        Rows   = rows;
        Token  = token;
    }
}

/// <summary>Result of a batched NON_QUERY: affected-row count plus the trailing causal token.</summary>
public sealed class NonQueryResult
{
    public int AffectedRows { get; }
    public CausalToken Token { get; }

    public NonQueryResult(int affectedRows, CausalToken token)
    {
        AffectedRows = affectedRows;
        Token        = token;
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
