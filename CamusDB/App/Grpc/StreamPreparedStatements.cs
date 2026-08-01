
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Core;
using CamusDB.App.Services;

namespace CamusDB.App.Grpc;

/// <summary>
/// The prepared statements registered by one <c>BatchExecute</c> duplex stream.
///
/// <para>The stream is the whole lifetime story: a handle is minted here, is meaningful only to ops
/// arriving on this call, and dies with it — normal completion, client cancel, or fault all free
/// everything with no teardown code and no cross-stream garbage to collect. That is why the gRPC
/// surface needs none of the machinery the REST registry does (unguessable ids, ownership checks,
/// idle expiry): a stream already answers who owns a handle and when it ends.</para>
///
/// <para>Concurrent by necessity, not by caution: a PREPARE op carries no transaction handle, so it
/// runs on the unchained concurrent path while executions referencing other ids may be running. The
/// client contract — await the PrepareReply before sending anything that references the id — is what
/// rules out an execution racing its own registration; this type still fails closed on an id it does
/// not hold rather than assuming that contract was honored.</para>
/// </summary>
internal sealed class StreamPreparedStatements
{
    private readonly ConcurrentDictionary<int, PreparedStatement> statements = new();

    /// <summary>
    /// Guards admission: the cap checks, the id allocation, and the counters, taken together.
    ///
    /// <para>Checking a cap and then inserting are two steps, and between them any number of
    /// concurrent PREPAREs can observe the same last free slot and all publish. The stream's
    /// in-flight limit bounds that overshoot incidentally, not deliberately, so admission takes a
    /// lock and either fits entirely or refuses. It runs once per statement, never per execution.</para>
    /// </summary>
    private readonly Lock admission = new();

    // Ids start at 1: 0 is reserved on the wire to mean "inline request, no prepared statement".
    private int lastId;

    private readonly CamusDBOptions options;

    public StreamPreparedStatements(CamusDBOptions options) => this.options = options;

    /// <summary>
    /// Test-only seam: starts the id counter at <paramref name="firstIdMinusOne"/> so the exhaustion
    /// boundary can be reached without issuing two billion registrations.
    /// </summary>
    internal StreamPreparedStatements(CamusDBOptions options, int firstIdMinusOne)
    {
        this.options = options;
        lastId = firstIdMinusOne;
    }

    private long retainedBytes;

    /// <summary>Live statement count, for tests and diagnostics.</summary>
    public int Count => statements.Count;

    /// <summary>Retained statement text in bytes, for tests and diagnostics.</summary>
    public long RetainedBytes
    {
        get { lock (admission) return retainedBytes; }
    }

    /// <summary>
    /// Registers <paramref name="statement"/> and returns its stream-local id. Refuses rather than
    /// evicting when the stream is at its cap — dropping a handle a client still believes in would
    /// turn a correct client into a failing one at an unpredictable later moment.
    /// </summary>
    /// <exception cref="CamusDBException">
    /// The stream is at its statement or retained-byte cap, or its id space is exhausted.
    /// </exception>
    public int Add(PreparedStatement statement)
    {
        int countLimit = options.GrpcMaxPreparedStatementsPerStream;
        long byteLimit = options.GrpcMaxPreparedStatementBytesPerStream;
        long bytes = RetainedBytesOf(statement);

        lock (admission)
        {
            if (countLimit > 0 && statements.Count >= countLimit)
                throw PreparedStatementBinder.LimitExceeded(countLimit, "per-stream");

            if (byteLimit > 0 && retainedBytes + bytes > byteLimit)
                throw PreparedStatementBinder.LimitExceeded(byteLimit, "per-stream retained-byte");

            // Ids are handed out once each and never reused, so the space is finite. Refuse at the
            // ceiling instead of wrapping into negatives: a wrapped id fails every resolve (they are
            // rejected as non-positive), and continuing past that would eventually collide with a
            // live id and hand a client someone else's statement.
            if (lastId == int.MaxValue)
                throw new CamusDBException(
                    CamusDBErrorCodes.PreparedStatementLimitExceeded,
                    "This stream has exhausted its prepared-statement ids; open a new stream");

            int id = ++lastId;
            statements[id] = statement;
            retainedBytes += bytes;
            return id;
        }
    }

    /// <summary>
    /// Bytes a registration keeps alive: database, SQL, and parameter names as UTF-16. Approximate on
    /// purpose — it exists to bound growth, not to account for allocations exactly.
    /// </summary>
    private static long RetainedBytesOf(PreparedStatement statement)
    {
        long chars = statement.Database.Length + statement.Sql.Length;
        foreach (string name in statement.ParameterNames)
            chars += name.Length;
        return chars * sizeof(char);
    }

    /// <summary>
    /// Resolves an id registered on this stream, or throws the shared unknown-statement error. An id
    /// minted by a different stream fails here, which is exactly what a client sees after its
    /// transport was rebuilt — its cue to prepare again.
    /// </summary>
    public PreparedStatement Resolve(int statementId)
    {
        if (statementId <= 0 || !statements.TryGetValue(statementId, out PreparedStatement? statement))
            throw PreparedStatementBinder.UnknownStatement("not prepared on this stream");

        return statement;
    }

    /// <summary>
    /// Releases an id. Closing one that is unknown or already closed succeeds: the caller asked for
    /// it to be gone and it is gone, and a client tearing down after a fault must not have to
    /// distinguish the two. Mirrors ROLLBACK's idempotence on this stream.
    /// </summary>
    public void Remove(int statementId)
    {
        lock (admission)
        {
            if (statements.TryRemove(statementId, out PreparedStatement? removed))
                retainedBytes -= RetainedBytesOf(removed);
        }
    }
}
