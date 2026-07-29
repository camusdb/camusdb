
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Grpc.Client.Batching;

/// <summary>
/// Identifies a statement in a slot's registration cache.
///
/// <para>The two components are kept <b>separate</b> rather than folded into one string. Joining them
/// with any delimiter means picking a character neither a database name nor SQL text can contain, and
/// no such character exists: both arrive from the caller unvalidated, and SQL can carry almost
/// anything inside a line comment. A delimiter that turns out to be forgeable lets two different
/// statements share one registration, so a statement executes SQL its caller never wrote — a
/// correctness bug and a confused-deputy hazard for any caller mixing trust domains.</para>
///
/// <para>Comparing the parts separately also costs nothing: equality short-circuits on the database
/// name, and building an instance never copies the SQL — which matters because a cache lookup happens
/// on every execution.</para>
/// </summary>
internal readonly struct PreparedStatementKey : IEquatable<PreparedStatementKey>
{
    private readonly int hash;

    public PreparedStatementKey(string database, string sql)
    {
        Database = database;
        Sql = sql;

        // Hashed once, here. A statement builds its key at construction and reuses it for every
        // execution, so hashing the SQL text on each lookup would put a scan of the whole statement
        // back on the hot path — a smaller version of the cost this feature exists to remove.
        hash = HashCode.Combine(
            string.GetHashCode(database, StringComparison.Ordinal),
            string.GetHashCode(sql, StringComparison.Ordinal));
    }

    public string Database { get; }

    public string Sql { get; }

    /// <summary>
    /// Ordinal equality, with the hash compared first to reject non-matches without touching the
    /// strings. <see cref="string.Equals(string, string, StringComparison)"/> itself short-circuits on
    /// reference equality, which is the common case here: a statement looks itself up with the very
    /// key instance it stored, so a hit usually costs two pointer comparisons rather than two scans.
    /// </summary>
    public bool Equals(PreparedStatementKey other) =>
        hash == other.hash &&
        string.Equals(Database, other.Database, StringComparison.Ordinal) &&
        string.Equals(Sql, other.Sql, StringComparison.Ordinal);

    public override bool Equals(object? obj) => obj is PreparedStatementKey other && Equals(other);

    public override int GetHashCode() => hash;

    public override string ToString() => $"{Database}: {Sql}";
}

/// <summary>
/// A statement's registration on one stream slot: the id the server minted, the parameter names it
/// published, and — critically — the id of the transport the PREPARE was actually written to.
///
/// <para>A server-side handle is only meaningful on the stream that minted it, so the transport id is
/// what makes an entry checkable. When a slot's stream faults and is rebuilt, its new transport gets
/// a new id, every cached entry for that slot stops matching, and the statement is transparently
/// prepared again. No invalidation callback is needed: staleness is a comparison, not an event.</para>
/// </summary>
internal readonly record struct PreparedSlotEntry(long TransportId, int StatementId, string[] ParameterNames);

/// <summary>
/// Raised when an execution is about to be written to a transport that is not the one its prepared
/// statement was registered on.
///
/// <para>The check happens immediately before the write, which is the only place it can be conclusive:
/// anywhere earlier and the transport could still be rebuilt in between. Without it the op would
/// travel to a stream that never saw the PREPARE and come back as an unknown-statement error — the
/// same outcome, but only after a needless round trip. Callers treat this as "re-prepare and retry
/// once", never as a user-facing failure.</para>
/// </summary>
internal sealed class PreparedStatementStaleException : Exception
{
    public PreparedStatementStaleException()
        : base("The prepared statement's stream was rebuilt before this execution could be written")
    {
    }
}
