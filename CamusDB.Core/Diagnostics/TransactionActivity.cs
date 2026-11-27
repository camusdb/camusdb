/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Auth;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Diagnostics;

/// <summary>
/// Where an open explicit transaction came from and what it ran last, for <c>SHOW TRANSACTIONS</c>.
/// The host creates one when a client begins an explicit transaction and puts it on
/// <see cref="Transactions.KvTransaction.Activity"/>. An autocommit transaction has none, so it pays
/// nothing for this.
///
/// <para><b>Cost.</b> The origin is copied once, at the begin. Each statement then writes one
/// reference: the SQL text is held as it arrived, and redaction and truncation run only when someone
/// reads the list, as for <see cref="QueryActivityRegistry"/>.</para>
///
/// <para><b>Owner.</b> The owner is the user who began the transaction. A transport that begins a
/// transaction without resolving its caller (a gRPC <c>StartTransaction</c>) leaves the owner unknown,
/// and the first statement with a principal sets it. It is set once and never changes, so a row cannot
/// move from one user's list to another's. A transaction with no owner is visible only to a viewer who
/// sees everything, as a connection with no user is.</para>
/// </summary>
public sealed class TransactionActivity
{
    private TransactionOwner? owner;

    private string? lastSql;

    /// <param name="database">The database the transaction was begun in.</param>
    /// <param name="origin">The request that began it, or null when no host request carried it.</param>
    /// <param name="principal">The caller that began it, or null when the transport did not resolve one.</param>
    public TransactionActivity(string database, StatementOrigin? origin, Principal? principal)
    {
        Database = database;
        Transport = origin?.Transport ?? StatementOrigin.EmbeddedTransport;
        ClientAddress = origin?.ClientAddress;
        ConnectionId = origin?.Connection?.Id;

        if (principal is not null)
            owner = new TransactionOwner(principal.UserName, principal.UserId);
    }

    /// <summary>The database the transaction was begun in.</summary>
    public string Database { get; }

    /// <summary>The transport that began the transaction: <c>http</c>, <c>grpc</c>, <c>grpc-stream</c> or <c>embedded</c>.</summary>
    public string Transport { get; }

    /// <summary>The client's address at the begin, or null when the transport has none.</summary>
    public string? ClientAddress { get; }

    /// <summary>The <c>SHOW CONNECTIONS</c> id of the connection that began the transaction, or null.</summary>
    public string? ConnectionId { get; }

    /// <summary>The user who owns the transaction, or null while it is not known.</summary>
    public string? OwnerName => Volatile.Read(ref owner)?.Name;

    /// <summary>The id of the user who owns the transaction, or null.</summary>
    public string? OwnerId => Volatile.Read(ref owner)?.Id;

    /// <summary>True when <paramref name="viewer"/> may see and kill this transaction.</summary>
    public bool VisibleTo(ActivityViewer viewer)
    {
        // One read of the owner: the same pair decides visibility. Two reads could see two owners.
        TransactionOwner? current = Volatile.Read(ref owner);
        return viewer.CanSee(current?.Name, current?.Id);
    }

    /// <summary>
    /// The text of the last statement. Unredacted when it was within the length limit at the time; never
    /// return it to a reader as is.
    /// </summary>
    internal string? LastSql => Volatile.Read(ref lastSql);

    /// <summary>
    /// Records a statement that starts on the transaction. Called for every SQL statement of the
    /// transaction, on the statement path.
    ///
    /// <para>A statement within <paramref name="maxSqlLength"/> is kept by reference, which costs two
    /// reference writes. A longer one is masked and then cut to that length here, so an idle
    /// transaction never keeps a large request alive for <c>SHOW TRANSACTIONS</c> (a statement may be
    /// up to <c>max_statement_bytes</c>). The mask runs before the cut: a cut first could split a
    /// password literal, and the mask would no longer find it. Only long statements pay for this, and
    /// they already pay for their parse.</para>
    /// </summary>
    public void NoteStatement(string sql, Principal? principal, int maxSqlLength)
    {
        if (sql.Length > maxSqlLength)
        {
            sql = SqlCredentialRedactor.Redact(sql);
            if (sql.Length > maxSqlLength)
                sql = sql[..maxSqlLength];
        }

        Volatile.Write(ref lastSql, sql);

        if (principal is not null && Volatile.Read(ref owner) is null)
            Interlocked.CompareExchange(ref owner, new TransactionOwner(principal.UserName, principal.UserId), null);
    }

    private sealed record TransactionOwner(string Name, string? Id);
}

/// <summary>
/// One open explicit transaction as the host sees it, for <see cref="QueryActivityService"/> to turn
/// into a <see cref="TransactionActivityRow"/>.
/// </summary>
/// <param name="Transaction">The transaction. Its <see cref="Transactions.KvTransaction.Activity"/> is set.</param>
/// <param name="State">One of the <see cref="TransactionActivityStates"/> values.</param>
/// <param name="AgeMs">How long the host has tracked the transaction.</param>
/// <param name="IdleMs">How long the transaction has waited for its client; zero unless the state is idle.</param>
public readonly record struct TrackedTransaction(
    Transactions.KvTransaction Transaction,
    string State,
    double AgeMs,
    double IdleMs);

/// <summary>The values of the <c>state</c> column of <c>SHOW TRANSACTIONS</c>.</summary>
public static class TransactionActivityStates
{
    /// <summary>No statement runs, and the transaction waits for its client.</summary>
    public const string Idle = "idle";

    /// <summary>At least one statement runs on the transaction.</summary>
    public const string Running = "running";

    /// <summary>A commit, a rollback, a reap or a kill's rollback runs on the transaction.</summary>
    public const string Finalizing = "finalizing";

    /// <summary>
    /// <c>KILL TRANSACTION</c> fenced the transaction, and its rollback waits for a running statement
    /// to end.
    /// </summary>
    public const string Killing = "killing";
}

/// <summary>
/// The host's registry of open explicit transactions, as the engine reads it for
/// <c>SHOW TRANSACTIONS</c> and <c>KILL TRANSACTION</c>.
///
/// <para>The host tracks the transactions that its clients hold open between requests; the engine
/// never sees a transaction between two statements. So the host implements this interface and
/// attaches itself with <see cref="QueryActivityService.AttachTransactions"/>. An engine with no host
/// has no open explicit transactions, and the statements then list nothing and find nothing.</para>
/// </summary>
public interface ITransactionActivitySource
{
    /// <summary>
    /// Adds every open explicit transaction of this node to <paramref name="into"/>, unfiltered. The
    /// caller applies the visibility rule.
    /// </summary>
    void Snapshot(List<TrackedTransaction> into);

    /// <summary>
    /// Ends the open explicit transaction whose client handle is (<paramref name="physical"/>,
    /// <paramref name="counter"/>), when <paramref name="viewer"/> may see it.
    ///
    /// <para>The kill fences the transaction at once: no further statement and no commit. It rolls the
    /// transaction back now when no statement runs on it, and otherwise when the last running statement
    /// ends; it waits up to <paramref name="wait"/> for that rollback. A commit or rollback that already
    /// claimed the transaction wins, and the outcome says which one it was.</para>
    /// </summary>
    /// <param name="killedBy">Who asked, for the audit log line and for the error the client receives.</param>
    /// <exception cref="CamusDBException">The rollback itself failed; the transaction is fenced and leaves the list.</exception>
    Task<TransactionKillOutcome> KillAsync(long physical, uint counter, ActivityViewer viewer, TransactionKiller killedBy, TimeSpan wait);
}

/// <summary>
/// Who ran <c>KILL TRANSACTION</c>. The client of the killed transaction learns only the user name;
/// the audit log line on the owner node also records the address and the node the kill came from.
/// </summary>
/// <param name="UserName">The caller's user name, or null when authentication is off.</param>
/// <param name="ClientAddress">The caller's address, or null when the transport has none.</param>
/// <param name="Node">The node that received the statement.</param>
public sealed record TransactionKiller(string? UserName, string? ClientAddress, string Node)
{
    /// <summary>The reason the killed client reads in its error, with the time of the kill.</summary>
    public string ReasonForClient(DateTime at) => UserName is null
        ? $"killed by KILL TRANSACTION at {at:yyyy-MM-ddTHH:mm:ss.fffZ}"
        : $"killed by KILL TRANSACTION, run by user '{UserName}' at {at:yyyy-MM-ddTHH:mm:ss.fffZ}";

    /// <summary>The caller as the audit log line names it.</summary>
    public override string ToString() =>
        $"user '{UserName ?? "(authentication off)"}' from {ClientAddress ?? "(no address)"} on node '{Node}'";
}

/// <summary>What <c>KILL TRANSACTION</c> did on the node that owns the transaction.</summary>
public enum TransactionKillOutcome
{
    /// <summary>The transaction is rolled back and its locks are released.</summary>
    Killed,

    /// <summary>
    /// The transaction is fenced, and its rollback waits for a running statement to end. It runs no
    /// further statement and cannot commit.
    /// </summary>
    Pending,

    /// <summary>No open explicit transaction has this id, or the requester may not see it.</summary>
    NotFound,

    /// <summary>A commit claimed the transaction before the kill, and the transaction committed.</summary>
    Committed,

    /// <summary>A rollback of its client or of the idle reaper ended the transaction before the kill.</summary>
    RolledBack,

    /// <summary>
    /// A commit or rollback owns the transaction and had not ended when the kill stopped waiting, or a
    /// commit of it got no answer, so its outcome belongs to the storage layer and a kill cannot roll it
    /// back.
    /// </summary>
    Finalizing,
}

/// <summary>
/// One row of <c>SHOW TRANSACTIONS</c>. Also the wire shape a peer returns for
/// <c>SHOW CLUSTER TRANSACTIONS</c>, so it holds only plain values.
/// </summary>
/// <param name="TransactionId">
/// The id <c>KILL TRANSACTION</c> takes: <c>{nodeTag}{instanceTag}-t{L}.{C}</c>, where <c>{L}.{C}</c>
/// is the client handle that <c>SHOW QUERIES</c> reports as <c>transaction_id</c>.
/// </param>
/// <param name="QueryId">The <c>SHOW QUERIES</c> id of the oldest statement running on the transaction, or null.</param>
/// <param name="LastSql">The last statement, redacted and cut to <see cref="CamusDBOptions.QueryActivityMaxSqlLength"/>; null before the first one.</param>
/// <param name="ModifiedKeys">The keys the transaction wrote so far, as its client-side mirror counts them.</param>
/// <param name="HoldsDdlLock">True when a DDL statement of the transaction holds its database's DDL lock (transactional DDL).</param>
/// <param name="Error">Null for a transaction row; set only on the row that stands in for an unreachable peer.</param>
public sealed record TransactionActivityRow(
    string TransactionId,
    string Node,
    string Database,
    string? UserName,
    string? ClientAddress,
    string Transport,
    string Isolation,
    string Locking,
    string Mode,
    DateTime StartedAt,
    double AgeMs,
    double IdleMs,
    string State,
    string? QueryId,
    string? LastSql,
    int ModifiedKeys,
    bool HoldsDdlLock,
    string? Error);
