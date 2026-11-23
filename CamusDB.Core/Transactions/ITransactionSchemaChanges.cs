
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Transactions;

/// <summary>
/// The schema changes that the DDL statements of one explicit transaction made, when
/// <see cref="CamusDBOptions.TransactionalDdlEnabled"/> makes DDL part of the transaction. The
/// transactions manager calls these hooks at commit and rollback; the catalog implements them, so the
/// transactions layer needs no knowledge of schema.
///
/// <para>The commit order is: <see cref="TryValidateSchemaPins"/>, <see cref="PrepareCommitAsync"/>
/// (announce the change set in the schema log and stage the decision key in the transaction), the KV
/// commit, then <see cref="OnCommitOutcomeAsync"/> with the KV outcome (publish or abort the change
/// set). A rollback, and a commit refused before its request left the node, call
/// <see cref="DiscardAsync"/>. Each hook is called at most once per outcome. Every path that leaves the
/// commit or the rollback, with an outcome or with an exception, calls <see cref="OnCommitOutcomeAsync"/>,
/// <see cref="DiscardAsync"/> or <see cref="ReleaseAsync"/>, and each of them frees the DDL lock. A path
/// that called none of them would keep the lock renewed for as long as the process lives.</para>
/// </summary>
public interface ITransactionSchemaChanges
{
    /// <summary>True when at least one DDL statement of the transaction changed the schema.</summary>
    bool HasChanges { get; }

    /// <summary>
    /// Checks the transaction's schema pins against the transaction's own view of the schema, so the
    /// tables it created or changed count as present. Replaces <see cref="KvTransaction.TryValidateSchemaPins"/>
    /// for a transaction with schema changes.
    /// </summary>
    bool TryValidateSchemaPins(KvTransaction tx, out string? refusal);

    /// <summary>
    /// Runs before the KV commit request leaves the node, while the transaction can still write:
    /// announces the change set in the schema log and writes the decision key and the new row layouts
    /// into the transaction. A throw refuses the commit; the caller rolls the transaction back.
    /// </summary>
    Task PrepareCommitAsync(KvTransaction tx, CancellationToken cancellationToken);

    /// <summary>
    /// True once <see cref="PrepareCommitAsync"/> finished: the change set is announced and the decision key
    /// is staged. A commit attempt that resumes a transaction without it must not run, because it would
    /// commit the rows and leave the schema changes out.
    /// </summary>
    bool IsCommitPrepared { get; }

    /// <summary>
    /// Runs once the KV commit has an outcome. <see cref="SchemaChangesCommitOutcome.Committed"/>
    /// publishes the change set, <see cref="SchemaChangesCommitOutcome.Aborted"/> aborts it, and
    /// <see cref="SchemaChangesCommitOutcome.Unknown"/> leaves it to a resumed commit or to the resolver.
    /// Throws only after <see cref="SchemaChangesCommitOutcome.Committed"/>, when the publish is not
    /// confirmed on every live node: then <see cref="PublicationOutstanding"/> is true, the error is the
    /// retryable <see cref="CamusDBErrorCodes.TransactionFinalizeUnresolved"/>, and a COMMIT retried on
    /// the same transaction resumes the publish (<see cref="ResumePublicationAsync"/>) instead of the
    /// transaction. The rows are committed either way.
    /// </summary>
    Task OnCommitOutcomeAsync(KvTransaction tx, SchemaChangesCommitOutcome outcome);

    /// <summary>
    /// True when the KV commit landed and the publish of the change set is not confirmed yet. The
    /// transaction then stays resumable: a retried COMMIT calls <see cref="ResumePublicationAsync"/>.
    /// </summary>
    bool PublicationOutstanding { get; }

    /// <summary>
    /// Completes the publish of a committed change set, or confirms that another party (the resolver)
    /// completed it, and waits for every live node to apply it. Throws the retryable
    /// <see cref="CamusDBErrorCodes.TransactionFinalizeUnresolved"/> while it cannot confirm it.
    /// </summary>
    Task ResumePublicationAsync(KvTransaction tx, CancellationToken cancellationToken);

    /// <summary>
    /// Runs when the transaction rolls back, or when its commit is refused before the KV commit
    /// request. Drops the private schema and frees the DDL lock. Never throws.
    /// </summary>
    Task DiscardAsync(KvTransaction tx);

    /// <summary>
    /// Runs when a finalize was cut off and no KV commit request left the node (a rollback without an
    /// outcome, a commit that failed before its request), and when the transaction is abandoned while it
    /// finalizes. Frees the DDL lock and decides nothing: a retried finalize, the resolver or the
    /// coordinator's resume ends what the transaction left. Safe to call more than once. Never throws.
    /// </summary>
    Task ReleaseAsync(KvTransaction tx);
}

/// <summary>The outcome of a KV commit, as <see cref="ITransactionSchemaChanges.OnCommitOutcomeAsync"/> sees it.</summary>
public enum SchemaChangesCommitOutcome
{
    /// <summary>The KV commit landed.</summary>
    Committed,

    /// <summary>The KV commit definitely did not land.</summary>
    Aborted,

    /// <summary>The KV outcome is not known yet; a resumed commit or the resolver decides it.</summary>
    Unknown
}
