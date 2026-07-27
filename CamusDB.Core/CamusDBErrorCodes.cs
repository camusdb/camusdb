
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core;

public static class CamusDBErrorCodes
{
    public const string DatabaseDoesntExist = "CADB0010";
    public const string TableDoesntExist = "CADB0011";
    public const string DatabaseAlreadyExists = "CADB0012";
    public const string TableAlreadyExists = "CADB0013";
    public const string SystemSpaceCorrupt = "CADB0014";
    public const string TableCorrupt = "CADB0015";
    public const string IndexDoesntExist = "CADB0016";
    public const string InvalidIndexLayout = "CADB0017";
    public const string DatabaseNameReserved = "CADB0018";

    /// <summary>
    /// Standalone mode: the database directory exists with a <c>creating.lock</c> sentinel but
    /// without a <c>kv/</c> sub-directory, indicating the process crashed after
    /// <c>RegisterAsync</c> committed but before <c>DatabaseCreator.Create</c> finished.
    /// The database is recoverable — drop it and recreate it.
    /// </summary>
    public const string DatabaseCreationIncomplete = "CADB0019";

    public const string InvalidPageOffset = "CADB00297";

    public const string InvalidInternalOperation = "CADB0099";
    public const string InvalidPageChecksum = "CADB0098";
    public const string InvalidPageLength = "CADB0097";
    public const string InvalidInformationSchema = "CADB0096";

    public const string InvalidInput = "CADB0400";
    public const string UnknownType = "CADB0401";
    public const string DuplicatePrimaryKey = "CADB0402";
    public const string DuplicateColumn = "CADB0403";
    public const string UnknownColumn = "CADB0404";
    public const string UnknownKey = "CADB0405";
    public const string SqlSyntaxError = "CADB0406";
    public const string InvalidAstStmt = "CADB0407";

    /// <summary>
    /// An <c>AS OF SYSTEM TIME</c> clause could not be honored: the value is malformed (bad duration
    /// or timestamp), resolves to a future or pre-epoch instant, or the statement is not an autocommit
    /// read-only SELECT (time-travel is rejected inside an explicit or promoted transaction, which
    /// already holds a live Kahuna session pinned to its own read snapshot). A permanent caller
    /// mistake — maps to HTTP 400.
    /// </summary>
    public const string InvalidAsOfSystemTime = "CADB0409";

    /// <summary>
    /// A schema operation would exceed a configured limit (identifier length, columns per table,
    /// indexes per table, or tables per database). The limit and offending value are named in the
    /// exception message. <c>&lt;= 0</c> values in <see cref="CamusDBConfig"/> disable the
    /// corresponding check.
    /// </summary>
    public const string SchemaLimitExceeded = "CADB0408";

    public const string DuplicateUniqueKeyValue = "CADB0300";
    public const string NotNullViolation = "CADB0301";
    public const string ValueTooLong = "CADB0302";

    /// <summary>
    /// A row was rejected because it evaluates to <c>false</c> against a named CHECK constraint.
    /// The exception message includes the constraint name. Maps to HTTP 400.
    /// </summary>
    public const string CheckConstraintViolation = "CADB0303";
    
    public const string TransactionAlreadyCompleted = "CADB0501";
    public const string TransactionConflict = "CADB0502";
    public const string SchemaCatchingUp = "CADB0503";

    /// <summary>
    /// A <b>pre-write</b> transient signal: an operation could not be routed or a lock could not be
    /// taken before any data was written — a leader flip/partition move during start
    /// (<c>MustRetry</c> after the bounded start retries) or a lock-wait deadline / write conflict in
    /// the storage layer. No write was applied, so the transaction is safe to replay from scratch.
    /// Unlike <see cref="TransactionAlreadyCompleted"/>, this is transient — the caller should retry
    /// the entire operation from BeginAsync.
    ///
    /// <para><b>Not</b> raised for a commit/rollback whose outcome is unknown — that is the
    /// non-terminal <see cref="TransactionFinalizeUnresolved"/> (CADB0509), which must be retried on
    /// the <em>same</em> handle rather than replayed, because the write may already have committed.</para>
    ///
    /// Retry boundary: the executor auto-retries the schema-catch-up fence (<see
    /// cref="SchemaCatchingUp"/>, CADB0503) inside ExecuteNonSQLQuery because the fence fires
    /// before any write and the same transaction is still usable. CADB0504 is auto-retried by
    /// <see cref="SerializableRetryHelper"/> for autocommit statements by replaying from a fresh
    /// BeginAsync — safe precisely because nothing was written when it is raised.
    /// </summary>
    public const string TransactionMustRetry = "CADB0504";

    /// <summary>
    /// A Serializable+ReadWrite transaction exceeded <see cref="CamusDBConfig.MaxSerializableTransactionLifetimeMs"/>
    /// while still holding range locks. The transaction has been invalidated to prevent the range
    /// locks from expiring while the transaction is still considered live (which would silently
    /// break the serializable guarantee). The caller must roll back and retry from BeginAsync.
    /// </summary>
    public const string TransactionLifetimeExceeded = "CADB0505";

    /// <summary>
    /// A read-write transaction exceeded <see cref="CamusDBConfig.MaxMutationsPerTransaction"/>.
    /// Permanent (non-retryable): the transaction must be split into smaller batches. Mirrors
    /// Cloud Spanner's "too many mutations" rejection. One CamusDB mutation = one row-blob
    /// write/delete or one secondary-index entry write/delete.
    /// </summary>
    public const string TransactionMutationLimitExceeded = "CADB0506";

    /// <summary>
    /// Spill-to-disk is required (the operator exceeded the in-memory row threshold) but the
    /// temp store is unavailable — either the spill root directory could not be created or a
    /// spill file could not be opened. The query cannot proceed without spill storage; the
    /// caller must either free disk space / fix permissions and retry, or run the query on a
    /// node where spill storage is accessible.
    ///
    /// This error is non-retryable by <see cref="SerializableRetryHelper"/>: disk-space /
    /// permission failures do not resolve on their own.
    /// </summary>
    public const string SpillStorageUnavailable = "CADB0507";

    /// <summary>
    /// Dropped database has one or more live branch descendants still registered in the database
    /// registry. Dropping it would release its snapshot-floor hold, invalidating the frozen views
    /// of those branches. Drop all descendant branches first, then drop this database.
    /// </summary>
    public const string DatabaseHasLiveDescendants = "CADB0508";

    /// <summary>
    /// A commit or rollback returned the coordinator's non-terminal <c>MustRetry</c> after the
    /// bounded same-handle finalize retries were exhausted: the final outcome is not known yet (a
    /// leadership flip mid-finalize, a drain still in progress, or a durable decision not yet marked
    /// Completed). The transaction is left in <see cref="Transactions.KvTransactionStatus.Finalizing"/>
    /// — <b>not</b> rolled back — and stays tracked with its handle valid.
    ///
    /// <para>The caller must retry the <em>same</em> commit/rollback on the <em>same</em> transaction
    /// (an explicit client re-issues COMMIT/ROLLBACK for the same id). It must <b>never</b> re-run the
    /// business operation from a fresh BeginAsync: the write may already have committed server-side, so
    /// replaying it could double-apply. For this reason CADB0509 is deliberately excluded from
    /// <see cref="SerializableRetryHelper"/>'s replay-from-BEGIN set. An abandoned finalizing session is
    /// bounded by the Kahuna session timeout as the ultimate backstop.</para>
    /// </summary>
    public const string TransactionFinalizeUnresolved = "CADB0509";

    /// <summary>
    /// A <c>RELINK</c> or orphan-purge target id has no orphan record: the database/table was never
    /// dropped under that id, or its orphan was already reclaimed by the garbage collector (retention
    /// window elapsed). A permanent caller mistake — maps to HTTP 404.
    /// </summary>
    public const string OrphanNotFound = "CADB0510";

    /// <summary>
    /// A <c>COMMENT ON</c> (or inline <c>COMMENT</c>) text exceeds
    /// <see cref="CamusDBConfig.MaxCommentLength"/>. The bound keeps the replicated schema blob from
    /// growing without limit, since comments ride the per-table metadata. A permanent caller mistake
    /// — maps to HTTP 400.
    /// </summary>
    public const string CommentTooLong = "CADB0511";

    public const string InvalidConfig = "CADB0600";

    /// <summary>
    /// Returns the HTTP status code that should be used when surfacing <paramref name="code"/>
    /// to an API caller. Client errors (permanent, non-retryable caller mistakes) map to 400;
    /// all other codes map to 500.
    /// </summary>
    public static int GetHttpStatus(string code) => code switch
    {
        TransactionMutationLimitExceeded => 400,
        CheckConstraintViolation => 400,
        InvalidAsOfSystemTime => 400,
        CommentTooLong => 400,
        OrphanNotFound => 404,
        DatabaseAlreadyExists => 409,
        TableAlreadyExists => 409,
        _ => 500
    };
}
