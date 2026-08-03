
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
    /// A Serializable+ReadWrite transaction exceeded <see cref="CamusDBOptions.MaxSerializableTransactionLifetimeMs"/>
    /// while still holding range locks. The transaction has been invalidated to prevent the range
    /// locks from expiring while the transaction is still considered live (which would silently
    /// break the serializable guarantee). The caller must roll back and retry from BeginAsync.
    /// </summary>
    public const string TransactionLifetimeExceeded = "CADB0505";

    /// <summary>
    /// A read-write transaction exceeded <see cref="CamusDBOptions.MaxMutationsPerTransaction"/>.
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
    /// <see cref="CamusDBConstants.MaxCommentLength"/>. The bound keeps the replicated schema blob from
    /// growing without limit, since comments ride the per-table metadata. A permanent caller mistake
    /// — maps to HTTP 400.
    /// </summary>
    public const string CommentTooLong = "CADB0511";

    /// <summary>
    /// <c>CREATE USER</c> (without <c>IF NOT EXISTS</c>) named a user that already exists. A permanent
    /// caller mistake — maps to HTTP 409.
    /// </summary>
    public const string UserAlreadyExists = "CADB0512";

    /// <summary>
    /// <c>ALTER USER</c> / <c>DROP USER</c> / <c>GRANT</c> / <c>REVOKE</c> named a user that does not
    /// exist. GRANT never implicitly creates a user. Maps to HTTP 404.
    /// </summary>
    public const string UserDoesNotExist = "CADB0513";

    /// <summary>
    /// <c>IDENTIFIED WITH &lt;plugin&gt;</c> named an authentication plugin CamusDB does not support.
    /// Only <c>sha256_password</c> is accepted today. A permanent caller mistake — maps to HTTP 400.
    /// </summary>
    public const string UnsupportedAuthPlugin = "CADB0514";

    /// <summary>
    /// A <c>GRANT</c>/<c>REVOKE</c> named a privilege that is not valid (unknown token, or not
    /// applicable to the object scope). A permanent caller mistake — maps to HTTP 400.
    /// </summary>
    public const string InvalidPrivilege = "CADB0515";

    /// <summary>
    /// Authentication failed: no/invalid/expired bearer token, unknown user, wrong password, or a
    /// disabled account. Deliberately indistinguishable across those causes to avoid account
    /// enumeration. Maps to HTTP 401.
    /// </summary>
    public const string AuthenticationFailed = "CADB0516";

    /// <summary>
    /// The caller is authenticated but lacks the privilege the statement requires. Maps to HTTP 403.
    /// </summary>
    public const string InsufficientPrivilege = "CADB0517";

    /// <summary>
    /// Too many authentication attempts (login rate limit / global KDF saturation). Maps to HTTP 429.
    /// </summary>
    public const string TooManyAuthAttempts = "CADB0518";

    /// <summary>
    /// A credential-bearing request arrived over a plaintext (non-TLS) connection while authentication
    /// is enabled and TLS is required. Refused so a token/password is never accepted in the clear.
    /// Maps to HTTP 400.
    /// </summary>
    public const string InsecureTransport = "CADB0519";

    /// <summary>
    /// The prepared statement handle named by the request is not registered on this node: it was
    /// closed, it expired after sitting idle, it was prepared on a different node or on a different
    /// gRPC stream, or it belongs to a different principal.
    ///
    /// <para>This is <b>not</b> a fatal error and does not indicate a broken client. Handles are
    /// deliberately node-local and session-scoped, so an unknown handle is a routine event after an
    /// idle period, a server restart, a stream rebuild, or a request that a load balancer sent to
    /// another node. The correct client response is to prepare the statement again and replay the
    /// execution once.</para>
    ///
    /// <para>Existence and ownership failures share this one code and message on purpose: a distinct
    /// "not yours" error would let a caller probe which handles exist. Maps to HTTP 404.</para>
    /// </summary>
    public const string UnknownPreparedStatement = "CADB0520";

    /// <summary>
    /// The caller holds more live prepared statements than the configured cap allows.
    ///
    /// <para>Deliberately a refusal rather than an eviction: silently dropping the least recently
    /// used handle would make a correct client's next execution fail at an unpredictable moment, so
    /// the server refuses the new registration and asks the caller to close what it no longer needs.
    /// Maps to HTTP 429.</para>
    /// </summary>
    public const string PreparedStatementLimitExceeded = "CADB0521";

    /// <summary>
    /// <c>ANALYZE</c> was issued on a transaction that has already written rows it has not committed.
    ///
    /// <para>ANALYZE deliberately scans under its own read-only snapshot so the statistics it
    /// publishes globally describe committed data only. That snapshot cannot read past the caller's
    /// own unresolved write intents: a snapshot read waits for an intent whose commit timestamp is
    /// still undetermined, and here only the caller could resolve it — but the caller is blocked
    /// waiting for ANALYZE. Refusing is the honest outcome; the alternative is a stall that lasts
    /// until the storage layer reaps the transaction.</para>
    ///
    /// <para>Permanent for this transaction, not transient: commit or roll back first, then run
    /// ANALYZE. Maps to HTTP 400.</para>
    /// </summary>
    public const string AnalyzeRequiresNoPendingWrites = "CADB0522";

    public const string InvalidConfig = "CADB0600";

    /// <summary>
    /// A backup/PITR operation was requested but the node has no backup directory configured
    /// (<c>kahuna.backup_dir</c> is unset). Backups are opt-in; set the directory and restart to enable
    /// them. Maps to HTTP 503 — the capability is unavailable on this node, not a caller mistake.
    /// </summary>
    public const string BackupNotConfigured = "CADB0700";

    /// <summary>
    /// A backup chain could not be resolved or validated: it does not start at a full backup, has a gap
    /// or a broken parent link, or contains a cycle. A restore refuses loudly rather than reconstruct a
    /// state that never existed. A permanent caller/data mistake — maps to HTTP 422.
    /// </summary>
    public const string BackupChainInvalid = "CADB0701";

    /// <summary>
    /// An incremental backup was requested but its parent has fallen below the retention floor (too old
    /// to still be in the WAL), so a contiguous increment is impossible. The caller must take a fresh
    /// full backup instead. Maps to HTTP 409.
    /// </summary>
    public const string BackupNeedsFullBackup = "CADB0702";

    /// <summary>
    /// A restore requested a point in time outside the recoverable window: in the future, or older than
    /// <c>now - </c><see cref="CamusDBOptions.PitrWindowSeconds"/>. The window is the upper bound on
    /// recoverability; actual reach is further limited by which backups survive in the chain. A
    /// permanent caller mistake — maps to HTTP 422.
    /// </summary>
    public const string RestorePointOutOfWindow = "CADB0703";

    /// <summary>
    /// A restore failed while copying the base image or replaying WAL (I/O error, corrupt artifact, or
    /// an unexpected engine failure). The partially written target directory should be discarded. Maps
    /// to HTTP 500.
    /// </summary>
    public const string RestoreFailed = "CADB0704";

    /// <summary>The parent backup named by an incremental request does not exist. Maps to HTTP 404.</summary>
    public const string BackupParentMissing = "CADB0705";

    /// <summary>
    /// A backup artifact is missing, truncated, extra, duplicated, or fails its recorded digest. The
    /// engine refuses to restore rather than reconstruct a state that never existed. Maps to HTTP 422.
    /// </summary>
    public const string BackupCorruptArtifact = "CADB0706";

    /// <summary>
    /// The restore destination already exists or overlaps a protected path (the live data root, its
    /// kv/wal, the backup root, or another job's target). Restore into a fresh directory. Maps to 409.
    /// </summary>
    public const string RestoreTargetConflict = "CADB0707";

    /// <summary>
    /// The backend cannot produce an exact as-of checkpoint at the requested cut, so a full backup with
    /// a proven base cut cannot be taken. The engine fails closed rather than publish an image that
    /// over-includes post-cut state. Maps to HTTP 409.
    /// </summary>
    public const string BackupExactCheckpointUnavailable = "CADB0708";

    /// <summary>
    /// A backup artifact/manifest is in a legacy or unsupported format and was not upgraded. Maps to 422.
    /// </summary>
    public const string BackupUnsupportedFormat = "CADB0709";

    /// <summary>
    /// A backup/restore operation lost partition leadership mid-flight; it applied nothing durable and
    /// the caller may retry. Transient — maps to HTTP 503.
    /// </summary>
    public const string BackupRetryableLeadershipLoss = "CADB070A";

    /// <summary>A backup/restore operation was cancelled by the caller. Maps to HTTP 499.</summary>
    public const string BackupCancelled = "CADB070B";

    /// <summary>
    /// Restore is not permitted on this node: no server-owned restore root is configured and the
    /// unconfined opt-in is off. Set <c>kahuna.restore_root</c> to a directory restore destinations must
    /// live under, then retry. Maps to HTTP 403.
    /// </summary>
    public const string RemoteRestoreDisabled = "CADB070C";

    /// <summary>
    /// The cluster topology (partition map or membership) changed during a coordinated backup, so the
    /// captured partition set is not one consistent snapshot; the backup was aborted and nothing
    /// published. Transient — retry once the topology is stable. Maps to HTTP 503.
    /// </summary>
    public const string BackupTopologyChanged = "CADB070D";

    /// <summary>
    /// A coordinated (cluster-wide) backup was requested on a node that does not lead the backup meta
    /// partition, so it cannot own the backup. Retry against the current coordinator node. Maps to
    /// HTTP 421 (Misdirected Request).
    /// </summary>
    public const string BackupNotCoordinator = "CADB070E";

    /// <summary>
    /// The configured backup or restore root is unsafe (a symlink/reparse point, or group/world-writable
    /// on POSIX), so backups are refused before anything is written. Restrict the directory to the
    /// server's user and retry. A server misconfiguration — maps to HTTP 500.
    /// </summary>
    public const string BackupInsecureRoot = "CADB070F";

    /// <summary>
    /// Returns the HTTP status code that should be used when surfacing <paramref name="code"/>
    /// to an API caller. Client errors (permanent, non-retryable caller mistakes) map to 400;
    /// all other codes map to 500.
    /// </summary>
    public static int GetHttpStatus(string code) => code switch
    {
        BackupNotConfigured => 503,
        BackupChainInvalid => 422,
        BackupNeedsFullBackup => 409,
        RestorePointOutOfWindow => 422,
        BackupParentMissing => 404,
        BackupCorruptArtifact => 422,
        RestoreTargetConflict => 409,
        BackupExactCheckpointUnavailable => 409,
        BackupUnsupportedFormat => 422,
        BackupRetryableLeadershipLoss => 503,
        BackupCancelled => 499,
        RemoteRestoreDisabled => 403,
        BackupTopologyChanged => 503,
        BackupNotCoordinator => 421,
        BackupInsecureRoot => 500,
        TransactionMutationLimitExceeded => 400,
        AnalyzeRequiresNoPendingWrites => 400,
        CheckConstraintViolation => 400,
        InvalidAsOfSystemTime => 400,
        CommentTooLong => 400,
        UnsupportedAuthPlugin => 400,
        InvalidPrivilege => 400,
        InsecureTransport => 400,
        AuthenticationFailed => 401,
        InsufficientPrivilege => 403,
        OrphanNotFound => 404,
        UserDoesNotExist => 404,
        UnknownPreparedStatement => 404,
        PreparedStatementLimitExceeded => 429,
        DatabaseAlreadyExists => 409,
        TableAlreadyExists => 409,
        UserAlreadyExists => 409,
        TooManyAuthAttempts => 429,
        _ => 500
    };
}
