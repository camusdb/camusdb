
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

    public const string DuplicateUniqueKeyValue = "CADB0300";
    public const string NotNullViolation = "CADB0301";
    
    public const string TransactionAlreadyCompleted = "CADB0501";
    public const string TransactionConflict = "CADB0502";
    public const string SchemaCatchingUp = "CADB0503";

    /// <summary>
    /// Kahuna returned MustRetry from LocateAndCommitTransaction after exhausting all internal
    /// retries (routing inconsistency during a leader flip). The transaction has been rolled back.
    /// Unlike <see cref="TransactionAlreadyCompleted"/>, this is transient — the caller should
    /// retry the entire operation from BeginAsync.
    ///
    /// §3.5 retry boundary: the executor auto-retries the schema-catch-up fence (<see
    /// cref="SchemaCatchingUp"/>, CADB0503) inside ExecuteNonSQLQuery because the fence fires
    /// before any write and the same transaction is still usable. CADB0504 is intentionally NOT
    /// auto-retried at the executor level: by the time CommitAsync throws it, the operation may
    /// have been partially applied and the transaction object is spent (Status = RolledBack).
    /// The caller must rebuild the operation from scratch starting with a new BeginAsync.
    /// </summary>
    public const string TransactionMustRetry = "CADB0504";

    /// <summary>
    /// A Serializable+ReadWrite transaction exceeded <see cref="CamusDBConfig.MaxSerializableTransactionLifetimeMs"/>
    /// while still holding range locks. The transaction has been invalidated to prevent the range
    /// locks from expiring while the transaction is still considered live (which would silently
    /// break the serializable guarantee). The caller must roll back and retry from BeginAsync.
    /// </summary>
    public const string TransactionLifetimeExceeded = "CADB0505";

    public const string InvalidConfig = "CADB0600";
}
