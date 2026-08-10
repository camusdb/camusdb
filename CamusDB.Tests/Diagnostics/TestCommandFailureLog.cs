
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.App.Services;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// Pins which failures are attributed to the caller (logged as warnings) and which are treated as
/// server faults (logged as errors with a stack trace). The classification exists so a misbehaving
/// client cannot flood an operator's error stream, so a code drifting to the wrong side is a real
/// regression even though nothing functional breaks.
/// </summary>
[TestFixture]
public sealed class TestCommandFailureLog
{
    [TestCase(CamusDBErrorCodes.InvalidInput)]
    [TestCase(CamusDBErrorCodes.SqlSyntaxError)]
    [TestCase(CamusDBErrorCodes.UnknownColumn)]
    [TestCase(CamusDBErrorCodes.NotNullViolation)]
    [TestCase(CamusDBErrorCodes.CheckConstraintViolation)]
    [TestCase(CamusDBErrorCodes.TableDoesntExist)]
    [TestCase(CamusDBErrorCodes.DatabaseDoesntExist)]
    [TestCase(CamusDBErrorCodes.UnknownPreparedStatement)]
    [TestCase(CamusDBErrorCodes.DuplicateUniqueKeyValue)]
    [TestCase(CamusDBErrorCodes.TableAlreadyExists)]
    [TestCase(CamusDBErrorCodes.AuthenticationFailed)]
    [TestCase(CamusDBErrorCodes.InsufficientPrivilege)]
    [TestCase(CamusDBErrorCodes.PreparedStatementLimitExceeded)]
    [TestCase(CamusDBErrorCodes.TransactionMutationLimitExceeded)]
    [TestCase(CamusDBErrorCodes.TooManyAuthAttempts)]
    public void CallerMistakesAreNotServerErrors(string code)
    {
        Assert.That(CommandFailureLog.IsCallerError(code), Is.True, code);
    }

    /// <summary>
    /// A transaction that aborts under contention is the expected outcome of serializable isolation,
    /// not a fault: the client replays it. Logging every abort at error level would make the error
    /// stream track write contention rather than server health.
    /// </summary>
    [TestCase(CamusDBErrorCodes.TransactionConflict)]
    [TestCase(CamusDBErrorCodes.TransactionMustRetry)]
    public void RetryableAbortsAreNotServerErrors(string code)
    {
        Assert.That(CommandFailureLog.IsCallerError(code), Is.True, code);
    }

    /// <summary>
    /// Conditions only an operator can clear stay at error level even when the caller sees them as a
    /// rejected request — including spill storage, which surfaces as resource exhaustion but means
    /// this node is out of disk or cannot write its temp directory.
    /// </summary>
    [TestCase(CamusDBErrorCodes.SpillStorageUnavailable)]
    [TestCase(CamusDBErrorCodes.BackupInsecureRoot)]
    [TestCase("CADB9999")]
    public void ServerFaultsStayErrors(string code)
    {
        Assert.That(CommandFailureLog.IsCallerError(code), Is.False, code);
    }
}
