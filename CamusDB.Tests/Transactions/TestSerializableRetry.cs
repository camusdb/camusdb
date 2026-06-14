
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Verifies the serializable retry contract: which error codes are retryable, the replay-from-BEGIN
/// rule, and the <see cref="SerializableRetryHelper.ExecuteAutocommitAsync"/> bounded retry wrapper.
///
/// Unit tests (1–5) exercise the helper's logic without Kahuna.
/// Integration tests (6–7) drive the full executor stack to confirm a real serializable conflict
/// is resolved transparently by the helper.
/// </summary>
[TestFixture]
public sealed class TestSerializableRetry : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)>
        SetupDbAsync()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "accounts",
            columns: new ColumnInfo[]
            {
                new("id",      ColumnType.Id),
                new("balance", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk",
                    new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        return (dbname, db, executor);
    }

    private static async Task<long> ReadBalanceAsync(
        string dbname, CommandExecutor executor, KvTransaction tx, string id)
    {
        QueryTicket q = new(
            txnState: tx, databaseName: dbname, tableName: "accounts",
            index: null, projection: null, where: null,
            filters: new() { new("id", "=", new(ColumnType.Id, id)) },
            orderBy: null, limit: null, offset: null, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) = await executor.Query(q);
        List<QueryResultRow> list = await rows.ToListAsync();
        return list[0].Row["balance"].LongValue;
    }

    // -----------------------------------------------------------------------
    // 1. Retryable code set — spot-check the three members
    // -----------------------------------------------------------------------

    [Test]
    public void RetryHelper_RetryableCodes_ContainsExpectedSet()
    {
        Assert.IsTrue(SerializableRetryHelper.RetryableCodes.Contains(CamusDBErrorCodes.TransactionConflict),
            "TransactionConflict (CADB0502) must be retryable");
        Assert.IsTrue(SerializableRetryHelper.RetryableCodes.Contains(CamusDBErrorCodes.TransactionMustRetry),
            "TransactionMustRetry (CADB0504) must be retryable");
        Assert.IsTrue(SerializableRetryHelper.RetryableCodes.Contains(CamusDBErrorCodes.TransactionLifetimeExceeded),
            "TransactionLifetimeExceeded (CADB0505) must be retryable");
    }

    // -----------------------------------------------------------------------
    // 2. Non-retryable codes must not be in the set
    // -----------------------------------------------------------------------

    [Test]
    public void RetryHelper_IsRetryable_ReturnsFalseForNonRetryableCodes()
    {
        string[] nonRetryable =
        [
            CamusDBErrorCodes.DuplicateUniqueKeyValue,
            CamusDBErrorCodes.NotNullViolation,
            CamusDBErrorCodes.InvalidInput,
            CamusDBErrorCodes.TransactionAlreadyCompleted,
            CamusDBErrorCodes.SystemSpaceCorrupt,
        ];

        foreach (string code in nonRetryable)
        {
            CamusDBException ex = new(code, "test");
            Assert.IsFalse(SerializableRetryHelper.IsRetryable(ex),
                $"Code {code} must NOT be retryable");
        }
    }

    // -----------------------------------------------------------------------
    // 3. ExecuteAutocommitAsync retries on TransactionConflict and succeeds
    // -----------------------------------------------------------------------

    [Test]
    public async Task RetryHelper_RetriesOnConflict_SucceedsOnThirdAttempt()
    {
        int attempts = 0;

        int result = await SerializableRetryHelper.ExecuteAutocommitAsync(async ct =>
        {
            attempts++;
            await Task.Yield();
            if (attempts < 3)
                throw new CamusDBException(CamusDBErrorCodes.TransactionConflict, "simulated conflict");
            return attempts;
        }, maxAttempts: 5);

        Assert.AreEqual(3, attempts, "Helper must try exactly 3 times before succeeding");
        Assert.AreEqual(3, result, "Result must come from the successful attempt");
    }

    // -----------------------------------------------------------------------
    // 4. Non-retryable error propagates immediately without retry
    // -----------------------------------------------------------------------

    [Test]
    public async Task RetryHelper_NonRetryableError_PropagatesImmediately()
    {
        int attempts = 0;

        CamusDBException? caught = null;
        try
        {
            await SerializableRetryHelper.ExecuteAutocommitAsync(async ct =>
            {
                attempts++;
                await Task.Yield();
                throw new CamusDBException(CamusDBErrorCodes.DuplicateUniqueKeyValue, "unique violation");
            }, maxAttempts: 5);
        }
        catch (CamusDBException ex)
        {
            caught = ex;
        }

        Assert.AreEqual(1, attempts, "Non-retryable error must not trigger a retry — exactly 1 attempt");
        Assert.IsNotNull(caught);
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, caught!.Code);
    }

    // -----------------------------------------------------------------------
    // 5. Exhausted max-attempts re-throws the last retryable error
    // -----------------------------------------------------------------------

    [Test]
    public async Task RetryHelper_ExhaustsMaxAttempts_RethrowsLastError()
    {
        int attempts = 0;

        CamusDBException? caught = null;
        try
        {
            await SerializableRetryHelper.ExecuteAutocommitAsync<int>(async ct =>
            {
                attempts++;
                await Task.Yield();
                throw new CamusDBException(CamusDBErrorCodes.TransactionMustRetry, $"attempt {attempts}");
            }, maxAttempts: 3);
        }
        catch (CamusDBException ex)
        {
            caught = ex;
        }

        Assert.AreEqual(3, attempts, "Helper must exhaust exactly maxAttempts before giving up");
        Assert.IsNotNull(caught);
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, caught!.Code);
        StringAssert.Contains("attempt 3", caught.Message,
            "The re-thrown exception must be from the final attempt");
    }

    // -----------------------------------------------------------------------
    // 6. Integration: real TransactionConflict resolved transparently by retry
    //
    // Alice holds a shared point lock on the row (Serializable+RW read).
    // Bob's autocommit UPDATE conflicts immediately (AlreadyLocked → TransactionConflict).
    // ExecuteAutocommitAsync catches the conflict, waits for the back-off, retries.
    // Meanwhile Alice commits. Bob's retry succeeds.
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_AutocommitRetry_ResolvesConflictTransparently()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        string id = ObjectIdGenerator.Generate().ToString();

        // Seed: balance=100
        KvTransaction setup = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        id)    },
                { "balance", new(ColumnType.Integer64, 100L)  },
            }}));
        await db.Transactions.CommitAsync(setup);

        // Alice reads the row under Serializable+RW, acquiring a shared point lock.
        KvTransaction alice = await db.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        long _ = await ReadBalanceAsync(dbname, executor, alice, id); // acquires S lock

        // Bob's autocommit UPDATE is wrapped in the retry helper. His first attempt will
        // conflict with Alice's S lock. After the back-off Alice commits and Bob's retry
        // acquires the X lock and succeeds.
        Task<int> aliceCommit = Task.Run(async () =>
        {
            // Give Bob's first attempt time to hit the conflict, then commit.
            await Task.Delay(300);
            await db.Transactions.CommitAsync(alice);
            return 0;
        });

        int bobAttempts = 0;

        await SerializableRetryHelper.ExecuteAutocommitAsync(async ct =>
        {
            bobAttempts++;
            KvTransaction bob = await db.Transactions.BeginAsync(
                CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite, ct);
            try
            {
                await executor.Update(new UpdateTicket(
                    txnState: bob, databaseName: dbname, tableName: "accounts",
                    plainValues: new() { { "balance", new(ColumnType.Integer64, 50L) } },
                    exprValues: null, where: null,
                    filters: new() { new("id", "=", new(ColumnType.Id, id)) },
                    parameters: null));
                await db.Transactions.CommitAsync(bob, ct);
            }
            catch
            {
                await db.Transactions.RollbackIfNotCompletedAsync(bob, ct);
                throw;
            }
        }, maxAttempts: 5);

        await aliceCommit;

        // Bob must have needed more than one attempt (first conflicted with Alice's S lock).
        Assert.GreaterOrEqual(bobAttempts, 2,
            "Bob's first attempt must have conflicted with Alice's shared lock; retry was required");

        // Final state: Bob's committed update is visible.
        KvTransaction reader = await db.Transactions.BeginAsync();
        long finalBalance = await ReadBalanceAsync(dbname, executor, reader, id);
        await db.Transactions.RollbackAsync(reader);

        Assert.AreEqual(50L, finalBalance,
            "Bob's autocommit UPDATE must have committed after the retry resolved the conflict");
    }

    // -----------------------------------------------------------------------
    // 7. Non-retryable error (duplicate key) is not swallowed by the helper
    // -----------------------------------------------------------------------

    [Test]
    public async Task SerializableRW_DuplicateKey_NotRetried()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();

        string id = ObjectIdGenerator.Generate().ToString();

        // Seed row
        KvTransaction setup = await db.Transactions.BeginAsync();
        await executor.Insert(new InsertTicket(
            txnState: setup, databaseName: dbname, tableName: "accounts",
            values: new() { new() {
                { "id",      new(ColumnType.Id,        id)   },
                { "balance", new(ColumnType.Integer64, 100L) },
            }}));
        await db.Transactions.CommitAsync(setup);

        int attempts = 0;
        CamusDBException? caught = null;

        try
        {
            await SerializableRetryHelper.ExecuteAutocommitAsync(async ct =>
            {
                attempts++;
                KvTransaction tx = await db.Transactions.BeginAsync(cancellationToken: ct);
                try
                {
                    // Inserting the same id again → DuplicatePrimaryKey (not retryable)
                    await executor.Insert(new InsertTicket(
                        txnState: tx, databaseName: dbname, tableName: "accounts",
                        values: new() { new() {
                            { "id",      new(ColumnType.Id,        id)   },
                            { "balance", new(ColumnType.Integer64, 999L) },
                        }}));
                    await db.Transactions.CommitAsync(tx, ct);
                }
                catch
                {
                    await db.Transactions.RollbackIfNotCompletedAsync(tx, ct);
                    throw;
                }
            }, maxAttempts: 5);
        }
        catch (CamusDBException ex)
        {
            caught = ex;
        }

        Assert.AreEqual(1, attempts,
            "Primary-key violation is not retryable — helper must surface the error after the first attempt");
        Assert.IsNotNull(caught);
        // PK violations are caught as DuplicateUniqueKeyValue (CADB0300) since the PK is a unique index.
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, caught!.Code);
    }
}
