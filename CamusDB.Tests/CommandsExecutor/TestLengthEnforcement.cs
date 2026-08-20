
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance tests: string/bytes length enforcement on INSERT and UPDATE.
/// </summary>
internal sealed class TestLengthEnforcement : SharedNodeBaseTest
{
    // ── Setup helpers ─────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupWithSizedString(int n)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            $"CREATE TABLE t (id OID NOT NULL, v string({n}), PRIMARY KEY (id))", null));
        return (dbname, db, executor);
    }

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupWithBareString()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE t (id OID NOT NULL, v string, PRIMARY KEY (id))", null));
        return (dbname, db, executor);
    }

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupWithSizedBytes(int n)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            $"CREATE TABLE t (id OID NOT NULL, v bytes({n}), PRIMARY KEY (id))", null));
        return (dbname, db, executor);
    }

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupWithBareBytes()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE t (id OID NOT NULL, v bytes, PRIMARY KEY (id))", null));
        return (dbname, db, executor);
    }

    private static async Task<InsertResult> Insert(CommandExecutor executor, DatabaseDescriptor db, string tableName,
        Dictionary<string, ColumnValue> row)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        InsertResult result = await executor.Insert(new InsertTicket(
            txnState: tx,
            databaseName: db.Name,
            tableName: tableName,
            values: [row]));
        await db.Transactions.CommitAsync(tx);
        return result;
    }

    private static async Task<UpdateResult> UpdateAll(CommandExecutor executor, DatabaseDescriptor db,
        string tableName, string colName, ColumnValue newValue)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        return await executor.Update(new UpdateTicket(
            txnState: tx,
            databaseName: db.Name,
            tableName: tableName,
            plainValues: new() { { colName, newValue } },
            exprValues: null,
            where: null,
            filters: null,
            parameters: null));
    }

    // ── string(N) on INSERT ───────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task StringSized_InsertAtLimit_Succeeds()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithSizedString(10);
        string val = new('x', 10);

        InsertResult rows = await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
        {
            { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
            { "v",  new(ColumnType.String, val) }
        });

        Assert.AreEqual(1, rows.InsertedRows);
    }

    [Test]
    [NonParallelizable]
    public async Task StringSized_InsertOverLimit_Throws()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithSizedString(10);
        string val = new('x', 11);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
            {
                { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "v",  new(ColumnType.String, val) }
            }));

        Assert.AreEqual(CamusDBErrorCodes.ValueTooLong, ex!.Code);
        StringAssert.Contains("'v'", ex.Message);
        StringAssert.Contains("10",  ex.Message);
        StringAssert.Contains("11",  ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task StringSized_InsertNull_Succeeds()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithSizedString(5);

        InsertResult rows = await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
        {
            { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
            { "v",  ColumnValue.Null }
        });

        Assert.AreEqual(1, rows.InsertedRows);
    }

    // ── bare string — DefaultStringMaxLength ─────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task BareString_InsertAtDefaultLimit_Succeeds()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithBareString();
        string val = new('a', CamusDBConstants.DefaultStringMaxLength);

        InsertResult rows = await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
        {
            { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
            { "v",  new(ColumnType.String, val) }
        });

        Assert.AreEqual(1, rows.InsertedRows);
    }

    [Test]
    [NonParallelizable]
    public async Task BareString_InsertOverDefaultLimit_Throws()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithBareString();
        string val = new('a', CamusDBConstants.DefaultStringMaxLength + 1);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
            {
                { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "v",  new(ColumnType.String, val) }
            }));

        Assert.AreEqual(CamusDBErrorCodes.ValueTooLong, ex!.Code);
    }

    // ── bare bytes — DefaultBytesMaxLength ────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task BareBytes_InsertAtDefaultLimit_Succeeds()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithBareBytes();
        byte[] val = new byte[CamusDBConstants.DefaultBytesMaxLength];

        InsertResult rows = await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
        {
            { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
            { "v",  new(val) }
        });

        Assert.AreEqual(1, rows.InsertedRows);
    }

    [Test]
    [NonParallelizable]
    public async Task BareBytes_InsertOverDefaultLimit_Throws()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithBareBytes();
        byte[] val = new byte[CamusDBConstants.DefaultBytesMaxLength + 1];

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
            {
                { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "v",  new(val) }
            }));

        Assert.AreEqual(CamusDBErrorCodes.ValueTooLong, ex!.Code);
        StringAssert.Contains("'v'", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BareBytes_InsertNull_Succeeds()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithBareBytes();

        InsertResult rows = await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
        {
            { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
            { "v",  ColumnValue.Null }
        });

        Assert.AreEqual(1, rows.InsertedRows);
    }

    // ── bytes(N) on INSERT ────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task BytesSized_InsertAtLimit_Succeeds()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithSizedBytes(3072);

        InsertResult rows = await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
        {
            { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
            { "v",  new(new byte[3072]) }
        });

        Assert.AreEqual(1, rows.InsertedRows);
    }

    [Test]
    [NonParallelizable]
    public async Task BytesSized_InsertOverLimit_Throws()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithSizedBytes(3072);

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
            {
                { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                { "v",  new(new byte[3073]) }
            }));

        Assert.AreEqual(CamusDBErrorCodes.ValueTooLong, ex!.Code);
        StringAssert.Contains("'v'", ex.Message);
    }

    [Test]
    [NonParallelizable]
    public async Task BytesSized_InsertUnderLimit_Succeeds()
    {
        // The declared size is a ceiling, not an exact width. A shorter value is accepted here;
        // an exact vector dimension needs a CHECK constraint instead.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithSizedBytes(3072);

        InsertResult rows = await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
        {
            { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
            { "v",  new(new byte[3068]) }
        });

        Assert.AreEqual(1, rows.InsertedRows);
    }

    // ── UPDATE path ───────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task StringSized_UpdateWithinBound_Succeeds()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithSizedString(20);

        await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
        {
            { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
            { "v",  new(ColumnType.String, "short") }
        });

        UpdateResult result = await UpdateAll(executor, db, "t", "v",
            new(ColumnType.String, new string('y', 20)));

        Assert.AreEqual(1, result.UpdatedRows);
    }

    [Test]
    [NonParallelizable]
    public async Task StringSized_UpdateOverBound_Throws()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupWithSizedString(20);

        await Insert(executor, db, "t", new Dictionary<string, ColumnValue>()
        {
            { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
            { "v",  new(ColumnType.String, "short") }
        });

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await UpdateAll(executor, db, "t", "v",
                new(ColumnType.String, new string('y', 21))));

        Assert.AreEqual(CamusDBErrorCodes.ValueTooLong, ex!.Code);
        StringAssert.Contains("'v'", ex.Message);
        StringAssert.Contains("20",  ex.Message);
    }
}
