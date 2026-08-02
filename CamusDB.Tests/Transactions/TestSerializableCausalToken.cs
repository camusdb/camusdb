
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Tests for Task 8 — session read-your-writes via causal token.
///
/// The causal token is an HLCTimestamp returned by every committed write/read.
/// When supplied on a subsequent read request, the server mints a snapshot T >= token,
/// guaranteeing the reader observes any write that produced that token — same-node
/// causality without TrueTime.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSerializableCausalToken : SharedNodeBaseTest
{
    /// <summary>Engine whose transactions default to Serializable, as the causal-token tests require.</summary>
    private CamusDBOptions SerializableByDefault =>
        Options with { DefaultIsolationLevel = CamusIsolationLevel.Serializable };

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupDbAsync(
        CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase(options ?? Options);

        KvTransaction tx = await db.Transactions.BeginAsync();
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
                new(ConstraintType.PrimaryKey, "pk_accounts", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false));
        await db.Transactions.CommitAsync(tx);

        return (dbname, db, executor);
    }

    private static async Task<(string id, HLCTimestamp token)> InsertAccount(
        string dbname, DatabaseDescriptor db, CommandExecutor executor, long balance)
    {
        string id = ObjectIdGenerator.Generate().ToString();
        KvTransaction tx = await db.Transactions.BeginAsync();
        InsertTicket ticket = new(
            txnState: tx,
            databaseName: dbname,
            tableName: "accounts",
            values: new List<Dictionary<string, ColumnValue>>
            {
                new() { ["id"] = new(ColumnType.Id, id), ["balance"] = new(ColumnType.Integer64, balance) }
            }
        );
        await executor.Insert(ticket);
        return (id, await db.Transactions.CommitAsync(tx));
    }

    private static async Task<(long balance, HLCTimestamp token)> ReadBalance(
        string dbname, DatabaseDescriptor db, CommandExecutor executor, string id, HLCTimestamp? causalToken = null)
    {
        KvTransaction tx = await db.Transactions.BeginReadOnlyAsync(promote: false, causalToken);
        QueryTicket ticket = new(
            txnState: tx,
            databaseName: dbname,
            tableName: "accounts",
            index: null, projection: null, where: null, filters: null, orderBy: null,
            limit: null, offset: null, parameters: null
        );
        List<IReadOnlyDictionary<string, ColumnValue>> rows = [];
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.Query(ticket);
        await foreach (QueryResultRow row in cursor)
            rows.Add(row.Row);
        HLCTimestamp token = await db.Transactions.CommitAsync(tx);
        long bal = -1;
        foreach (IReadOnlyDictionary<string, ColumnValue> row in rows)
        {
            if (row["id"].StrValue == id)
            {
                bal = row["balance"].LongValue;
                break;
            }
        }
        return (bal, token);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// CommitAsync returns a non-zero causal token after a successful write.
    [Test]
    public async Task CommitAsync_ReturnsNonZeroToken()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();
        (_, HLCTimestamp token) = await InsertAccount(dbname, db, executor, 100);
        Assert.That(token, Is.Not.EqualTo(HLCTimestamp.Zero), "commit must return a non-zero causal token");
        Assert.That(token.L, Is.GreaterThan(0), "token L (physical time) must be positive");
    }

    /// A read with no token succeeds and returns a non-zero token.
    [Test]
    public async Task Read_WithoutToken_ReturnsToken()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();
        (string id, _) = await InsertAccount(dbname, db, executor, 200);
        (_, HLCTimestamp token) = await ReadBalance(dbname, db, executor, id);
        Assert.That(token, Is.Not.EqualTo(HLCTimestamp.Zero), "read must return a non-zero causal token");
    }

    /// The causal token from a write, when supplied on a subsequent read, produces a snapshot
    /// T >= the write token, so the read always observes the write.
    [Test]
    public async Task ReadWithToken_ObservesWrite()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();
        (string id, HLCTimestamp writeToken) = await InsertAccount(dbname, db, executor, 300);
        (long balance, _) = await ReadBalance(dbname, db, executor, id, causalToken: writeToken);
        Assert.That(balance, Is.EqualTo(300), "read with causal token must observe the preceding write");
    }

    /// Same-node reads without a token still observe same-node writes.
    [Test]
    public async Task ReadWithoutToken_AfterWrite_StillObservesWrite_OnSameNode()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();
        (string id, _) = await InsertAccount(dbname, db, executor, 400);
        (long balance, _) = await ReadBalance(dbname, db, executor, id);
        Assert.That(balance, Is.EqualTo(400), "same-node read without token observes same-node write");
    }

    /// Tokens from successive writes on the same node are monotonically non-decreasing.
    [Test]
    public async Task CommitTokens_AreMonotonicallyIncreasing()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync();
        (_, HLCTimestamp t1) = await InsertAccount(dbname, db, executor, 500);
        (_, HLCTimestamp t2) = await InsertAccount(dbname, db, executor, 501);
        Assert.That(t2.CompareTo(t1), Is.GreaterThanOrEqualTo(0),
            "second commit token must be >= first commit token (HLC monotonicity)");
    }

    /// BeginReadOnlyAsync with a causal token mints a snapshot T >= that token (Serializable mode).
    [Test]
    public async Task ReadSnapshot_IsAtLeastCausalToken()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync(SerializableByDefault);
        (_, HLCTimestamp writeToken) = await InsertAccount(dbname, db, executor, 600);

        KvTransaction tx = await db.Transactions.BeginReadOnlyAsync(promote: false, writeToken);
        Assert.That(tx.ReadTimestamp.CompareTo(writeToken), Is.GreaterThanOrEqualTo(0),
            "snapshot ReadTimestamp must be >= the supplied causal token");
        await db.Transactions.CommitAsync(tx);
    }

    /// The causal token returned by a serializable read is >= the token that was supplied to it.
    [Test]
    public async Task ReadToken_IsAtLeastInputToken()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupDbAsync(SerializableByDefault);
        (string id, HLCTimestamp writeToken) = await InsertAccount(dbname, db, executor, 700);

        (_, HLCTimestamp readToken) = await ReadBalance(dbname, db, executor, id, causalToken: writeToken);
        Assert.That(readToken.CompareTo(writeToken), Is.GreaterThanOrEqualTo(0),
            "token returned from a read must be >= the token supplied to that read");
    }
}
