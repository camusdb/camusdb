/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;

using NUnit.Framework;

using CamusDB.Grpc;
using CamusDB.Grpc.Client;
using CamusDB.Grpc.Client.Batching;

namespace CamusDB.Grpc.Client.Tests;

/// <summary>
/// The client side of <c>INSERT … RETURNING</c> on the no-rows calls: the rows reach
/// <see cref="NonQueryResult"/> beside the count and never make the call fail, and every
/// <c>discardReturningRows</c> overload puts the flag on the wire. Driven against
/// <see cref="FakeBatchTransport"/>, which answers the way the server does.
/// </summary>
[TestFixture]
public class TestNonQueryReturning
{
    private FakeBatchTransport? transport;

    private GrpcBatcher NewBatcher()
        => new(new CamusGrpcOptions { ChannelPoolSize = 1 }, id =>
        {
            transport = new FakeBatchTransport(id) { ReturnRows = true };
            return transport;
        });

    private BatchExecuteRequest LastNonQuery()
        => transport!.Received.Last(r => r.Kind == BatchStatementKind.NonQuery);

    private static void AssertRows(NonQueryResult result)
    {
        Assert.That(result.AffectedRows, Is.EqualTo(2));
        Assert.That(result.ReturningSchema, Is.Not.Null);
        Assert.That(result.ReturningSchema!.Columns.Select(c => c.Name), Is.EqualTo(new[] { "n" }));
        Assert.That(result.ReturningRows!.Select(r => r.Values[0].Int64Value), Is.EqualTo(new long[] { 10, 11 }));
    }

    private static void AssertCountOnly(NonQueryResult result)
    {
        Assert.That(result.AffectedRows, Is.EqualTo(2));
        Assert.That(result.ReturningSchema, Is.Null);
        Assert.That(result.ReturningRows, Is.Null);
    }

    [Test]
    public async Task AutocommitNonQuery_ReturnsTheCountAndTheRows()
    {
        await using GrpcBatcher batcher = NewBatcher();
        CamusConnection connection = new(batcher);

        NonQueryResult result = await connection.ExecuteNonQueryAsync("db", "INSERT INTO t (n) VALUES (10), (11) RETURNING n");

        AssertRows(result);
        Assert.That(LastNonQuery().Request.DiscardReturningRows, Is.False);
    }

    [Test]
    public async Task AutocommitNonQuery_CountOnlySendsTheFlag()
    {
        await using GrpcBatcher batcher = NewBatcher();
        CamusConnection connection = new(batcher);

        NonQueryResult result = await connection.ExecuteNonQueryAsync(
            "db", "INSERT INTO t (n) VALUES (10), (11) RETURNING n", discardReturningRows: true);

        AssertCountOnly(result);
        Assert.That(LastNonQuery().Request.DiscardReturningRows, Is.True);
    }

    [Test]
    public async Task ReplyWithoutReturning_LeavesBothMembersNull()
    {
        await using GrpcBatcher batcher = new(new CamusGrpcOptions { ChannelPoolSize = 1 }, id => new FakeBatchTransport(id));
        CamusConnection connection = new(batcher);

        NonQueryResult result = await connection.ExecuteNonQueryAsync("db", "INSERT INTO t (n) VALUES (1)");

        Assert.That(result.AffectedRows, Is.EqualTo(1));
        Assert.That(result.ReturningSchema, Is.Null);
        Assert.That(result.ReturningRows, Is.Null);
    }

    [Test]
    public async Task TransactionNonQuery_ReturnsRowsAndHonorsTheFlag()
    {
        await using GrpcBatcher batcher = NewBatcher();
        CamusConnection connection = new(batcher);

        CamusTransactionSession session = await connection.BeginTransactionAsync("db");

        AssertRows(await session.ExecuteNonQueryAsync("INSERT INTO t (n) VALUES (10), (11) RETURNING n"));
        Assert.That(LastNonQuery().Request.DiscardReturningRows, Is.False);

        AssertCountOnly(await session.ExecuteNonQueryAsync("INSERT INTO t (n) VALUES (10), (11) RETURNING n", discardReturningRows: true));
        Assert.That(LastNonQuery().Request.DiscardReturningRows, Is.True);

        await session.CommitAsync();
    }

    [Test]
    public async Task PreparedNonQuery_ReturnsRowsAndHonorsTheFlag()
    {
        await using GrpcBatcher batcher = NewBatcher();
        CamusConnection connection = new(batcher);

        CamusPreparedStatement statement = await connection.PrepareAsync("db", "INSERT INTO t (n) VALUES (@a) RETURNING n");

        AssertRows(await statement.ExecuteNonQueryAsync([10L]));
        Assert.That(LastNonQuery().Request.DiscardReturningRows, Is.False);

        AssertCountOnly(await statement.ExecuteNonQueryAsync([10L], discardReturningRows: true));
        Assert.That(LastNonQuery().Request.DiscardReturningRows, Is.True);

        AssertCountOnly(await statement.ExecuteNonQueryAsync(new { a = 10L }, discardReturningRows: true));
        Assert.That(LastNonQuery().Request.DiscardReturningRows, Is.True);

        CamusTransactionSession session = await connection.BeginTransactionAsync("db");
        AssertRows(await session.ExecuteNonQueryAsync(statement, [10L]));
        AssertCountOnly(await session.ExecuteNonQueryAsync(statement, [10L], discardReturningRows: true));
        Assert.That(LastNonQuery().Request.DiscardReturningRows, Is.True);
        await session.CommitAsync();

        await statement.DisposeAsync();
    }
}
