
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs;
using CamusDB.Grpc;
using CamusDB.App.Grpc;
using CamusDB.App.Services;
using CamusDB.Grpc.Client;
using CamusDB.Grpc.Client.Batching;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// End-to-end test of the real multiplexing client (<see cref="GrpcBatcher"/> + <see cref="CamusConnection"/>)
/// against a real <see cref="CamusSqlService"/> over an in-process transport (no socket). Proves the whole
/// batched path works: autocommit ops and a full transactional unit of work (BEGIN → statements → COMMIT)
/// all pipelined over one <c>BatchExecute</c> stream, with results actually persisted in the engine.
/// </summary>
[TestFixture]
[NonParallelizable]
public class TestGrpcClientEndToEnd : BaseTest
{
    private CommandExecutor executor = null!;
    private HttpTransactionCoordinator coordinator = null!;
    private CamusSqlService service = null!;

    [SetUp]
    public void SetUpService()
    {
        CommandValidator validator = new();
        CatalogsManager catalogs = new(logger);
        executor = new(validator, catalogs, logger,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        coordinator = new(executor);
        service = new(executor, coordinator, logger, TestHostApplicationLifetime.Instance);
    }

    [TearDown]
    public async Task TearDownService()
    {
        try { await executor.DisposeAsync(); } catch { }
    }

    private CamusConnection NewConnection()
    {
        GrpcBatcher batcher = new(
            new CamusGrpcOptions { ChannelPoolSize = 1 },
            id => new InProcBatchTransport(id, service));
        return new CamusConnection(batcher);
    }

    private async Task<string> CreateDatabaseWithTableAsync()
    {
        string db = "db" + Guid.NewGuid().ToString("n");
        await service.ExecuteDdl(new SqlRequest { Database = db, Sql = $"CREATE DATABASE {db}" }, new TestServerCallContext());
        await service.ExecuteDdl(new SqlRequest
        {
            Database = db,
            Sql = "CREATE TABLE items (id oid PRIMARY KEY, name string(100))",
        }, new TestServerCallContext());
        return db;
    }

    [Test]
    public async Task Autocommit_InsertThenQuery_OverRealServer()
    {
        string db = await CreateDatabaseWithTableAsync();
        await using CamusConnection conn = NewConnection();

        NonQueryResult insert = await conn.ExecuteNonQueryAsync(db, "INSERT INTO items (id, name) VALUES (gen_id(), 'alpha')");
        Assert.That(insert.AffectedRows, Is.EqualTo(1));

        QueryResult query = await conn.ExecuteQueryAsync(db, "SELECT id, name FROM items");
        Assert.That(query.Schema.Columns.Select(c => c.Name), Is.EquivalentTo(new[] { "id", "name" }));
        Assert.That(query.Rows.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task Transaction_BeginStatementsCommit_OverRealServer_Persists()
    {
        string db = await CreateDatabaseWithTableAsync();
        await using CamusConnection conn = NewConnection();

        CamusTransactionSession tx = await conn.BeginTransactionAsync(db);
        await tx.ExecuteNonQueryAsync("INSERT INTO items (id, name) VALUES (gen_id(), 'a')");
        await tx.ExecuteNonQueryAsync("INSERT INTO items (id, name) VALUES (gen_id(), 'b')");
        await tx.CommitAsync();

        // A fresh autocommit query must see both committed rows.
        QueryResult after = await conn.ExecuteQueryAsync(db, "SELECT id FROM items");
        Assert.That(after.Rows.Count, Is.EqualTo(2));
    }

    [Test]
    public async Task Transaction_Rollback_OverRealServer_DiscardsWrites()
    {
        string db = await CreateDatabaseWithTableAsync();
        await using CamusConnection conn = NewConnection();

        CamusTransactionSession tx = await conn.BeginTransactionAsync(db);
        await tx.ExecuteNonQueryAsync("INSERT INTO items (id, name) VALUES (gen_id(), 'ghost')");
        await tx.RollbackAsync();

        QueryResult after = await conn.ExecuteQueryAsync(db, "SELECT id FROM items");
        Assert.That(after.Rows.Count, Is.EqualTo(0), "Rolled-back writes must not persist");
    }

    [Test]
    public async Task ManyConcurrentAutocommitInserts_OverRealServer_AllPersist()
    {
        string db = await CreateDatabaseWithTableAsync();
        await using CamusConnection conn = NewConnection();

        Task<NonQueryResult>[] inserts = Enumerable.Range(0, 15)
            .Select(i => conn.ExecuteNonQueryAsync(db, $"INSERT INTO items (id, name) VALUES (gen_id(), 'row{i}')"))
            .ToArray();
        NonQueryResult[] results = await Task.WhenAll(inserts);
        Assert.That(results.All(r => r.AffectedRows == 1), Is.True);

        QueryResult after = await conn.ExecuteQueryAsync(db, "SELECT id FROM items");
        Assert.That(after.Rows.Count, Is.EqualTo(15));
    }
}
