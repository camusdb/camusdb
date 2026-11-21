/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Expression indexes on the cluster DDL path: <see cref="SharedNodeBaseTest"/> runs with
/// <c>isClusterMode: true</c>, so CREATE INDEX goes through the replicated schema log and the staged
/// coordinator with its backfill. Covers the replicated stored form, the build over existing rows, and
/// a leader-change resume that must rebuild the same expression keys from the persisted job.
/// </summary>
[NonParallelizable]
internal sealed class TestExpressionIndexesCluster : SharedNodeBaseTest
{
    private static async Task ExecDDL(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await db.Transactions.CommitAsync(tx);
        }
        catch
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    private static async Task<List<QueryResultRow>> ExecSelect(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<string> ScanNode(CommandExecutor executor, string dbname, string select)
    {
        foreach (QueryResultRow row in await ExecSelect(executor, dbname, "EXPLAIN " + select))
        {
            string node = row.Row["node"].StrValue ?? "";
            if (node is "table-scan" or "index-range-scan" or "index-lookup")
                return node;
        }

        return "(no scan node)";
    }

    [Test]
    public async Task ClusterCreateIndex_OverExistingRows_ReplicatesAndServesReads()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE users (id oid primary key, email string, doc jsonb)");

        for (int i = 0; i < 12; i++)
            await ExecNonQuery(executor, dbname, $"INSERT INTO users (id, email, doc) VALUES (gen_id(), 'User{i}@X.io', '{{\"email\": \"u{i}@example.com\"}}')");

        await ExecDDL(executor, dbname, "CREATE UNIQUE INDEX users_lower_email ON users ((lower(email)))");
        await ExecDDL(executor, dbname, "CREATE INDEX users_email ON users ((doc->>'email'))");

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "users"));
        TableIndexSchema persisted = table.Schema.Indexes!.First(ix => ix.Name == "users_lower_email");
        Assert.That(persisted.State, Is.EqualTo(SchemaElementState.Public));
        Assert.That(persisted.KeyExpressions, Has.Length.EqualTo(1));
        Assert.That(persisted.ColumnIds![0], Is.EqualTo(IndexKeyExpression.ColumnIdMarker));

        List<QueryResultRow> found = await ExecSelect(executor, dbname, "SELECT email FROM users WHERE lower(email) = 'user7@x.io'");
        Assert.That(found.Select(r => r.Row["email"].StrValue), Is.EqualTo(new[] { "User7@X.io" }));
        Assert.That(await ScanNode(executor, dbname, "SELECT email FROM users WHERE lower(email) = 'user7@x.io'"), Is.EqualTo("index-lookup"));

        found = await ExecSelect(executor, dbname, "SELECT email FROM users WHERE doc->>'email' = 'u3@example.com'");
        Assert.That(found.Select(r => r.Row["email"].StrValue), Is.EqualTo(new[] { "User3@X.io" }));
        Assert.That(await ScanNode(executor, dbname, "SELECT email FROM users WHERE doc->>'email' = 'u3@example.com'"), Is.EqualTo("index-range-scan"));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, dbname, "INSERT INTO users (id, email) VALUES (gen_id(), 'user7@x.IO')"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecDDL(executor, dbname, "CREATE INDEX bad ON users ((now()))"));
        Assert.That(refused!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidIndexExpression));
    }

    [Test]
    public void PersistedCoordinatorJob_RoundTrips_KeyExpressions()
    {
        PersistedCoordinatorJob job = new()
        {
            TableName = "users",
            TableId = "tbl-1",
            ElementName = "users_lower_email",
            TargetState = SchemaElementState.Public,
            ElementKind = SchemaElementKind.Index,
            IndexId = "idx-1",
            IndexColumnIds = [IndexKeyExpression.ColumnIdMarker],
            IndexType = IndexType.Unique,
            IndexKeyExpressions = [new IndexKeyExpression(0, "lower(__camus_col_abc)", ColumnType.String, ["abc"])],
            Attempts = 0,
        };

        byte[] bytes = MetaJsonSerializer.Serialize(job, MetaJsonContext.Default.PersistedCoordinatorJob);
        PersistedCoordinatorJob decoded = MetaJsonSerializer.Deserialize(bytes, MetaJsonContext.Default.PersistedCoordinatorJob);

        Assert.That(decoded.IndexKeyExpressions, Has.Length.EqualTo(1));
        Assert.That(decoded.IndexKeyExpressions![0].Position, Is.EqualTo(0));
        Assert.That(decoded.IndexKeyExpressions[0].Sql, Is.EqualTo("lower(__camus_col_abc)"));
        Assert.That(decoded.IndexKeyExpressions[0].Type, Is.EqualTo(ColumnType.String));
        Assert.That(decoded.IndexKeyExpressions[0].ColumnIds, Is.EqualTo(new[] { "abc" }));
    }

    /// <summary>
    /// A job an interrupted leader left behind for an expression index, resumed through
    /// <see cref="SchemaChangeCoordinator.ResumeJobsAsync"/> with the backfill wired as the database
    /// opener wires it. The resumed index must carry its expression and hold an entry, keyed by the
    /// expression's value, for every row that existed.
    /// </summary>
    [Test]
    public async Task ResumedJob_BuildsTheSameExpressionKeys()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE users (id oid primary key, email string)");
        for (int i = 0; i < 5; i++)
            await ExecNonQuery(executor, dbname, $"INSERT INTO users (id, email) VALUES (gen_id(), 'Ann{i}@X.io')");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableSchema table = db.Schema.Tables["users"];

        // The stored form the proposer would have computed for ((lower(email))).
        (IndexKeyExpression stored, _) = IndexExpressionBinder.Prepare(
            "users", "users_lower_email", 0, "lower(email)", IndexExpressionBinder.ColumnsOf(table.Columns));

        CatalogsManager catalogs = new(logger);
        PersistedCoordinatorJob job = new()
        {
            TableName = "users",
            TableId = table.Id!,
            ElementName = "users_lower_email",
            TargetState = SchemaElementState.Public,
            ElementKind = SchemaElementKind.Index,
            IndexId = ObjectIdGenerator.Generate().ToString(),
            IndexColumnIds = [IndexKeyExpression.ColumnIdMarker],
            IndexType = IndexType.Unique,
            IndexKeyExpressions = [stored],
            Attempts = 0,
        };
        await catalogs.PersistCoordinatorJobAsync(db, job);

        SchemaChangeCoordinator coordinator = new(catalogs, logger)
        {
            IndexBackfillAsync = (d, tableName, info, start, checkpoint, ct) =>
                executor.BackfillIndexEntriesAsync(d, tableName, info, start, checkpoint, ct)
        };
        await coordinator.ResumeJobsAsync(db);

        TableIndexSchema created = db.Schema.Tables["users"].Indexes!.First(ix => ix.Name == "users_lower_email");
        Assert.That(created.State, Is.EqualTo(SchemaElementState.Public));
        Assert.That(created.KeyExpressions, Has.Length.EqualTo(1));
        Assert.That(created.KeyExpressions![0].Sql, Is.EqualTo(stored.Sql));

        List<QueryResultRow> found = await ExecSelect(executor, dbname, "SELECT email FROM users WHERE lower(email) = 'ann3@x.io'");
        Assert.That(found.Select(r => r.Row["email"].StrValue), Is.EqualTo(new[] { "Ann3@X.io" }));
        Assert.That(await ScanNode(executor, dbname, "SELECT email FROM users WHERE lower(email) = 'ann3@x.io'"), Is.EqualTo("index-lookup"));

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, dbname, "INSERT INTO users (id, email) VALUES (gen_id(), 'ANN1@x.io')"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.DuplicateUniqueKeyValue));
    }
}
