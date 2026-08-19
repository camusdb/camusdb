
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for the disk-space write-admission gate: an engine whose
/// <see cref="CamusDBOptions.MinFreeDiskBytes"/> watermark sits above the machine's real free
/// space must refuse DML with <see cref="CamusDBErrorCodes.InsufficientDiskSpace"/> while still
/// serving reads and DDL. The watermark is driven above/below the real free space instead of
/// filling a disk, so the gate is exercised through the genuine probe on the test data directory.
/// </summary>
[NonParallelizable]
public sealed class TestDiskFullAdmission : BaseTest
{
    /// <summary>A watermark no physical volume satisfies, so the gate reports low disk immediately.</summary>
    private const long AboveAnyDisk = long.MaxValue;

    private static async Task CreateRobotsTable(CommandExecutor executor, string dbname)
    {
        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "robots",
            columns:
            [
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true)
            ],
            constraints:
            [
                new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])
            ],
            ifNotExists: false
        );

        await executor.CreateTable(tableTicket);
    }

    private static async Task<ExecuteNonSQLResult> ExecNonQuery(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket);
            await database.Transactions.CommitAsync(tx);
            return result;
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    [Test]
    public async Task LowDisk_RejectsInsertButServesReadsAndDdl()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { MinFreeDiskBytes = AboveAnyDisk });

        // DDL is exempt from the gate: schema work runs with the mutation budget disabled,
        // which is also what keeps DROP available to free space.
        await CreateRobotsTable(executor, dbname);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, database, dbname,
                "INSERT INTO robots (id, name) VALUES (gen_id(), 'astro')"))!;

        Assert.AreEqual(CamusDBErrorCodes.InsufficientDiskSpace, exception.Code);

        // Reads must keep working while writes are refused.
        List<QueryResultRow> rows = await ExecQuery(executor, database, dbname, "SELECT * FROM robots");
        Assert.AreEqual(0, rows.Count);

        // The refused insert never reached storage, so the table stays droppable — the
        // operator's recovery path under a genuinely full disk.
        ExecuteNonSQLResult dropResult = await ExecNonQuery(executor, database, dbname, "DROP TABLE robots");
        Assert.IsNotNull(dropResult);
    }

    [Test]
    public async Task WatermarkIsRuntimeMutable_UpdateRefusedThenAdmittedAfterPublish()
    {
        // Runtime path: the gate reads the watermark from the live options snapshot, so a
        // published configuration change must flip admission without rebuilding the engine.
        CamusDBOptions initial = Options; // default watermark: far below the machine's free space
        CamusDBOptionsHolder holder = new(initial);

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabaseWith(CreateCommandExecutor(initial, holder));

        await CreateRobotsTable(executor, dbname);

        ExecuteNonSQLResult inserted = await ExecNonQuery(executor, database, dbname,
            "INSERT INTO robots (id, name) VALUES (gen_id(), 'astro')");
        Assert.AreEqual(1, inserted.ModifiedRows);

        // Raise the watermark above any physical disk: UPDATE and DELETE must now be refused.
        holder.Publish(initial with { MinFreeDiskBytes = AboveAnyDisk });

        CamusDBException updateRefused = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, database, dbname, "UPDATE robots SET name = 'nemo' WHERE 1=1"))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientDiskSpace, updateRefused.Code);

        CamusDBException deleteRefused = Assert.ThrowsAsync<CamusDBException>(() =>
            ExecNonQuery(executor, database, dbname, "DELETE FROM robots WHERE 1=1"))!;
        Assert.AreEqual(CamusDBErrorCodes.InsufficientDiskSpace, deleteRefused.Code);

        // Lower the watermark again — the equivalent of freeing space: DML must be admitted,
        // proving the node recovers without a restart.
        holder.Publish(initial);

        ExecuteNonSQLResult updated = await ExecNonQuery(executor, database, dbname,
            "UPDATE robots SET name = 'nemo' WHERE 1=1");
        Assert.AreEqual(1, updated.ModifiedRows);
    }

    [Test]
    public async Task DefaultWatermark_AdmitsWritesNormally()
    {
        // The shipped default (64 MiB) must not reject anything on a healthy volume.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await CreateRobotsTable(executor, dbname);

        ExecuteNonSQLResult inserted = await ExecNonQuery(executor, database, dbname,
            "INSERT INTO robots (id, name) VALUES (gen_id(), 'astro')");
        Assert.AreEqual(1, inserted.ModifiedRows);
    }
}
