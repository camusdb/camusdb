
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusConfig = CamusDB.Core.CamusDBConfig;

using Microsoft.Extensions.Logging;

using NUnit.Framework;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Verifies that committed row data survives closing and reopening a SQLite-backed database.
/// </summary>
[TestFixture]
public sealed class TestSqliteRowPersistence
{
    private static readonly ILoggerFactory LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
        b.AddFilter("Camus", LogLevel.Warning));

    [Test]
    [NonParallelizable]
    public async Task InsertedRowsSurviveCloseAndReopen()
    {
        string dbname = Guid.NewGuid().ToString("n");
        string dataPath = Path.Combine(CamusConfig.DataDirectory, dbname);

        try
        {
            Directory.CreateDirectory(Path.Combine(dataPath, "kv"));
            Directory.CreateDirectory(Path.Combine(dataPath, "wal"));

            ILogger<ICamusDB> logger = LoggerFactory.CreateLogger<ICamusDB>();

            EmbeddedKahuna node = EmbeddedKahuna.CreateSqlite(dataPath, LoggerFactory);
            await node.StartAsync(CancellationToken.None);
            await node.WaitForLeaderAsync("warmup", CancellationToken.None);
            await node.FlushAsync();

            CommandValidator validator = new();
            CatalogsManager catalogs = new(logger);
            CommandExecutor executor = new(validator, catalogs, logger,
                loggerFactory: LoggerFactory, sharedNode: node, isClusterMode: true);

            DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));

            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname,
                tableName: "items",
                columns: [new ColumnInfo("id", ColumnType.Id), new ColumnInfo("amount", ColumnType.Integer64)],
                constraints:
                [
                    new(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])
                ],
                ifNotExists: false));

            KvTransaction tx = await database.Transactions.BeginAsync();
            await executor.Insert(new InsertTicket(
                txnState: tx,
                databaseName: dbname,
                tableName: "items",
                values: [new Dictionary<string, ColumnValue>
                {
                    { "id", new(ColumnType.Id, "507f1f77bcf86cd799439011") },
                    { "amount", new(ColumnType.Integer64, 42) }
                }]));
            await database.Transactions.CommitAsync(tx);

            await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

            database = await executor.OpenDatabase(dbname);

            KvTransaction readTx = await database.Transactions.BeginAsync();
            List<Dictionary<string, ColumnValue>> rows = await (await executor.QueryById(new QueryByIdTicket(
                txnState: readTx,
                databaseName: dbname,
                tableName: "items",
                id: "507f1f77bcf86cd799439011"))).ToListAsync();
            await database.Transactions.RollbackIfNotCompletedAsync(readTx);

            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(42, rows[0]["amount"].LongValue);
        }
        finally
        {
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, recursive: true);
        }
    }
}
