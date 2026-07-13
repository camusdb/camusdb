
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;
using Microsoft.Extensions.Logging;

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Config;

/// <summary>
/// Boots a real standalone RocksDB-backed embedded Kahuna node built from the default
/// <see cref="EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb"/> options — which enable shared
/// RocksDB memory by default — and drives a create/insert/select round-trip through
/// <see cref="CommandExecutor"/>. This proves the default-on shared block-cache + write-buffer-manager
/// wiring does not break node startup or query serving on the standalone host path. It asserts
/// behaviour, not memory usage (a memory reduction is an operational measurement, not a unit assertion).
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestStandaloneRocksDbSharedMemoryBoot
{
    private static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(builder => builder.AddFilter("Camus", LogLevel.Warning));

    [Test]
    public async Task DefaultSharedMemoryOptions_BootAndServeRoundTrip()
    {
        string dataPath = Path.Combine(Path.GetTempPath(), "camusdb-sharedmem-boot-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dataPath);

        // Default standalone RocksDB options: both storage and WAL are rocksdb, so shared memory is on.
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(dataPath, new KahunaOptionsConfig());
        Assert.That(options.RocksDbSharedMemoryEnabled, Is.True, "shared memory must be on by default for the standalone RocksDB baseline");
        Assert.That(options.Storage, Is.EqualTo("rocksdb"));
        Assert.That(options.WalStorage, Is.EqualTo("rocksdb"));

        EmbeddedKahuna? node = null;
        DatabaseRegistry? registry = null;
        CommandExecutor? executor = null;

        try
        {
            node = new EmbeddedKahuna(options, LoggerFactory);
            await node.StartAsync(CancellationToken.None);
            await node.WaitForLeaderAsync("warmup", CancellationToken.None);
            await node.FlushAsync();

            registry = await DatabaseRegistry.OpenAsync(node);

            ILogger<ICamusDB> logger = LoggerFactory.CreateLogger<ICamusDB>();
            executor = new CommandExecutor(
                new CommandValidator(), new CatalogsManager(logger), logger,
                sharedNode: node, registry: registry, isClusterMode: false);

            string dbname = Guid.NewGuid().ToString("n");
            DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));

            KvTransaction txn = await database.Transactions.BeginAsync();

            ExecuteDDLSQLResult ddl = await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                txnState: txn, database: dbname,
                sql: "CREATE TABLE robots (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, year INT64 NOT NULL)",
                parameters: null));
            Assert.That(ddl.Success, Is.True);

            ExecuteNonSQLResult insert = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                txnState: txn, database: dbname,
                sql: "INSERT INTO robots (id, name, year) VALUES (GEN_ID(), \"astro boy\", 3000)",
                parameters: null));
            Assert.That(insert.ModifiedRows, Is.EqualTo(1));

            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
                txnState: txn, database: dbname, sql: "SELECT name, year FROM robots", parameters: null));

            List<QueryResultRow> rows = await cursor.ToListAsync();
            Assert.That(rows.Count, Is.EqualTo(1));
            Assert.That(rows[0].Row["name"].StrValue, Is.EqualTo("astro boy"));
            Assert.That(rows[0].Row["year"].LongValue, Is.EqualTo(3000));

            await database.Transactions.CommitAsync(txn);
        }
        finally
        {
            if (executor is not null) { try { await executor.DisposeAsync(); } catch { } }
            if (registry is not null) { try { await registry.DisposeAsync(); } catch { } }
            if (node is not null) { try { await node.DisposeAsync(); } catch { } }
            try { Directory.Delete(dataPath, recursive: true); } catch { }
        }
    }
}
