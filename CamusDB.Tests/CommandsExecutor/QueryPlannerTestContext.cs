
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

using Kahuna;
using Kommander.Time;
using Nito.AsyncEx;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// In-memory schema/index metadata for <see cref="QueryPlanner"/> unit tests.
/// Does not insert rows, open databases, or start Kahuna transactions.
/// </summary>
internal sealed class QueryPlannerTestContext : IAsyncDisposable
{
    public const string DatabaseName = "planner-tests";

    public const string SampleRowId = "0123456789abcdef01234567";

    public DatabaseDescriptor Database { get; }

    public TableDescriptor Table { get; }

    public KvTransaction Txn { get; }

    private readonly EmbeddedKahuna kahuna;

    private QueryPlannerTestContext(
        EmbeddedKahuna kahuna,
        DatabaseDescriptor database,
        TableDescriptor table,
        KvTransaction txn)
    {
        this.kahuna = kahuna;
        Database = database;
        Table = table;
        Txn = txn;
    }

    public static QueryPlannerTestContext Create()
    {
        EmbeddedKahuna kahuna = new(new EmbeddedKahunaOptions
        {
            NodeName = "query-planner-test",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        });

        TableSchema schema = BuildRobotsSchema();
        KvTableStore store = new(kahuna.Kahuna, schema.Id!);

        TableDescriptor table = new(schema.Id!, schema.Name!, schema, store);
        table.Indexes.Add(
            CamusDBConfig.PrimaryKeyInternalName,
            new TableIndexSchema(CamusDBConfig.PrimaryKeyInternalName, ["id"], IndexType.Unique));
        table.Indexes.Add(
            "year_idx",
            new TableIndexSchema("year_idx", ["year"], IndexType.Multi));
        table.Indexes.Add(
            "year_enabled_idx",
            new TableIndexSchema("year_enabled_idx", ["year", "enabled"], IndexType.Unique));
        table.Indexes.Add(
            "name_idx",
            new TableIndexSchema("name_idx", ["name"], IndexType.Multi));

        DatabaseDescriptor database = new(
            DatabaseName,
            kahuna,
            new KvTransactionsManager(kahuna.Kahuna),
            new ConcurrentDictionary<string, AsyncLazy<TableDescriptor>>());

        KvTransaction txn = new(HLCTimestamp.Zero, "planner-test");

        return new QueryPlannerTestContext(kahuna, database, table, txn);
    }

    private static TableSchema BuildRobotsSchema()
    {
        List<TableColumnSchema> columns =
        [
            new("col-id", "id", ColumnType.Id, notNull: true, defaultValue: null),
            new("col-name", "name", ColumnType.String, notNull: true, defaultValue: null),
            new("col-year", "year", ColumnType.Integer64, notNull: false, defaultValue: null),
            new("col-enabled", "enabled", ColumnType.Bool, notNull: false, defaultValue: null),
        ];

        return new TableSchema
        {
            Id = "robots-planner-fixture",
            Name = "robots",
            Version = 0,
            Columns = columns,
            SchemaHistory =
            [
                new TableSchemaHistory
                {
                    Version = 0,
                    Columns = columns,
                }
            ]
        };
    }

    public async ValueTask DisposeAsync()
    {
        Database.Dispose();
        await kahuna.DisposeAsync().ConfigureAwait(false);
    }
}
