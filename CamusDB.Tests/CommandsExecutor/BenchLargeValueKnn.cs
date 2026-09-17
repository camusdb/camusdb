/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Benchmark, not a correctness test: the exact nearest-neighbour workload from the vector-search
/// documentation (10,000 rows, 768-dimension embeddings) under each large-value storage setup. It
/// compares an engine that stores every value inline (the behaviour before large-value storage) with
/// the default strategy and with the two strategies the documentation recommends for embeddings.
/// Explicit, so it never runs in the normal suite; run it alone with nothing else on the machine.
/// </summary>
[Explicit("benchmark — run alone")]
[Category("Benchmark")]
[NonParallelizable]
internal sealed class BenchLargeValueKnn : SharedNodeBaseTest
{
    private const int Rows = 10_000;
    private const int Dimensions = 768;
    private const int WarmupRuns = 3;
    private const int MeasuredRuns = 15;

    private static byte[] Vector(Random random)
    {
        byte[] bytes = new byte[Dimensions * 4];
        for (int i = 0; i < Dimensions; i++)
            BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(i * 4), (float)(random.NextDouble() * 2 - 1));
        return bytes;
    }

    private static async Task<double> MedianMsAsync(CommandExecutor executor, string db, string sql, Dictionary<string, ColumnValue> parameters, int expectedRows)
    {
        List<double> samples = [];
        for (int run = 0; run < WarmupRuns + MeasuredRuns; run++)
        {
            KvTransaction tx = KvTransaction.CreateReadOnly();
            Stopwatch watch = Stopwatch.StartNew();
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            int count = 0;
            await foreach (QueryResultRow _ in cursor)
                count++;
            watch.Stop();

            Assert.AreEqual(expectedRows, count);
            if (run >= WarmupRuns)
                samples.Add(watch.Elapsed.TotalMilliseconds);
        }

        samples.Sort();
        return samples[samples.Count / 2];
    }

    private async Task<(double topK, double fullSort, double idOnly)> MeasureAsync(CamusDBOptions options, string storageClause)
    {
        string db = "bench_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor(options);
        TrackDatabase(db, executor);
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false));

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db,
            $"CREATE TABLE docs (id oid PRIMARY KEY, tenant_id int64, embedding bytes(3072) NOT NULL {storageClause})", null));

        Random random = new(768);
        for (int start = 0; start < Rows; start += 1000)
        {
            List<Dictionary<string, ColumnValue>> values = [];
            for (int i = start; i < Math.Min(Rows, start + 1000); i++)
            {
                values.Add(new()
                {
                    { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "tenant_id", new ColumnValue(ColumnType.Integer64, i % 10) },
                    { "embedding", new ColumnValue(Vector(random)) },
                });
            }

            KvTransaction tx = await database.Transactions.BeginAsync();
            await executor.Insert(new InsertTicket(tx, db, "docs", values));
            await database.Transactions.CommitAsync(tx);
        }

        Dictionary<string, ColumnValue> query = new() { { "@q", new ColumnValue(Vector(new Random(1))) } };

        double topK = await MedianMsAsync(executor, db, "SELECT id FROM docs ORDER BY l2_distance(embedding, @q) LIMIT 10", query, 10);
        double fullSort = await MedianMsAsync(executor, db, "SELECT id FROM docs ORDER BY l2_distance(embedding, @q)", query, Rows);
        double idOnly = await MedianMsAsync(executor, db, "SELECT id FROM docs", query, Rows);
        return (topK, fullSort, idOnly);
    }

    [Test]
    public async Task KnnWorkload_UnderEachStorageSetup()
    {
        (string name, CamusDBOptions options, string clause)[] setups =
        [
            ("inline engine (feature off)", Options with { LargeValueThresholdBytes = 0, LargeValueCompressionEnabled = false }, ""),
            ("STORAGE PLAIN", Options, "STORAGE PLAIN"),
            ("default EXTENDED", Options, ""),
            ("STORAGE EXTERNAL", Options, "STORAGE EXTERNAL"),
            // Measured again last: the first setup of a run also pays JIT and cache warm-up.
            ("inline engine, again", Options with { LargeValueThresholdBytes = 0, LargeValueCompressionEnabled = false }, ""),
            ("STORAGE PLAIN, again", Options, "STORAGE PLAIN"),
        ];

        List<string> report = [];
        foreach ((string name, CamusDBOptions options, string clause) in setups)
        {
            (double topK, double fullSort, double idOnly) = await MeasureAsync(options, clause);
            report.Add($"{name,-30} topk-10 {topK,8:F1} ms   full-sort {fullSort,8:F1} ms   id-only {idOnly,8:F1} ms");
        }

        foreach (string line in report)
            TestContext.Progress.WriteLine(line);
    }
}
