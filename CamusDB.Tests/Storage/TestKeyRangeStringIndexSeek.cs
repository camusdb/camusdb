/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Equality lookups through a String-keyed non-unique index must work when the index key space is
/// registered for key-range routing. The ordered-ASCII string encoding maps ' '..'&lt;' (digits
/// included) to C0 control characters and letters to printable characters, so a value's first
/// character decides which encoded-key region the lookup addresses. A lookup must succeed for both
/// regions, before and after the space splits at a boundary between them.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestKeyRangeStringIndexSeek : KeyRangeSplitFixture
{
    /// <summary>
    /// Production nodes persist through RocksDB, while the shared test defaults use the in-memory
    /// backend. This fixture exists to catch a defect that only appears when index keys round-trip
    /// through the persistent comparator, so it runs on the persistent backend.
    /// </summary>
    protected override void ConfigureNodeOptions(Kahuna.EmbeddedKahunaOptions options)
    {
        // Rooted under the per-test data directory, which BaseTest deletes in teardown.
        string dataDir = CamusDB.Core.CamusDBConfig.DataDirectory;

        options.Storage = "rocksdb";
        options.WalStorage = "rocksdb";
        options.StoragePath = System.IO.Path.Combine(dataDir, "rocks-store");
        options.WalPath = System.IO.Path.Combine(dataDir, "rocks-wal");
    }

    private static readonly string[] DigitLeading  = ["2NMIKZQK6Q", "2H8ZYMY3NE", "9DRZTHF6J4"];
    private static readonly string[] LetterLeading = ["ABMMLDVAZU", "LML8WUJQUU", "RCRUCZG8VA"];

    private async Task<(string db, CommandExecutor executor, TableDescriptor table)> SetupUsersTableAsync()
    {
        (string db, DatabaseDescriptor _, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: db,
            tableName: "users",
            columns:
            [
                new ColumnInfo("id", ColumnType.Id),
                new ColumnInfo("publicId", ColumnType.String, notNull: true),
            ],
            constraints:
            [
                new ConstraintInfo(ConstraintType.PrimaryKey, "~pk",
                    [new ColumnIndexInfo("id", OrderType.Ascending)]),
                new ConstraintInfo(ConstraintType.IndexMulti, "publicId_idx",
                    [new ColumnIndexInfo("publicId", OrderType.Ascending)]),
            ],
            ifNotExists: false
        ));

        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        foreach (string publicId in DigitLeading.Concat(LetterLeading))
        {
            await executor.Insert(new InsertTicket(
                txnState: tx, databaseName: db, tableName: "users",
                values: new() { new() {
                    { "id",       new(ColumnType.Id,     ObjectIdGenerator.Generate().ToString()) },
                    { "publicId", new(ColumnType.String, publicId) },
                }}));
        }

        await database.Transactions.CommitAsync(tx);

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(db, "users"));
        return (db, executor, table);
    }

    private static async Task<List<QueryResultRow>> RunSqlAsync(
        string db, CommandExecutor executor, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));

            List<QueryResultRow> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(row);

            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
            throw;
        }
    }

    private static void AssertLookupFinds(string publicId, List<QueryResultRow> rows)
    {
        Assert.That(rows, Has.Count.EqualTo(1),
            $"equality lookup for '{publicId}' must return exactly its row");
        Assert.That(rows[0].Row["publicId"].StrValue, Is.EqualTo(publicId));
    }

    [Test]
    public async Task StringIndexSpace_IsRegisteredForKeyRangeRouting()
    {
        (string _, CommandExecutor _, TableDescriptor table) = await SetupUsersTableAsync();

        string indexSpace = table.Store.IndexKeySpace(IndexKvId(table, "publicId_idx"));

        Assert.That(Descriptors(indexSpace), Is.Not.Empty,
            "the String-keyed index space must be registered for key-range routing");
    }

    [Test]
    public async Task EqualityLookup_BeforeSplit_FindsDigitAndLetterLeadingValues()
    {
        (string db, CommandExecutor executor, TableDescriptor _) = await SetupUsersTableAsync();

        foreach (string publicId in DigitLeading.Concat(LetterLeading))
            AssertLookupFinds(publicId, await RunSqlAsync(db, executor,
                $"SELECT * FROM users WHERE publicId = '{publicId}'"));
    }

    [Test]
    public async Task EqualityLookup_AfterSplitBetweenControlAndPrintableRegions_FindsAllValues()
    {
        (string db, CommandExecutor executor, TableDescriptor table) = await SetupUsersTableAsync();

        string indexSpace = table.Store.IndexKeySpace(IndexKvId(table, "publicId_idx"));

        // Split exactly at the encoded key of the smallest letter-leading value: every digit-leading
        // entry (control-character region) lands in the lower child, every letter-leading entry in
        // the upper child.
        string splitKey = indexSpace + "/" + KeyEncoder.Encode(
            new CompositeColumnValue(new ColumnValue(ColumnType.String, "ABMMLDVAZU")));

        await SplitAtAsync(indexSpace, splitKey);

        foreach (string publicId in DigitLeading.Concat(LetterLeading))
            AssertLookupFinds(publicId, await RunSqlAsync(db, executor,
                $"SELECT * FROM users WHERE publicId = '{publicId}'"));
    }
}
