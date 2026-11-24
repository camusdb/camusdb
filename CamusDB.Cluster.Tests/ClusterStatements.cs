
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Tests.Cluster;

/// <summary>Runs statements on one node of an <see cref="InProcessSchemaCluster"/>, for the fixtures of transactional DDL.</summary>
internal static class ClusterStatements
{
    internal static async Task<KvTransaction> BeginSessionAsync(InProcessSchemaCluster.Node node)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        tx.MarkSessionOwned();
        return tx;
    }

    internal static Task Ddl(InProcessSchemaCluster.Node node, KvTransaction tx, string db, string sql) =>
        node.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));

    internal static Task Write(InProcessSchemaCluster.Node node, KvTransaction tx, string db, string sql) =>
        node.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

    internal static async Task AutocommitAsync(InProcessSchemaCluster.Node node, string db, string sql, bool ddl)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync();
        try
        {
            if (ddl)
                await Ddl(node, tx, db, sql);
            else
                await Write(node, tx, db, sql);

            await node.Database!.Transactions.CommitAsync(tx);
        }
        finally
        {
            await node.Database!.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    internal static async Task<List<Dictionary<string, ColumnValue>>> QueryAsync(
        InProcessSchemaCluster.Node node, string db, string sql, CamusIsolationLevel? isolation = null)
    {
        KvTransaction tx = await node.Database!.Transactions.BeginAsync(isolation);
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

            List<Dictionary<string, ColumnValue>> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(new Dictionary<string, ColumnValue>(row.Row));

            return rows;
        }
        finally
        {
            await node.Database!.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Counts the stored keys under <paramref name="prefix"/> in one key space, with a plain read of the latest committed keys.</summary>
    internal static async Task<int> CountKeysAsync(DatabaseDescriptor database, string keySpace, string prefix)
    {
        int count = 0;

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in database.Kahuna.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, keySpace, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal) && entry.Value is not null)
                count++;
        }

        return count;
    }
}
