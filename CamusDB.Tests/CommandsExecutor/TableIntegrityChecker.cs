
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
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Detects a table whose primary key no longer identifies its rows. Three counts must agree: the rows a
/// full scan returns, the entries of the primary key index, and the distinct primary key values. A row
/// stored beside a twin that shares its key fails the check, as does an index entry with no row or a row
/// with no entry. The scan and the index are read in one transaction, so a concurrent commit cannot
/// separate them.
/// </summary>
internal static class TableIntegrityChecker
{
    /// <summary>
    /// Asserts that the full scan, the primary key index and the distinct key values of
    /// <paramref name="tableName"/> count the same rows, and that the index names exactly the rows the
    /// scan returns. <paramref name="primaryKeyColumns"/> names the key columns in index order.
    /// </summary>
    public static async Task AssertConsistentAsync(CommandExecutor executor, string dbname, string tableName, params string[] primaryKeyColumns)
    {
        Assert.IsNotEmpty(primaryKeyColumns, "name the primary key columns");

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, tableName));

        TableIndexSchema primaryKey = table.AllIndexes.Values.Single(index =>
            index.Type == IndexType.Unique
            && index.Columns.SequenceEqual(primaryKeyColumns, StringComparer.OrdinalIgnoreCase));

        ColumnType[] keyTypes = new ColumnType[primaryKeyColumns.Length];
        for (int i = 0; i < primaryKeyColumns.Length; i++)
            keyTypes[i] = table.Schema.Columns!.First(c => string.Equals(c.Name, primaryKeyColumns[i], StringComparison.OrdinalIgnoreCase)).Type;

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            // SELECT * needs every column, so the planner reads the rows themselves rather than the key index.
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, dbname, $"SELECT * FROM {tableName}", null));

            HashSet<string> scannedRowIds = [];
            HashSet<string> distinctKeys = [];
            int scannedRows = 0;

            await foreach (QueryResultRow row in cursor)
            {
                scannedRows++;
                scannedRowIds.Add(row.RowId.ToString());
                distinctKeys.Add(string.Join("\u0001", primaryKeyColumns.Select(c => row.Row[c].ToString())));
            }

            HashSet<string> indexedRowIds = [];
            int indexEntries = 0;

            await foreach ((CompositeColumnValue _, ObjectIdValue rowId, _) in table.Store.ScanIndex(
                tx, primaryKey.KvId, keyTypes, from: null, to: null, unique: true))
            {
                indexEntries++;
                indexedRowIds.Add(rowId.ToString());
            }

            Assert.AreEqual(scannedRows, distinctKeys.Count,
                $"{tableName}: {scannedRows} rows but {distinctKeys.Count} distinct primary keys; a key has a twin");
            Assert.AreEqual(scannedRows, indexEntries,
                $"{tableName}: {scannedRows} rows but {indexEntries} primary key index entries");
            Assert.IsTrue(scannedRowIds.SetEquals(indexedRowIds),
                $"{tableName}: the primary key index names other rows than the scan returns");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }
}
