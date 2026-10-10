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

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// SQL helpers shared by the UPDATE … RETURNING and DELETE … RETURNING engine tests. Every helper
/// drives real SQL through <see cref="CommandExecutor"/>; a read-back helper runs a separate SELECT in
/// its own transaction, so a test compares a returned value with the value that was stored.
/// </summary>
internal static class WriteReturningTestSql
{
    public static async Task Ddl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    public static Task<ExecuteNonSQLResult> NonQuery(
        CommandExecutor executor, string dbname, KvTransaction tx, string sql,
        Dictionary<string, ColumnValue>? parameters = null, bool discardReturningRows = false)
        => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            tx, dbname, sql, parameters, discardReturningRows: discardReturningRows));

    /// <summary>Runs one no-rows statement in its own transaction and commits it.</summary>
    public static async Task<ExecuteNonSQLResult> NonQueryCommitted(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null, bool discardReturningRows = false)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await NonQuery(executor, dbname, tx, sql, parameters, discardReturningRows);
            await database.Transactions.CommitAsync(tx);
            return result;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Runs a statement on the row-returning entry point in its own transaction and commits it.</summary>
    public static async Task<(IReadOnlyList<DerivedColumnSchema> Schema, List<QueryResultRow> Rows)> QueryCommitted(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            QuerySchemaHolder schema = new();
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, dbname, sql, parameters), schemaOut: schema);

            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return (schema.Schema, rows);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>The rows of a committed SELECT.</summary>
    public static async Task<List<QueryResultRow>> Select(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
        => (await QueryCommitted(executor, database, dbname, sql)).Rows;

    /// <summary>The cell of <paramref name="column"/> in <paramref name="row"/>, by the column's row key.</summary>
    public static ColumnValue Cell(QueryResultRow row, DerivedColumnSchema column) => row.Row[column.RowKey];

    public static ColumnValue Cell(ExecuteNonSQLResult result, int row, int column)
        => Cell(result.ReturningRows![row], result.ReturningColumns![column]);

    /// <summary>The cell named <paramref name="columnName"/> in returned row <paramref name="row"/>.</summary>
    public static ColumnValue Cell(ExecuteNonSQLResult result, int row, string columnName)
    {
        DerivedColumnSchema column = result.ReturningColumns!.First(c => string.Equals(c.Name, columnName, StringComparison.Ordinal));
        return Cell(result.ReturningRows![row], column);
    }

    public static string[] ColumnNames(IReadOnlyList<DerivedColumnSchema> columns) => columns.Select(c => c.Name).ToArray();

    /// <summary>The string cells of <paramref name="columnName"/> over every returned row, sorted.</summary>
    public static string[] SortedStrings(ExecuteNonSQLResult result, string columnName)
    {
        DerivedColumnSchema column = result.ReturningColumns!.First(c => c.Name == columnName);
        return result.ReturningRows!.Select(r => Cell(r, column).StrValue!).OrderBy(s => s, StringComparer.Ordinal).ToArray();
    }

    private static readonly Random Rng = new(9137);

    /// <summary>
    /// A string that does not compress, so a column with external storage keeps it out of line instead
    /// of compressing it into the row.
    /// </summary>
    public static string Incompressible(int length)
    {
        const string alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        char[] chars = new char[length];
        lock (Rng)
        {
            for (int i = 0; i < length; i++)
                chars[i] = alphabet[Rng.Next(alphabet.Length)];
        }
        return new string(chars);
    }

    /// <summary>Bytes that do not compress; see <see cref="Incompressible"/>.</summary>
    public static byte[] IncompressibleBytes(int length)
    {
        byte[] bytes = new byte[length];
        lock (Rng)
            Rng.NextBytes(bytes);
        return bytes;
    }
}
