/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An INSERT that leaves out a nullable column stores NULL in it. A non-unique index over that column
/// must index the row under NULL, as it does for an explicit NULL, rather than fail the statement.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestInsertOmittedIndexedColumn : BaseTest
{
    [Test]
    public async Task OmittedColumnOfANonUniqueIndexIsIndexedAsNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Exec(executor, database, dbname, "CREATE TABLE notes (id int64 PRIMARY KEY NOT NULL, tag string, KEY notes_tag (tag))", ddl: true);
        await Exec(executor, database, dbname, "INSERT INTO notes (id) VALUES (1)", ddl: false);
        await Exec(executor, database, dbname, "INSERT INTO notes (id, tag) VALUES (2, NULL), (3, 'a')", ddl: false);

        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> rows) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname, "SELECT id FROM notes WHERE tag IS NULL", null));

        int count = 0;
        await foreach (QueryResultRow _ in rows)
            count++;
        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(2, count, "The omitted column and the explicit NULL are both NULL");
    }

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql, bool ddl)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        if (ddl)
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        else
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }
}
