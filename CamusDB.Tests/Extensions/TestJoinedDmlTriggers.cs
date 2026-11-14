/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// Triggers on the target of <c>UPDATE … FROM</c> and <c>DELETE … USING</c>: each changed row fires
/// the row triggers once, a BEFORE UPDATE trigger can change the new row, a BEFORE trigger that throws
/// fails the statement, and a row that the statement does not change fires nothing.
/// </summary>
[NonParallelizable]
public sealed class TestJoinedDmlTriggers : ExtensionTestBase
{
    private const string Fixture = "CamusDB.Fixtures.Good.";

    private static async Task CreateTables(CommandExecutor executor, string db)
    {
        await ExecDdl(executor, db, "CREATE TABLE t (id INT64 PRIMARY KEY, name STRING NOT NULL, n INT64)");
        await ExecDdl(executor, db, "CREATE TABLE src (sid INT64 PRIMARY KEY, tid INT64, name STRING, n INT64)");
        await ExecDdl(executor, db, "CREATE TABLE audit_log (id OID PRIMARY KEY DEFAULT (gen_id()), op STRING, timing STRING, tbl STRING, detail STRING)");
        await ExecNonQuery(executor, db, "INSERT INTO t (id, name, n) VALUES (1, 'one', 1), (2, 'two', 2), (3, 'three', 3)");
        await ExecNonQuery(executor, db, "INSERT INTO src (sid, tid, name, n) VALUES (10, 1, 'uno', 100), (11, 3, 'bad', 300)");
    }

    private static async Task<List<string>> Audit(CommandExecutor executor, string db) =>
        (await ExecQuery(executor, db, "SELECT op, timing, detail FROM audit_log"))
            .Select(r => $"{r["op"].StrValue}:{r["timing"].StrValue}:{r["detail"].StrValue}")
            .OrderBy(s => s, System.StringComparer.Ordinal)
            .ToList();

    [Test]
    public async Task UpdateFromFiresTheUpdateTriggersOfEachChangedRow()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER a ON t AFTER UPDATE EXTERNAL NAME '{Fixture}AuditTrigger'");
        await ExecDdl(executor, db, $"CREATE TRIGGER b ON t BEFORE UPDATE EXTERNAL NAME '{Fixture}AuditTrigger'");

        int changed = await ExecNonQuery(executor, db, "UPDATE t SET n = t.n + s.n FROM src s WHERE s.tid = t.id AND s.sid = 10");

        Assert.AreEqual(1, changed);
        CollectionAssert.AreEqual(new[] { "Update:After:one", "Update:Before:one" }, await Audit(executor, db));
    }

    [Test]
    public async Task ABeforeUpdateTriggerChangesTheRowThatUpdateFromWrites()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER up ON t BEFORE UPDATE EXTERNAL NAME '{Fixture}UppercaseTrigger'");

        await ExecNonQuery(executor, db, "UPDATE t SET name = s.name FROM src s WHERE s.tid = t.id AND s.sid = 10");

        List<Dictionary<string, ColumnValue>> rows = await ExecQuery(executor, db, "SELECT name FROM t WHERE id = 1");
        Assert.AreEqual("UNO", rows[0]["name"].StrValue);
    }

    [Test]
    public async Task ABeforeTriggerThatThrowsFailsUpdateFromAndChangesNothing()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER g ON t BEFORE UPDATE EXTERNAL NAME '{Fixture}GuardTrigger'");

        Assert.ThrowsAsync<CamusDBException>(() => ExecNonQuery(executor, db,
            "UPDATE t SET name = s.name FROM src s WHERE s.tid = t.id"));

        List<Dictionary<string, ColumnValue>> rows = await ExecQuery(executor, db, "SELECT name FROM t ORDER BY id");
        CollectionAssert.AreEqual(new[] { "one", "two", "three" }, rows.Select(r => r["name"].StrValue).ToArray());
    }

    [Test]
    public async Task DeleteUsingFiresTheDeleteTriggersOfEachDeletedRow()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecDdl(executor, db, $"CREATE TRIGGER a ON t AFTER DELETE EXTERNAL NAME '{Fixture}AuditTrigger'");
        await ExecDdl(executor, db, $"CREATE TRIGGER b ON t BEFORE DELETE EXTERNAL NAME '{Fixture}AuditTrigger'");

        int deleted = await ExecNonQuery(executor, db, "DELETE FROM t USING src s WHERE s.tid = t.id");

        Assert.AreEqual(2, deleted);
        CollectionAssert.AreEqual(
            new[] { "Delete:After:one", "Delete:After:three", "Delete:Before:one", "Delete:Before:three" },
            await Audit(executor, db));
    }
}
