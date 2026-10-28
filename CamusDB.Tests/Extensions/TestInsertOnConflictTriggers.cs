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
/// Triggers and foreign keys on both paths of <c>INSERT … ON CONFLICT</c>: BEFORE INSERT runs for every
/// proposed row before the conflict check, an inserted row fires the INSERT triggers, an updated row
/// fires the UPDATE triggers, DO NOTHING fires no further trigger, and a foreign key is checked on the
/// row each path writes.
/// </summary>
[NonParallelizable]
public sealed class TestInsertOnConflictTriggers : ExtensionTestBase
{
    private const string Fixture = "CamusDB.Fixtures.Good.";

    private static async Task CreateTables(CommandExecutor executor, string db)
    {
        await ExecDdl(executor, db, "CREATE TABLE t (id OID PRIMARY KEY DEFAULT (gen_id()), name STRING NOT NULL, n INT64, UNIQUE KEY t_name (name))");
        await ExecDdl(executor, db, "CREATE TABLE audit_log (id OID PRIMARY KEY DEFAULT (gen_id()), op STRING, timing STRING, tbl STRING, detail STRING)");
    }

    private static async Task<List<string>> Audit(CommandExecutor executor, string db) =>
        (await ExecQuery(executor, db, "SELECT op, timing, detail FROM audit_log"))
            .Select(r => $"{r["op"].StrValue}:{r["timing"].StrValue}:{r["detail"].StrValue}")
            .OrderBy(s => s, System.StringComparer.Ordinal)
            .ToList();

    [Test]
    public async Task BeforeInsertTriggerChangesTheKeyTheConflictCheckUses()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecNonQuery(executor, db, "INSERT INTO t (name, n) VALUES ('ALPHA', 1)");
        await ExecDdl(executor, db, $"CREATE TRIGGER up ON t BEFORE INSERT EXTERNAL NAME '{Fixture}UppercaseTrigger'");

        int changed = await ExecNonQuery(executor, db,
            "INSERT INTO t (name, n) VALUES ('alpha', 5) ON CONFLICT (name) DO UPDATE SET n = t.n + excluded.n");

        Assert.AreEqual(1, changed);
        List<Dictionary<string, ColumnValue>> rows = await ExecQuery(executor, db, "SELECT name, n FROM t");
        Assert.AreEqual(1, rows.Count, "the trigger's key found the stored row, so nothing was inserted");
        Assert.AreEqual("ALPHA", rows[0]["name"].StrValue);
        Assert.AreEqual(6, rows[0]["n"].LongValue);
    }

    [Test]
    public async Task InsertedRowsFireInsertTriggersAndUpdatedRowsFireUpdateTriggers()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecNonQuery(executor, db, "INSERT INTO t (name, n) VALUES ('old', 1)");
        await ExecDdl(executor, db, $"CREATE TRIGGER a ON t AFTER INSERT, UPDATE EXTERNAL NAME '{Fixture}AuditTrigger'");
        await ExecDdl(executor, db, $"CREATE TRIGGER b ON t BEFORE INSERT, UPDATE EXTERNAL NAME '{Fixture}AuditTrigger'");

        int changed = await ExecNonQuery(executor, db,
            "INSERT INTO t (name, n) VALUES ('old', 2), ('new', 3) ON CONFLICT (name) DO UPDATE SET n = excluded.n");

        Assert.AreEqual(2, changed);

        // BEFORE INSERT runs for both proposed rows; the conflicting one then runs as an UPDATE.
        CollectionAssert.AreEqual(
            new[] { "Insert:After:new", "Insert:Before:new", "Insert:Before:old", "Update:After:old", "Update:Before:old" },
            await Audit(executor, db));
    }

    [Test]
    public async Task DoNothingFiresNoTriggerBeyondBeforeInsert()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await CreateTables(executor, db);
        await ExecNonQuery(executor, db, "INSERT INTO t (name, n) VALUES ('old', 1)");
        await ExecDdl(executor, db, $"CREATE TRIGGER a ON t AFTER INSERT, UPDATE EXTERNAL NAME '{Fixture}AuditTrigger'");
        await ExecDdl(executor, db, $"CREATE TRIGGER b ON t BEFORE INSERT, UPDATE EXTERNAL NAME '{Fixture}AuditTrigger'");

        int changed = await ExecNonQuery(executor, db, "INSERT INTO t (name, n) VALUES ('old', 2) ON CONFLICT DO NOTHING");

        Assert.AreEqual(0, changed);
        CollectionAssert.AreEqual(new[] { "Insert:Before:old" }, await Audit(executor, db));
    }

    [Test]
    public async Task ForeignKeyIsCheckedOnTheUpdatePathAndTheInsertPath()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithGoodFixture();
        await ExecDdl(executor, db, "CREATE TABLE parent (id INT64 PRIMARY KEY NOT NULL)");
        await ExecDdl(executor, db, "CREATE TABLE child (id INT64 PRIMARY KEY NOT NULL, parent_id INT64 REFERENCES parent (id))");
        await ExecNonQuery(executor, db, "INSERT INTO parent (id) VALUES (1), (2)");
        await ExecNonQuery(executor, db, "INSERT INTO child (id, parent_id) VALUES (10, 1)");

        // The update path moves the child to an existing parent, then to a missing one.
        await ExecNonQuery(executor, db, "INSERT INTO child (id, parent_id) VALUES (10, 2) ON CONFLICT (id) DO UPDATE SET parent_id = excluded.parent_id");
        Assert.AreEqual(2, (await Scalar(executor, db, "SELECT parent_id FROM child WHERE id = 10")).LongValue);

        CamusDBException update = Assert.ThrowsAsync<CamusDBException>(() => ExecNonQuery(executor, db,
            "INSERT INTO child (id, parent_id) VALUES (10, 99) ON CONFLICT (id) DO UPDATE SET parent_id = excluded.parent_id"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, update.Code);

        CamusDBException insert = Assert.ThrowsAsync<CamusDBException>(() => ExecNonQuery(executor, db,
            "INSERT INTO child (id, parent_id) VALUES (11, 99) ON CONFLICT (id) DO UPDATE SET parent_id = excluded.parent_id"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, insert.Code);

        Assert.AreEqual(2, (await Scalar(executor, db, "SELECT parent_id FROM child WHERE id = 10")).LongValue);
        Assert.AreEqual(1, (await ExecQuery(executor, db, "SELECT id FROM child")).Count);
    }
}
