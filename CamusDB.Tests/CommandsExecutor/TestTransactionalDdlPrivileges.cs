/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The privileges of the schema statements inside an explicit transaction with
/// <c>transactional_ddl_enabled</c> on. A user who lacks the privilege is refused with the code and the
/// message of autocommit, on committed objects and on objects that the transaction created, and a user who
/// holds exactly the privilege of the statement runs it, also when the statement has a staged form that reads
/// other tables.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestTransactionalDdlPrivileges : SharedNodeBaseTest
{
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        ForeignKeyPrivilegeScenarios.WithAuthentication(defaults) with
        {
            TransactionalDdlEnabled = true,
            InlineTriggersEnabled = true,
            CoordinatorJobSweepIntervalMs = 0,
        };

    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options) =>
        new(new CommandValidator(options), new CatalogsManager(logger), logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: false);

    private static readonly string[] Objects =
    [
        "CREATE TABLE p (id INT64 PRIMARY KEY)",
        "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, name STRING, KEY k (v), CONSTRAINT c CHECK (v > 0))",
        "CREATE VIEW w AS SELECT id FROM t",
    ];

    private static async Task<Principal> Login(CommandExecutor executor, string user, string password) =>
        await executor.ResolvePrincipalAsync((await executor.LoginAsync(user, password)).Token);

    private static Task ServerDdl(CommandExecutor executor, Principal root, string sql) =>
        executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: root));

    private static Task Run(CommandExecutor executor, KvTransaction tx, string db, string sql, Principal principal)
    {
        ExecuteSQLTicket ticket = new(tx, db, sql, null, principal);
        return sql.StartsWith("INSERT", StringComparison.Ordinal) ? executor.ExecuteNonSQLQuery(ticket) : executor.ExecuteDDLSQL(ticket);
    }

    private static async Task Autocommit(CommandExecutor executor, DatabaseDescriptor database, string sql, Principal principal)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await Run(executor, tx, database.Name, sql, principal);
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<KvTransaction> BeginSessionAsync(DatabaseDescriptor database)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        tx.MarkSessionOwned();
        return tx;
    }

    private static CamusDBException Refused(Func<Task> action) => Assert.ThrowsAsync<CamusDBException>(async () => await action())!;

    /// <summary>A database, its superuser, and the user <c>u</c> with <paramref name="grants"/> ({0} is the database).</summary>
    private async Task<(DatabaseDescriptor Database, CommandExecutor Executor, Principal Root, Principal User)> SetupAsync(bool withObjects, params string[] grants)
    {
        CamusDBOptions options = Options;
        CommandExecutor executor = CreateCommandExecutor(options);
        string db = "tp" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, executor);

        await executor.EnsureBootstrapSuperuserAsync(options.BootstrapSuperuser, options.BootstrapSuperuserPassword);
        Principal root = await Login(executor, "root", "root-pw");

        if (withObjects)
        {
            foreach (string sql in Objects)
                await Autocommit(executor, database, sql, root);
        }

        // The tests of the fixture share one node, and users live on the node.
        await ServerDdl(executor, root, "DROP USER IF EXISTS u");
        await ServerDdl(executor, root, "CREATE USER u IDENTIFIED BY 'pw'");

        foreach (string grant in grants)
            await ServerDdl(executor, root, $"GRANT {string.Format(grant, db)} TO u");

        return (database, executor, root, await Login(executor, "u", "pw"));
    }

    [TestCase("DROP TABLE t")]
    [TestCase("DROP TABLE t FORCE")]
    [TestCase("ALTER TABLE t RENAME TO x")]
    [TestCase("ALTER TABLE t RENAME COLUMN name TO x")]
    [TestCase("ALTER TABLE t RENAME INDEX k TO x")]
    [TestCase("ALTER TABLE t ADD COLUMN x INT64")]
    [TestCase("ALTER TABLE t ADD COLUMN x INT64 NOT NULL DEFAULT(1)")]
    [TestCase("ALTER TABLE t ADD COLUMN x INT64 GENERATED ALWAYS AS IDENTITY")]
    [TestCase("ALTER TABLE t DROP COLUMN name")]
    [TestCase("CREATE INDEX k2 ON t (name)")]
    [TestCase("CREATE UNIQUE INDEX k2 ON t (name)")]
    [TestCase("DROP INDEX k ON t")]
    [TestCase("ALTER TABLE t DROP PRIMARY KEY")]
    [TestCase("ALTER TABLE t ADD CONSTRAINT c2 CHECK (v > 1)")]
    [TestCase("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES p (id)")]
    [TestCase("ALTER TABLE t DROP CONSTRAINT c")]
    [TestCase("ALTER TABLE t ALTER COLUMN v SET NOT NULL")]
    [TestCase("ALTER TABLE t ALTER COLUMN v DROP NOT NULL")]
    [TestCase("ALTER TABLE t ALTER COLUMN name SET STORAGE EXTERNAL")]
    [TestCase("ALTER TABLE t SET (ttl_pause = true)")]
    [TestCase("ALTER TABLE t RESET (ttl_pause)")]
    [TestCase("COMMENT ON TABLE t IS 'x'")]
    [TestCase("COMMENT ON COLUMN t.v IS 'x'")]
    [TestCase("COMMENT ON INDEX t.k IS 'x'")]
    [TestCase("CREATE TRIGGER trg ON t BEFORE INSERT LANGUAGE csharp AS $$ return; $$")]
    [TestCase("DROP VIEW w")]
    [TestCase("ALTER VIEW w RENAME TO x")]
    [TestCase("COMMENT ON VIEW w IS 'x'")]
    public async Task AStatementWithoutThePrivilege_IsRefused(string statement)
    {
        string[] grants = ["SELECT ON {0}.*", "INSERT ON {0}.*", "CREATE TABLE ON {0}.*"];

        {
            (DatabaseDescriptor database, CommandExecutor executor, Principal _, Principal user) = await SetupAsync(withObjects: true, grants);

            CamusDBException outside = Refused(() => Autocommit(executor, database, statement, user));
            Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, outside.Code, outside.Message);

            long version = database.Schema.SchemaVersion;
            KvTransaction tx = await BeginSessionAsync(database);
            CamusDBException inside = Refused(() => Run(executor, tx, database.Name, statement, user));
            Assert.AreEqual((outside.Code, outside.Message), (inside.Code, inside.Message), "on committed objects");
            await database.Transactions.RollbackAsync(tx);
            Assert.AreEqual(version, database.Schema.SchemaVersion);
        }

        {
            (DatabaseDescriptor database, CommandExecutor executor, Principal _, Principal user) = await SetupAsync(withObjects: false, grants);

            KvTransaction tx = await BeginSessionAsync(database);
            foreach (string sql in Objects)
                await Run(executor, tx, database.Name, sql, user);
            await Run(executor, tx, database.Name, "INSERT INTO t (id, v) VALUES (1, 1)", user);

            CamusDBException inside = Refused(() => Run(executor, tx, database.Name, statement, user));
            Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, inside.Code, $"on objects of the transaction: {inside.Message}");
            await database.Transactions.RollbackAsync(tx);
        }
    }

    [TestCase("CREATE TABLE x (id INT64 PRIMARY KEY)")]
    [TestCase("CREATE TABLE x AS SELECT id FROM t")]
    [TestCase("CREATE VIEW x AS SELECT id FROM t")]
    [TestCase("CREATE SEQUENCE x")]
    public async Task ACreateWithoutThePrivilegeOnTheDatabase_IsRefused(string statement)
    {
        (DatabaseDescriptor database, CommandExecutor executor, Principal _, Principal user) =
            await SetupAsync(withObjects: true, "SELECT ON {0}.*", "ALTER ON {0}.*");

        CamusDBException outside = Refused(() => Autocommit(executor, database, statement, user));
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, outside.Code, outside.Message);

        // Also when the transaction already has a private schema, opened by a statement the user may run.
        KvTransaction tx = await BeginSessionAsync(database);
        await Run(executor, tx, database.Name, "COMMENT ON TABLE t IS 'x'", user);
        Assert.IsNotNull(tx.SchemaChanges);

        CamusDBException inside = Refused(() => Run(executor, tx, database.Name, statement, user));
        Assert.AreEqual((outside.Code, outside.Message), (inside.Code, inside.Message));
        await database.Transactions.RollbackAsync(tx);
    }

    /// <summary>
    /// Exactly the privilege of the statement is enough, on a committed table with rows. A foreign key needs
    /// ALTER on the child and SELECT on the parent, and no more on the parent.
    /// </summary>
    [TestCase("ALTER TABLE t ADD COLUMN x INT64 NOT NULL DEFAULT(1)", "ALTER ON {0}.t")]
    [TestCase("ALTER TABLE t ADD COLUMN x INT64 GENERATED ALWAYS AS IDENTITY", "ALTER ON {0}.t")]
    [TestCase("ALTER TABLE t ADD CONSTRAINT c2 CHECK (v > 1)", "ALTER ON {0}.t")]
    [TestCase("ALTER TABLE t ALTER COLUMN v SET NOT NULL", "ALTER ON {0}.t")]
    [TestCase("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES p (id)", "ALTER ON {0}.t", "SELECT ON {0}.p")]
    [TestCase("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES p (id)", "ALTER ON {0}.t", "SELECT ON {0}.p")]
    [TestCase("ALTER TABLE t RENAME COLUMN name TO x", "ALTER ON {0}.t")]
    [TestCase("ALTER TABLE t DROP CONSTRAINT c", "ALTER ON {0}.t")]
    [TestCase("CREATE INDEX k2 ON t (name)", "INDEX ON {0}.t")]
    [TestCase("CREATE UNIQUE INDEX k2 ON t (v)", "INDEX ON {0}.t")]
    [TestCase("DROP INDEX k ON t", "INDEX ON {0}.t")]
    [TestCase("DROP TABLE p", "DROP ON {0}.p")]
    [TestCase("CREATE TABLE c (id INT64 PRIMARY KEY, p INT64 REFERENCES p (id))", "CREATE TABLE ON {0}.*", "SELECT ON {0}.p")]
    public async Task AStatementWithItsPrivilege_RunsAndCommits(string statement, params string[] grants)
    {
        string[] shapes = new string[2];

        for (int i = 0; i < shapes.Length; i++)
        {
            (DatabaseDescriptor database, CommandExecutor executor, Principal root, Principal user) = await SetupAsync(withObjects: true, grants);
            await Autocommit(executor, database, "INSERT INTO p (id) VALUES (2), (3)", root);
            await Autocommit(executor, database, "INSERT INTO t (id, v, name) VALUES (2, 2, 'a'), (3, 3, 'b')", root);
            string before = SchemaShape.Of(database);

            if (i == 0)
            {
                await Autocommit(executor, database, statement, user);
            }
            else
            {
                KvTransaction tx = await BeginSessionAsync(database);
                await Run(executor, tx, database.Name, statement, user);
                await database.Transactions.CommitAsync(tx);
            }

            shapes[i] = SchemaShape.Of(database);
            Assert.AreNotEqual(before, shapes[i]);
        }

        Assert.AreEqual(shapes[0], shapes[1], "a transaction leaves the schema that autocommit leaves");
    }

    /// <summary>A foreign key needs SELECT on the parent, also when the transaction created that parent.</summary>
    [Test]
    public async Task AForeignKeyWithoutSelectOnTheParent_IsRefused([Values] bool privateParent, [Values] bool alter)
    {
        (DatabaseDescriptor database, CommandExecutor executor, Principal _, Principal user) =
            await SetupAsync(withObjects: true, "CREATE TABLE ON {0}.*", "ALTER ON {0}.*");

        KvTransaction tx = await BeginSessionAsync(database);
        string parent = privateParent ? "q" : "p";

        if (privateParent)
            await Run(executor, tx, database.Name, "CREATE TABLE q (id INT64 PRIMARY KEY)", user);

        CamusDBException refused = Refused(() => Run(executor, tx, database.Name, alter
            ? $"ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES {parent} (id)"
            : $"CREATE TABLE d (id INT64 PRIMARY KEY, p INT64 REFERENCES {parent} (id))", user));

        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, refused.Code, refused.Message);
        StringAssert.Contains($"Missing Select privilege on table '{database.Name}.{parent}'", refused.Message);
        await database.Transactions.RollbackAsync(tx);
    }

    /// <summary>
    /// A grant is on the table and not on its name. Inside the transaction it follows the table through a
    /// rename, and it does not reach a new table that takes the old name.
    /// </summary>
    [Test]
    public async Task AGrant_FollowsTheTableThroughARenameOfTheTransaction()
    {
        (DatabaseDescriptor database, CommandExecutor executor, Principal _, Principal user) =
            await SetupAsync(withObjects: true, "CREATE TABLE ON {0}.*", "ALTER ON {0}.t");

        KvTransaction tx = await BeginSessionAsync(database);
        await Run(executor, tx, database.Name, "ALTER TABLE t RENAME TO renamed", user);
        await Run(executor, tx, database.Name, "ALTER TABLE renamed ADD COLUMN x INT64", user);
        await Run(executor, tx, database.Name, "CREATE TABLE t (id INT64 PRIMARY KEY)", user);

        CamusDBException refused = Refused(() => Run(executor, tx, database.Name, "ALTER TABLE t ADD COLUMN x INT64", user));
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, refused.Code, refused.Message);
        await database.Transactions.RollbackAsync(tx);

        Assert.IsTrue(database.Schema.Tables.ContainsKey("t"));
        Assert.IsFalse(database.Schema.Tables.ContainsKey("renamed"));
    }
}
