/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The refusals of the table statements inside an explicit transaction with <c>transactional_ddl_enabled</c>
/// on. A statement that is refused outside a transaction is refused inside one, with the same code and for
/// the same reason, in two situations: what refuses it exists only in the private schema of the transaction,
/// and what refuses it is committed. In the other direction, a statement is accepted once the transaction
/// itself removed what refused it.
/// </summary>
internal abstract class TransactionalDdlTableRefusalScenarios : SharedNodeBaseTest
{
    protected abstract bool ClusterMode { get; }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        TransactionalDdlEnabled = true,
        InlineFunctionsEnabled = true,
        // A sweep would remove what a rollback must remove by itself.
        CoordinatorJobSweepIntervalMs = 0,
    };

    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options) =>
        new(new CommandValidator(options), new CatalogsManager(logger), logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: ClusterMode);

    // ── helpers ───────────────────────────────────────────────────────────────

    private sealed record Refusal(string Code, string Message);

    private static async Task<KvTransaction> BeginSessionAsync(DatabaseDescriptor database)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        tx.MarkSessionOwned();
        return tx;
    }

    private static async Task Run(CommandExecutor executor, KvTransaction tx, string db, string sql)
    {
        ExecuteSQLTicket ticket = new(tx, db, sql, null);

        if (sql.StartsWith("INSERT", StringComparison.Ordinal))
            await executor.ExecuteNonSQLQuery(ticket);
        else
            await executor.ExecuteDDLSQL(ticket);
    }

    private static async Task Autocommit(CommandExecutor executor, DatabaseDescriptor database, string db, string statements)
    {
        foreach (string sql in Statements(statements))
        {
            KvTransaction tx = await database.Transactions.BeginAsync();
            try
            {
                await Run(executor, tx, db, sql);
                await database.Transactions.CommitAsync(tx);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
            }
        }
    }

    private static async Task RunAll(CommandExecutor executor, KvTransaction tx, string db, string statements)
    {
        foreach (string sql in Statements(statements))
            await Run(executor, tx, db, sql);
    }

    /// <summary>The statements of <paramref name="text"/>. A body between dollar quotes has semicolons of its own.</summary>
    private static IEnumerable<string> Statements(string text)
    {
        int start = 0;
        bool body = false;

        for (int i = 0; i <= text.Length; i++)
        {
            if (i + 1 < text.Length && text[i] == '$' && text[i + 1] == '$')
            {
                body = !body;
                i++;
            }
            else if (i == text.Length || (text[i] == ';' && !body))
            {
                string sql = text[start..Math.Min(i, text.Length)].Trim();
                start = i + 1;

                if (sql.Length > 0)
                    yield return sql;
            }
        }
    }

    private static async Task<Refusal> RefusalOf(Func<Task> action, string what)
    {
        try
        {
            await action();
        }
        catch (CamusDBException refused)
        {
            return new Refusal(refused.Code, refused.Message);
        }

        Assert.Fail($"{what}: the statement was accepted");
        throw new InvalidOperationException();
    }

    private static void AssertRefusal(Refusal refusal, string code, string reason, string where)
    {
        Assert.AreEqual(code, refusal.Code, $"{where}: {refusal.Message}");
        StringAssert.Contains(reason, refusal.Message, where);
    }

    private static async Task<List<Dictionary<string, ColumnValue>>> Rows(CommandExecutor executor, DatabaseDescriptor database, string db, string sql, KvTransaction? transaction = null)
    {
        KvTransaction tx = transaction ?? await database.Transactions.BeginAsync();
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));

            List<Dictionary<string, ColumnValue>> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(new Dictionary<string, ColumnValue>(row.Row));

            return rows;
        }
        finally
        {
            if (transaction is null)
                await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<int> CountKeysAsync(DatabaseDescriptor database, string keySpace, string prefix)
    {
        int count = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in database.Kahuna.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, keySpace, null, true, null, true, 256, HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (entry.Value is not null && key.StartsWith(prefix, StringComparison.Ordinal))
                count++;
        }

        return count;
    }

    private static Task<int> CountJobRecordsAsync(DatabaseDescriptor database) =>
        CountKeysAsync(database, MetaKeys.MetaBucketPrefix(database.Id), MetaKeys.CoordinatorKeyPrefix(database.Id));

    private static async Task AssertUnchangedAsync(DatabaseDescriptor database, string shapeBefore)
    {
        Assert.AreEqual(shapeBefore, SchemaShape.Of(database), "the schema is as before the transaction");
        Assert.IsNull(database.Schema.PendingChangeSet);
        Assert.AreEqual(0, await CountJobRecordsAsync(database), "no job record is left");
    }

    private async Task<(string Name, DatabaseDescriptor Database, CommandExecutor Executor)> CreateNamedDatabaseAsync()
    {
        // BRANCH FROM takes an identifier, and a generated name can start with a digit.
        CommandExecutor executor = CreateCommandExecutor();
        string name = "tr" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(name: name, ifNotExists: false));
        TrackDatabase(name, executor);
        return (name, database, executor);
    }

    private static Task<ExecuteDDLSQLResult> BranchAsync(CommandExecutor executor, string source, string branch) =>
        executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: source, sql: $"CREATE DATABASE {branch} BRANCH FROM {source}", parameters: null));

    // ── the same refusal outside, inside on private objects, inside on committed objects ──────────

    private const string T = "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, name STRING)";

    private const string U = "CREATE TABLE u (id INT64 PRIMARY KEY)";

    private const string Indexed = "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, w INT64, KEY k (v), KEY k2 (v, id))";

    private const string Viewed = T + "; CREATE VIEW w AS SELECT id, v FROM t";

    private const string P = "CREATE TABLE p (id INT64 PRIMARY KEY, code INT64, e DATETIME, UNIQUE KEY uk (code))";

    private const string Child = P + "; CREATE TABLE c (id INT64 PRIMARY KEY, p INT64, KEY kp (p), CONSTRAINT fk FOREIGN KEY (p) REFERENCES p (id))";

    private const string ChildByCode = P + "; CREATE TABLE c (id INT64 PRIMARY KEY, p INT64, CONSTRAINT fk FOREIGN KEY (p) REFERENCES p (code))";

    private const string Ttl = "CREATE TABLE p (id INT64 PRIMARY KEY, e DATETIME); ALTER TABLE p SET (ttl_expiration_expression = 'e')";

    private const string Identity = "CREATE TABLE i (id INT64 PRIMARY KEY, n INT64 GENERATED ALWAYS AS IDENTITY)";

    private const string F = "CREATE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return x + 1; $$";

    private const string NotATable = "is a view and this statement cannot target one";

    // CREATE TABLE
    [TestCase(T, "CREATE TABLE t (id INT64 PRIMARY KEY)", CamusDBErrorCodes.TableAlreadyExists, "Table 't' already exists")]
    [TestCase(T, "CREATE TABLE T (id INT64 PRIMARY KEY)", CamusDBErrorCodes.TableAlreadyExists, "already exists")]
    [TestCase(Viewed, "CREATE TABLE w (id INT64 PRIMARY KEY)", CamusDBErrorCodes.ViewAlreadyExists, "Relation 'w' already exists")]
    [TestCase("", "CREATE TABLE t (id INT64)", CamusDBErrorCodes.InvalidInput, "A primary key column is mandatory")]
    [TestCase("", "CREATE TABLE t (id INT64 PRIMARY KEY, p INT64 REFERENCES missing (id))", CamusDBErrorCodes.TableDoesntExist, "references table 'missing', which does not exist")]
    [TestCase(Viewed, "CREATE TABLE c (id INT64 PRIMARY KEY, p INT64 REFERENCES w (id))", CamusDBErrorCodes.InvalidForeignKeyDefinition, "it is a view")]
    [TestCase(T, "CREATE TABLE c (id INT64 PRIMARY KEY, p INT64 REFERENCES t (v))", CamusDBErrorCodes.InvalidForeignKeyDefinition, "there is no unique index or primary key on 't'")]
    [TestCase(T, "CREATE TABLE c (id INT64 PRIMARY KEY, p STRING REFERENCES t (id))", CamusDBErrorCodes.InvalidForeignKeyDefinition, "column 'p' is String")]
    [TestCase(T, "CREATE TABLE c (id INT64 PRIMARY KEY, p INT64 REFERENCES t (missing))", CamusDBErrorCodes.InvalidForeignKeyDefinition, "referenced column 't.missing' does not exist")]
    [TestCase(Ttl, "CREATE TABLE c (id INT64 PRIMARY KEY, p INT64 REFERENCES p (id))", CamusDBErrorCodes.FeatureNotSupported, "it has row-level TTL")]
    [TestCase("", "CREATE TABLE t (id INT64 PRIMARY KEY, n INT64 DEFAULT(nextval('missing')))", CamusDBErrorCodes.SequenceDoesntExist, "sequence 'missing', which does not exist")]
    [TestCase(Identity, "CREATE TABLE t (id INT64 PRIMARY KEY, n INT64 DEFAULT(nextval('i_n_seq')))", CamusDBErrorCodes.SequenceInUse, "owned by an identity column")]
    [TestCase("", "CREATE TABLE t (id INT64 PRIMARY KEY, CONSTRAINT c CHECK (missing > 0))", CamusDBErrorCodes.InvalidInput, "unknown column 'missing'")]
    [TestCase("", "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, CONSTRAINT c CHECK (v > 0), CONSTRAINT c CHECK (v > 1))", CamusDBErrorCodes.InvalidInput, "Duplicate constraint name 'c'")]
    [TestCase("", "CREATE TABLE t (id INT64 PRIMARY KEY, tags array(INT64), KEY k (tags))", CamusDBErrorCodes.InvalidInput, "cannot be used in an index")]
    // DROP TABLE
    [TestCase("", "DROP TABLE t", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase(Viewed, "DROP TABLE w", CamusDBErrorCodes.ViewNotUpdatable, NotATable)]
    [TestCase(Viewed, "DROP TABLE t", CamusDBErrorCodes.DependentObjectsExist, "other objects depend on it: w")]
    [TestCase(Viewed, "DROP TABLE t FORCE", CamusDBErrorCodes.DependentObjectsExist, "other objects depend on it: w")]
    [TestCase(Child, "DROP TABLE p", CamusDBErrorCodes.DependentObjectsExist, "foreign key 'fk' on table 'c' references it")]
    [TestCase(Child, "DROP TABLE p FORCE", CamusDBErrorCodes.DependentObjectsExist, "foreign key 'fk' on table 'c' references it")]
    [TestCase("CREATE SEQUENCE s", "DROP TABLE s", CamusDBErrorCodes.TableDoesntExist, "Table 's' doesn't exist")]
    // RENAME TABLE
    [TestCase(T + ";" + U, "ALTER TABLE t RENAME TO u", CamusDBErrorCodes.TableAlreadyExists, "Table 'u' already exists")]
    [TestCase(T, "ALTER TABLE t RENAME TO t", CamusDBErrorCodes.InvalidInput, "is the same as the current name")]
    [TestCase("", "ALTER TABLE t RENAME TO u", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase(Viewed, "ALTER TABLE w RENAME TO x", CamusDBErrorCodes.ViewNotUpdatable, NotATable)]
    // RENAME COLUMN
    [TestCase("", "ALTER TABLE t RENAME COLUMN a TO b", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase(T, "ALTER TABLE t RENAME COLUMN missing TO b", CamusDBErrorCodes.UnknownColumn, "Column 'missing' does not exist")]
    [TestCase(T, "ALTER TABLE t RENAME COLUMN v TO id", CamusDBErrorCodes.DuplicateColumn, "Column 'id' already exists")]
    [TestCase(T, "ALTER TABLE t RENAME COLUMN v TO v", CamusDBErrorCodes.InvalidInput, "is the same as the current name")]
    [TestCase(T, "ALTER TABLE t RENAME COLUMN v TO _id", CamusDBErrorCodes.InvalidInput, "reserved column name")]
    [TestCase(Viewed, "ALTER TABLE w RENAME COLUMN v TO x", CamusDBErrorCodes.ViewNotUpdatable, NotATable)]
    // RENAME INDEX
    [TestCase("", "ALTER TABLE t RENAME INDEX a TO b", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase(Indexed, "ALTER TABLE t RENAME INDEX missing TO b", CamusDBErrorCodes.InvalidInput, "Index 'missing' does not exist")]
    [TestCase(Indexed, "ALTER TABLE t RENAME INDEX k TO k2", CamusDBErrorCodes.InvalidInput, "Index 'k2' already exists")]
    [TestCase(Indexed, "ALTER TABLE t RENAME INDEX k TO k", CamusDBErrorCodes.InvalidInput, "is the same as the current name")]
    // ADD COLUMN
    [TestCase("", "ALTER TABLE t ADD COLUMN x INT64", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase(T, "ALTER TABLE t ADD COLUMN v INT64", CamusDBErrorCodes.InvalidInput, "Duplicate column 'v'")]
    [TestCase(T, "ALTER TABLE t ADD COLUMN V STRING DEFAULT('a')", CamusDBErrorCodes.InvalidInput, "Duplicate column 'V'")]
    [TestCase(T, "ALTER TABLE t ADD COLUMN v INT64 NOT NULL", CamusDBErrorCodes.InvalidInput, "Duplicate column 'v'")]
    [TestCase(T + "; INSERT INTO t (id) VALUES (1)", "ALTER TABLE t ADD COLUMN x INT64 NOT NULL", CamusDBErrorCodes.NotNullViolation, "contains null values")]
    [TestCase(T, "ALTER TABLE t ADD COLUMN n INT64 DEFAULT(nextval('missing'))", CamusDBErrorCodes.SequenceDoesntExist, "sequence 'missing', which does not exist")]
    [TestCase(T + ";" + Identity, "ALTER TABLE t ADD COLUMN n INT64 DEFAULT(nextval('i_n_seq'))", CamusDBErrorCodes.SequenceInUse, "owned by an identity column")]
    [TestCase(T, "ALTER TABLE t ADD COLUMN x INT64 PRIMARY KEY", CamusDBErrorCodes.FeatureNotSupported, "cannot declare it as PRIMARY KEY")]
    [TestCase(T, "ALTER TABLE t ADD COLUMN x INT64 UNIQUE", CamusDBErrorCodes.FeatureNotSupported, "cannot declare it UNIQUE")]
    [TestCase(Viewed, "ALTER TABLE w ADD COLUMN x INT64", CamusDBErrorCodes.ViewNotUpdatable, NotATable)]
    [TestCase("CREATE TABLE t (id INT64 PRIMARY KEY, v INT64 CONSTRAINT nn NOT NULL)", "ALTER TABLE t ADD COLUMN x INT64 CONSTRAINT nn NOT NULL DEFAULT(1)", CamusDBErrorCodes.InvalidInput, "Constraint 'nn' already exists")]
    // DROP COLUMN
    [TestCase("", "ALTER TABLE t DROP COLUMN v", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase(T, "ALTER TABLE t DROP COLUMN missing", CamusDBErrorCodes.InvalidInput, "Column missing does not exist")]
    [TestCase(T, "ALTER TABLE t DROP COLUMN id", CamusDBErrorCodes.InvalidInput, "it is part of an index")]
    [TestCase(Indexed, "ALTER TABLE t DROP COLUMN v", CamusDBErrorCodes.InvalidInput, "it is part of an index")]
    [TestCase(Indexed + "; CREATE INDEX k3 ON t (id) INCLUDE (w)", "ALTER TABLE t DROP COLUMN w", CamusDBErrorCodes.InvalidInput, "INCLUDE column of index 'k3'")]
    [TestCase(T + "; CREATE INDEX k ON t (lower(name))", "ALTER TABLE t DROP COLUMN name", CamusDBErrorCodes.InvalidInput, "the expression index 'k' reads it")]
    [TestCase(Viewed, "ALTER TABLE t DROP COLUMN v", CamusDBErrorCodes.DependentObjectsExist, "other objects depend on it: w")]
    [TestCase(Viewed, "ALTER TABLE w DROP COLUMN v", CamusDBErrorCodes.ViewNotUpdatable, NotATable)]
    [TestCase(Child, "ALTER TABLE c DROP COLUMN p", CamusDBErrorCodes.DependentObjectsExist, "foreign key 'fk' uses it")]
    [TestCase(ChildByCode, "ALTER TABLE p DROP COLUMN code", CamusDBErrorCodes.DependentObjectsExist, "foreign key 'fk' on table 'c' references it")]
    [TestCase(Ttl, "ALTER TABLE p DROP COLUMN e", CamusDBErrorCodes.InvalidInput, "row-level TTL expiration column")]
    // CREATE INDEX, ADD UNIQUE, ADD PRIMARY KEY
    [TestCase("", "CREATE INDEX k ON t (v)", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase(T, "CREATE INDEX k ON t (missing)", CamusDBErrorCodes.InvalidInput, "Column 'missing' does not exist")]
    [TestCase(Indexed, "CREATE INDEX k ON t (id)", CamusDBErrorCodes.InvalidInput, "Index 'k' already exists")]
    [TestCase(Indexed, "CREATE UNIQUE INDEX K ON t (id)", CamusDBErrorCodes.InvalidInput, "Index 'K' already exists")]
    [TestCase(T + "; INSERT INTO t (id, v) VALUES (1, 1), (2, 1)", "CREATE UNIQUE INDEX k ON t (v)", CamusDBErrorCodes.DuplicateUniqueKeyValue, "t.k")]
    [TestCase(T, "CREATE INDEX k ON t USING GIN (v)", CamusDBErrorCodes.InvalidInput, "A GIN index needs a jsonb column")]
    [TestCase(T, "CREATE INDEX k ON t (v) INCLUDE (missing)", CamusDBErrorCodes.InvalidInput, "INCLUDE column 'missing' does not exist")]
    [TestCase(T, "CREATE INDEX k ON t (v) INCLUDE (name, name)", CamusDBErrorCodes.InvalidInput, "Duplicate INCLUDE column 'name'")]
    [TestCase(T, "CREATE INDEX k ON t (v) INCLUDE (v)", CamusDBErrorCodes.InvalidInput, "already indexed as a key column")]
    [TestCase(Viewed, "CREATE INDEX k ON w (id)", CamusDBErrorCodes.ViewNotUpdatable, NotATable)]
    [TestCase(T, "CREATE INDEX k ON t (lower(missing))", CamusDBErrorCodes.InvalidIndexExpression, "column 'missing' does not exist")]
    [TestCase(T, "CREATE INDEX k ON t (random())", CamusDBErrorCodes.InvalidIndexExpression, "its result can change")]
    [TestCase(T + ";" + F, "CREATE INDEX k ON t (f(v))", CamusDBErrorCodes.InvalidIndexExpression, "only immutable built-in functions can")]
    [TestCase(T, "ALTER TABLE t ADD PRIMARY KEY (v)", CamusDBErrorCodes.InvalidInput, "Primary key already exists")]
    [TestCase(T, "ALTER TABLE t ADD UNIQUE INDEX k (missing)", CamusDBErrorCodes.InvalidInput, "Column 'missing' does not exist")]
    // DROP INDEX, DROP PRIMARY KEY
    [TestCase("", "DROP INDEX k ON t", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase(T, "DROP INDEX k ON t", CamusDBErrorCodes.InvalidInput, "Index 'k' does not exist")]
    [TestCase(Child, "DROP INDEX kp ON c", CamusDBErrorCodes.DependentObjectsExist, "foreign key 'fk' uses it")]
    [TestCase(ChildByCode, "DROP INDEX uk ON p", CamusDBErrorCodes.DependentObjectsExist, "foreign key 'fk' on table 'c' references it")]
    [TestCase(Child, "ALTER TABLE p DROP PRIMARY KEY", CamusDBErrorCodes.DependentObjectsExist, "foreign key 'fk' on table 'c' references it")]
    // ADD CHECK
    [TestCase("", "ALTER TABLE t ADD CONSTRAINT c CHECK (v > 0)", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase("CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, CONSTRAINT c CHECK (v > 0))", "ALTER TABLE t ADD CONSTRAINT c CHECK (v > 1)", CamusDBErrorCodes.InvalidInput, "Constraint 'c' already exists")]
    [TestCase("CREATE TABLE t (id INT64 PRIMARY KEY, v INT64 CONSTRAINT c NOT NULL)", "ALTER TABLE t ADD CONSTRAINT c CHECK (v > 1)", CamusDBErrorCodes.InvalidInput, "Constraint 'c' already exists")]
    [TestCase(Child, "ALTER TABLE c ADD CONSTRAINT fk CHECK (p > 1)", CamusDBErrorCodes.InvalidInput, "Constraint 'fk' already exists")]
    [TestCase(T, "ALTER TABLE t ADD CONSTRAINT c CHECK (missing > 1)", CamusDBErrorCodes.InvalidInput, "unknown column 'missing'")]
    [TestCase(T, "ALTER TABLE t ADD CONSTRAINT c CHECK (v > (SELECT 1))", CamusDBErrorCodes.InvalidInput, "cannot contain subqueries")]
    [TestCase(T, "ALTER TABLE t ADD CONSTRAINT c CHECK (sum(v) > 1)", CamusDBErrorCodes.InvalidInput, "cannot contain aggregate functions")]
    [TestCase(T, "ALTER TABLE t ADD CONSTRAINT c CHECK (random() > 0.5)", CamusDBErrorCodes.InvalidInput, "volatile")]
    [TestCase(T, "ALTER TABLE t ADD CONSTRAINT c CHECK (nosuch(v) > 1)", CamusDBErrorCodes.InvalidAstStmt, "Function not found 'nosuch'")]
    [TestCase(T, "ALTER TABLE t ADD CONSTRAINT c CHECK (v > 'a')", CamusDBErrorCodes.InvalidInput, "No matching signature for operator")]
    [TestCase(T + "; INSERT INTO t (id, v) VALUES (1, 0)", "ALTER TABLE t ADD CONSTRAINT c CHECK (v > 0)", CamusDBErrorCodes.CheckConstraintViolation, "existing row violates check constraint")]
    // ADD FOREIGN KEY
    [TestCase(T, "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (v) REFERENCES missing (id)", CamusDBErrorCodes.TableDoesntExist, "references table 'missing', which does not exist")]
    [TestCase(T + ";" + P, "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (missing) REFERENCES p (id)", CamusDBErrorCodes.UnknownColumn, "Column 'missing' named in a foreign key")]
    [TestCase(T + ";" + P, "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (v) REFERENCES p (missing)", CamusDBErrorCodes.InvalidForeignKeyDefinition, "referenced column 'p.missing' does not exist")]
    [TestCase(T + ";" + U + "; CREATE TABLE q (id INT64 PRIMARY KEY, x INT64)", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (v) REFERENCES q (x)", CamusDBErrorCodes.InvalidForeignKeyDefinition, "there is no unique index or primary key on 'q'")]
    [TestCase(T + ";" + P, "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (name) REFERENCES p (id)", CamusDBErrorCodes.InvalidForeignKeyDefinition, "column 'name' is String")]
    [TestCase(P + "; CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, CONSTRAINT c CHECK (v > 0))", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (v) REFERENCES p (id)", CamusDBErrorCodes.InvalidInput, "Duplicate constraint name 'c'")]
    [TestCase(Child, "ALTER TABLE p ADD CONSTRAINT back FOREIGN KEY (code) REFERENCES c (id)", CamusDBErrorCodes.ForeignKeyCycle, "would close a cycle")]
    [TestCase(P + ";" + T + "; INSERT INTO t (id, v) VALUES (1, 9)", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (v) REFERENCES p (id)", CamusDBErrorCodes.ForeignKeyViolation, "key (v)=(9) is not present in table 'p'")]
    [TestCase(Viewed + "; CREATE TABLE c (id INT64 PRIMARY KEY, p INT64)", "ALTER TABLE c ADD CONSTRAINT fk FOREIGN KEY (p) REFERENCES w (id)", CamusDBErrorCodes.InvalidForeignKeyDefinition, "cannot reference 'w': it is a view")]
    [TestCase(Ttl + ";" + T, "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (v) REFERENCES p (id)", CamusDBErrorCodes.FeatureNotSupported, "it has row-level TTL")]
    [TestCase(P + ";" + T, "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (v) REFERENCES p (id) ON DELETE CASCADE", CamusDBErrorCodes.FeatureNotSupported, "ON DELETE CASCADE is not supported")]
    // NOT NULL, storage, settings
    [TestCase(T, "ALTER TABLE t ALTER COLUMN missing SET NOT NULL", CamusDBErrorCodes.InvalidInput, "Column 'missing' does not exist")]
    [TestCase("CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, CONSTRAINT t_v_not_null CHECK (id > 0))", "ALTER TABLE t ALTER COLUMN v SET NOT NULL", CamusDBErrorCodes.InvalidInput, "Constraint 't_v_not_null' already exists")]
    [TestCase(T + "; INSERT INTO t (id) VALUES (1)", "ALTER TABLE t ALTER COLUMN v SET NOT NULL", CamusDBErrorCodes.NotNullViolation, "contains null values")]
    [TestCase(T, "ALTER TABLE t ALTER COLUMN name SET STORAGE NOSUCH", CamusDBErrorCodes.InvalidInput, "Unknown storage strategy")]
    [TestCase(T, "ALTER TABLE t SET (nosuch = '1')", CamusDBErrorCodes.InvalidInput, "Unknown table setting 'nosuch'")]
    [TestCase(T, "ALTER TABLE t SET (sql_stats_automatic_collection_enabled = 7)", CamusDBErrorCodes.InvalidInput, "must be true or false")]
    [TestCase(T, "ALTER TABLE t SET (ttl = true)", CamusDBErrorCodes.InvalidInput, "is set by the engine")]
    [TestCase(T, "ALTER TABLE t SET (ttl_expiration_expression = 'missing')", CamusDBErrorCodes.UnknownColumn, "names column 'missing', which does not exist")]
    [TestCase(T, "ALTER TABLE t SET (ttl_expiration_expression = 'name')", CamusDBErrorCodes.InvalidInput, "column 'name' is String")]
    [TestCase(T, "ALTER TABLE t SET (ttl_expiration_expression = 'id')", CamusDBErrorCodes.InvalidInput, "cannot name primary key column 'id'")]
    [TestCase(Indexed, "ALTER TABLE t SET (ttl_delete_batch_size = 100000000)", CamusDBErrorCodes.InvalidInput, "exceeding the limit of")]
    [TestCase(Child, "ALTER TABLE p SET (ttl_expiration_expression = 'e')", CamusDBErrorCodes.FeatureNotSupported, "foreign key 'fk' on table 'c' references it")]
    [TestCase("", "ALTER TABLE t SET (ttl_pause = true)", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase(Viewed, "ALTER TABLE w SET (ttl_pause = true)", CamusDBErrorCodes.ViewNotUpdatable, NotATable)]
    [TestCase(T, "ALTER TABLE t RESET (nosuch)", CamusDBErrorCodes.InvalidInput, "Unknown table setting 'nosuch'")]
    [TestCase("", "ALTER TABLE t RESET (ttl_pause)", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    // comments
    [TestCase("", "COMMENT ON TABLE t IS 'x'", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase(T, "COMMENT ON COLUMN t.missing IS 'x'", CamusDBErrorCodes.UnknownColumn, "Column 'missing' does not exist")]
    [TestCase(T, "COMMENT ON INDEX t.missing IS 'x'", CamusDBErrorCodes.InvalidInput, "Index 'missing' does not exist")]
    [TestCase(T, "COMMENT ON COLUMN id IS 'x'", CamusDBErrorCodes.InvalidInput, "requires a table-qualified name")]
    [TestCase(Viewed, "COMMENT ON TABLE w IS 'x'", CamusDBErrorCodes.ViewNotUpdatable, NotATable)]
    // the names and bodies of views
    [TestCase(T, "CREATE VIEW t AS SELECT 1 AS a", CamusDBErrorCodes.TableAlreadyExists, "Relation 't' already exists")]
    [TestCase(Viewed, "CREATE VIEW w AS SELECT id FROM t", CamusDBErrorCodes.ViewAlreadyExists, "Relation 'w' already exists")]
    [TestCase(T, "CREATE OR REPLACE VIEW t AS SELECT 1 AS a", CamusDBErrorCodes.TableAlreadyExists, "is a table, not a view")]
    [TestCase("CREATE SEQUENCE s", "CREATE VIEW s AS SELECT 1 AS a", CamusDBErrorCodes.SequenceAlreadyExists, "Relation 's' already exists")]
    [TestCase("", "CREATE VIEW w AS SELECT id FROM missing", CamusDBErrorCodes.TableDoesntExist, "Table 'missing' doesn't exist")]
    [TestCase(Viewed, "CREATE OR REPLACE VIEW w AS SELECT id FROM t", CamusDBErrorCodes.CannotChangeViewShape, "Cannot drop columns from view 'w'")]
    [TestCase(T, "DROP VIEW t", CamusDBErrorCodes.ViewDoesntExist, "is a table, not a view")]
    [TestCase("", "DROP VIEW w", CamusDBErrorCodes.ViewDoesntExist, "View 'w' does not exist")]
    [TestCase(Viewed, "ALTER VIEW w RENAME TO t", CamusDBErrorCodes.TableAlreadyExists, "Relation 't' already exists")]
    [TestCase("", "ALTER VIEW w RENAME TO t", CamusDBErrorCodes.ViewDoesntExist, "View 'w' does not exist")]
    public async Task ARefusalOfAutocommit_AlsoHappensInsideATransaction(string setup, string statement, string code, string reason)
    {
        {
            (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
            await Autocommit(executor, database, db, setup);
            AssertRefusal(await RefusalOf(() => Autocommit(executor, database, db, statement), "outside"), code, reason, "outside a transaction");
        }

        {
            (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
            string shape = SchemaShape.Of(database);

            KvTransaction tx = await BeginSessionAsync(database);
            await RunAll(executor, tx, db, setup);
            AssertRefusal(await RefusalOf(() => Run(executor, tx, db, statement), "private objects"), code, reason, "inside, on objects of the transaction");

            // A refusal aborts the transaction, so a COMMIT rolls it back and says so.
            Refusal commit = await RefusalOf(() => database.Transactions.CommitAsync(tx), "commit");
            Assert.AreEqual(CamusDBErrorCodes.TransactionRollbackOnly, commit.Code);
            await AssertUnchangedAsync(database, shape);
        }

        {
            (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
            await Autocommit(executor, database, db, setup);
            string shape = SchemaShape.Of(database);

            KvTransaction tx = await BeginSessionAsync(database);
            AssertRefusal(await RefusalOf(() => Run(executor, tx, db, statement), "committed objects"), code, reason, "inside, on committed objects");
            await database.Transactions.RollbackAsync(tx);
            await AssertUnchangedAsync(database, shape);

            // The DDL lock is free, and the statement meets the same refusal again.
            AssertRefusal(await RefusalOf(() => Autocommit(executor, database, db, statement), "again"), code, reason, "outside, after the rollback");
        }
    }

    /// <summary>
    /// The same comparison for statements that name a materialized view, which a transaction cannot create.
    /// </summary>
    [TestCase("DROP TABLE mv", CamusDBErrorCodes.TableDoesntExist, "use DROP MATERIALIZED VIEW")]
    [TestCase("ALTER TABLE mv RENAME TO x", CamusDBErrorCodes.TableDoesntExist, "use ALTER MATERIALIZED VIEW ... RENAME TO")]
    [TestCase("DROP TABLE t", CamusDBErrorCodes.DependentObjectsExist, "other objects depend on it: mv")]
    [TestCase("ALTER TABLE t DROP COLUMN v", CamusDBErrorCodes.DependentObjectsExist, "other objects depend on it: mv")]
    [TestCase("ALTER TABLE u ADD CONSTRAINT c FOREIGN KEY (id) REFERENCES mv (id)", CamusDBErrorCodes.InvalidForeignKeyDefinition, "it is a materialized view")]
    [TestCase("CREATE TABLE c (id INT64 PRIMARY KEY, p INT64 REFERENCES mv (id))", CamusDBErrorCodes.InvalidForeignKeyDefinition, "it is a materialized view")]
    [TestCase("CREATE TABLE mv (id INT64 PRIMARY KEY)", CamusDBErrorCodes.TableAlreadyExists, "Table 'mv' already exists")]
    [TestCase("DROP VIEW mv", CamusDBErrorCodes.ViewDoesntExist, "use DROP MATERIALIZED VIEW")]
    public async Task ARefusalAroundAMaterializedView_AlsoHappensInsideATransaction(string statement, string code, string reason)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Autocommit(executor, database, db, T + ";" + U + "; CREATE MATERIALIZED VIEW mv AS SELECT id, v FROM t");
        string shape = SchemaShape.Of(database);

        AssertRefusal(await RefusalOf(() => Autocommit(executor, database, db, statement), "outside"), code, reason, "outside a transaction");

        KvTransaction tx = await BeginSessionAsync(database);
        AssertRefusal(await RefusalOf(() => Run(executor, tx, db, statement), "inside"), code, reason, "inside a transaction");
        await database.Transactions.RollbackAsync(tx);
        await AssertUnchangedAsync(database, shape);
    }

    /// <summary>A client that sends a schema statement to the endpoint for writes meets the same refusal.</summary>
    [TestCase("DROP TABLE p", CamusDBErrorCodes.DependentObjectsExist)]
    [TestCase("CREATE INDEX kp ON c (id)", CamusDBErrorCodes.InvalidInput)]
    [TestCase("ALTER TABLE c ADD CONSTRAINT fk CHECK (p > 0)", CamusDBErrorCodes.InvalidInput)]
    [TestCase("COMMENT ON TABLE missing IS 'x'", CamusDBErrorCodes.TableDoesntExist)]
    public async Task TheEndpointForWrites_RefusesTheSameStatements(string statement, string code)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Autocommit(executor, database, db, Child);
        string shape = SchemaShape.Of(database);

        KvTransaction tx = await BeginSessionAsync(database);
        Refusal refusal = await RefusalOf(() => executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, statement, null)), "inside");
        Assert.AreEqual(code, refusal.Code, refusal.Message);
        await database.Transactions.RollbackAsync(tx);
        await AssertUnchangedAsync(database, shape);
    }

    // ── what the transaction itself removed or added decides ──────────────────────────────────────

    /// <summary>
    /// A statement that a committed object refuses is accepted once the transaction removed that object. The
    /// commit leaves the schema that the same statements leave outside a transaction, also after a reopen.
    /// </summary>
    [TestCase(Viewed, "DROP VIEW w", "DROP TABLE t")]
    [TestCase(Viewed, "DROP VIEW w", "ALTER TABLE t DROP COLUMN v")]
    [TestCase(Viewed, "DROP VIEW w", "CREATE TABLE w (id INT64 PRIMARY KEY)")]
    [TestCase(Viewed, "DROP VIEW w", "CREATE SEQUENCE w")]
    [TestCase(Child, "DROP TABLE c", "DROP TABLE p")]
    [TestCase(Child, "ALTER TABLE c DROP CONSTRAINT fk", "DROP TABLE p")]
    [TestCase(Child, "ALTER TABLE c DROP CONSTRAINT fk", "DROP INDEX kp ON c")]
    [TestCase(Child, "ALTER TABLE c DROP CONSTRAINT fk; DROP INDEX kp ON c", "ALTER TABLE c DROP COLUMN p")]
    [TestCase(ChildByCode, "ALTER TABLE c DROP CONSTRAINT fk", "DROP INDEX uk ON p")]
    [TestCase(ChildByCode, "ALTER TABLE c DROP CONSTRAINT fk; DROP INDEX uk ON p", "ALTER TABLE p DROP COLUMN code")]
    [TestCase(Child, "DROP TABLE c", "ALTER TABLE p SET (ttl_expiration_expression = 'e')")]
    [TestCase(Indexed, "DROP INDEX k ON t; DROP INDEX k2 ON t", "ALTER TABLE t DROP COLUMN v")]
    [TestCase(Indexed, "DROP INDEX k ON t", "ALTER TABLE t RENAME INDEX k2 TO k")]
    [TestCase(T, "DROP TABLE t", "CREATE TABLE t (id INT64 PRIMARY KEY, other STRING)")]
    [TestCase(T, "DROP TABLE t", "CREATE VIEW t AS SELECT 1 AS a")]
    [TestCase(T, "DROP TABLE t", "CREATE SEQUENCE t")]
    [TestCase(T + ";" + U, "DROP TABLE t", "ALTER TABLE u RENAME TO t")]
    [TestCase(T + ";" + U, "ALTER TABLE t RENAME TO old", "ALTER TABLE u RENAME TO t")]
    [TestCase("CREATE SEQUENCE s", "DROP SEQUENCE s", "CREATE TABLE s (id INT64 PRIMARY KEY)")]
    [TestCase("CREATE SEQUENCE s", "DROP SEQUENCE s", "CREATE VIEW s AS SELECT 1 AS a")]
    [TestCase(T, "ALTER TABLE t DROP COLUMN v", "ALTER TABLE t ADD COLUMN v STRING")]
    [TestCase(T, "ALTER TABLE t RENAME COLUMN v TO x", "ALTER TABLE t ADD COLUMN v STRING")]
    [TestCase(T, "ALTER TABLE t RENAME COLUMN v TO x", "ALTER TABLE t RENAME COLUMN name TO v")]
    [TestCase(Ttl, "ALTER TABLE p RESET (ttl_expiration_expression)", "ALTER TABLE p DROP COLUMN e")]
    [TestCase("CREATE SEQUENCE s; CREATE TABLE t (id INT64 PRIMARY KEY, n INT64 DEFAULT(nextval('s')))", "ALTER TABLE t DROP COLUMN n", "DROP SEQUENCE s")]
    [TestCase("CREATE SEQUENCE s; CREATE TABLE t (id INT64 PRIMARY KEY, n INT64 DEFAULT(nextval('s')))", "DROP TABLE t", "DROP SEQUENCE s")]
    [TestCase(F + ";" + T + "; ALTER TABLE t ADD CONSTRAINT c CHECK (f(v) > 0)", "ALTER TABLE t DROP CONSTRAINT c", "DROP FUNCTION f")]
    public async Task AStatementIsAccepted_OnceTheTransactionRemovedWhatRefusesIt(string setup, string removal, string statement)
    {
        string expected;

        {
            (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
            await Autocommit(executor, database, db, setup);
            await RefusalOf(() => Autocommit(executor, database, db, statement), "with the blocker");

            await Autocommit(executor, database, db, removal);
            await Autocommit(executor, database, db, statement);
            expected = SchemaShape.Of(database);
        }

        {
            (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
            await Autocommit(executor, database, db, setup);
            string before = SchemaShape.Of(database);

            KvTransaction tx = await BeginSessionAsync(database);
            await RunAll(executor, tx, db, removal);
            await Run(executor, tx, db, statement);
            Assert.AreEqual(before, SchemaShape.Of(database), "no other transaction sees the change set");

            await database.Transactions.RollbackAsync(tx);
            await AssertUnchangedAsync(database, before);

            tx = await BeginSessionAsync(database);
            await RunAll(executor, tx, db, removal);
            await Run(executor, tx, db, statement);
            await database.Transactions.CommitAsync(tx);
            await AssertUnchangedAsync(database, expected);

            await executor.CloseDatabase(new CloseDatabaseTicket(db));
            Assert.AreEqual(expected, SchemaShape.Of(await executor.OpenDatabase(db)), "after a reopen");
        }
    }

    /// <summary>
    /// A statement on committed objects is refused, with the code and the reason of autocommit, when the
    /// transaction itself added what refuses it, also when it added it with a staged statement.
    /// </summary>
    [TestCase(T, "CREATE VIEW w AS SELECT id, v FROM t", "DROP TABLE t", CamusDBErrorCodes.DependentObjectsExist, "depend on it: w")]
    [TestCase(T, "CREATE VIEW w AS SELECT id, v FROM t", "ALTER TABLE t DROP COLUMN v", CamusDBErrorCodes.DependentObjectsExist, "depend on it: w")]
    [TestCase(P, "CREATE TABLE c (id INT64 PRIMARY KEY, p INT64 REFERENCES p (id))", "DROP TABLE p", CamusDBErrorCodes.DependentObjectsExist, "on table 'c' references it")]
    [TestCase(P, "CREATE TABLE c (id INT64 PRIMARY KEY, p INT64 REFERENCES p (id))", "ALTER TABLE p DROP PRIMARY KEY", CamusDBErrorCodes.DependentObjectsExist, "on table 'c' references it")]
    [TestCase(P, "CREATE TABLE c (id INT64 PRIMARY KEY, p INT64 REFERENCES p (code))", "DROP INDEX uk ON p", CamusDBErrorCodes.DependentObjectsExist, "on table 'c' references it")]
    [TestCase(P, "CREATE TABLE c (id INT64 PRIMARY KEY, p INT64 REFERENCES p (id))", "ALTER TABLE p SET (ttl_expiration_expression = 'e')", CamusDBErrorCodes.FeatureNotSupported, "on table 'c' references it")]
    [TestCase(P + ";" + T, "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES p (id)", "DROP TABLE p", CamusDBErrorCodes.DependentObjectsExist, "on table 't' references it")]
    [TestCase(P + ";" + T, "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES p (id)", "ALTER TABLE t DROP COLUMN v", CamusDBErrorCodes.DependentObjectsExist, "foreign key 'fk' uses it")]
    [TestCase(P + ";" + T, "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES p (id)", "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES p (id)", CamusDBErrorCodes.InvalidInput, "Duplicate constraint name 'fk'")]
    [TestCase(P + ";" + T, "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES p (id)", "ALTER TABLE p ADD CONSTRAINT back FOREIGN KEY (code) REFERENCES t (id)", CamusDBErrorCodes.ForeignKeyCycle, "would close a cycle")]
    [TestCase(T, "CREATE INDEX k ON t (v)", "ALTER TABLE t DROP COLUMN v", CamusDBErrorCodes.InvalidInput, "it is part of an index")]
    [TestCase(T, "CREATE INDEX k ON t (v)", "CREATE INDEX k ON t (id)", CamusDBErrorCodes.InvalidInput, "Index 'k' already exists")]
    [TestCase(T, "ALTER TABLE t ADD COLUMN n INT64 NOT NULL DEFAULT(1)", "ALTER TABLE t ADD COLUMN n INT64 NOT NULL DEFAULT(1)", CamusDBErrorCodes.InvalidInput, "Duplicate column 'n'")]
    [TestCase(T, "ALTER TABLE t ADD CONSTRAINT c CHECK (v > 0)", "ALTER TABLE t ADD CONSTRAINT c CHECK (v > 1)", CamusDBErrorCodes.InvalidInput, "Constraint 'c' already exists")]
    [TestCase(T, "ALTER TABLE t ALTER COLUMN v SET NOT NULL", "ALTER TABLE t ADD CONSTRAINT t_v_not_null CHECK (v > 1)", CamusDBErrorCodes.InvalidInput, "Constraint 't_v_not_null' already exists")]
    [TestCase(T, "ALTER TABLE t ADD CONSTRAINT t_v_not_null CHECK (v > 1)", "ALTER TABLE t ALTER COLUMN v SET NOT NULL", CamusDBErrorCodes.InvalidInput, "Constraint 't_v_not_null' already exists")]
    [TestCase(T, "ALTER TABLE t ADD COLUMN e DATETIME; ALTER TABLE t SET (ttl_expiration_expression = 'e')", "ALTER TABLE t DROP COLUMN e", CamusDBErrorCodes.InvalidInput, "row-level TTL expiration column")]
    [TestCase("CREATE SEQUENCE s;" + T, "ALTER TABLE t ADD COLUMN n INT64 DEFAULT(nextval('s'))", "DROP SEQUENCE s", CamusDBErrorCodes.SequenceInUse, "t.n")]
    [TestCase(F + ";" + T, "ALTER TABLE t ADD CONSTRAINT c CHECK (f(v) > 0)", "DROP FUNCTION f", CamusDBErrorCodes.FunctionHasDependents, "CHECK constraint c on table t")]
    [TestCase(F + ";" + T, "ALTER TABLE t ADD CONSTRAINT c CHECK (f(v) > 0)", "CREATE OR REPLACE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return -x; $$", CamusDBErrorCodes.FunctionHasDependents, "CHECK constraint c on table t")]
    [TestCase("CREATE SEQUENCE s;" + T, "DROP SEQUENCE s", "ALTER TABLE t ADD COLUMN n INT64 DEFAULT(nextval('s'))", CamusDBErrorCodes.SequenceDoesntExist, "sequence 's', which does not exist")]
    [TestCase("CREATE SEQUENCE s;" + T, "ALTER SEQUENCE s RENAME TO s2", "ALTER TABLE t ADD COLUMN n INT64 DEFAULT(nextval('s'))", CamusDBErrorCodes.SequenceDoesntExist, "sequence 's', which does not exist")]
    [TestCase(F + ";" + T, "DROP FUNCTION f", "ALTER TABLE t ADD CONSTRAINT c CHECK (f(v) > 0)", CamusDBErrorCodes.InvalidAstStmt, "Function not found 'f'")]
    [TestCase(P + ";" + T, "DROP TABLE p", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (v) REFERENCES p (id)", CamusDBErrorCodes.TableDoesntExist, "references table 'p', which does not exist")]
    [TestCase(P + ";" + T, "ALTER TABLE p RENAME TO q", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (v) REFERENCES p (id)", CamusDBErrorCodes.TableDoesntExist, "references table 'p', which does not exist")]
    [TestCase(P + ";" + T, "DROP INDEX uk ON p", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (v) REFERENCES p (code)", CamusDBErrorCodes.InvalidForeignKeyDefinition, "there is no unique index or primary key on 'p'")]
    [TestCase(T, "ALTER TABLE t DROP COLUMN v", "CREATE INDEX k ON t (v)", CamusDBErrorCodes.InvalidInput, "Column 'v' does not exist")]
    [TestCase(T, "ALTER TABLE t RENAME COLUMN v TO x", "CREATE INDEX k ON t (v)", CamusDBErrorCodes.InvalidInput, "Column 'v' does not exist")]
    [TestCase(T, "ALTER TABLE t DROP COLUMN v", "ALTER TABLE t ADD CONSTRAINT c CHECK (v > 0)", CamusDBErrorCodes.InvalidInput, "unknown column 'v'")]
    [TestCase(T, "ALTER TABLE t DROP COLUMN v", "ALTER TABLE t DROP COLUMN v", CamusDBErrorCodes.InvalidInput, "Column v does not exist")]
    [TestCase(T, "DROP TABLE t", "DROP TABLE t", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase(T, "ALTER TABLE t RENAME TO x", "ALTER TABLE t ADD COLUMN n INT64", CamusDBErrorCodes.TableDoesntExist, "Table 't' doesn't exist")]
    [TestCase(Indexed, "DROP INDEX k ON t", "DROP INDEX k ON t", CamusDBErrorCodes.InvalidInput, "Index 'k' does not exist")]
    [TestCase(Indexed, "ALTER TABLE t RENAME INDEX k TO x", "DROP INDEX k ON t", CamusDBErrorCodes.InvalidInput, "Index 'k' does not exist")]
    [TestCase(Viewed, "ALTER TABLE t RENAME TO x", "DROP TABLE x", CamusDBErrorCodes.DependentObjectsExist, "depend on it: w")]
    [TestCase(Child, "ALTER TABLE p RENAME TO q", "DROP TABLE q", CamusDBErrorCodes.DependentObjectsExist, "on table 'c' references it")]
    [TestCase(Child, "ALTER TABLE c RENAME TO d", "DROP TABLE p", CamusDBErrorCodes.DependentObjectsExist, "on table 'd' references it")]
    [TestCase(ChildByCode, "ALTER TABLE p RENAME INDEX uk TO uk2", "DROP INDEX uk2 ON p", CamusDBErrorCodes.DependentObjectsExist, "on table 'c' references it")]
    [TestCase(ChildByCode, "ALTER TABLE p RENAME COLUMN code TO kode", "ALTER TABLE p DROP COLUMN kode", CamusDBErrorCodes.DependentObjectsExist, "on table 'c' references it")]
    public async Task AStatementIsRefused_WhenTheTransactionAddedWhatRefusesIt(string setup, string addition, string statement, string code, string reason)
    {
        {
            (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
            await Autocommit(executor, database, db, setup);
            await Autocommit(executor, database, db, addition);
            AssertRefusal(await RefusalOf(() => Autocommit(executor, database, db, statement), "outside"), code, reason, "outside a transaction");
        }

        {
            (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
            await Autocommit(executor, database, db, setup);
            string shape = SchemaShape.Of(database);

            KvTransaction tx = await BeginSessionAsync(database);
            await RunAll(executor, tx, db, addition);
            AssertRefusal(await RefusalOf(() => Run(executor, tx, db, statement), "inside"), code, reason, "inside a transaction");
            await database.Transactions.RollbackAsync(tx);
            await AssertUnchangedAsync(database, shape);
        }
    }

    /// <summary>A statement that changes nothing because of what the transaction staged is accepted, as in autocommit.</summary>
    [TestCase(T, "CREATE INDEX k ON t (v)", "CREATE INDEX IF NOT EXISTS k ON t (v)")]
    [TestCase(Indexed, "DROP INDEX k ON t", "DROP INDEX IF EXISTS k ON t")]
    [TestCase(T, "DROP TABLE t", "DROP TABLE IF EXISTS t")]
    [TestCase(Viewed, "DROP VIEW w", "DROP VIEW IF EXISTS w")]
    public async Task AConditionalStatement_ReadsWhatTheTransactionChanged(string setup, string change, string statement)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Autocommit(executor, database, db, setup);

        KvTransaction tx = await BeginSessionAsync(database);
        await Run(executor, tx, db, change);
        await Run(executor, tx, db, statement);
        await database.Transactions.CommitAsync(tx);

        (string plainDb, DatabaseDescriptor plain, CommandExecutor plainExecutor) = await CreateDatabase();
        await Autocommit(plainExecutor, plain, plainDb, setup + ";" + change);
        await AssertUnchangedAsync(database, SchemaShape.Of(plain));
    }

    // ── a staged statement on a committed table that needs a change of the transaction ────────────

    /// <summary>
    /// A staged statement runs on the shared table and holds other transactions to its result before the
    /// commit, so it cannot use an object that only the transaction has. It is refused with
    /// <c>CADB0538</c> and leaves nothing. The same statement runs in a new transaction once the change is
    /// committed, with the answer that autocommit gives.
    /// </summary>
    [TestCase(T, "CREATE SEQUENCE s", "ALTER TABLE t ADD COLUMN n INT64 DEFAULT(nextval('s'))", "draws from sequence 's'", null)]
    [TestCase("CREATE SEQUENCE s;" + T, "ALTER SEQUENCE s RENAME TO s2", "ALTER TABLE t ADD COLUMN n INT64 DEFAULT(nextval('s2'))", "draws from sequence 's2'", null)]
    [TestCase("CREATE SEQUENCE s;" + T, "DROP SEQUENCE s; CREATE SEQUENCE s", "ALTER TABLE t ADD COLUMN n INT64 DEFAULT(nextval('s'))", "draws from sequence 's'", null)]
    [TestCase(T, F, "ALTER TABLE t ADD CONSTRAINT c CHECK (f(v) > 0)", "calls function 'f'", null)]
    [TestCase(F + ";" + T, "CREATE OR REPLACE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return x + 2; $$", "ALTER TABLE t ADD CONSTRAINT c CHECK (v > 0 AND f(v) > 0)", "calls function 'f'", null)]
    [TestCase(F + ";" + T, "CREATE OR REPLACE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return -x; $$", "ALTER TABLE t ADD CONSTRAINT c CHECK (f(v) > 0)", "calls function 'f'", CamusDBErrorCodes.CheckConstraintViolation)]
    [TestCase(T, P, "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (v) REFERENCES p (id)", "references table 'p', which the transaction created", CamusDBErrorCodes.ForeignKeyViolation)]
    [TestCase(T + "; CREATE TABLE q (id INT64 PRIMARY KEY); INSERT INTO q (id) VALUES (5)", "ALTER TABLE q RENAME TO p", "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (v) REFERENCES p (id)", "already changed the schema of the referenced table 'p'", null)]
    [TestCase(T, "ALTER TABLE t RENAME COLUMN v TO x", "CREATE INDEX k ON t (x)", "already changed the table's schema", null)]
    [TestCase("CREATE TABLE p (id INT64 PRIMARY KEY, v INT64);" + T + "; CREATE TABLE x (id INT64 PRIMARY KEY, v INT64, CONSTRAINT xp FOREIGN KEY (v) REFERENCES p (id)); CREATE TABLE r (id INT64 PRIMARY KEY, v INT64, CONSTRAINT rx FOREIGN KEY (v) REFERENCES x (id))",
        "ALTER TABLE x DROP CONSTRAINT xp", "ALTER TABLE p ADD CONSTRAINT pr FOREIGN KEY (v) REFERENCES r (id)", "closes a cycle", null)]
    public async Task AStagedStatementThatNeedsAnUncommittedChange_IsRefused(string setup, string change, string statement, string reason, string? codeAfterTheCommit)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Autocommit(executor, database, db, setup + "; INSERT INTO t (id, v) VALUES (1, 5)");
        string shape = SchemaShape.Of(database);

        KvTransaction tx = await BeginSessionAsync(database);
        await RunAll(executor, tx, db, change);
        AssertRefusal(await RefusalOf(() => Run(executor, tx, db, statement), "inside"),
            CamusDBErrorCodes.StatementNotAllowedInTransaction, reason, "inside the transaction of the change");
        await database.Transactions.RollbackAsync(tx);
        await AssertUnchangedAsync(database, shape);

        await Autocommit(executor, database, db, change);

        (string plainDb, DatabaseDescriptor plain, CommandExecutor plainExecutor) = await CreateDatabase();
        await Autocommit(plainExecutor, plain, plainDb, setup + "; INSERT INTO t (id, v) VALUES (1, 5); " + change);

        tx = await BeginSessionAsync(database);

        if (codeAfterTheCommit is not null)
        {
            Assert.AreEqual(codeAfterTheCommit, (await RefusalOf(() => Autocommit(plainExecutor, plain, plainDb, statement), "outside")).Code);
            Assert.AreEqual(codeAfterTheCommit, (await RefusalOf(() => Run(executor, tx, db, statement), "in a new transaction")).Code);
            await database.Transactions.RollbackAsync(tx);
        }
        else
        {
            await Autocommit(plainExecutor, plain, plainDb, statement);
            await Run(executor, tx, db, statement);
            await database.Transactions.CommitAsync(tx);
        }

        await AssertUnchangedAsync(database, SchemaShape.Of(plain));
    }

    /// <summary>
    /// A change of a sequence or function that does not change what the staged statement uses does not refuse
    /// it: a comment, the same body again, and a change of an object that the statement does not name.
    /// </summary>
    [TestCase("CREATE SEQUENCE s;" + T, "COMMENT ON SEQUENCE s IS 'x'", "ALTER TABLE t ADD COLUMN n INT64 DEFAULT(nextval('s'))")]
    [TestCase("CREATE SEQUENCE s; CREATE SEQUENCE other;" + T, "DROP SEQUENCE other", "ALTER TABLE t ADD COLUMN n INT64 DEFAULT(nextval('s'))")]
    [TestCase(F + ";" + T, "CREATE OR REPLACE FUNCTION f(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return x + 1; $$", "ALTER TABLE t ADD CONSTRAINT c CHECK (f(v) > 0)")]
    [TestCase(F + ";" + T, "DROP FUNCTION f; " + F, "ALTER TABLE t ADD CONSTRAINT c CHECK (f(v) > 0)")]
    [TestCase(F + ";" + T, "CREATE FUNCTION g(x INT64) RETURNS INT64 LANGUAGE csharp AS $$ return x; $$", "ALTER TABLE t ADD CONSTRAINT c CHECK (f(v) > abs(0))")]
    public async Task AStagedStatement_RunsAfterAChangeThatItDoesNotNeed(string setup, string change, string statement)
    {
        (string plainDb, DatabaseDescriptor plain, CommandExecutor plainExecutor) = await CreateDatabase();
        await Autocommit(plainExecutor, plain, plainDb, setup + "; INSERT INTO t (id, v) VALUES (1, 5);" + change + ";" + statement);

        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Autocommit(executor, database, db, setup + "; INSERT INTO t (id, v) VALUES (1, 5)");

        KvTransaction tx = await BeginSessionAsync(database);
        await RunAll(executor, tx, db, change);
        await Run(executor, tx, db, statement);
        await database.Transactions.CommitAsync(tx);

        await AssertUnchangedAsync(database, SchemaShape.Of(plain));
        Assert.AreEqual(1, (await Rows(executor, database, db, "SELECT * FROM t")).Count);
        await Autocommit(executor, database, db, "INSERT INTO t (id, v) VALUES (2, 6)");
    }

    /// <summary>
    /// An identity column on a committed table creates its sequence in the shared schema at the statement.
    /// The name it takes is free in the private schema too, so the publish adds both relations.
    /// </summary>
    [Test]
    public async Task AnIdentityColumnOfACommittedTable_TakesANameThatTheTransactionDidNotTake(
        [Values("CREATE SEQUENCE t_n_seq", "CREATE TABLE t_n_seq (id INT64 PRIMARY KEY)", "CREATE VIEW t_n_seq AS SELECT 1 AS a")] string taker,
        [Values] bool commit)
    {
        const string column = "ALTER TABLE t ADD COLUMN n INT64 GENERATED ALWAYS AS IDENTITY";

        (string plainDb, DatabaseDescriptor plain, CommandExecutor plainExecutor) = await CreateDatabase();
        await Autocommit(plainExecutor, plain, plainDb, T + "; INSERT INTO t (id) VALUES (1), (2);" + taker + ";" + column);
        Assert.IsTrue(plain.Schema.Sequences.ContainsKey("t_n_seq1"), "the name outside a transaction");

        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Autocommit(executor, database, db, T + "; INSERT INTO t (id) VALUES (1), (2)");
        string before = SchemaShape.Of(database);

        KvTransaction tx = await BeginSessionAsync(database);
        await Run(executor, tx, db, taker);
        await Run(executor, tx, db, column);
        await Run(executor, tx, db, "INSERT INTO t (id) VALUES (3)");
        Assert.AreEqual(3, (await Rows(executor, database, db, "SELECT DISTINCT n FROM t WHERE n IS NOT NULL", tx)).Count);

        if (!commit)
        {
            await database.Transactions.RollbackAsync(tx);
            await AssertUnchangedAsync(database, before);
            return;
        }

        await database.Transactions.CommitAsync(tx);
        await AssertUnchangedAsync(database, SchemaShape.Of(plain));
        await Autocommit(executor, database, db, "INSERT INTO t (id) VALUES (4)");
        Assert.AreEqual(4, (await Rows(executor, database, db, "SELECT DISTINCT n FROM t WHERE n IS NOT NULL")).Count);
    }

    // ── limits, letter case, staged elements that the transaction drops again ─────────────────────

    [TestCase("CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, KEY k1 (v), KEY k2 (v, id))", "CREATE INDEX k3 ON t (id)", "maximum of 2 indexes per table")]
    [TestCase("CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, KEY k1 (v), KEY k2 (v, id))", "CREATE UNIQUE INDEX k3 ON t (id)", "maximum of 2 indexes per table")]
    [TestCase("CREATE TABLE t (id INT64 PRIMARY KEY, a INT64, b INT64, c INT64)", "ALTER TABLE t ADD COLUMN d INT64", "maximum of 4 columns per table")]
    [TestCase("CREATE TABLE t (id INT64 PRIMARY KEY, a INT64, b INT64, c INT64)", "ALTER TABLE t ADD COLUMN d INT64 NOT NULL DEFAULT(1)", "maximum of 4 columns per table")]
    [TestCase("CREATE TABLE a (id INT64 PRIMARY KEY); CREATE TABLE b (id INT64 PRIMARY KEY); CREATE TABLE c (id INT64 PRIMARY KEY)", "CREATE TABLE d (id INT64 PRIMARY KEY)", "maximum of 3 tables per database")]
    [TestCase("CREATE TABLE a (id INT64 PRIMARY KEY); CREATE TABLE b (id INT64 PRIMARY KEY); CREATE TABLE c (id INT64 PRIMARY KEY)", "CREATE TABLE d AS SELECT id FROM a", "maximum of 3 tables per database")]
    public async Task ALimitOfAutocommit_AlsoHoldsInsideATransaction(string setup, string statement, string reason)
    {
        CamusDBOptions limits = Options with { MaxIndexesPerTable = 2, MaxColumnsPerTable = 4, MaxTablesPerDatabase = 3 };

        foreach (bool privateObjects in new[] { true, false })
        {
            (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(limits);

            if (!privateObjects)
            {
                await Autocommit(executor, database, db, setup);
                AssertRefusal(await RefusalOf(() => Autocommit(executor, database, db, statement), "outside"),
                    CamusDBErrorCodes.SchemaLimitExceeded, reason, "outside a transaction");
            }

            string shape = SchemaShape.Of(database);
            KvTransaction tx = await BeginSessionAsync(database);

            if (privateObjects)
                await RunAll(executor, tx, db, setup);

            AssertRefusal(await RefusalOf(() => Run(executor, tx, db, statement), "inside"),
                CamusDBErrorCodes.SchemaLimitExceeded, reason, $"inside a transaction, private objects: {privateObjects}");
            await database.Transactions.RollbackAsync(tx);
            await AssertUnchangedAsync(database, shape);
        }
    }

    public enum Where
    {
        Autocommit,
        PrivateTable,
        CommittedTable,
    }

    /// <summary>
    /// An index statement that names the columns in another letter case builds the index that the declared
    /// names build: it holds the rows from before and after it, with its INCLUDE values.
    /// </summary>
    [Test]
    public async Task AnIndexThatNamesItsColumnsInAnotherCase_HoldsEveryRow([Values] Where where)
    {
        const string setup = "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, w INT64); INSERT INTO t (id, v, w) VALUES (1, 5, 7), (2, 6, 8)";
        const string index = "CREATE UNIQUE INDEX k ON t (V) INCLUDE (W)";

        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        if (where == Where.Autocommit)
        {
            await Autocommit(executor, database, db, setup + ";" + index);
        }
        else
        {
            if (where == Where.CommittedTable)
                await Autocommit(executor, database, db, setup);

            KvTransaction tx = await BeginSessionAsync(database);

            if (where == Where.PrivateTable)
                await RunAll(executor, tx, db, setup);

            await Run(executor, tx, db, index);
            await database.Transactions.CommitAsync(tx);
        }

        await Autocommit(executor, database, db, "INSERT INTO t (id, v, w) VALUES (3, 9, 10)");

        foreach ((long v, long w) in new[] { (5L, 7L), (6L, 8L), (9L, 10L) })
            Assert.AreEqual(w, (await Rows(executor, database, db, $"SELECT w FROM t@{{FORCE_INDEX=k}} WHERE v = {v}")).Single()["w"].LongValue);

        Refusal duplicate = await RefusalOf(() => Autocommit(executor, database, db, "INSERT INTO t (id, v, w) VALUES (4, 9, 11)"), "a duplicate");
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, duplicate.Code);
    }

    /// <summary>
    /// A transaction that drops what it staged leaves the schema that the same two statements leave outside a
    /// transaction, with no job record and no index entry, after a COMMIT and after a ROLLBACK.
    /// </summary>
    [Test]
    public async Task AStagedElementThatTheTransactionDropped_LeavesNothing(
        [Values(
            "CREATE INDEX k ON t (v)|DROP INDEX k ON t",
            "CREATE INDEX k ON t (v)|ALTER TABLE t RENAME INDEX k TO k2; DROP INDEX k2 ON t",
            "CREATE INDEX k ON t (v)|DROP TABLE t",
            "ALTER TABLE t ADD COLUMN n INT64 NOT NULL DEFAULT(1)|ALTER TABLE t DROP COLUMN n",
            "ALTER TABLE t ADD COLUMN n INT64 GENERATED ALWAYS AS IDENTITY|ALTER TABLE t RENAME COLUMN n TO m; ALTER TABLE t DROP COLUMN m",
            "ALTER TABLE t ADD CONSTRAINT c CHECK (v > 0)|ALTER TABLE t DROP CONSTRAINT c",
            "ALTER TABLE t ALTER COLUMN v SET NOT NULL|ALTER TABLE t ALTER COLUMN v DROP NOT NULL",
            "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES p (id)|ALTER TABLE t DROP CONSTRAINT fk",
            "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES p (id)|DROP TABLE t; DROP TABLE p")] string statements,
        [Values] bool commit)
    {
        const string setup = P + ";" + T + "; INSERT INTO p (id) VALUES (5), (6); INSERT INTO t (id, v) VALUES (1, 5), (2, 6)";
        string[] parts = statements.Split('|');

        (string plainDb, DatabaseDescriptor plain, CommandExecutor plainExecutor) = await CreateDatabase();
        await Autocommit(plainExecutor, plain, plainDb, setup + ";" + parts[0] + ";" + parts[1]);

        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Autocommit(executor, database, db, setup);
        string before = SchemaShape.Of(database);
        string storageId = database.Schema.Tables["t"].EffectiveStorageId;

        KvTransaction tx = await BeginSessionAsync(database);
        await Run(executor, tx, db, parts[0]);

        List<string> stagedSpaces = [.. database.SharedSchema.Tables["t"].Indexes!
            .Where(i => !i.Name.StartsWith("~pk", StringComparison.Ordinal))
            .Select(i => KvKeyBuilder.IndexSpaceOf(database.Id, storageId, i.KvId))];

        await RunAll(executor, tx, db, parts[1]);

        if (commit)
            await database.Transactions.CommitAsync(tx);
        else
            await database.Transactions.RollbackAsync(tx);

        await AssertUnchangedAsync(database, commit ? SchemaShape.Of(plain) : before);

        // A table that was dropped without FORCE keeps its rows and entries for a RELINK.
        if (!commit || database.Schema.Tables.ContainsKey("t"))
        {
            await executor.DeferredPurgesForTests.WaitForScheduledRunsForTestsAsync();

            foreach (string space in stagedSpaces)
                Assert.AreEqual(0, await CountKeysAsync(database, space, space + "/"), "no entry of the staged index is left");

            Assert.AreEqual(2, (await Rows(executor, database, db, "SELECT id FROM t")).Count);
        }

        await executor.CloseDatabase(new CloseDatabaseTicket(db));
        Assert.AreEqual(commit ? SchemaShape.Of(plain) : before, SchemaShape.Of(await executor.OpenDatabase(db)), "after a reopen");
    }

    // ── branches ──────────────────────────────────────────────────────────────

    /// <summary>On a branch, the rows that it inherits refuse a statement inside a transaction as they do outside.</summary>
    [TestCase("CREATE UNIQUE INDEX k ON t (v)", CamusDBErrorCodes.DuplicateUniqueKeyValue)]
    [TestCase("ALTER TABLE t ADD CONSTRAINT c CHECK (v > 5)", CamusDBErrorCodes.CheckConstraintViolation)]
    [TestCase("ALTER TABLE t ALTER COLUMN name SET NOT NULL", CamusDBErrorCodes.NotNullViolation)]
    [TestCase("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES p (id)", CamusDBErrorCodes.ForeignKeyViolation)]
    [TestCase("ALTER TABLE t ADD COLUMN x INT64 NOT NULL", CamusDBErrorCodes.NotNullViolation)]
    [TestCase("DROP TABLE p", CamusDBErrorCodes.DependentObjectsExist)]
    [TestCase("ALTER TABLE p DROP COLUMN code", CamusDBErrorCodes.DependentObjectsExist)]
    public async Task OnABranch_TheInheritedObjectsRefuseTheSameStatements(string statement, string code)
    {
        (string source, DatabaseDescriptor sourceDatabase, CommandExecutor executor) = await CreateNamedDatabaseAsync();
        await Autocommit(executor, sourceDatabase, source,
            ChildByCode + ";" + T + "; INSERT INTO t (id, v) VALUES (1, 5), (2, 5); INSERT INTO p (id, code) VALUES (1, 1)");

        string branch = "br" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor database = (await BranchAsync(executor, source, branch)).Database!;
        TrackDatabase(branch, executor);
        string shape = SchemaShape.Of(database);

        Assert.AreEqual(code, (await RefusalOf(() => Autocommit(executor, database, branch, statement), "outside")).Code);

        KvTransaction tx = await BeginSessionAsync(database);
        Refusal refusal = await RefusalOf(() => Run(executor, tx, branch, statement), "inside");
        Assert.AreEqual(code, refusal.Code, refusal.Message);
        await database.Transactions.RollbackAsync(tx);

        await AssertUnchangedAsync(database, shape);
        Assert.AreEqual(shape, SchemaShape.Of(sourceDatabase), "the source is as it was at the fork");
    }

    /// <summary>
    /// A database cannot be branched while a transaction holds an element it staged on a committed table:
    /// the branch would inherit an element that nobody finishes. It can be branched once the transaction
    /// ended. A transaction with metadata changes only does not block a branch, which forks without them.
    /// </summary>
    [Test]
    public async Task ABranch_IsRefusedWhileATransactionHoldsAStagedElement(
        [Values(
            "CREATE INDEX k ON t (v)",
            "ALTER TABLE t ADD COLUMN n INT64 NOT NULL DEFAULT(1)",
            "ALTER TABLE t ADD CONSTRAINT c CHECK (v > 0)",
            "ALTER TABLE t ALTER COLUMN v SET NOT NULL",
            "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES p (id)")] string statement,
        [Values] bool commit)
    {
        (string source, DatabaseDescriptor database, CommandExecutor executor) = await CreateNamedDatabaseAsync();
        await Autocommit(executor, database, source, P + ";" + T + "; INSERT INTO p (id) VALUES (5); INSERT INTO t (id, v) VALUES (1, 5)");
        string before = SchemaShape.Of(database);

        KvTransaction tx = await BeginSessionAsync(database);
        await Run(executor, tx, source, "CREATE TABLE only_private (id INT64 PRIMARY KEY)");

        string early = "br" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor earlyBranch = (await BranchAsync(executor, source, early)).Database!;
        TrackDatabase(early, executor);
        Assert.AreEqual(before, SchemaShape.Of(earlyBranch), "a metadata change of an open transaction is not in the branch");

        await Run(executor, tx, source, statement);

        string branch = "br" + Guid.NewGuid().ToString("n");
        Refusal refusal = await RefusalOf(() => BranchAsync(executor, source, branch), "a branch");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, refusal.Code, refusal.Message);

        if (commit)
            await database.Transactions.CommitAsync(tx);
        else
            await database.Transactions.RollbackAsync(tx);

        DatabaseDescriptor late = (await BranchAsync(executor, source, branch)).Database!;
        TrackDatabase(branch, executor);
        Assert.AreEqual(SchemaShape.Of(database), SchemaShape.Of(late));
        Assert.AreEqual(commit, late.Schema.Tables.ContainsKey("only_private"));
        await AssertUnchangedAsync(database, commit ? SchemaShape.Of(late) : before);
    }
}

/// <summary><see cref="TransactionalDdlTableRefusalScenarios"/> on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestTransactionalDdlTableRefusalsStandalone : TransactionalDdlTableRefusalScenarios
{
    protected override bool ClusterMode => false;
}

/// <summary><see cref="TransactionalDdlTableRefusalScenarios"/> on an engine in cluster mode, whose autocommit statements take the staged paths.</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestTransactionalDdlTableRefusalsCluster : TransactionalDdlTableRefusalScenarios
{
    protected override bool ClusterMode => true;
}
