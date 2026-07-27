/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for <c>COMMENT ON</c> and the inline <c>COMMENT</c> clauses in
/// <c>CREATE TABLE</c> / <c>ALTER TABLE … ADD COLUMN</c>, driven through the real SQL entry points.
///
/// <para>The recurring theme is the null-vs-empty distinction: <c>IS NULL</c> removes a comment and
/// <c>IS ''</c> stores an empty one, and every layer (parser, ticket, schema, SHOW CREATE TABLE)
/// has to keep those apart. Several tests exist specifically to fail if some layer coalesces
/// them.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestCommentOn : BaseTest
{
    private async Task<(string dbname, CommandExecutor executor)> CreateUsersTable(string createTableSql)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecuteDdl(executor, dbname, createTableSql);
        return (dbname, executor);
    }

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
    }

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<string> ShowCreateTableAsync(CommandExecutor executor, string dbname, string tableName)
    {
        List<QueryResultRow> rows = await QueryAsync(executor, dbname, $"SHOW CREATE TABLE {tableName}");

        if (rows.Count == 0)
            throw new AssertionException("SHOW CREATE TABLE returned no rows");

        return rows[0].Row["Create Table"].StrValue!;
    }

    private static async Task<TableSchema> GetSchemaAsync(CommandExecutor executor, string dbname, string tableName)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        return database.Schema.Tables[tableName];
    }

    private const string PlainUsers =
        "CREATE TABLE users (id oid PRIMARY KEY NOT NULL, email string NOT NULL, age int64 NULL, KEY email_idx (email))";

    // ── Parsing ──────────────────────────────────────────────────────────────

    [Test]
    public void Parse_AllFourCommentOnTargets()
    {
        Assert.AreEqual(NodeType.CommentOnTable, SQLParserProcessor.Parse("COMMENT ON TABLE users IS 'a'").nodeType);
        Assert.AreEqual(NodeType.CommentOnColumn, SQLParserProcessor.Parse("COMMENT ON COLUMN users.id IS 'a'").nodeType);
        Assert.AreEqual(NodeType.CommentOnIndex, SQLParserProcessor.Parse("COMMENT ON INDEX users.email_idx IS 'a'").nodeType);
        Assert.AreEqual(NodeType.CommentOnDatabase, SQLParserProcessor.Parse("COMMENT ON DATABASE app IS 'a'").nodeType);
    }

    [Test]
    public void Parse_IsNullIsDistinctFromEmptyLiteral()
    {
        NodeAst removal = SQLParserProcessor.Parse("COMMENT ON TABLE users IS NULL");
        NodeAst empty = SQLParserProcessor.Parse("COMMENT ON TABLE users IS ''");

        // The parser encodes "IS NULL" as an absent value node; "IS ''" carries a real literal.
        Assert.IsNull(removal.rightAst, "IS NULL must produce no value node");
        Assert.IsNotNull(empty.rightAst, "IS '' must produce a String literal node");
        Assert.AreEqual(NodeType.String, empty.rightAst!.nodeType);
    }

    [Test]
    public void Parse_MalformedCommentFormsAreRejected()
    {
        // Missing IS.
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("COMMENT ON TABLE users 'a'"));
        // Missing value.
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("COMMENT ON TABLE users IS"));
        // Unknown target kind.
        Assert.Throws<CamusDBException>(() => SQLParserProcessor.Parse("COMMENT ON SCHEMA users IS 'a'"));
    }

    [Test]
    public async Task UnqualifiedColumnAndIndexReferencesAreRejected()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        CamusDBException colEx = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "COMMENT ON COLUMN id IS 'x'"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, colEx.Code);

        CamusDBException idxEx = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "COMMENT ON INDEX email_idx IS 'x'"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, idxEx.Code);
    }

    [Test]
    public void ReservedWord_PluralIdentifierStillParsesUnquoted()
    {
        // GPLEX longest-match must lex "comments" as an identifier, not TCOMMENT followed by "s".
        // A table actually named `comment` now needs backticks — that is the documented breaking change.
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("SELECT * FROM comments"));
        Assert.DoesNotThrow(() => SQLParserProcessor.Parse("SELECT `comment` FROM t"));
    }

    // ── Set and read back (standalone) ───────────────────────────────────────

    [Test]
    public async Task SetAndReadBack_Table_Column_Index()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        await ExecuteDdl(executor, dbname, "COMMENT ON TABLE users IS 'Application users'");
        await ExecuteDdl(executor, dbname, "COMMENT ON COLUMN users.email IS 'Unique login email'");
        await ExecuteDdl(executor, dbname, "COMMENT ON INDEX users.email_idx IS 'Lookup by login email'");

        string ddl = await ShowCreateTableAsync(executor, dbname, "users");

        StringAssert.Contains("COMMENT 'Application users'", ddl);
        StringAssert.Contains("COMMENT 'Unique login email'", ddl);
        StringAssert.Contains("COMMENT 'Lookup by login email'", ddl);
    }

    [Test]
    public async Task OverwriteThenRemoveThenEmpty()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        await ExecuteDdl(executor, dbname, "COMMENT ON TABLE users IS 'first'");
        await ExecuteDdl(executor, dbname, "COMMENT ON TABLE users IS 'second'");

        TableSchema schema = await GetSchemaAsync(executor, dbname, "users");
        Assert.AreEqual("second", schema.Comment);

        // IS NULL removes: the clause disappears from the rendered DDL entirely.
        await ExecuteDdl(executor, dbname, "COMMENT ON TABLE users IS NULL");
        Assert.IsNull((await GetSchemaAsync(executor, dbname, "users")).Comment);
        StringAssert.DoesNotContain("COMMENT", await ShowCreateTableAsync(executor, dbname, "users"));

        // IS '' stores a present-but-empty comment, which still renders a clause.
        await ExecuteDdl(executor, dbname, "COMMENT ON TABLE users IS ''");
        Assert.AreEqual("", (await GetSchemaAsync(executor, dbname, "users")).Comment);
        StringAssert.Contains("COMMENT ''", await ShowCreateTableAsync(executor, dbname, "users"));
    }

    [Test]
    public async Task SetIsIdempotent()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        await ExecuteDdl(executor, dbname, "COMMENT ON COLUMN users.age IS 'age in years'");
        await ExecuteDdl(executor, dbname, "COMMENT ON COLUMN users.age IS 'age in years'");

        TableSchema schema = await GetSchemaAsync(executor, dbname, "users");
        Assert.AreEqual("age in years", schema.Columns!.Single(c => c.Name == "age").Comment);
    }

    // ── Inline CREATE TABLE / ADD COLUMN ─────────────────────────────────────

    [Test]
    public async Task InlineCreateTableComments_AreStored()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(
            "CREATE TABLE users (" +
            "  id oid PRIMARY KEY NOT NULL COMMENT 'Internal user identifier'," +
            "  email string NOT NULL COMMENT 'Unique login email address'," +
            "  KEY email_idx (email) COMMENT 'Lookup by login email'" +
            ") COMMENT 'Application users'");

        TableSchema schema = await GetSchemaAsync(executor, dbname, "users");

        Assert.AreEqual("Application users", schema.Comment);
        Assert.AreEqual("Internal user identifier", schema.Columns!.Single(c => c.Name == "id").Comment);
        Assert.AreEqual("Unique login email address", schema.Columns!.Single(c => c.Name == "email").Comment);
        Assert.AreEqual("Lookup by login email", schema.Indexes!.Single(i => i.Name == "email_idx").Comment);
    }

    [Test]
    public async Task CreateTableWithoutComments_IsUnaffected()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        TableSchema schema = await GetSchemaAsync(executor, dbname, "users");

        Assert.IsNull(schema.Comment);
        Assert.IsTrue(schema.Columns!.All(c => c.Comment is null));
        Assert.IsTrue(schema.Indexes!.All(i => i.Comment is null));
        StringAssert.DoesNotContain("COMMENT", await ShowCreateTableAsync(executor, dbname, "users"));
    }

    [Test]
    public async Task AddColumnWithInlineComment()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        await ExecuteDdl(executor, dbname, "ALTER TABLE users ADD COLUMN nickname string NULL COMMENT 'Display name'");

        TableSchema schema = await GetSchemaAsync(executor, dbname, "users");
        Assert.AreEqual("Display name", schema.Columns!.Single(c => c.Name == "nickname").Comment);
    }

    // ── SHOW CREATE TABLE round-trip ─────────────────────────────────────────

    [Test]
    public async Task ShowCreateTable_RoundTripsCommentsVerbatim()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(
            "CREATE TABLE users (" +
            "  id oid PRIMARY KEY NOT NULL COMMENT 'Internal id'," +
            "  email string NOT NULL," +
            "  KEY email_idx (email) COMMENT 'Lookup by email'" +
            ") COMMENT 'Application users'");

        // Mix both authoring routes: one comment inline at create time, one added afterwards, and
        // one containing a quote so the escaping is exercised by the round-trip rather than by eye.
        await ExecuteDdl(executor, dbname, "COMMENT ON COLUMN users.email IS 'The user''s email'");

        string ddl = await ShowCreateTableAsync(executor, dbname, "users");

        await ExecuteDdl(executor, dbname, "DROP TABLE users");
        await ExecuteDdl(executor, dbname, ddl);

        TableSchema schema = await GetSchemaAsync(executor, dbname, "users");

        Assert.AreEqual("Application users", schema.Comment);
        Assert.AreEqual("Internal id", schema.Columns!.Single(c => c.Name == "id").Comment);
        Assert.AreEqual("The user's email", schema.Columns!.Single(c => c.Name == "email").Comment);
        Assert.AreEqual("Lookup by email", schema.Indexes!.Single(i => i.Name == "email_idx").Comment);

        // Re-rendering the recreated table must produce byte-identical DDL.
        Assert.AreEqual(ddl, await ShowCreateTableAsync(executor, dbname, "users"));
    }

    [Test]
    public async Task QuoteInCommentIsDoubledWhenRendered()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        await ExecuteDdl(executor, dbname, "COMMENT ON TABLE users IS 'it''s here'");

        Assert.AreEqual("it's here", (await GetSchemaAsync(executor, dbname, "users")).Comment);
        StringAssert.Contains("COMMENT 'it''s here'", await ShowCreateTableAsync(executor, dbname, "users"));
    }

    // ── Descriptor coherence ─────────────────────────────────────────────────

    [Test]
    public async Task IndexCommentIsVisibleOnAnAlreadyOpenTable()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        // Force the table descriptor to be built and cached first. Index comments live on a *copy*
        // of the index entry inside the descriptor, so without an eviction this render stays stale.
        await ShowCreateTableAsync(executor, dbname, "users");

        await ExecuteDdl(executor, dbname, "COMMENT ON INDEX users.email_idx IS 'Lookup by email'");

        StringAssert.Contains("COMMENT 'Lookup by email'", await ShowCreateTableAsync(executor, dbname, "users"));
    }

    // ── Preservation across schema rebuild-copies ────────────────────────────

    [Test]
    public async Task ColumnCommentSurvivesRenameColumn()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        await ExecuteDdl(executor, dbname, "COMMENT ON COLUMN users.age IS 'age in years'");
        await ExecuteDdl(executor, dbname, "ALTER TABLE users RENAME COLUMN age TO years");

        TableSchema schema = await GetSchemaAsync(executor, dbname, "users");
        Assert.AreEqual("age in years", schema.Columns!.Single(c => c.Name == "years").Comment);
    }

    [Test]
    public async Task ColumnCommentSurvivesSetAndDropNotNull()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        await ExecuteDdl(executor, dbname, "COMMENT ON COLUMN users.age IS 'age in years'");
        await ExecuteDdl(executor, dbname, "ALTER TABLE users ALTER COLUMN age SET NOT NULL");

        TableSchema afterSet = await GetSchemaAsync(executor, dbname, "users");
        Assert.AreEqual("age in years", afterSet.Columns!.Single(c => c.Name == "age").Comment);

        await ExecuteDdl(executor, dbname, "ALTER TABLE users ALTER COLUMN age DROP NOT NULL");

        TableSchema afterDrop = await GetSchemaAsync(executor, dbname, "users");
        Assert.AreEqual("age in years", afterDrop.Columns!.Single(c => c.Name == "age").Comment);
    }

    [Test]
    public async Task ColumnCommentSurvivesAddingAnotherColumn()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        await ExecuteDdl(executor, dbname, "COMMENT ON COLUMN users.email IS 'login email'");
        await ExecuteDdl(executor, dbname, "ALTER TABLE users ADD COLUMN nickname string NULL");

        TableSchema schema = await GetSchemaAsync(executor, dbname, "users");
        Assert.AreEqual("login email", schema.Columns!.Single(c => c.Name == "email").Comment);
    }

    [Test]
    public async Task IndexCommentSurvivesRenameIndex()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        await ExecuteDdl(executor, dbname, "COMMENT ON INDEX users.email_idx IS 'Lookup by email'");
        await ExecuteDdl(executor, dbname, "ALTER TABLE users RENAME INDEX email_idx TO login_idx");

        TableSchema schema = await GetSchemaAsync(executor, dbname, "users");
        Assert.AreEqual("Lookup by email", schema.Indexes!.Single(i => i.Name == "login_idx").Comment);
    }

    [Test]
    public async Task TableCommentSurvivesRenameTable()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        await ExecuteDdl(executor, dbname, "COMMENT ON TABLE users IS 'Application users'");
        await ExecuteDdl(executor, dbname, "ALTER TABLE users RENAME TO people");

        TableSchema schema = await GetSchemaAsync(executor, dbname, "people");
        Assert.AreEqual("Application users", schema.Comment);
    }

    // ── Persistence ──────────────────────────────────────────────────────────

    [Test]
    public async Task CommentsSurviveCloseAndReopen()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        await ExecuteDdl(executor, dbname, "COMMENT ON TABLE users IS 'Application users'");
        await ExecuteDdl(executor, dbname, "COMMENT ON COLUMN users.email IS 'login email'");
        await ExecuteDdl(executor, dbname, "COMMENT ON INDEX users.email_idx IS 'Lookup by email'");

        int versionBefore = (await GetSchemaAsync(executor, dbname, "users")).Version;

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

        TableSchema reloaded = await GetSchemaAsync(executor, dbname, "users");

        Assert.AreEqual("Application users", reloaded.Comment);
        Assert.AreEqual("login email", reloaded.Columns!.Single(c => c.Name == "email").Comment);
        Assert.AreEqual("Lookup by email", reloaded.Indexes!.Single(i => i.Name == "email_idx").Comment);

        // Comments do not affect row encoding, so the column-layout version must not have moved.
        Assert.AreEqual(versionBefore, reloaded.Version);
    }

    // ── Database comments ────────────────────────────────────────────────────

    /// <summary>
    /// Creates a database whose name begins with a letter. The shared <c>CreateDatabase</c> helper
    /// names databases after a raw GUID, which can start with a digit — and no identifier form in
    /// the lexer (bare or backticked) admits a leading digit, so such a name cannot be written in
    /// SQL at all. That is a pre-existing constraint on every database-scoped statement, not
    /// something COMMENT ON introduces.
    /// </summary>
    private async Task<(string dbname, CommandExecutor executor)> CreateNamedDatabase()
    {
        string dbname = "db" + Guid.NewGuid().ToString("n");

        CommandExecutor executor = CreateCommandExecutor();
        await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);

        return (dbname, executor);
    }

    [Test]
    public async Task DatabaseCommentSetReadBackAndRemove()
    {
        (string dbname, CommandExecutor executor) = await CreateNamedDatabase();

        await ExecuteDdl(executor, dbname, $"COMMENT ON DATABASE {dbname} IS 'Primary application database'");

        Assert.AreEqual("Primary application database", await ShowDatabaseCommentAsync(executor, dbname));

        // Overwriting keeps the latest value.
        await ExecuteDdl(executor, dbname, $"COMMENT ON DATABASE {dbname} IS 'Renamed description'");
        Assert.AreEqual("Renamed description", await ShowDatabaseCommentAsync(executor, dbname));

        await ExecuteDdl(executor, dbname, $"COMMENT ON DATABASE {dbname} IS NULL");
        Assert.AreEqual("", await ShowDatabaseCommentAsync(executor, dbname));
    }

    [Test]
    public async Task DatabaseCommentOnMissingDatabaseFails()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "COMMENT ON DATABASE nonexistent_db_xyz IS 'x'"))!;

        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex.Code);
    }

    private static async Task<string> ShowDatabaseCommentAsync(CommandExecutor executor, string dbname)
    {
        List<QueryResultRow> rows = await QueryAsync(executor, dbname, "SHOW DATABASE");

        if (rows.Count == 0)
            throw new AssertionException("SHOW DATABASE returned no rows");

        return rows[0].Row["comment"].StrValue!;
    }

    // ── Error paths and bounds ───────────────────────────────────────────────

    [Test]
    public async Task CommentOnMissingTargetsFails()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        CamusDBException tableEx = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "COMMENT ON TABLE nosuchtable IS 'x'"))!;
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, tableEx.Code);

        CamusDBException colEx = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "COMMENT ON COLUMN users.nosuchcol IS 'x'"))!;
        Assert.AreEqual(CamusDBErrorCodes.UnknownColumn, colEx.Code);

        CamusDBException idxEx = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, "COMMENT ON INDEX users.nosuchindex IS 'x'"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, idxEx.Code);
    }

    [Test]
    public async Task CommentOnPrimaryKeyIndexIsRejected()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        // Driven through the ticket API rather than SQL: the internal primary-index name starts with
        // '~', which no identifier form in the grammar can spell, so SQL cannot reach this path at
        // all. The guard still matters for direct ticket callers — the PRIMARY KEY line has no
        // inline COMMENT form, so a comment stored here could never be rendered or round-tripped.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Comment(new CommentTicket(
                CommentTarget.Index, dbname, "users", CamusDBConfig.PrimaryKeyInternalName, "x")))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    [Test]
    public async Task CommentOnPrimaryKeyIndexIsUnreachableFromSql()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        // '~pk' matches neither the bare nor the backtick-escaped identifier pattern.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, $"COMMENT ON INDEX users.`{CamusDBConfig.PrimaryKeyInternalName}` IS 'x'"))!;

        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, ex.Code);
    }

    [Test]
    public async Task CommentLongerThanTheMaximumIsRejected()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        string tooLong = new('x', CamusDBConfig.MaxCommentLength + 1);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname, $"COMMENT ON TABLE users IS '{tooLong}'"))!;

        Assert.AreEqual(CamusDBErrorCodes.CommentTooLong, ex.Code);
        Assert.AreEqual(400, CamusDBErrorCodes.GetHttpStatus(ex.Code));
    }

    [Test]
    public async Task CommentAtExactlyTheMaximumIsAccepted()
    {
        (string dbname, CommandExecutor executor) = await CreateUsersTable(PlainUsers);

        string atLimit = new('x', CamusDBConfig.MaxCommentLength);
        await ExecuteDdl(executor, dbname, $"COMMENT ON TABLE users IS '{atLimit}'");

        Assert.AreEqual(atLimit, (await GetSchemaAsync(executor, dbname, "users")).Comment);
    }
}
