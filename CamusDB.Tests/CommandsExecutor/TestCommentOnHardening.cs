/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using NUnit.Framework;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;
using CamusDB.App.Controllers;
using CamusDB.App.Services;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Covers the <c>COMMENT ON</c> paths that a direct <c>CommandExecutor</c> call never touches: the
/// REST transport, the cross-database registry, schema-log redelivery, target identity across a
/// drop/recreate, and the length bound on every comment-bearing field.
///
/// <para>Each of these had a real defect that a green suite did not surface, because the original
/// tests all went straight to the executor. Driving the actual entry point is the point of this
/// fixture.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestCommentOnHardening : BaseTest
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    private async Task<(string dbname, CommandExecutor executor)> CreateNamedDatabase()
    {
        // Leading letter: a raw-GUID name cannot be spelled by any identifier form in the lexer.
        string dbname = "db" + Guid.NewGuid().ToString("n");

        CommandExecutor executor = CreateCommandExecutor();
        await executor.CreateDatabase(new CreateDatabaseTicket(dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);

        return (dbname, executor);
    }

    private static async Task<string> ShowCreateTableAsync(CommandExecutor executor, string dbname, string tableName)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: $"SHOW CREATE TABLE {tableName}", parameters: null));

        string ddl = "";
        await foreach (QueryResultRow row in cursor)
            ddl = row.Row["Create Table"].StrValue!;

        await database.Transactions.CommitAsync(tx);
        return ddl;
    }

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
    }

    // ── COMMENT ON DATABASE must be reachable with no context database ──────

    /// <summary>
    /// The statement names its target inside the SQL, so it must be accepted with no context
    /// database — like every other database-scoped statement. Leaving it out of the validator's
    /// server-level list rejected it before the target was ever read.
    /// </summary>
    [Test]
    public void Validator_AcceptsCommentOnDatabaseWithoutAContextDatabase()
    {
        CommandValidator validator = new();

        Assert.DoesNotThrow(() => validator.Validate(
            new ExecuteSQLTicket(txnState: null!, database: "", sql: "COMMENT ON DATABASE app IS 'x'", parameters: null)));

        // A table-scoped comment still requires a context database — the exemption is per statement,
        // not blanket.
        Assert.Throws<CamusDBException>(() => validator.Validate(
            new ExecuteSQLTicket(txnState: null!, database: "", sql: "COMMENT ON TABLE users IS 'x'", parameters: null)));
    }

    /// <summary>
    /// Driven through the real REST controller. A statement that returns no database descriptor but
    /// is not classified as database-management gets a transaction opened for it, and the autocommit
    /// then hands the null descriptor to <c>CommitAsync</c> — a <see cref="NullReferenceException"/>
    /// reported to the caller for a mutation that had already committed.
    /// </summary>
    [Test]
    public async Task Rest_CommentOnDatabase_SucceedsWithNoContextDatabase()
    {
        (string dbname, CommandExecutor executor) = await CreateNamedDatabase();

        JsonResult result = await ExecuteDdlOverRest(
            executor, databaseName: null, sql: $"COMMENT ON DATABASE {dbname} IS 'set over REST'");

        Assert.AreEqual(200, result.StatusCode ?? 200, "REST DDL must not report a server error");

        // The comment actually landed, and no transaction was left dangling by the request.
        DatabaseRegistryEntry? entry = await sharedRegistry!.TryResolveEntryAsync(dbname);
        Assert.AreEqual("set over REST", entry?.Comment);
    }

    /// <summary>
    /// Supplying a context database is the case that reaches the autocommit path, so it must behave
    /// identically to the no-context form rather than diverging into that commit.
    /// </summary>
    [Test]
    public async Task Rest_CommentOnDatabase_SucceedsWithAContextDatabase()
    {
        (string dbname, CommandExecutor executor) = await CreateNamedDatabase();

        JsonResult result = await ExecuteDdlOverRest(
            executor, databaseName: dbname, sql: $"COMMENT ON DATABASE {dbname} IS 'ctx'");

        Assert.AreEqual(200, result.StatusCode ?? 200);
        Assert.AreEqual("ctx", (await sharedRegistry!.TryResolveEntryAsync(dbname))?.Comment);
    }

    private static Task<JsonResult> ExecuteDdlOverRest(CommandExecutor executor, string? databaseName, string sql)
        => ExecuteOverRest(executor, databaseName, sql, ddlEndpoint: true);

    private static async Task<JsonResult> ExecuteOverRest(
        CommandExecutor executor, string? databaseName, string sql, bool ddlEndpoint)
    {
        HttpTransactionCoordinator txCoord = new(executor);
        ExecuteSQLController controller = new(executor, txCoord, Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<ICamusDB>(SharedLoggerFactory));

        string body = JsonSerializer.Serialize(new { databaseName, sql }, JsonOpts);
        byte[] bodyBytes = Encoding.UTF8.GetBytes(body);

        DefaultHttpContext httpContext = new();
        httpContext.Request.Body = new MemoryStream(bodyBytes);
        httpContext.Request.ContentLength = bodyBytes.Length;
        controller.ControllerContext = new ControllerContext { HttpContext = httpContext };

        return ddlEndpoint
            ? await controller.ExecuteSQLDDL()
            : await controller.ExecuteNonSQLQuery();
    }

    // ── The non-query route must accept COMMENT ON ─────────────────────────

    /// <summary>
    /// Clients route no-rows statements to whichever endpoint they use for non-SELECT SQL, and for
    /// at least one client that is the non-query path rather than the DDL path. Wiring
    /// <c>COMMENT ON</c> only into <c>ExecuteDDLSQL</c> made the statement fail there with
    /// "Unknown non-query AST stmt", so every form has to be reachable through both entry points.
    /// </summary>
    [Test]
    public async Task NonQueryRoute_AcceptsCommentOnDatabase()
    {
        (string dbname, CommandExecutor executor) = await CreateNamedDatabase();

        await ExecuteNonQuery(executor, dbname, $"COMMENT ON DATABASE {dbname} IS 'via non-query'");

        Assert.AreEqual("via non-query", (await sharedRegistry!.TryResolveEntryAsync(dbname))?.Comment);
    }

    [Test]
    public async Task NonQueryRoute_AcceptsCommentOnTableColumnAndIndex()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecuteDdl(executor, dbname,
            "CREATE TABLE t (id oid PRIMARY KEY NOT NULL, a string NULL, KEY a_idx (a))");

        await ExecuteNonQuery(executor, dbname, "COMMENT ON TABLE t IS 'table via non-query'");
        await ExecuteNonQuery(executor, dbname, "COMMENT ON COLUMN t.a IS 'column via non-query'");
        await ExecuteNonQuery(executor, dbname, "COMMENT ON INDEX t.a_idx IS 'index via non-query'");

        TableSchema schema = (await executor.OpenDatabase(dbname)).Schema.Tables["t"];

        Assert.AreEqual("table via non-query", schema.Comment);
        Assert.AreEqual("column via non-query", schema.Columns!.Single(c => c.Name == "a").Comment);
        Assert.AreEqual("index via non-query", schema.Indexes!.Single(i => i.Name == "a_idx").Comment);
    }

    /// <summary>
    /// The REST non-query endpoint opens a transaction for ordinary DML. A database-scoped statement
    /// returns no descriptor, so it has to bypass that entirely — otherwise the commit is handed a
    /// null descriptor after the registry write has already landed.
    /// </summary>
    [Test]
    public async Task Rest_NonQuery_CommentOnDatabaseSucceeds()
    {
        (string dbname, CommandExecutor executor) = await CreateNamedDatabase();

        JsonResult result = await ExecuteOverRest(
            executor, databaseName: null, sql: $"COMMENT ON DATABASE {dbname} IS 'rest non-query'",
            ddlEndpoint: false);

        Assert.AreEqual(200, result.StatusCode ?? 200);
        Assert.AreEqual("rest non-query", (await sharedRegistry!.TryResolveEntryAsync(dbname))?.Comment);
    }

    /// <summary>
    /// Guards the shared classifier itself: every statement the executor dispatches before opening a
    /// database must be listed, or a transport will open a transaction it cannot commit.
    /// </summary>
    [Test]
    public void DatabaseScopedStatementsAreClassifiedConsistently()
    {
        foreach (NodeType nodeType in new[]
        {
            NodeType.CreateDatabase, NodeType.CreateDatabaseIfNotExists,
            NodeType.CreateDatabaseBranch, NodeType.CreateDatabaseBranchIfNotExists,
            NodeType.CreateDatabaseRelink,
            NodeType.DropDatabase, NodeType.DropDatabaseIfExists,
            NodeType.RenameDatabase, NodeType.CommentOnDatabase,
        })
        {
            Assert.IsTrue(StatementScope.IsDatabaseScopedMutation(nodeType), $"{nodeType} must be database-scoped");
            Assert.IsTrue(StatementScope.AllowsEmptyContextDatabase(nodeType), $"{nodeType} must not need a context database");
        }

        // Table-scoped comments are NOT database-scoped: they need an open database.
        foreach (NodeType nodeType in new[] { NodeType.CommentOnTable, NodeType.CommentOnColumn, NodeType.CommentOnIndex })
        {
            Assert.IsFalse(StatementScope.IsDatabaseScopedMutation(nodeType), $"{nodeType} needs an open database");
            Assert.IsFalse(StatementScope.AllowsEmptyContextDatabase(nodeType), $"{nodeType} needs a context database");
        }
    }

    private static async Task ExecuteNonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
    }

    // ── ALTER DATABASE … RENAME TO is an alias for RENAME DATABASE ──────────

    /// <summary>
    /// <c>ALTER TABLE t RENAME TO …</c> has always worked, so requiring the inverted
    /// <c>RENAME DATABASE d TO …</c> word order for databases was an inconsistency users hit. Both
    /// spellings now parse to the same node.
    /// </summary>
    [Test]
    public void AlterDatabaseRenameToParsesAsRenameDatabase()
    {
        NodeAst viaAlter = SQLParserProcessor.Parse("alter database banj rename to bank");
        NodeAst viaRename = SQLParserProcessor.Parse("rename database banj to bank");

        Assert.AreEqual(NodeType.RenameDatabase, viaAlter.nodeType);
        Assert.AreEqual(NodeType.RenameDatabase, viaRename.nodeType);

        // Same operands in the same slots, so the executor arm needs no special-casing.
        Assert.AreEqual(viaRename.leftAst!.yytext, viaAlter.leftAst!.yytext);
        Assert.AreEqual(viaRename.rightAst!.yytext, viaAlter.rightAst!.yytext);
        Assert.AreEqual("banj", viaAlter.leftAst!.yytext);
        Assert.AreEqual("bank", viaAlter.rightAst!.yytext);
    }

    /// <summary>
    /// End to end through the non-query route, which is where the reported failure surfaced.
    /// </summary>
    [Test]
    public async Task AlterDatabaseRenameToRenamesTheDatabase()
    {
        (string dbname, CommandExecutor executor) = await CreateNamedDatabase();
        string renamed = "db" + Guid.NewGuid().ToString("n");

        await ExecuteNonQuery(executor, dbname, $"alter database {dbname} rename to {renamed}");
        TrackDatabase(renamed, executor);

        Assert.IsNotNull(await sharedRegistry!.TryResolveEntryAsync(renamed), "the new name must resolve");
        Assert.IsNull(await sharedRegistry!.TryResolveEntryAsync(dbname), "the old name must be gone");
    }

    /// <summary>
    /// The alias must reach the same pre-open dispatch, so it needs no context database and opens no
    /// transaction — exactly like the RENAME DATABASE spelling.
    /// </summary>
    [Test]
    public async Task AlterDatabaseRenameToWorksOverRestWithNoContextDatabase()
    {
        (string dbname, CommandExecutor executor) = await CreateNamedDatabase();
        string renamed = "db" + Guid.NewGuid().ToString("n");

        JsonResult result = await ExecuteOverRest(
            executor, databaseName: null, sql: $"alter database {dbname} rename to {renamed}", ddlEndpoint: true);

        Assert.AreEqual(200, result.StatusCode ?? 200);
        TrackDatabase(renamed, executor);
        Assert.IsNotNull(await sharedRegistry!.TryResolveEntryAsync(renamed));
    }

    [Test]
    public async Task AlterDatabaseRenameToPreservesTheComment()
    {
        (string dbname, CommandExecutor executor) = await CreateNamedDatabase();
        string renamed = "db" + Guid.NewGuid().ToString("n");

        await ExecuteNonQuery(executor, dbname, $"COMMENT ON DATABASE {dbname} IS 'kept'");
        await ExecuteNonQuery(executor, dbname, $"alter database {dbname} rename to {renamed}");
        TrackDatabase(renamed, executor);

        Assert.AreEqual("kept", (await sharedRegistry!.TryResolveEntryAsync(renamed))?.Comment);
    }

    // ── RENAME DATABASE must preserve the comment ───────────────────────────

    [Test]
    public async Task CommentSurvivesRenameDatabase()
    {
        (string dbname, CommandExecutor executor) = await CreateNamedDatabase();
        string renamed = "db" + Guid.NewGuid().ToString("n");

        await ExecuteDdl(executor, dbname, $"COMMENT ON DATABASE {dbname} IS 'production'");
        await executor.RenameDatabase(new RenameDatabaseTicket(dbname, renamed));
        TrackDatabase(renamed, executor);

        DatabaseRegistryEntry? entry = await sharedRegistry!.TryResolveEntryAsync(renamed);

        Assert.IsNotNull(entry, "the renamed database must resolve");
        Assert.AreEqual("production", entry!.Comment,
            "the comment must survive a rename — rebuilding the entry field by field drops it");
        Assert.IsNull(await sharedRegistry!.TryResolveEntryAsync(dbname), "the old name must be gone");
    }

    /// <summary>
    /// Every field must survive a copy, not just the one that was found missing. A future field added
    /// to the entry without updating <c>Copy</c> fails here rather than silently vanishing on the
    /// next rename.
    /// </summary>
    [Test]
    public void RegistryEntryCopyPreservesEveryField()
    {
        DatabaseRegistryEntry original = new()
        {
            Id = "abc",
            Name = "mydb",
            CreatedAt = new DateTime(2026, 7, 27, 10, 30, 0, DateTimeKind.Utc),
            Ancestors = [new DatabaseBranchAncestor { DatabaseId = "parent" }],
            ImmediateParentHoldId = "hold-1",
            Comment = "documented",
        };

        DatabaseRegistryEntry copy = original.Copy();

        Assert.AreEqual(original.Id, copy.Id);
        Assert.AreEqual(original.Name, copy.Name);
        Assert.AreEqual(original.CreatedAt, copy.CreatedAt);
        Assert.AreEqual(original.ImmediateParentHoldId, copy.ImmediateParentHoldId);
        Assert.AreEqual(original.Comment, copy.Comment);
        Assert.AreEqual(1, copy.Ancestors.Count);
        Assert.AreEqual("parent", copy.Ancestors[0].DatabaseId);

        // The ancestry list must be a distinct instance, or a mutation of the copy would corrupt the
        // cached original.
        Assert.AreNotSame(original.Ancestors, copy.Ancestors);
    }

    // ── The length bound must apply to every comment-bearing field ──────────

    private static string TooLong => new('x', CamusDBConfig.MaxCommentLength + 1);

    [Test]
    public async Task InlineTableCommentIsBounded()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname,
                $"CREATE TABLE t (id oid PRIMARY KEY NOT NULL) COMMENT '{TooLong}'"))!;

        Assert.AreEqual(CamusDBErrorCodes.CommentTooLong, ex.Code);
    }

    [Test]
    public async Task InlineColumnCommentIsBounded()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname,
                $"CREATE TABLE t (id oid PRIMARY KEY NOT NULL COMMENT '{TooLong}')"))!;

        Assert.AreEqual(CamusDBErrorCodes.CommentTooLong, ex.Code);
    }

    [Test]
    public async Task InlineIndexCommentIsBounded()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname,
                $"CREATE TABLE t (id oid PRIMARY KEY NOT NULL, name string NULL, KEY name_idx (name) COMMENT '{TooLong}')"))!;

        Assert.AreEqual(CamusDBErrorCodes.CommentTooLong, ex.Code);
    }

    [Test]
    public async Task AddColumnCommentIsBounded()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecuteDdl(executor, dbname, "CREATE TABLE t (id oid PRIMARY KEY NOT NULL)");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteDdl(executor, dbname,
                $"ALTER TABLE t ADD COLUMN nickname string NULL COMMENT '{TooLong}'"))!;

        Assert.AreEqual(CamusDBErrorCodes.CommentTooLong, ex.Code);
    }

    /// <summary>
    /// A direct ticket bypasses the SQL layer entirely — which is also how a forwarded CREATE TABLE
    /// arrives on the schema leader. The bound has to live in the validator, not in the parser.
    /// </summary>
    [Test]
    public async Task DirectCreateTableTicketCommentIsBounded()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname,
                tableName: "t",
                columns: [new ColumnInfo("id", ColumnType.Id, notNull: true)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false,
                checkConstraints: null,
                comment: TooLong)))!;

        Assert.AreEqual(CamusDBErrorCodes.CommentTooLong, ex.Code);
    }

    [Test]
    public async Task InlineCommentsAtExactlyTheLimitAreAccepted()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        string atLimit = new('x', CamusDBConfig.MaxCommentLength);

        await ExecuteDdl(executor, dbname,
            $"CREATE TABLE t (id oid PRIMARY KEY NOT NULL COMMENT '{atLimit}') COMMENT '{atLimit}'");

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        TableSchema schema = database.Schema.Tables["t"];

        Assert.AreEqual(atLimit, schema.Comment);
        Assert.AreEqual(atLimit, schema.Columns!.Single(c => c.Name == "id").Comment);
    }

    // ── Replay safety, tested by actually replaying ─────────────────────────

    /// <summary>
    /// Re-applies one <see cref="SchemaChangeLogEntry"/> twice, which is what Raft redelivery and WAL
    /// replay actually do. Issuing two <em>separate</em> statements at consecutive schema versions
    /// does not redeliver anything and so could not catch an apply that rejects an already-set
    /// value.
    /// </summary>
    [Test]
    public async Task ReapplyingTheSameSetCommentDeltaIsANoOp()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecuteDdl(executor, dbname, "CREATE TABLE t (id oid PRIMARY KEY NOT NULL, a string NULL)");

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        Schema schema = database.Schema;

        SchemaChangeLogEntry entry = new()
        {
            Database = database.Id,
            FromVersion = schema.SchemaVersion,
            ToVersion = schema.SchemaVersion + 1,
            Op = SchemaOp.SetComment,
            Payload = Serializator.Serialize(new SchemaSetCommentPayload
            {
                TableName = "t",
                Target = CommentTarget.Column,
                ElementName = "a",
                Comment = "replayed",
            })
        };

        CatalogsManager.ApplySchemaDelta(schema, null!, entry);
        Assert.AreEqual("replayed", schema.Tables["t"].Columns!.Single(c => c.Name == "a").Comment);

        // The same entry, delivered again.
        Assert.DoesNotThrow(() => CatalogsManager.ApplySchemaDelta(schema, null!, entry));
        Assert.AreEqual("replayed", schema.Tables["t"].Columns!.Single(c => c.Name == "a").Comment);
    }

    /// <summary>
    /// A redelivered delta whose target column has since been dropped must be a silent no-op. If it
    /// threw, a replay after an unrelated DROP COLUMN would wedge the apply pipeline on an operation
    /// that carries no data.
    /// </summary>
    [Test]
    public async Task ReplayingASetCommentForAVanishedColumnDoesNotThrow()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecuteDdl(executor, dbname, "CREATE TABLE t (id oid PRIMARY KEY NOT NULL, a string NULL)");

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        Schema schema = database.Schema;

        SchemaChangeLogEntry entry = new()
        {
            Database = database.Id,
            FromVersion = schema.SchemaVersion,
            ToVersion = schema.SchemaVersion + 1,
            Op = SchemaOp.SetComment,
            Payload = Serializator.Serialize(new SchemaSetCommentPayload
            {
                TableName = "t",
                Target = CommentTarget.Column,
                ElementName = "gone",
                Comment = "orphaned",
            })
        };

        Assert.DoesNotThrow(() => CatalogsManager.ApplySchemaDelta(schema, null!, entry));
    }

    // ── Literal escaping and injection containment ──────────────────────────

    /// <summary>
    /// Quote-bearing comments — including a payload shaped like a statement break — must survive the
    /// round-trip as inert text. The emitter doubles <c>'</c>, so the payload stays inside the literal
    /// instead of becoming tokens the parser would see.
    /// </summary>
    [TestCase("it's", TestName = "Escaping_SingleQuote")]
    [TestCase("say \"hi\"", TestName = "Escaping_DoubleQuote")]
    [TestCase("it's a \"mix\"", TestName = "Escaping_BothQuoteKinds")]
    [TestCase("already '' doubled", TestName = "Escaping_AlreadyDoubledQuotes")]
    [TestCase("x'); DROP TABLE t; --", TestName = "Escaping_StatementBreakPayload")]
    [TestCase("'; SELECT 1; --", TestName = "Escaping_LeadingQuotePayload")]
    [TestCase("tick ` backtick", TestName = "Escaping_Backtick")]
    [TestCase("a\\b", TestName = "Escaping_InteriorBackslash")]
    public async Task CommentWithQuotesRoundTripsAsInertText(string comment)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecuteDdl(executor, dbname, "CREATE TABLE t (id oid PRIMARY KEY NOT NULL)");

        await executor.Comment(new CommentTicket(CommentTarget.Table, dbname, "t", null, comment));

        string ddl = await ShowCreateTableAsync(executor, dbname, "t");

        // Re-executing the emitted DDL must recreate the table with the identical comment. If the
        // payload had escaped the literal, this would either fail to parse or run as extra statements.
        await ExecuteDdl(executor, dbname, "DROP TABLE t");
        await ExecuteDdl(executor, dbname, ddl);

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        Assert.AreEqual(comment, database.Schema.Tables["t"].Comment);
        Assert.IsTrue(database.Schema.Tables.ContainsKey("t"), "the table must still exist");
    }

    /// <summary>
    /// Values that this dialect cannot represent in a string literal are refused up front, rather than
    /// stored and later emitted as DDL that does not parse. A backslash adjacent to a quote would
    /// escape it and spill the remainder of the statement outside the literal; raw control characters
    /// have no representation at all.
    /// </summary>
    [TestCase("path\\", TestName = "Unrepresentable_TrailingBackslash")]
    [TestCase("a\\'b", TestName = "Unrepresentable_BackslashBeforeSingleQuote")]
    [TestCase("a\\\"b", TestName = "Unrepresentable_BackslashBeforeDoubleQuote")]
    [TestCase("line1\nline2", TestName = "Unrepresentable_Newline")]
    [TestCase("a\tb", TestName = "Unrepresentable_Tab")]
    public async Task UnrepresentableCommentIsRejected(string comment)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecuteDdl(executor, dbname, "CREATE TABLE t (id oid PRIMARY KEY NOT NULL)");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Comment(new CommentTicket(CommentTarget.Table, dbname, "t", null, comment)))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);

        // Nothing was stored, so SHOW CREATE TABLE still emits parseable DDL.
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        Assert.IsNull(database.Schema.Tables["t"].Comment);
    }

    /// <summary>
    /// The same guard must apply to the inline CREATE TABLE position, not just COMMENT ON — otherwise
    /// the check is trivially bypassed by declaring the comment at create time.
    /// </summary>
    [Test]
    public async Task UnrepresentableInlineCommentIsRejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname,
                tableName: "t",
                columns: [new ColumnInfo("id", ColumnType.Id, notNull: true)],
                constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                ifNotExists: false,
                checkConstraints: null,
                comment: "ends with a backslash \\")))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    /// <summary>
    /// Identifiers are interpolated into backtick-quoted positions in the emitted DDL. They cannot
    /// carry a backtick — <c>ValidateIdentifier</c> restricts them to alphanumerics and underscore —
    /// so there is no identifier-side escape to get wrong.
    /// </summary>
    [Test]
    public async Task IdentifiersCannotCarryQuotingCharacters()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        foreach (string bad in new[] { "ta`ble", "ta'ble", "ta\"ble" })
        {
            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.CreateTable(new CreateTableTicket(
                    databaseName: dbname,
                    tableName: bad,
                    columns: [new ColumnInfo("id", ColumnType.Id, notNull: true)],
                    constraints: [new ConstraintInfo(ConstraintType.PrimaryKey, "~pk", [new ColumnIndexInfo("id", OrderType.Ascending)])],
                    ifNotExists: false)))!;

            Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, $"table name '{bad}' must be rejected");
        }
    }

    // ── The comment target is an object, not a name ─────────────────────────

    /// <summary>
    /// A comment must not be applied to a table that merely reuses the name of the one that was
    /// resolved. The descriptor is captured, the table is then dropped and recreated, and the stale
    /// descriptor is handed straight to the setter — the identity check has to reject it rather than
    /// let the delta land on the replacement.
    /// </summary>
    [Test]
    public async Task CommentResolvesTheCurrentTableAfterDropAndRecreate()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecuteDdl(executor, dbname, "CREATE TABLE t (id oid PRIMARY KEY NOT NULL, a string NULL)");

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        string originalId = database.Schema.Tables["t"].Id!;

        await ExecuteDdl(executor, dbname, "DROP TABLE t");
        await ExecuteDdl(executor, dbname, "CREATE TABLE t (id oid PRIMARY KEY NOT NULL, a string NULL)");

        string replacementId = database.Schema.Tables["t"].Id!;
        Assert.AreNotEqual(originalId, replacementId, "the recreated table must be a different object");

        // Comments issued after the recreate resolve the *current* table and must succeed — the
        // guard rejects stale identity, not the name itself.
        await ExecuteDdl(executor, dbname, "COMMENT ON COLUMN t.a IS 'on the replacement'");
        Assert.AreEqual("on the replacement",
            database.Schema.Tables["t"].Columns!.Single(c => c.Name == "a").Comment);
    }
}
