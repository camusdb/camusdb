/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Core.Extensions.Inline;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.InlineFunctions;

/// <summary>
/// The rules of <see cref="InlineDefinitionRateLimiter"/> on a clock the test moves: how many
/// statements run at once, when the next one may run, who is not limited, and what a change of the
/// limit does to a count already spent.
/// </summary>
[TestFixture]
internal sealed class TestInlineDefinitionRateLimiter
{
    private long now = 1_000_000;

    private InlineDefinitionRateLimiter NewLimiter() => new(() => now);

    private static CamusDBOptions Limit(int perMinute, bool authentication = true) => new()
    {
        AuthenticationEnabled = authentication,
        MaxInlineDefinitionsPerUserPerMinute = perMinute,
    };

    private static Principal User(string name, string? id = null, bool superuser = false) =>
        new(name, superuser, [], id ?? "id-" + name);

    private static CamusDBException Refused(InlineDefinitionRateLimiter limiter, CamusDBOptions options, Principal user) =>
        Assert.Throws<CamusDBException>(() => limiter.Admit(options, user, "CREATE FUNCTION"))!;

    [Test]
    public void AUserRunsTheLimitAtOnce_ThenOneEachShareOfTheMinute()
    {
        InlineDefinitionRateLimiter limiter = NewLimiter();
        CamusDBOptions options = Limit(3);
        Principal dev = User("dev");

        for (int i = 0; i < 3; i++)
            limiter.Admit(options, dev, "CREATE FUNCTION");

        CamusDBException refused = Refused(limiter, options, dev);
        Assert.AreEqual(CamusDBErrorCodes.InlineDefinitionRateExceeded, refused.Code);
        StringAssert.Contains("'dev'", refused.Message);
        StringAssert.Contains("max_inline_definitions_per_user_per_minute", refused.Message);
        StringAssert.Contains("CREATE FUNCTION can run again in 20 s", refused.Message);

        // A refused statement adds nothing: the wait it was told is still the wait.
        now += 19_500;
        StringAssert.Contains("in 1 s", Refused(limiter, options, dev).Message);

        now += 500;
        limiter.Admit(options, dev, "CREATE FUNCTION");
        Refused(limiter, options, dev);

        // A minute without statements drains the whole count.
        now += 60_000;
        for (int i = 0; i < 3; i++)
            limiter.Admit(options, dev, "CREATE FUNCTION");
        Refused(limiter, options, dev);
    }

    [Test]
    public void EachUserHasACountOfTheirOwn_KeyedByTheUserId()
    {
        InlineDefinitionRateLimiter limiter = NewLimiter();
        CamusDBOptions options = Limit(1);

        limiter.Admit(options, User("dev"), "CREATE FUNCTION");
        limiter.Admit(options, User("ops"), "CREATE FUNCTION");

        // The same user after a rename: the id carries the count.
        Refused(limiter, options, User("dev-renamed", id: "id-dev"));

        // A user from before ids is matched by name, and never shares a count with an id.
        limiter.Admit(options, new Principal("id-dev", false, []), "CREATE FUNCTION");
        Assert.Throws<CamusDBException>(() => limiter.Admit(options, new Principal("id-dev", false, []), "CREATE TRIGGER"));
    }

    [Test]
    public void SuperusersEngineWork_AuthenticationOff_AndALimitOfZero_AreNotLimited()
    {
        InlineDefinitionRateLimiter limiter = NewLimiter();

        for (int i = 0; i < 10; i++)
        {
            limiter.Admit(Limit(1), User("root", superuser: true), "CREATE FUNCTION");
            limiter.Admit(Limit(1), null, "CREATE FUNCTION");
            limiter.Admit(Limit(1, authentication: false), User("dev"), "CREATE FUNCTION");
            limiter.Admit(Limit(0), User("ops"), "CREATE FUNCTION");
            limiter.Admit(Limit(-1), User("ops"), "CREATE FUNCTION");
        }

        Assert.AreEqual(0, limiter.TrackedUsers);
    }

    [Test]
    public void ARaisedLimitAppliesToTheNextStatement_ALoweredOneToTheCountSpent()
    {
        InlineDefinitionRateLimiter limiter = NewLimiter();
        Principal dev = User("dev");

        limiter.Admit(Limit(2), dev, "CREATE FUNCTION");
        limiter.Admit(Limit(2), dev, "CREATE FUNCTION");
        Refused(limiter, Limit(2), dev);

        limiter.Admit(Limit(4), dev, "CREATE FUNCTION");
        limiter.Admit(Limit(4), dev, "CREATE FUNCTION");
        Refused(limiter, Limit(4), dev);

        // Four spent against a limit of one: the count drains at one a minute down to zero.
        StringAssert.Contains("in 240 s", Refused(limiter, Limit(1), dev).Message);
    }

    [Test]
    public void TheMapDropsUsersWhoseCountHasDrained()
    {
        InlineDefinitionRateLimiter limiter = NewLimiter();
        CamusDBOptions options = Limit(10);

        for (int i = 0; i < InlineDefinitionRateLimiter.PruneThreshold; i++)
            limiter.Admit(options, User("u" + i), "CREATE FUNCTION");

        Assert.AreEqual(InlineDefinitionRateLimiter.PruneThreshold, limiter.TrackedUsers);

        // Still counting: nothing has drained, so nothing is dropped.
        limiter.Admit(options, User("late"), "CREATE FUNCTION");
        Assert.AreEqual(InlineDefinitionRateLimiter.PruneThreshold + 1, limiter.TrackedUsers);

        now += 6_000;
        limiter.Admit(options, User("later"), "CREATE FUNCTION");
        Assert.AreEqual(1, limiter.TrackedUsers);
    }
}

/// <summary>
/// The limit through the statements, with authentication on and a limit of one statement a minute,
/// so a second statement within the test is always refused. <c>CREATE [OR REPLACE] FUNCTION</c> and
/// <c>CREATE TRIGGER ... LANGUAGE csharp</c> add to one count; a statement refused before its compile
/// adds nothing; a body that fails to compile still counts; superusers are not limited.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestInlineDefinitionRateLimit : BaseTest
{
    private const string Body = "RETURNS INT64 LANGUAGE csharp AS $$ return n + 1; $$";

    private const string TriggerBody = "BEFORE INSERT LANGUAGE csharp AS $$ NEW!.title = \"x\"; $$";

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        InlineFunctionsEnabled = true,
        InlineTriggersEnabled = true,
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-key-padded-to-meet-the-32-byte-secret-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-pw",
        MaxInlineDefinitionsPerUserPerMinute = 1,
    };

    private static async Task<Principal> Login(CommandExecutor ex, string u, string p)
        => await ex.ResolvePrincipalAsync((await ex.LoginAsync(u, p)).Token);

    private static Task ServerDdl(CommandExecutor ex, string sql, Principal? p)
        => ex.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: "", sql: sql, parameters: null, principal: p));

    private static async Task Ddl(CommandExecutor ex, string db, string sql, Principal? p)
    {
        DatabaseDescriptor d = await ex.OpenDatabase(db);
        KvTransaction tx = await d.Transactions.BeginAsync();

        try
        {
            await ex.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null, p));
            await d.Transactions.CommitAsync(tx);
        }
        finally
        {
            await d.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static CamusDBException Refused(Func<Task> action) =>
        Assert.ThrowsAsync<CamusDBException>(async () => await action())!;

    private async Task<(CommandExecutor Executor, string Database, Principal Root)> Start(CommandExecutor? executor = null)
    {
        CommandExecutor ex = executor ?? CreateCommandExecutor();
        string db = "fnrate" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        await ex.EnsureBootstrapSuperuserAsync(Options.BootstrapSuperuser, Options.BootstrapSuperuserPassword);
        Principal root = await Login(ex, "root", "root-pw");

        await Ddl(ex, db, "CREATE TABLE posts (id INT64 PRIMARY KEY NOT NULL, title STRING)", root);
        return (ex, db, root);
    }

    /// <summary>A user with <c>CREATE</c> on the database and <c>ALTER</c> on the table, logged in.</summary>
    private static async Task<Principal> Author(CommandExecutor ex, string db, Principal root, string name)
    {
        await ServerDdl(ex, $"CREATE USER {name} IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT CREATE, SELECT ON {db}.* TO {name}", root);
        await ServerDdl(ex, $"GRANT ALTER ON {db}.posts TO {name}", root);
        return await Login(ex, name, "pw");
    }

    [Test]
    public async Task ASecondDefinitionWithinTheMinute_IsRefused_AndChangesNothing()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();
        Principal dev = await Author(ex, db, root, "dev");

        await Ddl(ex, db, $"CREATE FUNCTION bump(n INT64) {Body}", dev);

        CamusDBException refused = Refused(() => Ddl(ex, db, "CREATE OR REPLACE FUNCTION bump(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return n + 2; $$", dev));
        Assert.AreEqual(CamusDBErrorCodes.InlineDefinitionRateExceeded, refused.Code, refused.Message);
        StringAssert.Contains("CREATE OR REPLACE FUNCTION can run again in", refused.Message);
        Assert.AreEqual(429, CamusDBErrorCodes.GetHttpStatus(refused.Code));

        DatabaseDescriptor database = await ex.OpenDatabase(db);
        StringAssert.Contains("n + 1", database.Schema.Functions["bump"].Source);

        // A new name is a definition too, and so is a drop and a create again.
        Assert.AreEqual(CamusDBErrorCodes.InlineDefinitionRateExceeded, Refused(() => Ddl(ex, db, $"CREATE FUNCTION other(n INT64) {Body}", dev)).Code);
        await Ddl(ex, db, "DROP FUNCTION bump", dev);
        Assert.AreEqual(CamusDBErrorCodes.InlineDefinitionRateExceeded, Refused(() => Ddl(ex, db, $"CREATE FUNCTION bump(n INT64) {Body}", dev)).Code);
        Assert.IsFalse(database.Schema.Functions.ContainsKey("bump"));
    }

    [Test]
    public async Task FunctionsAndInlineTriggers_ShareOneCount()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        Principal functionsFirst = await Author(ex, db, root, "fnfirst");
        await Ddl(ex, db, $"CREATE FUNCTION bump(n INT64) {Body}", functionsFirst);
        CamusDBException refused = Refused(() => Ddl(ex, db, "CREATE TRIGGER slug ON posts " + TriggerBody, functionsFirst));
        Assert.AreEqual(CamusDBErrorCodes.InlineDefinitionRateExceeded, refused.Code, refused.Message);
        StringAssert.Contains("CREATE TRIGGER can run again in", refused.Message);

        Principal triggersFirst = await Author(ex, db, root, "tgfirst");
        await Ddl(ex, db, "CREATE TRIGGER slug ON posts " + TriggerBody, triggersFirst);
        Assert.AreEqual(CamusDBErrorCodes.InlineDefinitionRateExceeded, Refused(() => Ddl(ex, db, $"CREATE FUNCTION bump2(n INT64) {Body}", triggersFirst)).Code);
    }

    [Test]
    public async Task ARefusalBeforeTheCompileAddsNothing_AFailedCompileCounts()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        // No CREATE privilege: refused before the compile, as often as the user tries.
        await ServerDdl(ex, "CREATE USER reader IDENTIFIED BY 'pw'", root);
        await ServerDdl(ex, $"GRANT SELECT ON {db}.* TO reader", root);
        Principal reader = await Login(ex, "reader", "pw");

        for (int i = 0; i < 3; i++)
            Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege, Refused(() => Ddl(ex, db, $"CREATE FUNCTION bump(n INT64) {Body}", reader)).Code);

        // A name a built-in function has, and a type that is not accepted: refused before the compile.
        await ServerDdl(ex, $"GRANT CREATE ON {db}.* TO reader", root);
        reader = await Login(ex, "reader", "pw");
        Assert.AreEqual(CamusDBErrorCodes.FunctionAlreadyExists, Refused(() => Ddl(ex, db, $"CREATE FUNCTION lower(n INT64) {Body}", reader)).Code);
        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionRefused, Refused(() => Ddl(ex, db, "CREATE FUNCTION bump(n DATE) RETURNS INT64 LANGUAGE csharp AS $$ return 1; $$", reader)).Code);

        await Ddl(ex, db, $"CREATE FUNCTION bump(n INT64) {Body}", reader);

        // A body that does not compile used a compile, so it counts.
        Principal dev = await Author(ex, db, root, "dev");
        Assert.AreEqual(CamusDBErrorCodes.InlineFunctionCompileFailed, Refused(() => Ddl(ex, db, "CREATE FUNCTION broken(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return n +; $$", dev)).Code);
        Assert.AreEqual(CamusDBErrorCodes.InlineDefinitionRateExceeded, Refused(() => Ddl(ex, db, $"CREATE FUNCTION fixed(n INT64) {Body}", dev)).Code);
    }

    [Test]
    public async Task ASuperuserIsNotLimited()
    {
        (CommandExecutor ex, string db, Principal root) = await Start();

        for (int i = 0; i < 4; i++)
            await Ddl(ex, db, $"CREATE OR REPLACE FUNCTION bump(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return n + {i}; $$", root);

        await Ddl(ex, db, "CREATE TRIGGER slug ON posts " + TriggerBody, root);
    }

    [Test]
    public async Task APublishedLimitAppliesToTheNextStatement()
    {
        CamusDBOptions initial = Options;
        CamusDBOptionsHolder holder = new(initial);

        (CommandExecutor ex, string db, Principal root) = await Start(CreateCommandExecutor(initial, holder));
        Principal dev = await Author(ex, db, root, "dev");

        await Ddl(ex, db, $"CREATE FUNCTION f1(n INT64) {Body}", dev);
        Assert.AreEqual(CamusDBErrorCodes.InlineDefinitionRateExceeded, Refused(() => Ddl(ex, db, $"CREATE FUNCTION f2(n INT64) {Body}", dev)).Code);

        // Raised to three: two more now, the count already holds one.
        holder.Publish(initial with { MaxInlineDefinitionsPerUserPerMinute = 3 });
        await Ddl(ex, db, $"CREATE FUNCTION f2(n INT64) {Body}", dev);
        await Ddl(ex, db, $"CREATE FUNCTION f3(n INT64) {Body}", dev);
        Assert.AreEqual(CamusDBErrorCodes.InlineDefinitionRateExceeded, Refused(() => Ddl(ex, db, $"CREATE FUNCTION f4(n INT64) {Body}", dev)).Code);

        holder.Publish(initial with { MaxInlineDefinitionsPerUserPerMinute = 0 });
        await Ddl(ex, db, $"CREATE FUNCTION f4(n INT64) {Body}", dev);
        await Ddl(ex, db, $"CREATE FUNCTION f5(n INT64) {Body}", dev);
    }

    [Test]
    public async Task WithAuthenticationOff_NothingIsLimited()
    {
        CommandExecutor ex = CreateCommandExecutor(Options with { AuthenticationEnabled = false });
        string db = "fnrateoff" + Guid.NewGuid().ToString("n");
        await ex.CreateDatabase(new CreateDatabaseTicket(name: db, ifNotExists: false));
        TrackDatabase(db, ex);

        for (int i = 0; i < 3; i++)
            await Ddl(ex, db, $"CREATE OR REPLACE FUNCTION bump(n INT64) RETURNS INT64 LANGUAGE csharp AS $$ return n + {i}; $$", null);
    }
}
