/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using Grpc.Core;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Auth;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs;
using CamusDB.Grpc;
using CamusDB.App.Grpc;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// A batched gRPC stream presents its token once, when it opens, and is meant to stay open for a whole
/// client session — far longer than a token lives. It must therefore survive its token's ordinary
/// expiry, and must still end when the session is logged out or revoked, the account's password
/// changes, or the account is dropped.
///
/// <para><b>The failure this guards against.</b> Re-resolving the opening token per operation ended
/// every healthy stream one token lifetime after it opened, together with every transaction pinned to
/// it, and ended it as a raw handler exception the client could not recognise as "log in again".</para>
///
/// <para><b>Why these settings.</b> A three-second token, so a test can watch a stream cross its
/// expiry. A zero authorization cache, so every operation takes the refresh path rather than being
/// answered from a snapshot that would hide the defect. Zero retention of expired sessions, so the
/// sweep a test runs deletes the record at once — the harshest schedule the stream can meet.</para>
///
/// <para>Serial: boots an embedded Kahuna node per test, and is sensitive to wall-clock time.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestGrpcBatchStreamOutlivesToken : BaseTest
{
    private const string AppPassword = "Pw123456789012";

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-grpc-key-padded-to-meet-the-32-byte-secret-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-password",
        AccessTokenTtl = TimeSpan.FromSeconds(3),
        AuthenticationCacheTtl = TimeSpan.Zero,
        ExpiredSessionRetentionMs = 0,
    };

    private CamusSqlService service = null!;
    private CommandExecutor serviceExecutor = null!;

    [SetUp]
    public void SetUpServices()
    {
        CommandValidator validator = new(Options);
        CatalogsManager catalogsManager = new(logger);
        serviceExecutor = new(validator, catalogsManager, logger, Options,
            sharedNode: TestNode!, registry: sharedRegistry!, isClusterMode: false);
        service = new(serviceExecutor, new HttpTransactionCoordinator(serviceExecutor), logger,
            TestHostApplicationLifetime.Instance, new ForegroundRequestGauge(), Options);
    }

    [TearDown]
    public async Task TearDownServices()
    {
        try { await serviceExecutor.DisposeAsync(); } catch { }
    }

    // ─── Harness ──────────────────────────────────────────────────────────────

    /// <summary>
    /// One open <c>BatchExecute</c> call driven request by request. Unlike a harness that only sends,
    /// this one keeps the handler's own outcome, because how a stream <em>ends</em> is half of what is
    /// under test: the status it ends with decides whether a client logs in again.
    /// </summary>
    private sealed class OpenBatchStream : IAsyncDisposable
    {
        private readonly ChannelAsyncStreamReader<BatchExecuteRequest> requests = new();
        private readonly Channel<BatchExecuteResponse> responses = Channel.CreateUnbounded<BatchExecuteResponse>();
        private int nextRequestId;

        /// <summary>Completes when the handler returns; faults with whatever it threw.</summary>
        internal Task Handler { get; }

        internal OpenBatchStream(CamusSqlService service, string bearer)
        {
            TestServerCallContext context = new();
            context.RequestHeaders.Add("authorization", $"Bearer {bearer}");

            ChannelServerStreamWriter<BatchExecuteResponse> writer = new(responses);

            Handler = Task.Run(async () =>
            {
                try { await service.BatchExecute(requests, writer, context); }
                finally { responses.Writer.TryComplete(); }
            });
        }

        /// <summary>Sends one query and returns its terminal response, or null when the stream ended
        /// before the operation terminated.</summary>
        internal async Task<BatchExecuteResponse?> QueryAsync(string database, string sql)
        {
            int id = Interlocked.Increment(ref nextRequestId);

            requests.Push(new BatchExecuteRequest
            {
                RequestId = id,
                Kind = BatchStatementKind.Query,
                Request = new SqlRequest { Database = database, Sql = sql },
            });

            await foreach (BatchExecuteResponse response in responses.Reader.ReadAllAsync())
            {
                if (response.RequestId != id)
                    continue;

                if (response.QueryComplete is not null || response.Error is not null)
                    return response;
            }

            return null;
        }

        public async ValueTask DisposeAsync()
        {
            requests.Complete();
            try { await Handler; } catch { /* asserted by the tests that care */ }
        }
    }

    /// <summary>Root's own token lives three seconds too, so root logs in for every statement.</summary>
    private async Task AsRootAsync(string sql, string database = "")
    {
        string rootToken = (await serviceExecutor.LoginAsync("root", "root-password")).Token;

        TestServerCallContext asRoot = new();
        asRoot.RequestHeaders.Add("authorization", $"Bearer {rootToken}");
        await service.ExecuteDdl(new SqlRequest { Database = database, Sql = sql }, asRoot);
    }

    /// <summary>A database with one table, and an account that may read it.</summary>
    private async Task<string> SetupAsync()
    {
        await serviceExecutor.EnsureBootstrapSuperuserAsync("root", "root-password");

        string database = "grpcexpirydb" + Guid.NewGuid().ToString("n");
        await AsRootAsync($"CREATE DATABASE {database}");
        TrackDatabase(database, serviceExecutor);

        await AsRootAsync("CREATE TABLE items (id int64 PRIMARY KEY NOT NULL)", database);
        await AsRootAsync($"CREATE USER app IDENTIFIED BY '{AppPassword}'");
        await AsRootAsync($"GRANT SELECT ON {database}.* TO app");

        return database;
    }

    /// <summary>
    /// Waits until the token is past its expiry, with room for the stream's scheduled expiry lookup to
    /// have run, and then proves the token really is dead. Without that proof a test that "outlives the
    /// token" could pass simply because the token was still good.
    /// </summary>
    private async Task WaitPastExpiryAsync(LoginResult login)
    {
        TimeSpan remaining = login.ExpiresAt - DateTime.UtcNow;
        await Task.Delay((remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero) + TimeSpan.FromMilliseconds(800));

        Assert.ThrowsAsync<CamusDBException>(
            async () => await serviceExecutor.ResolvePrincipalAsync(login.Token),
            "the token must be expired for this test to mean anything");
    }

    private static async Task AssertEndedUnauthenticatedAsync(OpenBatchStream stream)
    {
        Task finished = await Task.WhenAny(stream.Handler, Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.AreSame(stream.Handler, finished, "the stream must end once its authority is gone");

        RpcException? ended = Assert.ThrowsAsync<RpcException>(async () => await stream.Handler);

        // The status is the contract with the client: UNAUTHENTICATED is what makes it discard its
        // token and log in again. Anything else reads as a server fault, and it retries with the same
        // rejected token for ever.
        Assert.AreEqual(StatusCode.Unauthenticated, ended!.StatusCode);
    }

    // ─── An expiry is not a revocation ────────────────────────────────────────

    /// <summary>
    /// The reported failure, end to end: a stream that idles past its token's lifetime still serves
    /// the account it authenticated, because nothing about that account was revoked.
    /// </summary>
    [Test]
    public async Task AStreamOutlivesItsTokenWhenNothingWasRevoked()
    {
        string database = await SetupAsync();
        LoginResult login = await serviceExecutor.LoginAsync("app", AppPassword);

        await using OpenBatchStream stream = new(service, login.Token);

        BatchExecuteResponse? before = await stream.QueryAsync(database, "SELECT id FROM items");
        Assert.IsNull(before?.Error, before?.Error?.Message);

        await WaitPastExpiryAsync(login);

        BatchExecuteResponse? after = await stream.QueryAsync(database, "SELECT id FROM items");
        Assert.IsNotNull(after, "the stream ended at its token's expiry");
        Assert.IsNull(after!.Error, after.Error?.Message);
        Assert.IsNotNull(after.QueryComplete);
    }

    /// <summary>
    /// The session sweep deletes an expired record, and a deleted record is also what a logout leaves
    /// behind. The stream must have settled the question before the sweep removes the evidence, which
    /// is why its expiry lookup is scheduled rather than left to its next operation — this stream sends
    /// none until the record is already gone.
    /// </summary>
    [Test]
    public async Task AnIdleStreamOutlivesItsTokenEvenAfterItsSessionRecordWasSwept()
    {
        string database = await SetupAsync();
        LoginResult login = await serviceExecutor.LoginAsync("app", AppPassword);

        await using OpenBatchStream stream = new(service, login.Token);

        await WaitPastExpiryAsync(login);

        Assert.GreaterOrEqual(await serviceExecutor.ReapExpiredSessionsAsync(), 1, "the expired session was not swept");

        BatchExecuteResponse? after = await stream.QueryAsync(database, "SELECT id FROM items");
        Assert.IsNotNull(after, "a swept session record was read as a revocation");
        Assert.IsNull(after!.Error, after.Error?.Message);
    }

    /// <summary>
    /// Outliving the token must not mean freezing the authorization it had. A stream past its expiry
    /// resolves from the account, so a revoke and a grant both still reach its next operation.
    /// </summary>
    [Test]
    public async Task GrantsAndRevokesStillReachAStreamThatOutlivedItsToken()
    {
        string database = await SetupAsync();
        LoginResult login = await serviceExecutor.LoginAsync("app", AppPassword);

        await using OpenBatchStream stream = new(service, login.Token);
        await WaitPastExpiryAsync(login);

        await AsRootAsync($"REVOKE SELECT ON {database}.* FROM app");

        BatchExecuteResponse? refused = await stream.QueryAsync(database, "SELECT id FROM items");
        Assert.IsNotNull(refused?.Error, "the revoke must reach a stream that outlived its token");
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege.ToString(), refused!.Error.Code);

        await AsRootAsync($"GRANT SELECT ON {database}.* TO app");

        BatchExecuteResponse? admitted = await stream.QueryAsync(database, "SELECT id FROM items");
        Assert.IsNull(admitted?.Error, admitted?.Error?.Message);
        Assert.IsNotNull(admitted?.QueryComplete);
    }

    // ─── A revocation is still a revocation ───────────────────────────────────

    /// <summary>
    /// The case the scheduled lookup exists for. A logout deletes the session; if the stream then
    /// stays idle until after the expiry, "the token no longer resolves" looks exactly like an ordinary
    /// expiry. The stream must end anyway, without being sent anything, and tell the client why.
    /// </summary>
    [Test]
    public async Task ALogoutEndsAnIdleStreamEvenWhenItIsOnlyNoticedAfterTheExpiry()
    {
        string database = await SetupAsync();
        LoginResult login = await serviceExecutor.LoginAsync("app", AppPassword);

        await using OpenBatchStream stream = new(service, login.Token);

        BatchExecuteResponse? before = await stream.QueryAsync(database, "SELECT id FROM items");
        Assert.IsNull(before?.Error, before?.Error?.Message);

        await serviceExecutor.LogoutAsync(login.Token);

        await AssertEndedUnauthenticatedAsync(stream);
    }

    /// <summary>A logout inside the token's lifetime ends the stream on its next operation.</summary>
    [Test]
    public async Task ALogoutEndsAStreamOnItsNextOperation()
    {
        string database = await SetupAsync();
        LoginResult login = await serviceExecutor.LoginAsync("app", AppPassword);

        await using OpenBatchStream stream = new(service, login.Token);

        await serviceExecutor.LogoutAsync(login.Token);

        Assert.IsNull(await stream.QueryAsync(database, "SELECT id FROM items"), "a logged-out stream served an operation");
        await AssertEndedUnauthenticatedAsync(stream);
    }

    /// <summary>
    /// <c>FLUSH SESSIONS</c> after the expiry. The stream's session record may already be gone, so the
    /// deletion cannot be the evidence; the catalog's revocation epoch is.
    /// </summary>
    [Test]
    public async Task FlushSessionsEndsAStreamThatOutlivedItsToken()
    {
        string database = await SetupAsync();
        LoginResult login = await serviceExecutor.LoginAsync("app", AppPassword);

        await using OpenBatchStream stream = new(service, login.Token);
        await WaitPastExpiryAsync(login);

        BatchExecuteResponse? before = await stream.QueryAsync(database, "SELECT id FROM items");
        Assert.IsNull(before?.Error, before?.Error?.Message);

        await AsRootAsync("FLUSH SESSIONS");

        Assert.IsNull(await stream.QueryAsync(database, "SELECT id FROM items"), "the stream survived FLUSH SESSIONS");
        await AssertEndedUnauthenticatedAsync(stream);
    }

    /// <summary>A password change invalidates every token the account holds, and a stream with them.</summary>
    [Test]
    public async Task APasswordChangeEndsAStreamThatOutlivedItsToken()
    {
        string database = await SetupAsync();
        LoginResult login = await serviceExecutor.LoginAsync("app", AppPassword);

        await using OpenBatchStream stream = new(service, login.Token);
        await WaitPastExpiryAsync(login);

        await AsRootAsync("ALTER USER app IDENTIFIED BY 'A-different-pw-123'");

        Assert.IsNull(await stream.QueryAsync(database, "SELECT id FROM items"), "the stream survived a password change");
        await AssertEndedUnauthenticatedAsync(stream);
    }

    /// <summary>
    /// A dropped account ends the stream, and a new account under the same name does not inherit it:
    /// the stream is bound to the account's immutable id, not to its name.
    /// </summary>
    [Test]
    public async Task ADroppedAndReCreatedAccountDoesNotInheritTheStream()
    {
        string database = await SetupAsync();
        LoginResult login = await serviceExecutor.LoginAsync("app", AppPassword);

        await using OpenBatchStream stream = new(service, login.Token);
        await WaitPastExpiryAsync(login);

        await AsRootAsync("DROP USER app");
        await AsRootAsync($"CREATE USER app IDENTIFIED BY '{AppPassword}'");
        await AsRootAsync($"GRANT SELECT ON {database}.* TO app");

        Assert.IsNull(await stream.QueryAsync(database, "SELECT id FROM items"), "a re-created account inherited the stream");
        await AssertEndedUnauthenticatedAsync(stream);
    }
}
