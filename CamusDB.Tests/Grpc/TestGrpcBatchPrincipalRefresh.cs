/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Catalogs;
using CamusDB.Grpc;
using CamusDB.App.Grpc;
using CamusDB.App.Services;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Grpc;

/// <summary>
/// A batched gRPC stream must not carry the authorization it was opened with for the whole life of the
/// stream.
///
/// <para><b>This is the regression the feature was reported as.</b> <c>BatchExecute</c> is long-lived
/// by design — a multiplexing client keeps one open for a whole session — and it resolved the principal
/// once, at the top of the call. Every operation on that stream then ran under the privileges the
/// client had when it connected. Since re-login is the only token refresh, the practical staleness was
/// the token's own lifetime, which is why a <c>GRANT</c> appeared to take about fifteen minutes to
/// reach such a client. Every other transport resolves per request and was never affected, which is
/// exactly why an engine-level test could not see this.</para>
///
/// <para>The second half of the same defect is revocation: a session that was logged out, or an account
/// that was dropped, kept working on an open stream. Both directions are covered below.</para>
///
/// <para>Serial: boots an embedded Kahuna node per test.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestGrpcBatchPrincipalRefresh : BaseTest
{
    /// <summary>
    /// Authentication on, with an authorization cache that will not expire during a test. The long
    /// lifetime is what makes this a real test: with the one-second default, a stream would pick the
    /// change up from the expiry alone and the refresh under test would prove nothing.
    /// </summary>
    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) => defaults with
    {
        AuthenticationEnabled = true,
        AccessTokenServerKey = "test-grpc-key-padded-to-meet-the-32-byte-secret-floor",
        BootstrapSuperuser = "root",
        BootstrapSuperuserPassword = "root-password",
        AuthenticationCacheTtl = TimeSpan.FromMinutes(30),
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
    /// One open <c>BatchExecute</c> call, driven request by request, standing in for the duplex stream
    /// a multiplexing client holds. Operations are sent one at a time and drained to their terminator,
    /// so each assertion is about a single operation on a stream that was opened earlier.
    /// </summary>
    private sealed class OpenBatchStream : IAsyncDisposable
    {
        private readonly ChannelAsyncStreamReader<BatchExecuteRequest> requests = new();
        private readonly Channel<BatchExecuteResponse> responses = Channel.CreateUnbounded<BatchExecuteResponse>();
        private readonly Task server;
        private int nextRequestId;

        internal OpenBatchStream(CamusSqlService service, string bearer)
        {
            TestServerCallContext context = new();
            context.RequestHeaders.Add("authorization", $"Bearer {bearer}");

            ChannelServerStreamWriter<BatchExecuteResponse> writer = new(responses);

            server = Task.Run(async () =>
            {
                try { await service.BatchExecute(requests, writer, context); }
                finally { responses.Writer.TryComplete(); }
            });
        }

        /// <summary>
        /// Sends one query and returns its terminal response — the completion when it ran, or the error
        /// when it was refused. Schema and row messages before the terminator are drained.
        /// </summary>
        internal async Task<BatchExecuteResponse> QueryAsync(string database, string sql)
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

            throw new InvalidOperationException("the stream ended before the operation terminated");
        }

        public async ValueTask DisposeAsync()
        {
            requests.Complete();
            try { await server; } catch { /* teardown */ }
        }
    }

    private async Task<(string database, string rootToken)> SetupAsync()
    {
        await serviceExecutor.EnsureBootstrapSuperuserAsync("root", "root-password");
        string rootToken = (await serviceExecutor.LoginAsync("root", "root-password")).Token;

        TestServerCallContext asRoot = new();
        asRoot.RequestHeaders.Add("authorization", $"Bearer {rootToken}");

        string database = "grpcbatchdb" + Guid.NewGuid().ToString("n");
        await service.ExecuteDdl(new SqlRequest { Sql = $"CREATE DATABASE {database}" }, asRoot);
        TrackDatabase(database, serviceExecutor);

        await service.ExecuteDdl(
            new SqlRequest { Database = database, Sql = "CREATE TABLE items (id int64 PRIMARY KEY NOT NULL)" }, asRoot);

        return (database, rootToken);
    }

    private async Task AsRootAsync(string rootToken, string sql, string database = "")
    {
        TestServerCallContext asRoot = new();
        asRoot.RequestHeaders.Add("authorization", $"Bearer {rootToken}");
        await service.ExecuteDdl(new SqlRequest { Database = database, Sql = sql }, asRoot);
    }

    // ─── The regression ───────────────────────────────────────────────────────

    /// <summary>
    /// A grant made after the stream opened must reach it, on the stream's next operation, without the
    /// client reconnecting or logging in again.
    /// </summary>
    [Test]
    public async Task AGrantReachesAStreamThatWasAlreadyOpen()
    {
        (string database, string rootToken) = await SetupAsync();

        await AsRootAsync(rootToken, "CREATE USER app IDENTIFIED BY 'Pw123456789012'");
        string appToken = (await serviceExecutor.LoginAsync("app", "Pw123456789012")).Token;

        await using OpenBatchStream stream = new(service, appToken);

        BatchExecuteResponse refused = await stream.QueryAsync(database, "SELECT id FROM items");
        Assert.IsNotNull(refused.Error, "the account holds no grant yet");
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege.ToString(), refused.Error.Code);

        await AsRootAsync(rootToken, $"GRANT SELECT ON {database}.* TO app");

        BatchExecuteResponse admitted = await stream.QueryAsync(database, "SELECT id FROM items");
        Assert.IsNull(admitted.Error, admitted.Error?.Message);
        Assert.IsNotNull(admitted.QueryComplete, "the grant must reach the open stream's next operation");
    }

    /// <summary>
    /// The direction that matters more. A revoke must stop an operation on a stream that is already
    /// open — an authorization snapshot pinned at connect time is a revocation that never lands.
    /// </summary>
    [Test]
    public async Task ARevokeStopsAStreamThatWasAlreadyOpen()
    {
        (string database, string rootToken) = await SetupAsync();

        await AsRootAsync(rootToken, "CREATE USER app IDENTIFIED BY 'Pw123456789012'");
        await AsRootAsync(rootToken, $"GRANT SELECT ON {database}.* TO app");
        string appToken = (await serviceExecutor.LoginAsync("app", "Pw123456789012")).Token;

        await using OpenBatchStream stream = new(service, appToken);

        BatchExecuteResponse admitted = await stream.QueryAsync(database, "SELECT id FROM items");
        Assert.IsNull(admitted.Error, admitted.Error?.Message);

        await AsRootAsync(rootToken, $"REVOKE SELECT ON {database}.* FROM app");

        BatchExecuteResponse refused = await stream.QueryAsync(database, "SELECT id FROM items");
        Assert.IsNotNull(refused.Error, "the revoke must reach the open stream's next operation");
        Assert.AreEqual(CamusDBErrorCodes.InsufficientPrivilege.ToString(), refused.Error.Code);
    }

    /// <summary>
    /// <c>FLUSH SESSIONS</c> ends the session a stream is running under, and the stream must stop
    /// working. It is the statement an operator reaches for when a client is believed to be holding
    /// authority it should not have, so a stream that survived it would defeat the whole point.
    /// </summary>
    [Test]
    public async Task FlushSessionsEndsAStreamThatWasAlreadyOpen()
    {
        (string database, string rootToken) = await SetupAsync();

        await AsRootAsync(rootToken, "CREATE USER app IDENTIFIED BY 'Pw123456789012'");
        await AsRootAsync(rootToken, $"GRANT SELECT ON {database}.* TO app");
        string appToken = (await serviceExecutor.LoginAsync("app", "Pw123456789012")).Token;

        await using OpenBatchStream stream = new(service, appToken);

        BatchExecuteResponse admitted = await stream.QueryAsync(database, "SELECT id FROM items");
        Assert.IsNull(admitted.Error, admitted.Error?.Message);

        await AsRootAsync(rootToken, "FLUSH SESSIONS");

        // The stream's own token no longer names a session, so the re-resolve fails and the call ends
        // rather than serving another operation on authority that was revoked.
        Assert.CatchAsync(async () => await stream.QueryAsync(database, "SELECT id FROM items"),
            "a stream whose session was ended must stop working");
    }
}
