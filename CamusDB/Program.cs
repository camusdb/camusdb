
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB;
using CamusDB.Core;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.App.Services;
using CommandLine;
using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Configuration;
using Kommander;
using Kommander.Communication.Grpc;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

Console.WriteLine("   ____                          ____  ____  ");
Console.WriteLine("  / ___|__ _ _ __ ___  _   _ ___|  _ \\| __ ) ");
Console.WriteLine(" | |   / _` | '_ ` _ \\| | | / __| | | |  _ \\ ");
Console.WriteLine(" | |__| (_| | | | | | | |_| \\__ \\ |_| | |_) |");
Console.WriteLine("  \\____\\__,_|_| |_| |_|\\__,_|___/____/|____/ ");
Console.WriteLine();

// Parse CLI flags; fall back to defaults on parse failure so the server still starts.
ParserResult<CamusCommandLineOptions> optsResult = Parser.Default.ParseArguments<CamusCommandLineOptions>(args);
CamusCommandLineOptions opts = optsResult.Value ?? new();

// Read and merge config before building the host so cluster-mode detection
// can gate DI service registration. CAMUS_CONFIG_PATH lets an orchestration script (e.g. the
// bottleneck-snapshot harness) supply its own config file without mutating the checked-in default.
string configPath = Environment.GetEnvironmentVariable("CAMUS_CONFIG_PATH") ?? "Config/config.yml";
string configYml = await File.ReadAllTextAsync(configPath);
ConfigDefinition config = new ConfigReader().Read(configYml);

// CLI > env > YAML > default — only explicitly provided flags override YAML.
ConfigResolver.ApplyCliOverrides(config, opts.ToOverrides());
config.Validate();
ConfigResolver.ApplyToCamusDBConfig(config);

// Diagnostics are opt-in and standalone-only this phase: enable the Core Meter/ActivitySource gate
// only for a standalone node with diagnostics turned on. When false, every ServerDiagnostics record
// helper short-circuits, so an unconfigured or cluster node emits nothing and pays no cost.
bool diagnosticsActive = !config.IsClusterMode && config.Diagnostics.Enabled;
CamusDB.Core.Diagnostics.ServerDiagnostics.Enabled = diagnosticsActive;

// Build the singleton query-result cache once, after config statics are set.
// When the feature is off we pass null (not NullQueryResultCache.Instance): every hot-path
// guard is `_cache is not null` / `database.Cache is { }`, so null short-circuits the whole
// cache/gate protocol on the read and write paths with zero overhead — which is what the
// KvTransactionsManager._cache contract ("null when disabled → gate calls skipped") assumes.
// QueryResultCache(sweepIntervalMs: 0) reads QueryResultCacheSweepIntervalMs from config.
IQueryResultCache? queryResultCache = CamusDBConfig.QueryResultCacheEnabled
    ? new QueryResultCache(sweepIntervalMs: 0)
    : null;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.ListenAnyIP(config.HttpPort);
    if (!string.IsNullOrEmpty(config.HttpsCertificate))
        kestrel.ListenAnyIP(config.HttpsPort, o => o.UseHttps(config.HttpsCertificate));
    if (config.IsClusterMode)
    {
        kestrel.ListenAnyIP(config.RaftPort, o =>
        {
            o.Protocols = HttpProtocols.Http2;
            if (!string.IsNullOrEmpty(config.RaftCertificate))
                o.UseHttps(config.RaftCertificate);
        });
    }
    // Dedicated client-facing gRPC port — HTTP/2 only. Plaintext (h2c) is fine for local dev;
    // set grpc_cert (reuses raft_certificate) for TLS in exposed deployments.
    if (config.GrpcEnabled)
    {
        kestrel.ListenAnyIP(config.GrpcPort, o =>
        {
            o.Protocols = HttpProtocols.Http2;
            if (!string.IsNullOrEmpty(config.RaftCertificate))
                o.UseHttps(config.RaftCertificate);
        });
    }
});

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
});
builder.Logging.AddFilter("Kahuna", LogLevelResolver.FromEnvironment("CAMUS_LOG_LEVEL_KAHUNA") ?? LogLevel.Warning);
builder.Logging.AddFilter("Kommander", LogLevelResolver.FromEnvironment("CAMUS_LOG_LEVEL_KOMMANDER") ?? LogLevel.Warning);

// A client that disconnects or cancels mid-stream makes Kestrel abort the request stream, and the
// gRPC handler reports that as an informational "Error reading message" / "canceled" entry with a
// full IOException stack trace. Closing a connection is normal client behavior, not a server fault,
// so those entries are pure noise; raising the floor to Warning drops them while keeping genuine
// handler failures (logged at Error) visible.
builder.Logging.AddFilter("Grpc", LogLevelResolver.FromEnvironment("CAMUS_LOG_LEVEL_GRPC") ?? LogLevel.Warning);

// Everything without a more specific rule. Both calls are needed: the rule (null category) beats the
// "Default" entry from appsettings.json because it is registered later, while the minimum level is
// the floor consulted when no rule matches at all — setting only one of them leaves Debug/Trace
// filtered out. Applied last so an explicit per-category variable above still wins.
if (LogLevelResolver.FromEnvironment("CAMUS_LOG_LEVEL") is { } camusLogLevel)
{
    builder.Logging.SetMinimumLevel(camusLogLevel);
    builder.Logging.AddFilter(null, camusLogLevel);
}

// Escape hatch for any category the variables above do not name, e.g.
// CAMUS_LOG_FILTERS="Microsoft.AspNetCore=Debug,Kahuna.Server.KeyValues=Trace".
foreach ((string category, LogLevel level) in LogLevelResolver.ParseFilters(Environment.GetEnvironmentVariable("CAMUS_LOG_FILTERS")))
    builder.Logging.AddFilter(category, level);

// Add services to the container.
builder.Services.AddRazorPages();

if (config.IsClusterMode)
{
    builder.Services.AddSingleton<ISchemaDdlForwarder>(services =>
    {
        // Build a raft-endpoint → HTTP base-URI map from the (Peers, HttpPeers)
        // config pair.  When http_peers is populated and its count matches peers,
        // each entry gives the exact HTTP address for that node regardless of port
        // or host topology.  When http_peers is absent the resolver falls back to
        // extracting the host from the raft endpoint and appending this node's
        // HTTP port, which is correct for uniform-port clusters.
        Dictionary<string, Uri> peerEndpointMap = [];
        if (config.HttpPeers.Count == config.Peers.Count && config.HttpPeers.Count > 0)
        {
            for (int i = 0; i < config.Peers.Count; i++)
                peerEndpointMap[config.Peers[i]] = new Uri($"http://{config.HttpPeers[i]}");
        }

        int httpPort = config.HttpPort;
        ILogger<ICamusDB> resolverLogger = services.GetRequiredService<ILogger<ICamusDB>>();
        Func<string, Uri> resolver = raftEndpoint =>
        {
            if (peerEndpointMap.TryGetValue(raftEndpoint, out Uri? mapped))
                return mapped;

            // Uniform-port fallback: same host as the raft endpoint, this node's HTTP port.
            // This fires when the map is populated but the key didn't match — either because
            // http_peers was omitted (expected) or because a peers entry doesn't byte-match
            // the format Raft reports for that node (misconfiguration).  Log at Warning so
            // operators can catch the latter case; a silent miss would route DDL to the
            // wrong address.
            if (peerEndpointMap.Count > 0)
                resolverLogger.LogWarning(
                    "Raft endpoint '{RaftEndpoint}' not found in http_peers map (keys: {Keys}); " +
                    "falling back to uniform-port heuristic. If this is unexpected, verify that " +
                    "each peers entry byte-matches the format Raft reports (host:raftPort).",
                    raftEndpoint,
                    string.Join(", ", peerEndpointMap.Keys));

            string host = raftEndpoint.Contains(':') ? raftEndpoint.Split(':')[0] : raftEndpoint;
            return new Uri($"http://{host}:{httpPort}");
        };
        return new HttpSchemaDdlForwarder(new HttpClient(), resolver, resolverLogger);
    });

    builder.Services.AddSingleton<CommandExecutor>(services =>
        new CommandExecutor(
            services.GetRequiredService<CommandValidator>(),
            services.GetRequiredService<CatalogsManager>(),
            services.GetRequiredService<ILogger<ICamusDB>>(),
            services.GetRequiredService<EmbeddedKahuna>(),
            services.GetRequiredService<ISchemaDdlForwarder>(),
            isClusterMode: true,
            cache: queryResultCache
        ));
}
else
{
    builder.Services.AddSingleton<EmbeddedKahuna>(services =>
    {
        string dataPath = CamusDBConfig.DataDirectory;
        ILoggerFactory loggerFactory = services.GetRequiredService<ILoggerFactory>();
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(dataPath, CamusDBConfig.Kahuna);
        options.InitialPartitions = config.InitialPartitions;
        return new EmbeddedKahuna(options, loggerFactory);
    });

    builder.Services.AddSingleton<CommandExecutor>(services =>
        new CommandExecutor(
            services.GetRequiredService<CommandValidator>(),
            services.GetRequiredService<CatalogsManager>(),
            services.GetRequiredService<ILogger<ICamusDB>>(),
            services.GetRequiredService<EmbeddedKahuna>(),
            isClusterMode: false,
            cache: queryResultCache
        ));
}

builder.Services.AddSingleton<CommandValidator>();
builder.Services.AddSingleton<CatalogsManager>();

if (config.IsClusterMode)
    builder.Services.AddSingleton<DdlOperationIdCache>();

builder.Services.AddSingleton<HttpTransactionCoordinator>();
builder.Services.AddSingleton<CamusDB.App.Services.ForegroundRequestGauge>();

// Opt-in OpenTelemetry export (standalone only). Subscribes to the CamusDB.Server meter/activity
// source plus the embedded dependency meters ("Kahuna", "Kommander") and standard runtime/ASP.NET
// signals, so a workload run can attribute time to server handler, execution, commit, and the
// underlying KV/WAL/runtime layers. A run id supplied via CAMUS_DIAGNOSTICS_RUN_ID rides along as a
// resource attribute (never a per-request tag).
if (diagnosticsActive)
{
    string? diagnosticsRunId = Environment.GetEnvironmentVariable("CAMUS_DIAGNOSTICS_RUN_ID");

    builder.Services.AddOpenTelemetry()
        .ConfigureResource(resource =>
        {
            resource.AddService("camusdb", serviceVersion: "0.8.0");
            if (!string.IsNullOrWhiteSpace(diagnosticsRunId))
                resource.AddAttributes(new[] { new KeyValuePair<string, object>("camus.run_id", diagnosticsRunId) });
        })
        .WithMetrics(metrics =>
        {
            metrics.AddMeter(CamusDB.Core.Diagnostics.ServerDiagnostics.MeterName);
            metrics.AddMeter("Kahuna");
            metrics.AddMeter("Kommander");
            metrics.AddAspNetCoreInstrumentation();
            if (config.Diagnostics.IncludeRuntimeMetrics)
                metrics.AddRuntimeInstrumentation();
            if (config.Diagnostics.PrometheusEnabled)
                metrics.AddPrometheusExporter();
            if (!string.IsNullOrWhiteSpace(config.Diagnostics.OtlpEndpoint))
                metrics.AddOtlpExporter(otlp => otlp.Endpoint = new Uri(config.Diagnostics.OtlpEndpoint!));
        })
        .WithTracing(tracing =>
        {
            tracing.AddSource(CamusDB.Core.Diagnostics.ServerDiagnostics.MeterName);
            tracing.SetSampler(new TraceIdRatioBasedSampler(config.Diagnostics.TraceSampleRatio));
            tracing.AddAspNetCoreInstrumentation();
            if (!string.IsNullOrWhiteSpace(config.Diagnostics.OtlpEndpoint))
                tracing.AddOtlpExporter(otlp => otlp.Endpoint = new Uri(config.Diagnostics.OtlpEndpoint!));
        });
}

// Reclaims abandoned explicit transactions (client opened one and never committed/rolled back) so
// their locks — renewed forever by the range-lock heartbeat — cannot be held indefinitely.
builder.Services.AddHostedService<AbandonedTransactionReaper>();

if (config.IsClusterMode)
{
    builder.Services.AddSingleton<EmbeddedKahuna>(services =>
    {
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildCluster(config);

        ILoggerFactory loggerFactory = services.GetRequiredService<ILoggerFactory>();
        EmbeddedKahuna kahuna = EmbeddedKahuna.CreateCluster(options, config.Peers, loggerFactory);

        // Apply the validated schema two-version-gate tunables from config.
        kahuna.SchemaAckWaitTimeout = TimeSpan.FromMilliseconds(config.SchemaAckWaitTimeoutMs);
        kahuna.SchemaAckLiveNodeLease = config.SchemaAckLiveNodeLeaseMs == -1
            ? Timeout.InfiniteTimeSpan
            : TimeSpan.FromMilliseconds(config.SchemaAckLiveNodeLeaseMs);

        return kahuna;
    });

    // Expose Raft and Kahuna interfaces so gRPC services can resolve them via DI.
    builder.Services.AddSingleton<IRaft>(services =>
        services.GetRequiredService<EmbeddedKahuna>().Raft);

    builder.Services.AddSingleton<IKahuna>(services =>
        services.GetRequiredService<EmbeddedKahuna>().Kahuna);

    builder.Services.AddSingleton(new KahunaConfiguration());

    builder.Services.AddGrpc(options =>
        options.Interceptors.Add<CamusDB.App.Grpc.ForegroundRequestGaugeInterceptor>());
}

// In standalone mode AddGrpc() was not called by the cluster branch above; call it now so
// the client-facing CamusSqlService can be resolved in both modes. The Kestrel gRPC port is
// only bound when grpc_enabled is true, so the service is externally reachable only then.
if (!config.IsClusterMode)
    builder.Services.AddGrpc(options =>
    {
        options.Interceptors.Add<CamusDB.App.Grpc.ForegroundRequestGaugeInterceptor>();
        options.Interceptors.Add<CamusDB.App.Grpc.MetricsServerInterceptor>();
    });

// Initialize min threads
ThreadPool.SetMinThreads(1024, 512);

WebApplication app = builder.Build();

// Warn early when key-range sharding is enabled but InitialPartitions < 2.
// With a single partition Kahuna treats RegisterKeyRangeAsync as a no-op, so the flag is
// harmless but silent — operators must know they need ≥ 2 partitions to benefit.
if (CamusDBConfig.KeyRangeShardingEnabled && config.InitialPartitions < 2)
    app.Logger.LogWarning(
        "key_range_sharding is enabled but initial_partitions={InitialPartitions} < 2; " +
        "key-range routing is a no-op on a single-partition node. " +
        "Set initial_partitions >= 2 in config.yml to activate key-range sharding.",
        config.InitialPartitions);

if (config.IsClusterMode)
{
    app.MapGrpcRaftRoutes();
    app.MapGrpcKahunaRoutes();
}

// Client-facing gRPC services (bound only when grpc_enabled; the port is only open then).
if (config.GrpcEnabled)
{
    app.MapGrpcService<CamusDB.App.Grpc.CamusSqlService>();
    app.MapGrpcService<CamusDB.App.Grpc.CamusRowsService>();
    app.MapGrpcService<CamusDB.App.Grpc.CamusAuthService>();
}

// Prometheus scrape endpoint (standalone + diagnostics + prometheus_enabled only). It exposes
// operational metadata; protect it or bind it to a trusted interface in production.
if (diagnosticsActive && config.Diagnostics.PrometheusEnabled)
    app.MapPrometheusScrapingEndpoint(config.Diagnostics.PrometheusPath);

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    app.UseHsts();
}

if (!config.IsClusterMode && !string.IsNullOrEmpty(config.HttpsCertificate))
    app.UseHttpsRedirection();
app.UseStaticFiles();

// Track in-flight data requests so auto-analyze can back off when the node is busy with foreground
// SQL/API work (autocommit statements the explicit-transaction coordinator does not see).
app.UseMiddleware<CamusDB.App.Services.ForegroundRequestGaugeMiddleware>();

// Transport-wide authentication: rejects unauthenticated requests to every data/DDL/transaction route
// (not just /execute-sql-*) when auth is enabled, and publishes the principal for per-table enforcement.
app.UseMiddleware<CamusDB.App.Middleware.AuthenticationMiddleware>();

app.MapControllers();

app.UseRouting();

app.UseAuthorization();

app.MapRazorPages();

// Start Kestrel first so the gRPC Raft port is bound before the cluster node
// begins leader election and tries to reach peers.
await app.StartAsync();

EmbeddedKahuna sharedNode = app.Services.GetRequiredService<EmbeddedKahuna>();

if (config.IsClusterMode)
{
    // Wire the ack transport so follower applies are delivered to the leader's tracker.
    ISchemaDdlForwarder ddlForwarder = app.Services.GetRequiredService<ISchemaDdlForwarder>();
    sharedNode.SetSchemaAckForwarder(ddlForwarder);
}

await sharedNode.StartAsync();

SpillFileManager.AcquireInstanceLock(CamusDBConfig.DataDirectory);
SpillFileManager.RunStartupSweep(CamusDBConfig.DataDirectory);

// Initialize DB system
CamusStartup camus = new(
    app.Services.GetRequiredService<CommandExecutor>()
);

await camus.Initialize();

CommandExecutor commandExecutor = app.Services.GetRequiredService<CommandExecutor>();

// Authentication configuration is sourced from the environment / secret provider, never config.yml
// (a plaintext key or bootstrap password in config.yml would be a leak). The flag defaults off, so a
// deployment that sets nothing keeps today's unauthenticated behavior.
CamusDBConfig.AuthenticationEnabled = string.Equals(
    Environment.GetEnvironmentVariable("CAMUSDB_AUTH_ENABLED"), "true", StringComparison.OrdinalIgnoreCase);
CamusDBConfig.AccessTokenServerKey =
    Environment.GetEnvironmentVariable("CAMUSDB_AUTH_TOKEN_KEY") ?? CamusDBConfig.AccessTokenServerKey;
CamusDBConfig.BootstrapSuperuser =
    Environment.GetEnvironmentVariable("CAMUSDB_BOOTSTRAP_USER") ?? "";
CamusDBConfig.BootstrapSuperuserPassword =
    Environment.GetEnvironmentVariable("CAMUSDB_BOOTSTRAP_PASSWORD") ?? "";
CamusDBConfig.NodeSecret =
    Environment.GetEnvironmentVariable("CAMUSDB_NODE_SECRET") ?? "";

// Seed the bootstrap superuser when auth is enabled and the catalog is empty; fail-closed (refuse to
// start) if enabled with an empty catalog and no bootstrap secret. No-op when auth is disabled.
await commandExecutor.EnsureBootstrapSuperuserAsync();

// The bootstrap password was only needed to seed the first hash — drop it from process memory.
CamusDBConfig.BootstrapSuperuserPassword = "";

// Give the background auto-analyze scheduler a foreground-load signal so it backs off under load:
// explicit transactions plus in-flight autocommit data requests.
{
    HttpTransactionCoordinator loadCoordinator = app.Services.GetRequiredService<HttpTransactionCoordinator>();
    CamusDB.App.Services.ForegroundRequestGauge requestGauge =
        app.Services.GetRequiredService<CamusDB.App.Services.ForegroundRequestGauge>();
    commandExecutor.ForegroundLoadProbe = () => loadCoordinator.ActiveCount + requestGauge.InFlight;
}

try
{
    await app.WaitForShutdownAsync();
}
finally
{
    ILogger<ICamusDB> shutdownLogger = app.Services.GetRequiredService<ILogger<ICamusDB>>();
    shutdownLogger.LogInformation("Graceful shutdown started");

    SpillFileManager.ReleaseInstanceLock();

    await commandExecutor.DisposeAsync();
    shutdownLogger.LogInformation("Databases closed");

    if (queryResultCache is IDisposable disposableCache)
        disposableCache.Dispose();

    await app.Services.GetRequiredService<EmbeddedKahuna>().DisposeAsync();
    shutdownLogger.LogInformation("Node stopped");

    shutdownLogger.LogInformation("Graceful shutdown complete");
}
