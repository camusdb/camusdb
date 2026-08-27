
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
using CommandLine.Text;
using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Configuration;
using Kommander;
using Kommander.Communication.Grpc;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

// `camusdb init` writes a starter configuration and exits. Dispatched on the bare first argument
// rather than through CommandLineParser verbs: verbs would change how every existing server flag is
// parsed, and the server invocation must stay byte-for-byte compatible. Handled before the banner so
// the command's output is only the paths it wrote.
if (InitCommand.Matches(args))
    return InitCommand.Run(args);

// Parse CLI flags before anything is printed or created. `--help` and `--version` are requests for
// text, not for a node: CommandLineParser reports them as parse "errors" after writing the text
// itself, so without this the process would go on to start a server the user never asked for. Any
// other parse failure (an unknown or malformed flag) exits non-zero as well — starting on defaults
// would silently ignore what the operator asked for, which is worse than refusing.
// The help text is written here rather than by the parser so an explicit `--help` lands on stdout
// (where a pager or a redirect can see it) while a genuine usage error still goes to stderr.
Parser parser = new(settings => settings.HelpWriter = null);
ParserResult<CamusCommandLineOptions> optsResult = parser.ParseArguments<CamusCommandLineOptions>(args);

if (optsResult is NotParsed<CamusCommandLineOptions> notParsed)
{
    bool textOnlyRequest = notParsed.Errors.All(
        error => error.Tag is ErrorType.HelpRequestedError or ErrorType.HelpVerbRequestedError or ErrorType.VersionRequestedError);

    HelpText helpText = HelpText.AutoBuild(
        optsResult,
        help =>
        {
            help.AddPostOptionsLine("Run 'camusdb' with no arguments to start a node, or 'camusdb init' to write");
            help.AddPostOptionsLine("a starter configuration file.");
            return HelpText.DefaultParsingErrorsHandler(optsResult, help);
        },
        example => example);

    (textOnlyRequest ? Console.Out : Console.Error).WriteLine(helpText);
    return textOnlyRequest ? 0 : 1;
}

CamusCommandLineOptions opts = optsResult.Value;

Console.WriteLine("   ____                          ____  ____  ");
Console.WriteLine("  / ___|__ _ _ __ ___  _   _ ___|  _ \\| __ ) ");
Console.WriteLine(" | |   / _` | '_ ` _ \\| | | / __| | | |  _ \\ ");
Console.WriteLine(" | |__| (_| | | | | | | |_| \\__ \\ |_| | |_) |");
Console.WriteLine("  \\____\\__,_|_| |_| |_|\\__,_|___/____/|____/ ");
Console.WriteLine();

// Read and merge config before building the host so cluster-mode detection can gate DI service
// registration. The lookup is ordered and first-hit-wins (--config, CAMUS_CONFIG_PATH, the working
// directory, the user configuration directory, built-in defaults) — see ConfigLocator for why each
// step exists. Finding no file at all is normal for a freshly installed tool and is not an error.
(ConfigDefinition config, ConfigLocation configLocation) = ConfigLocator.Load(opts.ConfigPath);

// The boot-time YAML text, captured once so runtime cluster-setting changes re-resolve against the
// configuration this node actually started with. Re-reading the file at apply time would silently
// fold in any edits made since boot, turning a SET into a partial config reload nobody asked for.
string bootConfigYaml = configLocation.Path is null ? "" : File.ReadAllText(configLocation.Path);

// CLI > env > YAML > default — only explicitly provided flags override YAML.
ConfigResolver.ApplyCliOverrides(config, opts.ToOverrides());

// Settings whose default depends on the rest of the configuration (data directory, standalone
// partition count) are filled in only where the operator said nothing.
ConfigResolver.ApplyEffectiveDefaults(config);

config.Validate();
CamusDBOptions camusOptions = ConfigResolver.ApplyToCamusDBConfig(config);

// Rebuilds the local layers (file text captured above, then CLI flags, then context-dependent
// defaults) as a fresh definition. The cluster-settings overlay is applied on top of this and the
// result re-validated and re-resolved, so a runtime change flows through the same chain a boot
// does and cluster-beats-local falls out of the ordering.
Func<ConfigDefinition> baseDefinitionFactory = () =>
{
    ConfigDefinition definition = new ConfigReader().Read(bootConfigYaml);
    ConfigResolver.ApplyCliOverrides(definition, opts.ToOverrides());
    ConfigResolver.ApplyEffectiveDefaults(definition);
    return definition;
};

// A layered lookup makes "which configuration is this node running?" unanswerable from the console
// unless the resolved source is stated, and that is the first question asked whenever a setting
// appears not to have taken effect. Printed before the host is built so it survives a later failure.
Console.WriteLine($"Configuration: {configLocation.Describe()}");
Console.WriteLine($"Data directory: {camusOptions.DataDirectory}");

// The small profile is worth stating, and stating alongside its limit: it caps the caches this node
// allocates, not the managed heap the runtime grows under a burst of large statements. With server GC
// (one heap per core, the build default) that heap is what dominates peak RSS, and no configuration
// value the node reads can change it — the setting is read by the runtime before Main. Naming the
// environment variable here is the difference between "the profile did nothing" and "the profile did
// its half".
if (camusOptions.MemoryProfile == MemoryProfile.Dev)
{
    Console.WriteLine("Memory profile: dev (caches capped at ~96 MiB; not for production)");

    if (System.Runtime.GCSettings.IsServerGC)
        Console.WriteLine("  For a smaller managed heap too, start with DOTNET_gcServer=0 in the environment.");
}

Console.WriteLine();

Directory.CreateDirectory(camusOptions.DataDirectory);

// Authentication policy is resolved here, before anything is registered or the socket is bound, so no
// component can observe a half-configured policy and no request is served while the flag still holds
// its default. (The bootstrap superuser is seeded later, after StartAsync — it needs the shared node —
// and until it exists an auth-enabled server resolves no token and fails closed, which is safe.)
// Sourced from the environment / secret provider, never config.yml: a plaintext key or bootstrap
// password in config.yml would be a leak. The flag defaults off, so a deployment that sets nothing
// keeps today's unauthenticated behavior.
// These are applied after Resolve, so their provenance has to be recorded here rather than in the
// resolver: without it they would report as built-in defaults, which for a value the operator set in
// the environment is exactly the wrong answer.
// Factored into a function because it runs twice per lifetime kind: once here at boot, and again
// on every runtime cluster-setting re-resolution — the environment layer must survive a re-resolve
// or a SET would silently strip the node's auth policy.
Func<CamusDBOptions, CamusDBOptions> applyEnvironmentSecrets = resolved =>
{
    Dictionary<string, CamusDB.Core.Config.ConfigValueSource> valueSources =
        new(resolved.ValueSources, StringComparer.OrdinalIgnoreCase);

    void RecordIfSetFromEnvironment(string environmentVariable, string variableName)
    {
        if (Environment.GetEnvironmentVariable(environmentVariable) is not null)
            valueSources[variableName] = CamusDB.Core.Config.ConfigValueSource.Environment;
    }

    RecordIfSetFromEnvironment("CAMUSDB_AUTH_ENABLED", "authentication_enabled");
    RecordIfSetFromEnvironment("CAMUSDB_AUTH_TOKEN_KEY", "access_token_server_key");
    RecordIfSetFromEnvironment("CAMUSDB_BOOTSTRAP_USER", "bootstrap_superuser");
    RecordIfSetFromEnvironment("CAMUSDB_BOOTSTRAP_PASSWORD", "bootstrap_superuser_password");
    RecordIfSetFromEnvironment("CAMUSDB_NODE_SECRET", "node_secret");

    return resolved with
    {
        AuthenticationEnabled = string.Equals(
            Environment.GetEnvironmentVariable("CAMUSDB_AUTH_ENABLED"), "true", StringComparison.OrdinalIgnoreCase),
        AccessTokenServerKey =
            Environment.GetEnvironmentVariable("CAMUSDB_AUTH_TOKEN_KEY") ?? resolved.AccessTokenServerKey,
        BootstrapSuperuser =
            Environment.GetEnvironmentVariable("CAMUSDB_BOOTSTRAP_USER") ?? "",
        BootstrapSuperuserPassword =
            Environment.GetEnvironmentVariable("CAMUSDB_BOOTSTRAP_PASSWORD") ?? "",
        NodeSecret =
            Environment.GetEnvironmentVariable("CAMUSDB_NODE_SECRET") ?? "",
        ValueSources = valueSources,
    };
};

camusOptions = applyEnvironmentSecrets(camusOptions);

CamusDBConfig.SetAmbient(camusOptions);

// The swappable source of configuration snapshots. Seeded with the scrubbed record (the bootstrap
// password never enters the holder — the seeding path takes it from the local variable); every
// runtime cluster-setting change publishes a new record here, which fans out to the executor's
// controllers and keeps the ambient value in sync.
CamusDB.Core.Config.CamusDBOptionsHolder optionsHolder = new(camusOptions with { BootstrapSuperuserPassword = "" });

// Diagnostics are opt-in for both modes: enable the Core Meter/ActivitySource gate whenever the
// operator turned diagnostics on. When false, every ServerDiagnostics record helper
// short-circuits, so an unconfigured node emits nothing and pays no cost. Cluster nodes were
// excluded in the first diagnostics phase; chaos/reliability harnesses need Prometheus /metrics
// (including the kahuna.placement.* counters) on every node of a cluster, so the gate is now
// purely the operator's diagnostics switch.
bool diagnosticsActive = config.Diagnostics.Enabled;
CamusDB.Core.Diagnostics.ServerDiagnostics.Enabled = diagnosticsActive;

// Build the singleton query-result cache once, after config statics are set.
// When the feature is off we pass null (not NullQueryResultCache.Instance): every hot-path
// guard is `_cache is not null` / `database.Cache is { }`, so null short-circuits the whole
// cache/gate protocol on the read and write paths with zero overhead — which is what the
// KvTransactionsManager._cache contract ("null when disabled → gate calls skipped") assumes.
// QueryResultCache(sweepIntervalMs: 0) reads QueryResultCacheSweepIntervalMs from config.
IQueryResultCache? queryResultCache = camusOptions.QueryResultCacheEnabled
    ? new QueryResultCache(camusOptions, sweepIntervalMs: 0)
    : null;

// The content root must be the directory the application was deployed to, not the process working
// directory. They coincide in a repository checkout and in the container, but not for an installed
// global tool, where the working directory is wherever the user happened to run `camusdb` from —
// leaving the Razor UI serving 404s for every asset under wwwroot and silently dropping the log-level
// defaults in appsettings.json.
WebApplicationBuilder builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory,
});

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
    // One raft-endpoint → HTTP base-URI resolver shared by every node-to-node channel
    // (schema DDL, cluster settings, query fragments). See PeerEndpointResolver for the
    // http_peers/uniform-port semantics; it used to live inline here, duplicated per channel.
    builder.Services.AddSingleton<CamusDB.Core.Config.PeerEndpointResolver>(services =>
        new CamusDB.Core.Config.PeerEndpointResolver(
            config.Peers, config.HttpPeers, config.HttpPort,
            services.GetRequiredService<ILogger<ICamusDB>>()));

    builder.Services.AddSingleton<ISchemaDdlForwarder>(services =>
    {
        CamusDB.Core.Config.PeerEndpointResolver resolver =
            services.GetRequiredService<CamusDB.Core.Config.PeerEndpointResolver>();
        return new HttpSchemaDdlForwarder(
            new HttpClient(), resolver.Resolve, services.GetRequiredService<ILogger<ICamusDB>>());
    });

    // Unlike the DDL forwarder, the settings forwarder attaches the node secret, without
    // which an auth-enabled receiving node refuses /internal/*.
    builder.Services.AddSingleton<CamusDB.Core.Config.IClusterSettingsForwarder>(services =>
    {
        CamusDB.Core.Config.PeerEndpointResolver resolver =
            services.GetRequiredService<CamusDB.Core.Config.PeerEndpointResolver>();
        return new HttpClusterSettingsForwarder(
            new HttpClient(), resolver.Resolve, camusOptions.NodeSecret,
            services.GetRequiredService<ILogger<ICamusDB>>());
    });

    // Query fragment channel: executes eligible span scans on the span's leader node.
    builder.Services.AddSingleton<IQueryFragmentTransport>(services =>
        new HttpQueryFragmentTransport(
            new HttpClient(),
            services.GetRequiredService<CamusDB.Core.Config.PeerEndpointResolver>(),
            camusOptions.NodeSecret));

    builder.Services.AddSingleton<CommandExecutor>(services =>
        new CommandExecutor(
            services.GetRequiredService<CommandValidator>(),
            services.GetRequiredService<CatalogsManager>(),
            services.GetRequiredService<ILogger<ICamusDB>>(),
            services.GetRequiredService<CamusDBOptions>(),
            services.GetRequiredService<EmbeddedKahuna>(),
            services.GetRequiredService<ISchemaDdlForwarder>(),
            isClusterMode: true,
            cache: queryResultCache,
            optionsHolder: services.GetRequiredService<CamusDB.Core.Config.CamusDBOptionsHolder>(),
            clusterSettings: services.GetRequiredService<CamusDB.Core.Config.ClusterSettingsService>(),
            fragmentTransport: services.GetRequiredService<IQueryFragmentTransport>()
        ));
}
else
{
    builder.Services.AddSingleton<EmbeddedKahuna>(services =>
    {
        string dataPath = camusOptions.DataDirectory;
        ILoggerFactory loggerFactory = services.GetRequiredService<ILoggerFactory>();
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(dataPath, camusOptions.Kahuna, camusOptions);
        options.InitialPartitions = config.InitialPartitions;
        return new EmbeddedKahuna(options, loggerFactory);
    });

    builder.Services.AddSingleton<CommandExecutor>(services =>
        new CommandExecutor(
            services.GetRequiredService<CommandValidator>(),
            services.GetRequiredService<CatalogsManager>(),
            services.GetRequiredService<ILogger<ICamusDB>>(),
            services.GetRequiredService<CamusDBOptions>(),
            services.GetRequiredService<EmbeddedKahuna>(),
            isClusterMode: false,
            cache: queryResultCache,
            optionsHolder: services.GetRequiredService<CamusDB.Core.Config.CamusDBOptionsHolder>(),
            clusterSettings: services.GetRequiredService<CamusDB.Core.Config.ClusterSettingsService>()
        ));
}

// The swappable holder is the source of truth for the current configuration snapshot; the
// CamusDBOptions registration below resolves from it per activation, so transient/per-request
// consumers (controllers, gRPC services, the auth middleware) always see the latest published
// snapshot while singletons keep the snapshot they were constructed with — restart-class by
// construction, exactly what their classification declares.
//
// The bootstrap password never enters the holder: it is a one-shot startup secret used to seed the
// first user, and the seeding path takes it from the startup local. No resolved component can
// retain it.
builder.Services.AddSingleton(optionsHolder);
builder.Services.AddTransient<CamusDBOptions>(services =>
    services.GetRequiredService<CamusDB.Core.Config.CamusDBOptionsHolder>().Current);

// Owns the replicated runtime-settings overlay: validates and proposes changes, applies committed
// changes from the settings log, persists the load-time checkpoint, and publishes merged snapshots
// through the holder. In standalone mode the same pipeline runs with no replication.
builder.Services.AddSingleton<CamusDB.Core.Config.ClusterSettingsService>(services =>
    new CamusDB.Core.Config.ClusterSettingsService(
        services.GetRequiredService<EmbeddedKahuna>(),
        services.GetRequiredService<CamusDB.Core.Config.CamusDBOptionsHolder>(),
        baseDefinitionFactory,
        postResolve: resolved => applyEnvironmentSecrets(resolved) with { BootstrapSuperuserPassword = "" },
        isClusterMode: config.IsClusterMode,
        forwarder: services.GetService<CamusDB.Core.Config.IClusterSettingsForwarder>(),
        logger: services.GetRequiredService<ILogger<ICamusDB>>()));

builder.Services.AddSingleton<CommandValidator>();
builder.Services.AddSingleton<CatalogsManager>();

if (config.IsClusterMode)
    builder.Services.AddSingleton<DdlOperationIdCache>();

builder.Services.AddSingleton<HttpTransactionCoordinator>();
builder.Services.AddSingleton<CamusDB.App.Services.ForegroundRequestGauge>();

// Holds the statements REST clients prepared. Node-local by construction — a handle minted here is
// meaningless on another node, which is why clients must re-prepare on CADB0520.
builder.Services.AddSingleton<CamusDB.App.Services.PreparedStatementRegistry>();

// Opt-in OpenTelemetry export. Subscribes to the CamusDB.Server meter/activity
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

// Drops prepared statements a REST client stopped using. A gRPC handle dies with its stream; a REST
// handle has no such event, so an idle timeout is what keeps abandoned ones from accumulating.
builder.Services.AddHostedService<PreparedStatementReaper>();

if (config.IsClusterMode)
{
    builder.Services.AddSingleton<EmbeddedKahuna>(services =>
    {
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildCluster(config, camusOptions);

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

// Warn early about every configuration that leaves key-range routing, or its automatic split
// policy, inert. Each of these states starts the node successfully and then does nothing the
// operator asked for, so nothing else would ever report them. See KeyRangeSplitStartupChecks for
// the individual causes and why each one is invisible without this.
foreach (string warning in KeyRangeSplitStartupChecks.Inspect(config, camusOptions))
    app.Logger.LogWarning("{KeyRangeSplitWarning}", warning);

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

// Prometheus scrape endpoint (diagnostics + prometheus_enabled only). It exposes
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

// Load the persisted cluster-settings overlay and publish the merged configuration NOW — after the
// store is readable, before the executor below is resolved — so everything the executor constructs
// (schedulers, caches, table stores) is built from the merged snapshot and never holds a boot-only
// value. This is the whole boot-window hazard reduced to an ordering constraint.
CamusDB.Core.Config.ClusterSettingsService clusterSettingsService =
    app.Services.GetRequiredService<CamusDB.Core.Config.ClusterSettingsService>();
await clusterSettingsService.StartAsync();

SpillFileManager.AcquireInstanceLock(camusOptions.DataDirectory);
SpillFileManager.RunStartupSweep(camusOptions.DataDirectory);

// Initialize DB system
CamusStartup camus = new(
    app.Services.GetRequiredService<CommandExecutor>()
);

await camus.Initialize();

CommandExecutor commandExecutor = app.Services.GetRequiredService<CommandExecutor>();

// Seed the bootstrap superuser when auth is enabled and the catalog is empty; fail-closed (refuse to
// start) if enabled with an empty catalog and no bootstrap secret. No-op when auth is disabled.
//
// The credentials are passed explicitly from this scope's unscrubbed copy: the CamusDBOptions in DI is
// registered with the password blanked, so the executor cannot read it back off its injected options.
await commandExecutor.EnsureBootstrapSuperuserAsync(
    camusOptions.BootstrapSuperuser,
    camusOptions.BootstrapSuperuserPassword);

// The bootstrap password was only needed to seed the first hash — drop it from process memory.
// Only this scope's copy still carries it: the holder was seeded scrubbed, and the settings
// service's boot publish already replaced the ambient value with the scrubbed merged snapshot
// (re-installing the boot record here would undo any overlay the cluster carries).
camusOptions = camusOptions with { BootstrapSuperuserPassword = "" };

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

// A clean shutdown is exit code 0. The entry point returns a code because `camusdb init` exits
// early with a non-zero status when it refuses to overwrite an existing configuration.
return 0;
