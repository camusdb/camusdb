
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.App.Services;
using CommandLine;
using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.Configuration;
using Kommander;
using Kommander.Communication.Grpc;
using Microsoft.AspNetCore.Server.Kestrel.Core;

// Parse CLI flags; fall back to defaults on parse failure so the server still starts.
ParserResult<CamusCommandLineOptions> optsResult = Parser.Default.ParseArguments<CamusCommandLineOptions>(args);
CamusCommandLineOptions opts = optsResult.Value ?? new();

// Read and merge config before building the host so cluster-mode detection
// can gate DI service registration.
string configYml = await File.ReadAllTextAsync("Config/config.yml");
ConfigDefinition config = new ConfigReader().Read(configYml);

// CLI > env > YAML > default — only explicitly provided flags override YAML.
ConfigResolver.ApplyCliOverrides(config, opts.ToOverrides());
config.Validate();
ConfigResolver.ApplyToCamusDBConfig(config);

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
});

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
});
builder.Logging.AddFilter("Kahuna", LogLevel.Warning);
builder.Logging.AddFilter("Kommander", LogLevel.Warning);

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
            services.GetRequiredService<ILoggerFactory>(),
            services.GetRequiredService<EmbeddedKahuna>(),
            services.GetRequiredService<ISchemaDdlForwarder>()
        ));
}
else
{
    builder.Services.AddSingleton<CommandExecutor>();
}
builder.Services.AddSingleton<CommandValidator>();
builder.Services.AddSingleton<CatalogsManager>();
if (config.IsClusterMode)
    builder.Services.AddSingleton<DdlOperationIdCache>();
builder.Services.AddSingleton<HttpTransactionCoordinator>();

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

    builder.Services.AddGrpc();
}

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

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    app.UseHsts();
}

if (!config.IsClusterMode && !string.IsNullOrEmpty(config.HttpsCertificate))
    app.UseHttpsRedirection();
app.UseStaticFiles();

app.MapControllers();

app.UseRouting();

app.UseAuthorization();

app.MapRazorPages();

// Start Kestrel first so the gRPC Raft port is bound before the cluster node
// begins leader election and tries to reach peers.
await app.StartAsync();

if (config.IsClusterMode)
{
    EmbeddedKahuna clusterNode = app.Services.GetRequiredService<EmbeddedKahuna>();

    // Wire the ack transport so follower applies are delivered to the leader's tracker.
    ISchemaDdlForwarder ddlForwarder = app.Services.GetRequiredService<ISchemaDdlForwarder>();
    clusterNode.SetSchemaAckForwarder(ddlForwarder);

    await clusterNode.StartAsync();
}

// Initialize DB system
CamusStartup camus = new(
    app.Services.GetRequiredService<CommandExecutor>()
);

await camus.Initialize();

CommandExecutor commandExecutor = app.Services.GetRequiredService<CommandExecutor>();

try
{
    await app.WaitForShutdownAsync();
}
finally
{
    ILogger<ICamusDB> shutdownLogger = app.Services.GetRequiredService<ILogger<ICamusDB>>();
    shutdownLogger.LogInformation("Graceful shutdown started");

    await commandExecutor.DisposeAsync();
    shutdownLogger.LogInformation("Databases closed");

    if (config.IsClusterMode)
    {
        await app.Services.GetRequiredService<EmbeddedKahuna>().DisposeAsync();
        shutdownLogger.LogInformation("Cluster node stopped");
    }

    shutdownLogger.LogInformation("Graceful shutdown complete");
}
