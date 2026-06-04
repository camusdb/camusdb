
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

// CLI flags override config.yml values where explicitly provided.
if (opts.Mode != "standalone") config.Mode = opts.Mode;
if (!string.IsNullOrEmpty(opts.RaftNodeName)) config.NodeName = opts.RaftNodeName;
if (opts.RaftHost != "localhost") config.RaftHost = opts.RaftHost;
if (opts.RaftPort != 7070) config.RaftPort = opts.RaftPort;
if (opts.InitialClusterPartitions > 1) config.InitialPartitions = opts.InitialClusterPartitions;
if (opts.InitialCluster.Any()) config.Peers = [.. opts.InitialCluster];

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.ListenAnyIP(opts.HttpPort);
    kestrel.ListenAnyIP(opts.HttpsPort, o => o.UseHttps());
    if (config.IsClusterMode)
    {
        kestrel.ListenAnyIP(config.RaftPort, o =>
        {
            o.Protocols = HttpProtocols.Http2;
            if (!string.IsNullOrEmpty(opts.RaftCertificate))
                o.UseHttps(opts.RaftCertificate);
        });
    }
});

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
});
builder.Logging.AddFilter("Kahuna", LogLevel.Debug);
builder.Logging.AddFilter("Kommander", LogLevel.Information);

// Add services to the container.
builder.Services.AddRazorPages();

if (config.IsClusterMode)
{
    builder.Services.AddSingleton<ISchemaDdlForwarder>(services =>
    {
        int httpPort = opts.HttpPort;
        Func<string, Uri> resolver = raftEndpoint =>
        {
            string host = raftEndpoint.Contains(':') ? raftEndpoint.Split(':')[0] : raftEndpoint;
            return new Uri($"http://{host}:{httpPort}");
        };
        return new HttpSchemaDdlForwarder(new HttpClient(), resolver, services.GetRequiredService<ILogger<ICamusDB>>());
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
builder.Services.AddSingleton<HttpTransactionCoordinator>();

if (config.IsClusterMode)
{
    builder.Services.AddSingleton<EmbeddedKahuna>(services =>
    {
        EmbeddedKahunaOptions options = new()
        {
            NodeName = !string.IsNullOrEmpty(config.NodeName) ? config.NodeName : Environment.MachineName,
            NodeId = opts.RaftNodeId,
            Host = config.RaftHost,
            Port = config.RaftPort,
            InitialPartitions = config.InitialPartitions,
            Storage = "sqlite",
            StoragePath = Path.Combine(config.DataDir, "kv"),
            StorageRevision = "v1",
            WalStorage = "sqlite",
            WalPath = Path.Combine(config.DataDir, "wal"),
            WalRevision = "v1",
            StartElectionTimeout = 2000,
            EndElectionTimeout = 4000
        };

        ILoggerFactory loggerFactory = services.GetRequiredService<ILoggerFactory>();
        return EmbeddedKahuna.CreateCluster(options, config.Peers, loggerFactory);
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

if (!config.IsClusterMode)
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
    await clusterNode.StartAsync();
}

// Initialize DB system
CamusStartup camus = new(
    app.Services.GetRequiredService<CommandExecutor>()
);

await camus.Initialize(configYml);

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
