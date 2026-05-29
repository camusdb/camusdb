
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
using Kommander;
using Kommander.Communication.Grpc;

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

// Add logging
builder.Services.AddLogging(logging =>
    logging.AddSimpleConsole(options =>
    {
        options.SingleLine = true;
        options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
    })
);

builder.Logging.ClearProviders();
builder.Logging.AddConsole();

// Add services to the container.
builder.Services.AddRazorPages();

if (config.IsClusterMode)
{
    builder.Services.AddSingleton<CommandExecutor>(services =>
        new CommandExecutor(
            services.GetRequiredService<CommandValidator>(),
            services.GetRequiredService<CatalogsManager>(),
            services.GetRequiredService<ILogger<ICamusDB>>(),
            services.GetRequiredService<ILoggerFactory>(),
            services.GetRequiredService<EmbeddedKahuna>()
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
            Host = config.RaftHost,
            Port = config.RaftPort,
            InitialPartitions = config.InitialPartitions,
            Storage = "sqlite",
            StoragePath = Path.Combine(config.DataDir, "kv"),
            StorageRevision = "v1",
            WalStorage = "sqlite",
            WalPath = Path.Combine(config.DataDir, "wal"),
            WalRevision = "v1"
        };

        ILoggerFactory loggerFactory = services.GetRequiredService<ILoggerFactory>();
        return EmbeddedKahuna.CreateCluster(options, config.Peers, loggerFactory);
    });

    // Expose Raft and Kahuna interfaces so gRPC services can resolve them via DI.
    builder.Services.AddSingleton<IRaft>(services =>
        services.GetRequiredService<EmbeddedKahuna>().Raft);

    builder.Services.AddSingleton<IKahuna>(services =>
        services.GetRequiredService<EmbeddedKahuna>().Kahuna);

    builder.Services.AddGrpc();
}

// Initialize min threads
ThreadPool.SetMinThreads(1024, 512);

WebApplication app = builder.Build();

if (config.IsClusterMode)
{
    // Raft consensus endpoint — handles leader election and log replication between nodes.
    app.MapGrpcRaftRoutes();

    // Kahuna inter-node KV/lock endpoints (LocksService, KeyValuesService, SequencesService)
    // require the Kahuna server gRPC services to be available in this project.
    // Pending Kahuna.Core package update to expose these service types.
    // app.MapGrpcKahunaRoutes();

    // Start the process-level cluster node and wait for Raft leader election before
    // accepting any database operations.
    EmbeddedKahuna clusterNode = app.Services.GetRequiredService<EmbeddedKahuna>();
    await clusterNode.StartAsync();
}

// Initialize DB system
CamusStartup camus = new(
    app.Services.GetRequiredService<CommandExecutor>()
);

await camus.Initialize(configYml);

CommandExecutor commandExecutor = app.Services.GetRequiredService<CommandExecutor>();
app.Lifetime.ApplicationStopping.Register(() =>
    commandExecutor.DisposeAsync().AsTask().GetAwaiter().GetResult());

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();

app.MapControllers();

app.UseRouting();

app.UseAuthorization();

app.MapRazorPages();

app.Run();
