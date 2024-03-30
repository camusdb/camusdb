
using CamusDB.Core;
using Microsoft.Extensions.Logging;

namespace CamusDB.Tests.CommandsExecutor;

public abstract class BaseTest
{
    private readonly ILoggerFactory loggerFactory;

    protected readonly ILogger<ICamusDB> logger;

    protected BaseTest()
    {
        loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddFilter("Camus", LogLevel.Debug).AddConsole();
        });

        logger = loggerFactory.CreateLogger<ICamusDB>();
    }
}

