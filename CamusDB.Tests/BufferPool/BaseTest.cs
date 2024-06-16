
using CamusDB.Core;
using Microsoft.Extensions.Logging;

namespace CamusDB.Tests.BufferPool;

public abstract class BaseTest
{
	protected readonly ILoggerFactory loggerFactory;

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

