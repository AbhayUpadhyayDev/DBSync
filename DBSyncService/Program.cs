using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using DBSyncService;

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
    })
    .ConfigureServices((hostContext, services) =>
    {
        services.AddHostedService<Worker>();
    })
    .Build();

await builder.RunAsync();
