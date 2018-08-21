using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using System;
using System.Net;
using System.Threading.Tasks;

namespace Orleans2GettingStarted
{
    class Program
    {
        static async Task Main(string[] args)
        {
            const string connectionString = "server=<hostname or IP>;uid=<type username here>;pwd=<type pwd here>;database=orleanstest;SslMode=none";

            var silo = new SiloHostBuilder()
            .Configure<ClusterOptions>(options =>
            {
                options.ClusterId = "dev";
                options.ServiceId = "Orleans2GettingStarted2";
            })
            .UseAdoNetClustering(options =>
            {
                options.ConnectionString = connectionString;
                options.Invariant = "MySql.Data.MySqlClient";
            })
            .ConfigureEndpoints(siloPort: 11111, gatewayPort: 30000)
            .ConfigureLogging(builder => builder.SetMinimumLevel(LogLevel.Warning).AddConsole())
            .Build();

            await silo.StartAsync();
            Console.ReadLine();
        }
    }
}
