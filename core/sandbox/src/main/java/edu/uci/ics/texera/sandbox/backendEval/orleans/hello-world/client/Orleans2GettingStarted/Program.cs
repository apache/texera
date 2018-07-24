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
            var clientBuilder = new ClientBuilder()
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
                .ConfigureLogging(builder => builder.SetMinimumLevel(LogLevel.Warning).AddConsole());

            using (var client = clientBuilder.Build())
            {
                await client.Connect();

                var random = new Random();
                string sky = "blue";

                while (sky == "blue") // if run in Ireland, it exits loop immediately
                {
                    int grainId = random.Next(0, 500);
                    double temperature = random.NextDouble() * 40;
                    var sensor = client.GetGrain<ITemperatureSensorGrain>(grainId);
                    await sensor.SubmitTemperatureAsync((float)temperature);
                }
            }
        }
    }
}
