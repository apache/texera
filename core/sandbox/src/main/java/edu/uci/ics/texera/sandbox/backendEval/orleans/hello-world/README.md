# Hello World implementation on two machines

We will be using two Ubuntu servers to run a basic HelloWorld program. One machine will run the silo and other the client. We will provide MySQL to Orleans to maintain cluster membership. The code is a simple modification of the Gigilabs code. The client sends fake temperature readings to different grains which just outputs it to the terminal.

## 1. Install .Net Core on both machines
1. Use [this](https://docs.microsoft.com/en-us/dotnet/core/linux-prerequisites?tabs=netcore2x#linux-distribution-dependencies) to install pre-requisite libraries.
2. Use [this](https://docs.microsoft.com/en-us/dotnet/core/linux-prerequisites?tabs=netcore2x#install-net-core-for-supported-ubuntu-and-linux-mint-distributionsversions-64-bit) to install both .Net SDK and runtime.

## 2. Start Silo on one machine

1. Create a mysql database called 'orleanstest'. Then, run the scripts [1](https://github.com/dotnet/orleans/blob/master/src/AdoNet/Shared/MySQL-Main.sql), [2](https://github.com/dotnet/orleans/blob/master/src/AdoNet/Orleans.Clustering.AdoNet/MySQL-Clustering.sql) to create the necessary tables and insert entries in the database.
2. Use the following code snippet (entire code is in sandbox folder). Note that in the connection string you have to set username and password. Also, we won't be currently using ssl connections to mysql server. The below code starts the silo and waits unless some input is given to it:
```C#
            const string connectionString = "server=<hostname>;uid=<username>;pwd=<password>;database=orleanstest;SslMode=none";

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
            // Wait for user's input, otherwise it will immediately exit.
            Console.ReadLine();
``` 

## 3. Start client on different machine

1. Client uses the same mysql as the silo. So, the connection string should be the same. 
2. Use the following code to start client
```C#
            const string connectionString = "server=<hostname>;uid=<username>;pwd=<password>;database=orleanstest;SslMode=none";
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

                while (true)
                {
                    int grainId = random.Next(0, 500);
                    double temperature = random.NextDouble() * 40;
                    var sensor = client.GetGrain<ITemperatureSensorGrain>(grainId);
                    await sensor.SubmitTemperatureAsync((float)temperature);
                }
            }
```
