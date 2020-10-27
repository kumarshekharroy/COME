using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Net.Http;
using System.Net;

namespace COME
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Environment.SetEnvironmentVariable("ServerID", Guid.NewGuid().ToString());
            Environment.SetEnvironmentVariable("ServerInitTime", DateTime.UtcNow.ToString());

            Console.WriteLine("------------------------------------------------------------");
            Console.WriteLine($"ServerID : {Environment.GetEnvironmentVariable("ServerID")}");
            Console.WriteLine($"ServerInitTime : {Environment.GetEnvironmentVariable("ServerInitTime")}");

            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("ServerPort")) || !int.TryParse(Environment.GetEnvironmentVariable("ServerPort"), out var _))
                Environment.SetEnvironmentVariable("ServerPort", "8080");

            Console.WriteLine($"ServerPort : {Environment.GetEnvironmentVariable("ServerPort")}");

            Console.WriteLine("------------------------------------------------------------");

            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("Redis_ClientName")))
                Environment.SetEnvironmentVariable("Redis_ClientName", Environment.GetEnvironmentVariable("ServerID"));

            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("Redis_EndPoints")))
                Environment.SetEnvironmentVariable("Redis_EndPoints", "127.0.0.1:6379");

            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("Redis_Password")))
                Environment.SetEnvironmentVariable("Redis_Password", "ABCD@1234");


            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>().ConfigureKestrel(options =>
                    {
                        options.Listen(IPAddress.Any, int.Parse(Environment.GetEnvironmentVariable("ServerPort")));
                    }).UseKestrel();
                });
    }
}
