#region copyright
// -----------------------------------------------------------------------
//  <copyright file="Program.cs" company="Bartosz Sypytkowski">
//      Copyright (C) 2019-2019 Bartosz Sypytkowski <b.sypytkowski@gmail.com>
//  </copyright>
// -----------------------------------------------------------------------
#endregion

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Cluster;
using Akka.Configuration;
using Eventuate.Rocks;
using Serilog;

namespace Chatter
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .MinimumLevel.Information()
                .CreateLogger();

            Serilog.Log.Logger = logger;
            
            await Run("Alice", "lunch", 7000, "akka.tcp://chatter@127.0.0.1:7000/", "reference.conf", default);
        }

        static async Task Run(string user, string chatroom, int port, string seedNode, string configFile, CancellationToken token)
        {
            var config = ConfigurationFactory.ParseString(await File.ReadAllTextAsync(configFile, token));
            var c = ConfigurationFactory.ParseString(@"akka.remote.dot-netty.tcp.port = " + port);
            using var system = ActorSystem.Create("chatter", c.WithFallback(config));

            await Cluster.Get(system).JoinAsync(Address.Parse(seedNode), token);

            var log = system.ActorOf(RocksDbEventLog.Props(user, new RocksDbSettings(config)));
            var chat = system.ActorOf(Props.Create(() => new Chat(chatroom, log)), chatroom);

            var msg = Console.ReadLine();
            while (msg != "EXIT")
            {
                await chat.Ask(new Chat.Send(user, msg));
                msg = Console.ReadLine();
            }
        }
    }
}