using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using RabbitHub.Config;
using RabbitHub.DI;
using Examples.DI;

var conf = ConnectionConfig.GetDefault("test");
var queueConf = QueueConfig.Create("q_users");
queueConf.AutoDeclare = true;
queueConf.AutoBindTopics = true;
queueConf.Durable = true;

var host = Host
  .CreateDefaultBuilder()
  .ConfigureServices((context, services) =>
  {
    services.AddRabbitHub(hub => 
      hub
      .Connect(conf)
      .UseDefaultConsumer(cons => 
        cons
        .Queue(queueConf)
        .HandleRpc<EmailRpcHandler>("getUsers.rpc")
      )
    );
  });

await host.Build().RunAsync();