using WorkerServiceBusTopic;

Console.WriteLine(
    "Testando o consumo de mensagens com Azure Service Bus + Topicos");

if (args.Length != 3)
{
    Console.WriteLine(
        "Informe 3 parametros: " +
        "no primeiro a string de conexao com o Azure Service Bus, " +
        "no segundo o Topico a ser utilizado no recebimento das mensagens, " +
        "no terceiro a Subscription da aplicacao...");
    return;
}

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddSingleton<ExecutionParameters>(
            new ExecutionParameters()
            {
                ConnectionString = args[0],
                Topic = args[1],
                Subscription = args[2]
            });

        services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();