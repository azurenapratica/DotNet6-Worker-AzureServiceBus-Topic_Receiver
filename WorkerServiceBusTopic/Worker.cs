using System.Text;
using Microsoft.Azure.ServiceBus;

namespace WorkerServiceBusTopic;

public class Worker : IHostedService
{
    private readonly ILogger<Worker> _logger;
    private readonly ExecutionParameters _executionParameters;
    private readonly ISubscriptionClient _client;
    private readonly string _subscription;

    public Worker(ILogger<Worker> logger,
        ExecutionParameters executionParameters)
    {
        _logger = logger;
        _executionParameters = executionParameters;

        _subscription = executionParameters.Subscription!;
        var topic = executionParameters.Topic!;
        
        _client = new SubscriptionClient(
            _executionParameters.ConnectionString,
            topic, _subscription);

        _logger.LogInformation($"Topic = {topic}");
        _logger.LogInformation($"Subscription = {_subscription}");
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Carregando configuracoes para o processamento de mensagens...");
        RegisterOnMessageHandlerAndReceiveMessages();
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken stoppingToken)
    {
        await _client.CloseAsync();
        _logger.LogInformation(
            "Conexao com o Azure Service Bus fechada!");
    }

    private void RegisterOnMessageHandlerAndReceiveMessages()
    {
        var messageHandlerOptions = new MessageHandlerOptions(
            ExceptionReceivedHandler)
        {
            MaxConcurrentCalls = 1,
            AutoComplete = false
        };

        _client.RegisterMessageHandler(
            ProcessMessagesAsync, messageHandlerOptions);
    }

    private async Task ProcessMessagesAsync(Message message, CancellationToken token)
    {
        _logger.LogInformation($"[{_subscription} | Nova mensagem] " +
            Encoding.UTF8.GetString(message.Body));
        await _client.CompleteAsync(
            message.SystemProperties.LockToken);
    }

    private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
    {
        _logger.LogError($"Message handler - Tratamento - Exception: {exceptionReceivedEventArgs.Exception}.");

        var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
        _logger.LogError("Exception context - informacoes para resolucao de problemas:");
        _logger.LogError($"- Endpoint: {context.Endpoint}");
        _logger.LogError($"- Entity Path: {context.EntityPath}");
        _logger.LogError($"- Executing Action: {context.Action}");

        return Task.CompletedTask;
    }
}