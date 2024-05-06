using Azure.Messaging.ServiceBus;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Queue {
    public abstract class QueueReader {
        public QueueReader(string connectionString, string listenToQueue) {
            ServiceBusConnectionString = connectionString;
            QueueName = listenToQueue;

            /*if (logger is null) {
                logger = loggerFactory.CreateLogger<QueueReader>();
            }*/
        }

        public QueueReader(string connectionString, string listenToQueue, ILogger log) {
            ServiceBusConnectionString = connectionString;
            QueueName = listenToQueue;

            if (logger is null) {
                logger = log;
            }
        }

        private string ServiceBusConnectionString;
        private string QueueName;
        private ServiceBusClient queueClient;
        private ServiceBusReceiver queueReceiver;
        SemaphoreSlim sLock = new SemaphoreSlim(5);

        //private ILoggerFactory loggerFactory = new LoggerFactory().AddConsole().AddAzureWebAppDiagnostics();
        private ILogger logger = null;

        public void Connect() {
            try {
                queueClient = new ServiceBusClient(ServiceBusConnectionString);
                queueReceiver = queueClient.CreateReceiver(QueueName);
                /*var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler) {
                    // Maximum number of concurrent calls to the callback ProcessMessagesAsync(), set to 1 for simplicity.
                    // Set it according to how many messages the application wants to process in parallel.
                    MaxConcurrentCalls = 5,

                    // Indicates whether the message pump should automatically complete the messages after returning from user callback.
                    // False below indicates the complete operation is handled by the user callback as in ProcessMessagesAsync().
                    AutoComplete = false
                };*/
                //queueReceiver.RegisterMessageHandler(ProcessMessagesAsync); // TODO: commented out - deprecated but need replacement
            } catch (Exception ex) {
                if (!(logger is null)) {
                    logger.LogError(ex.Message + ex.StackTrace);
                }
                throw new ApplicationException(ex.Message + ex.StackTrace);
            }
        }

        protected abstract void ProcessMessage(ServiceBusReceivedMessage messageAsObject, string messageAsUTF8);

        private async Task ProcessMessagesAsync(ServiceBusReceivedMessage message, CancellationToken token) {
            string messageAsString = "";
            Task completionTask;
            try {
                if (!(logger is null)) {
                    logger.LogInformation("===================== Processing Message =====================");
                }
                completionTask = queueReceiver.CompleteMessageAsync(message);
                messageAsString = Encoding.Default.GetString(message.Body);
                if (!(logger is null)) {
                    logger.LogInformation(messageAsString);
                    logger.LogInformation("==============================================================");
                }
            } catch (Exception ex) {
                if (!(logger is null)) {
                    logger.LogError("ERROR while receiving message from queue: " + ex.Message + ex.StackTrace);
                }
                throw new ApplicationException(ex.Message);
            }

            try {
                ProcessMessage(message, messageAsString);
            } catch (Exception ex) {
                if (!(logger is null)) {
                    logger.LogError("ERROR processing message from queue: " + ex.Message + ex.StackTrace);
                }
                throw new ApplicationException(ex.Message);
            }

            try {
                IList<Task> tasksList = new List<Task>();
                await sLock.WaitAsync();
                tasksList.Add(completionTask);
                if (tasksList.Count > 0) {
                    await Task.WhenAll(tasksList).ContinueWith((t) => sLock.Release());
                }
            } catch (Exception ex) {
                sLock.Release();
                if (!(logger is null)) {
                    logger.LogError("ERROR completing message on queue: " + ex.Message + ex.StackTrace);
                }
                throw new ApplicationException(ex.Message);
            }
            // --- Send message
            //await TopicSender.MainAsync(jsonResult); //.GetAwaiter().GetResult();

            // --- Complete the message
            //await queueClient.CompleteAsync(message.SystemProperties.LockToken);

            // Complete the message so that it is not received again.
            // This can be done only if the queue Client is created in ReceiveMode.PeekLock mode (which is the default).
            //await queueClient.CompleteAsync(message.SystemProperties.LockToken);

            // Note: Use the cancellationToken passed as necessary to determine if the queueClient has already been closed.
            // If queueClient has already been closed, you can choose to not call CompleteAsync() or AbandonAsync() etc.
            // to avoid unnecessary exceptions.
        }
    }
}
