using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Queue {
    public class QueueBatchSender {
        private QueueBatchSender() {
            // --- Use parameterized constructor
        }

        public QueueBatchSender(string connectionString, string toQueue) {
            ServiceBusConnectionString = connectionString;
            QueueName = toQueue;
            /*if (logger is null) {
                logger = loggerFactory.CreateLogger<QueueBatchSender>();
            }*/

        }

        public QueueBatchSender(string connectionString, string toQueue, ILogger log) {
            ServiceBusConnectionString = connectionString;
            QueueName = toQueue;
            if (logger is null) {
                logger = log;
            }

        }

        string ServiceBusConnectionString;
        string QueueName;
        private ServiceBusClient queueClient;
        private ServiceBusSender queueSender;
        private List<List<ServiceBusMessage>> _messageListStructure = new List<List<ServiceBusMessage>>();
        private long _currentSizeTotal = 0;

        //private ILoggerFactory loggerFactory = new LoggerFactory().AddConsole().AddAzureWebAppDiagnostics();
        private ILogger logger = null;

        public async Task<bool> Send(string msg, string groupId, string context) {
            await Send(new List<string>(new string[] { msg }), groupId, context);

            return true;
        }

        public async Task<bool> Send(IList<string> msgs, string groupId, string context) {
            bool ret = false;

            // --- Open TopicClient
            queueClient = new ServiceBusClient(ServiceBusConnectionString);
            queueSender = queueClient.CreateSender(QueueName);

            ret = await SendMessagesAsync(msgs, groupId, context);

            return ret;
        }

        private async Task SendMessagesAsync(string msg, string correlation) {
            try {
                var message = new ServiceBusMessage(Encoding.UTF8.GetBytes(msg));

                // Send the message to the queue.
                await queueSender.SendMessageAsync(message);
            } catch (Exception exception) {
                if (!(logger is null)) {
                    logger.LogError($"{exception.Message} {exception.StackTrace}");
                }
                throw new ApplicationException($"{exception.Message} {exception.StackTrace}");
            }
        }

        protected virtual async Task<bool> SendMessagesAsync(IList<string> msgs, string correlation, string usage) {
            try {
                // --- Setup
                _messageListStructure = new List<List<ServiceBusMessage>>();
                _messageListStructure.Add(new List<ServiceBusMessage>());
                _currentSizeTotal = 0;
                int messageCount = 0;

                // --- Loop through message IList
                foreach (string m in msgs) {
                    messageCount = messageCount + 1;
                    ServiceBusMessage msg = new ServiceBusMessage(Encoding.UTF8.GetBytes(m)) {
                        CorrelationId = correlation
                    };
                    if (messageCount == msgs.Count) {
                        msg.Subject = "last";
                    } else if (messageCount == 1) {
                        msg.Subject = "first";
                    } else {
                        msg.Subject = "interim";
                    }
                    msg.ApplicationProperties.Add("CollectionId", correlation);
                    msg.ApplicationProperties.Add("Count", msgs.Count);
                    msg.ApplicationProperties.Add("Context", usage);
                    msg.MessageId = Guid.NewGuid().ToString("D");
                    /*if (_currentSizeTotal + msg.Size > 100000) { // --- DEPRECATED: Cannot read ServiceBusMessage size
                        _currentSizeTotal = 0;
                        _messageListStructure.Add(new List<ServiceBusMessage>());
                    }
                    _currentSizeTotal = _currentSizeTotal + msg.Size;
                    if (!(logger is null)) {
                        logger.LogInformation("Adding message with size " + msg.Size.ToString() + " | Total messages size " + _currentSizeTotal.ToString());
                    }*/
                    _messageListStructure[_messageListStructure.Count - 1].Add(msg);
                }

                List<Task> taskList = new List<Task>();
                foreach (List<ServiceBusMessage> l in _messageListStructure) {
                    if (!(logger is null)) {
                        logger.LogInformation("Adding task to send message (" + (taskList.Count + 1).ToString() + ")");
                    }
                    taskList.Add(queueSender.SendMessagesAsync(l));
                }
                // --- Send the Messages to the queue.
                await Task.WhenAll(taskList);

                // --- Close queue
                //await queueClient.CloseAsync();

                return true;
            } catch (Exception exception) {
                if (!(logger is null)) {
                    logger.LogError(exception.Message + exception.StackTrace);
                }
                throw new ApplicationException(exception.Message);
            }
        }
    }
}
