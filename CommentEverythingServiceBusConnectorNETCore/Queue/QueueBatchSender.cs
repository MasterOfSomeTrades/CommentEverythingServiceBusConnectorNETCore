using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorLib.Queue
{
    public class QueueBatchSender {
        private QueueBatchSender() {
            // --- Use parameterized constructor
        }

        public QueueBatchSender(string connectionString, string toQueue) {
            ServiceBusConnectionString = connectionString;
            QueueName = toQueue;
            if (logger is null) {
                logger = loggerFactory.CreateLogger<QueueBatchSender>();
            }

        }

        string ServiceBusConnectionString;
        string QueueName;
        private IQueueClient queueClient;
        private List<List<Message>> _messageListStructure = new List<List<Message>>();
        private long _currentSizeTotal = 0;

        private ILoggerFactory loggerFactory = new LoggerFactory().AddConsole().AddAzureWebAppDiagnostics();
        private ILogger logger = null;

        public async Task<bool> Send(string msg, string groupId, string context) {
            await Send(new List<string>(new string[] { msg }), groupId, context);

            return true;
        }

        public async Task<bool> Send(IList<string> msgs, string groupId, string context) {
            bool ret = false;

            // --- Open TopicClient
            queueClient = new QueueClient(ServiceBusConnectionString, QueueName);

            ret = await SendMessagesAsync(msgs, groupId, context);

            return ret;
        }

        private async Task SendMessagesAsync(string msg, string correlation) {
            try {
                var message = new Message(Encoding.UTF8.GetBytes(msg));

                // Send the message to the queue.
                await queueClient.SendAsync(message);
            } catch (Exception exception) {
                logger.LogError(exception.Message + exception.StackTrace);
                throw new ApplicationException(exception.Message);
            }
        }

        protected virtual async Task<bool> SendMessagesAsync(IList<string> msgs, string correlation, string usage) {
            try {
                // --- Setup
                _messageListStructure = new List<List<Message>>();
                _messageListStructure.Add(new List<Message>());
                _currentSizeTotal = 0;
                int messageCount = 0;

                // --- Loop through message IList
                foreach (string m in msgs) {
                    messageCount = messageCount + 1;
                    Message msg = new Message(Encoding.UTF8.GetBytes(m)) {
                        CorrelationId = correlation
                    };
                    if (messageCount == msgs.Count) {
                        msg.Label = "last";
                    } else if (messageCount == 1) {
                        msg.Label = "first";
                    } else {
                        msg.Label = "interim";
                    }
                    msg.UserProperties.Add("CollectionId", correlation);
                    msg.UserProperties.Add("Count", msgs.Count);
                    msg.UserProperties.Add("Context", usage);
                    msg.MessageId = Guid.NewGuid().ToString("D");
                    if (_currentSizeTotal + msg.Size > 100000) {
                        _currentSizeTotal = 0;
                        _messageListStructure.Add(new List<Message>());
                    }
                    _currentSizeTotal = _currentSizeTotal + msg.Size;
                    logger.LogInformation("Adding message with size " + msg.Size.ToString() + " | Total messages size " + _currentSizeTotal.ToString());
                    _messageListStructure[_messageListStructure.Count - 1].Add(msg);
                }

                List<Task> taskList = new List<Task>();
                foreach (List<Message> l in _messageListStructure) {
                    logger.LogInformation("Adding task to send message (" + (taskList.Count + 1).ToString() + ")");
                    taskList.Add(queueClient.SendAsync(l));
                }
                // --- Send the Messages to the queue.
                await Task.WhenAll(taskList);

                // --- Close queue
                await queueClient.CloseAsync();

                return true;
            } catch (Exception exception) {
                logger.LogError(exception.Message + exception.StackTrace);
                throw new ApplicationException(exception.Message);
            }
        }
    }
}
