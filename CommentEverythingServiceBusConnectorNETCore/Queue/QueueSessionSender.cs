using Azure.Messaging.ServiceBus;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Queue {
    public class QueueSessionSender : QueueBatchSender {
        public QueueSessionSender(string connectionString, string toQueue) : base(connectionString, toQueue) {
            ServiceBusConnectionString = connectionString;
            QueueName = toQueue;

            /*if (logger is null) {
                logger = loggerFactory.CreateLogger<QueueBatchSender>();
            }*/
            throw new Exception("QueueSessionSender has been deprecated");
        }

        string ServiceBusConnectionString;
        string QueueName;
        private ServiceBusSender queueClient;
        private List<List<ServiceBusMessage>> _messageListStructure = new List<List<ServiceBusMessage>>();
        private long _currentSizeTotal = 0;

        //private ILoggerFactory loggerFactory = new LoggerFactory().AddConsole().AddAzureWebAppDiagnostics();
        //private ILogger logger = null;

        protected override async Task<bool> SendMessagesAsync(IList<string> msgs, string correlation, string usage) {
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
                        SessionId = correlation
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
                    /*if (_currentSizeTotal + msg.Size > 100000) {
                        _currentSizeTotal = 0;
                        _messageListStructure.Add(new List<ServiceBusMessage>());
                    }
                    _currentSizeTotal = _currentSizeTotal + msg.Size;
                    //logger.LogInformation("Adding message with size " + msg.Size.ToString() + " | Total messages size " + _currentSizeTotal.ToString());*/
                    _messageListStructure[_messageListStructure.Count - 1].Add(msg);
                }

                List<Task> taskList = new List<Task>();
                foreach (List<ServiceBusMessage> l in _messageListStructure) {
                    //logger.LogInformation("Adding task to send message (" + (taskList.Count + 1).ToString() + ")");
                    taskList.Add(queueClient.SendMessagesAsync(l));
                }

                // --- Send the Messages to the queue.
                await Task.WhenAll(taskList);

                // --- Close queue
                //await queueClient.CloseAsync();

                return true;
            } catch (Exception exception) {
                //logger.LogError(exception.Message + exception.StackTrace);
                throw new ApplicationException(exception.Message);
            }
        }
    }
}
