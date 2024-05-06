using Azure.Messaging.ServiceBus;
using CommentEverythingServiceBusConnectorNETCore.Monitoring.Exceptions;
using CommentEverythingServiceBusConnectorNETCore.Monitoring.Instrumentation.InstrumentedObjects;
using CommentEverythingServiceBusConnectorNETCore.Topic;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Monitoring.Instrumentation.Library {
    class SyntheticMessageProcessor : ServerlessSubscriptionReceiver {
        private static SessionlessTopicSender ts = null;
        private static ILogger _logger = null;

        /// <summary>
        /// Processes a synthetic message by forwarding it to a monitoring topic (e.g. monitoring.infrastructure.topic)
        /// with eventType="LAST_SYNTHETIC_MESSAGE_PUBLISHED" to be monitored for arrival at monitoring endpoint
        /// </summary>
        /// <param name="servicebusConnectionStr">Azure Service Bus connection string</param>
        /// <param name="monitoringTopic">Monitoring topic name to which synthetic messages should be forwarded</param>
        /// <param name="log">Log to write errors and warnings</param>
        public SyntheticMessageProcessor(string servicebusConnectionStr, string monitoringTopic, ILogger log) : base(log) {
            if (ts is null) {
                ts = new SessionlessTopicSender(servicebusConnectionStr, monitoringTopic, log);
            }
            if (_logger is null) {
                _logger = log;
            }
        }

        public override Task<string> ProcessMessage(ServiceBusReceivedMessage messageAsObject, string messageAsUTF8) {
            Task<string> returnTask;

            try {
                MonitorMessage monitoredMessage = JsonConvert.DeserializeObject<MonitorMessage>(messageAsUTF8);
                monitoredMessage.AssociatedId = messageAsObject.MessageId;

                returnTask = Task.FromResult(JsonConvert.SerializeObject(monitoredMessage));
            } catch (Exception ex) {
                ApplicationException exp = new ApplicationException($"ERROR in synthetic monitoring service - unable to process synthetic message caused by {ex.Message}");
                _logger.LogError($"Failed to process synthetic message - {ex.Message} {ex.StackTrace}");
                throw exp;
            }

            return returnTask;
        }

        public override Task ProcessMessagesWhenLastReceived(IList<string> listOfOriginalMessagesAsUTF8, ServiceBusReceivedMessage lastMessage, IList<string> listOfProcessedMessagesAsUTF8) {
            Task returnTask;

            try {
                Task sendTask = ts.Send(listOfProcessedMessagesAsUTF8, lastMessage.ApplicationProperties["CollectionId"].ToString(), lastMessage.ApplicationProperties["Context"].ToString(), DateTime.MinValue, "LAST_SYNTHETIC_MESSAGE_PUBLISHED");
                returnTask = Task.WhenAll(new Task[] { sendTask });
            } catch (Exception ex) {
                MonitoringException exp = new MonitoringException($"ERROR in synthetic monitoring service - unable to process group of synthetic messages caused by {ex.Message}");
                _logger.LogError($"Failed to process group of synthetic messages - {ex.Message} {ex.StackTrace}");
                throw exp;
            }

            return returnTask;
        }

        public override Task ProcessCollectionMessagesWhenAllReceived(Dictionary<string, IList<string>> dictionaryOfOriginalMessagesAsUTF8, ServiceBusReceivedMessage lastMessage, Dictionary<string, IList<string>> dictionaryOfProcessedMessagesAsUTF8) {
            // --- Do nothing
            return Task.CompletedTask;
        }
    }
}
