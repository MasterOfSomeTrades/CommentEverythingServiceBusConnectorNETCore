using CommentEverythingServiceBusConnectorNETCore.Topic;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorLib.Topic {
    public class GenericServerlessSubscriptionReceiver : ServerlessSubscriptionReceiver {
        private static ILogger _logger;
        private IMessageCollectionProcessor _commonProcessor;

        public GenericServerlessSubscriptionReceiver(string[] events, string listenerGroup, ILogger log, IMessageCollectionProcessor messageCollectionProcessor) : base(events, listenerGroup, log) {
            if (_logger is null) {
                _logger = log;
            }

            if (_commonProcessor is null) {
                _commonProcessor = messageCollectionProcessor;
                _commonProcessor.Setup(_logger);
            }
        }

        public override Task<string> ProcessMessage(Message messageAsObject, string messageAsUTF8) {
            // --- Do nothing
            return Task.FromResult(messageAsUTF8);
        }

        public override Task ProcessMessagesWhenLastReceived(IList<string> listOfOriginalMessagesAsUTF8, Message lastMessage, IList<string> listOfProcessedMessagesAsUTF8) {
            // --- Do nothing
            return Task.CompletedTask;
        }

        public override Task ProcessCollectionMessagesWhenAllReceived(Dictionary<string, IList<string>> dictionaryOfOriginalMessagesAsUTF8, Message lastMessage, Dictionary<string, IList<string>> dictionaryOfProcessedMessagesAsUTF8) {
            Task returnTask;

            try {
                Task processTask = _commonProcessor.ProcessCollectionMessagesWhenAllReceived(dictionaryOfOriginalMessagesAsUTF8, lastMessage, dictionaryOfProcessedMessagesAsUTF8);
                returnTask = Task.WhenAll(new Task[] { processTask });
            } catch (Exception ex) {
                _logger.LogError($"Failed to format notification - {ex.Message} {ex.StackTrace}");
                returnTask = Task.FromException(ex);
            }

            return returnTask;
        }
    }
}
