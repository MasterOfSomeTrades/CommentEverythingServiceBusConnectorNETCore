using Azure.Messaging.ServiceBus;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorLib.Topic {
    public interface IMessageCollectionProcessor {
        public IMessageCollectionProcessor Setup(ILogger logger);
        public Task ProcessCollectionMessagesWhenAllReceived(Dictionary<string, IList<string>> dictionaryOfOriginalMessagesAsUTF8, ServiceBusMessage lastMessage, Dictionary<string, IList<string>> dictionaryOfProcessedMessagesAsUTF8);
    }
}
