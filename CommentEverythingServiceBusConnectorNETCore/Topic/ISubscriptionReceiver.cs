using Azure.Messaging.ServiceBus;
using Microsoft.Azure.ServiceBus;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Topic {
    public interface ISubscriptionReceiver {
        Task<string> ProcessMessage(ServiceBusMessage messageAsObject, string messageAsUTF8);
        Task ProcessMessagesWhenLastReceived(IList<string> listOfOriginalMessagesAsUTF8, ServiceBusMessage lastMessage, IList<string> listOfProcessedMessagesAsUTF8);
    }
}
