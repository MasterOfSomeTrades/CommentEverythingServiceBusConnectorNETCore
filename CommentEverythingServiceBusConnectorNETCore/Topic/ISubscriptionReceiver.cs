﻿using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Topic {
    public interface ISubscriptionReceiver {
        Task<string> ProcessMessage(Message messageAsObject, string messageAsUTF8);
        void ProcessMessagesWhenLastReceived(IList<string> listOfOriginalMessagesAsUTF8, Message lastMessage, IList<string> listOfProcessedMessagesAsUTF8);
    }
}