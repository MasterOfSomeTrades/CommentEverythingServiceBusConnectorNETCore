using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Topic {
    public abstract class OrchestratedSubscriptionReceiver : ISubscriptionReceiver {
        private static ILogger log = null;
        private const string EventName = "CETOPIC_MESSAGE_RECEIVED";
        private static DurableOrchestrationClient _client = null;

        public static async Task OnMessage(Message theMessage, DurableOrchestrationClient client, ILogger logger, string orchestrationStarterName = "StartMessagesOrchestrator") {
            try {
                log = logger;
                if (_client is null) {
                    _client = client;
                }
                string groupId = theMessage.UserProperties["CollectionId"].ToString();
                if (await _client.GetStatusAsync(groupId + "_topics") is null) {
                    string clientId = await _client.StartNewAsync(orchestrationStarterName, groupId + "_topics", theMessage);
                    log.LogInformation(string.Format("Durable client started with ID {0}", clientId));
                }

                log.LogInformation(string.Format("Event raised: {0}", EventName));
                await _client.RaiseEventAsync(groupId + "_topics", EventName, theMessage);
            } catch (Exception ex) {
                log.LogError(ex.Message + ex.StackTrace);
            }
        }

        public async Task StartOrchestrator(DurableOrchestrationContext context, Message firstMessage) {
            HashSet<string> messageIds = new HashSet<string>();
            IList<string> originalMessages = new List<string>();
            IList<string> processedMessages = new List<string>();
            int remainingToBeProcessed = int.Parse(firstMessage.UserProperties["Count"].ToString());
            int overrideNumber = remainingToBeProcessed + 3;
            Message thisMessage = null;

            while (remainingToBeProcessed > 0 && overrideNumber > 0) {
                thisMessage = await context.WaitForExternalEvent<Message>(EventName, TimeSpan.FromMinutes(1));
                overrideNumber--;
                if (!messageIds.Contains(thisMessage.MessageId)) {
                    remainingToBeProcessed--;
                    messageIds.Add(thisMessage.MessageId);
                    string dataJSON = Encoding.UTF8.GetString(thisMessage.Body);
                    string processedMessage = await ProcessMessage(thisMessage, dataJSON);
                    originalMessages.Add(dataJSON);
                    processedMessages.Add(processedMessage);
                }
            }

            if (remainingToBeProcessed == 0) {
                ProcessMessagesWhenLastReceived(originalMessages, thisMessage, processedMessages);
            } else {
                throw new ApplicationException("Missing messages - some messages have not been received by the orchestrator");
            }

            await _client.TerminateAsync(thisMessage.UserProperties["CollectionId"].ToString() + "_topics", "All messages processed for this context");
        }

        [FunctionName("StartMessagesOrchestrator")]
        public abstract void StartMessagesOrchestrator([OrchestrationTrigger] DurableOrchestrationContext context);

        public abstract Task<string> ProcessMessage(Message messageAsObject, string messageAsUTF8);

        public abstract void ProcessMessagesWhenLastReceived(IList<string> listOfOriginalMessagesAsUTF8, Message lastMessage, IList<string> listOfProcessedMessagesAsUTF8);
    }
}
