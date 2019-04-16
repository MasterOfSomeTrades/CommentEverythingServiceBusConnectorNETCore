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

        public static async void OnMessage(Message theMessage, DurableOrchestrationClient client, ILogger logger) {
            try {
                log = logger;
                string groupId = theMessage.UserProperties["CollectionId"].ToString();
                if (await client.GetStatusAsync(groupId + "_topics") == null) {
                    string clientId = await client.StartNewAsync("StartMessagesOrchestrator", groupId + "_topics", theMessage);
                    log.LogInformation(string.Format("Durable client started with ID {0}", clientId));
                }

                log.LogInformation(string.Format("Raising event {0}", EventName));
                await client.RaiseEventAsync(groupId + "_topics", EventName, theMessage);
            } catch (Exception ex) {
                log.LogError(ex.Message + ex.StackTrace);
            }
        }

        public async void StartOrchestrator(DurableOrchestrationContext context, Message firstMessage) {
            HashSet<string> messageIds = new HashSet<string>();
            IList<string> originalMessages = new List<string>();
            IList<string> processedMessages = new List<string>();
            int remainingToBeProcessed = int.Parse(firstMessage.UserProperties["Count"].ToString());
            Message thisMessage = null;

            while (remainingToBeProcessed > 0) {
                thisMessage = await context.WaitForExternalEvent<Message>(EventName, TimeSpan.FromMinutes(5));
                if (!messageIds.Contains(thisMessage.MessageId)) {
                    remainingToBeProcessed--;
                    messageIds.Add(thisMessage.MessageId);
                    string dataJSON = Encoding.UTF8.GetString(thisMessage.Body);
                    string processedMessage = await ProcessMessage(thisMessage, dataJSON);
                    originalMessages.Add(dataJSON);
                    processedMessages.Add(processedMessage);
                }
            }

            ProcessMessagesWhenLastReceived(originalMessages, thisMessage, processedMessages);
        }

        [FunctionName("StartMessagesOrchestrator")]
        public abstract void StartMessagesOrchestrator([OrchestrationTrigger] DurableOrchestrationContext context);

        public abstract Task<string> ProcessMessage(Message messageAsObject, string messageAsUTF8);

        public abstract void ProcessMessagesWhenLastReceived(IList<string> listOfOriginalMessagesAsUTF8, Message lastMessage, IList<string> listOfProcessedMessagesAsUTF8);
    }
}
