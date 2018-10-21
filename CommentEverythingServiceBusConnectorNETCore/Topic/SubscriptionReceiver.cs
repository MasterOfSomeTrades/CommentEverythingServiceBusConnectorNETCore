using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorLib.Topic
{
    abstract class SubscriptionReceiver {
        ISubscriptionClient subscriptionClient;
        private string ServiceBusConnectionString;
        private string TopicName;
        private string SubscriptionName;
        private Dictionary<string, List<string>> MessagesListedBySession = new Dictionary<string, List<string>>();

        private ILoggerFactory loggerFactory = new LoggerFactory().AddConsole().AddAzureWebAppDiagnostics();
        private ILogger logger = null;

        SemaphoreSlim sLock = new SemaphoreSlim(5);

        protected SubscriptionReceiver() {
            // --- Use parameterized constructor
        }

        public SubscriptionReceiver(string connectionString, string topicName, string subscriptionName) {
            ServiceBusConnectionString = connectionString;
            TopicName = topicName;
            SubscriptionName = subscriptionName;

            if (logger is null) {
                logger = loggerFactory.CreateLogger<SubscriptionReceiver>();
            }
        }

        public void Listen() {
            subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);

            var sessionOptions = new SessionHandlerOptions(ExceptionReceivedHandler) {
                AutoComplete = false,
                MaxConcurrentSessions = 5,
                MessageWaitTimeout = TimeSpan.FromSeconds(30)
            };

            subscriptionClient.PrefetchCount = 250;
            subscriptionClient.RegisterSessionHandler(OnMessage, sessionOptions);
        }

        protected abstract void ProcessMessage(IMessageSession session, Message messageAsObject, string messageAsUTF8);
        protected abstract void ProcessMessagesWhenLastReceived(IMessageSession session, IList<string> listOfMessageAsUTF8InSession);

        private async Task OnMessage(IMessageSession session, Message messageToHandle, CancellationToken lockToken) {
            try {
                session.PrefetchCount = 500;
                IList<Message> batchList;
                List<Message> fullList = new List<Message> {
                    messageToHandle
                };
                batchList = await session.ReceiveAsync(500);
                if (!(batchList is null) && batchList.Count > 0) {
                    fullList.AddRange(batchList);
                }

                foreach (Message msg in fullList) {
                    if (!MessagesListedBySession.ContainsKey(session.SessionId)) {
                        MessagesListedBySession.TryAdd(session.SessionId, new List<string>());
                    }

                    string dataJSON = Encoding.UTF8.GetString(msg.Body);

                    MessagesListedBySession[session.SessionId].Add(dataJSON);

                    ProcessMessage(session, msg, dataJSON);

                    if (msg.Label.Equals("last", StringComparison.InvariantCultureIgnoreCase)) {
                        try {
                            ProcessMessagesWhenLastReceived(session, MessagesListedBySession[session.SessionId]);
                        } catch (Exception ex) {
                            logger.LogError(ex.Message);
                            logger.LogDebug(ex.StackTrace);
                            MessagesListedBySession.Remove(session.SessionId);
                            throw new ApplicationException(ex.Message);
                        } finally {
                            MessagesListedBySession.Remove(session.SessionId);
                        }
                    }
                }
                await sLock.WaitAsync();
                await session.CompleteAsync(fullList.Select(m => m.SystemProperties.LockToken)).ContinueWith((t) => sLock.Release());

            } catch (Exception ex) {
                logger.LogError(ex.Message);
                logger.LogDebug(ex.StackTrace);
                throw new ApplicationException(ex.Message);
            }
        }

        Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs) {
            sLock.Release();

            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            string exMsg = exceptionReceivedEventArgs.Exception.Message;
            string stackTrace = exceptionReceivedEventArgs.Exception.StackTrace;

            logger.LogError(exMsg);
            logger.LogDebug(stackTrace);

            return Task.CompletedTask;
        }
    }
}
