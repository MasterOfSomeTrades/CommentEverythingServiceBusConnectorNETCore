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
    public abstract class SubscriptionReceiver {
        ISubscriptionClient subscriptionClient;
        private string ServiceBusConnectionString;
        private string TopicName;
        private string SubscriptionName;
        private Dictionary<string, List<string>> MessagesListedBySession = new Dictionary<string, List<string>>();

        private ILoggerFactory loggerFactory = new LoggerFactory().AddConsole().AddAzureWebAppDiagnostics();
        private ILogger logger = null;
        private int _concurrentSessions;

        //SemaphoreSlim sLock = new SemaphoreSlim(5);

        protected SubscriptionReceiver() {
            // --- Use parameterized constructor
        }

        public SubscriptionReceiver(string connectionString, string topicName, string subscriptionName, int concurrentSessions = 5) {
            ServiceBusConnectionString = connectionString;
            TopicName = topicName;
            SubscriptionName = subscriptionName;
            _concurrentSessions = concurrentSessions;

            if (logger is null) {
                logger = loggerFactory.CreateLogger<SubscriptionReceiver>();
            }
        }

        public void Listen() {
            subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);

            var sessionOptions = new SessionHandlerOptions(ExceptionReceivedHandler) {
                AutoComplete = false,
                MaxConcurrentSessions = _concurrentSessions,
                MaxAutoRenewDuration = TimeSpan.FromSeconds(29)
                //MessageWaitTimeout = TimeSpan.FromSeconds(30)
            };

            subscriptionClient.PrefetchCount = 250;
            subscriptionClient.RegisterSessionHandler(OnMessage, sessionOptions);
        }

        protected abstract void ProcessMessage(IMessageSession session, Message messageAsObject, string messageAsUTF8);
        protected abstract void ProcessMessagesWhenLastReceived(IMessageSession session, IList<string> listOfMessageAsUTF8InSession, Message lastMessage = null);

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
                            ProcessMessagesWhenLastReceived(session, MessagesListedBySession[session.SessionId], msg);
                        } catch (Exception ex) {
                            logger.LogError(ex.Message);
                            logger.LogDebug(ex.StackTrace);
                        } finally {
                            MessagesListedBySession.Remove(session.SessionId);
                        }
                    }
                }
                //await sLock.WaitAsync();
                //await session.CompleteAsync(fullList.Select(m => m.SystemProperties.LockToken)).ContinueWith((t) => sLock.Release());
                await session.CompleteAsync(fullList.Select(m => m.SystemProperties.LockToken));

                var sessionOptions = new SessionHandlerOptions(ExceptionReceivedHandler) {
                    AutoComplete = false,
                    MaxConcurrentSessions = _concurrentSessions,
                    MaxAutoRenewDuration = TimeSpan.FromSeconds(29)
                    //MessageWaitTimeout = TimeSpan.FromSeconds(30)
                };
                subscriptionClient.RegisterSessionHandler(OnMessage, sessionOptions);
            } catch (Exception ex) {
                logger.LogError(ex.Message);
                logger.LogDebug(ex.StackTrace);
                //throw new ApplicationException(ex.Message + ex.StackTrace);
            }
        }

        Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs) {
            //sLock.Release();

            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            string exMsg = exceptionReceivedEventArgs.Exception.Message;
            string stackTrace = exceptionReceivedEventArgs.Exception.StackTrace;

            logger.LogError(exMsg);
            logger.LogDebug(stackTrace);

            return Task.CompletedTask;
        }
    }
}
