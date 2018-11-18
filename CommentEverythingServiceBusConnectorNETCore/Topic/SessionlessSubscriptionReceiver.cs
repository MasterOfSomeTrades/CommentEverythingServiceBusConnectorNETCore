using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Topic
{
    public abstract class SessionlessSubscriptionReceiver {
        ISubscriptionClient subscriptionClient;
        private string ServiceBusConnectionString;
        private string TopicName;
        private string SubscriptionName;
        private ConcurrentDictionary<string, ConcurrentBag<string>> MessagesListedByGroup = new ConcurrentDictionary<string, ConcurrentBag<string>>();

        private ILoggerFactory loggerFactory = new LoggerFactory().AddConsole().AddAzureWebAppDiagnostics();
        private ILogger logger = null;
        private int _concurrentSessions;
        private int _sessionsInitializedCount = 0;
        private bool _autoTryReconnect = false;

        //SemaphoreSlim sLock = new SemaphoreSlim(5);

        protected SessionlessSubscriptionReceiver() {
            // --- Use parameterized constructor
        }

        public async void TryReconnect() {
            if (MessagesListedByGroup.Count == 0 && _sessionsInitializedCount >= _concurrentSessions) {
                _sessionsInitializedCount = 0;
                try {
                    await subscriptionClient.CloseAsync();
                } catch (Exception ex) {
                    logger.LogWarning(ex.Message);
                }
                subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);

                var sessionOptions = new MessageHandlerOptions(ExceptionReceivedHandler) {
                    AutoComplete = false,
                    MaxConcurrentCalls = _concurrentSessions, 
                    MaxAutoRenewDuration = TimeSpan.FromMinutes(16)
                    //MessageWaitTimeout = TimeSpan.FromSeconds(30)
                };

                subscriptionClient.PrefetchCount = 250;
                subscriptionClient.RegisterMessageHandler(OnMessage, sessionOptions);
            }
        }

        public SessionlessSubscriptionReceiver(string connectionString, string topicName, string subscriptionName, int concurrentSessions = 10, bool autoTryReconnect = false) {
            ServiceBusConnectionString = connectionString;
            TopicName = topicName;
            SubscriptionName = subscriptionName;
            _concurrentSessions = concurrentSessions;
            _autoTryReconnect = autoTryReconnect;

            if (logger is null) {
                logger = loggerFactory.CreateLogger<SessionlessSubscriptionReceiver>();
            }
        }

        public void Listen() {
            subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);
            RetryPolicy policy = new RetryExponential(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(29), 10);
            subscriptionClient.ServiceBusConnection.RetryPolicy = policy;

            var sessionOptions = new MessageHandlerOptions(ExceptionReceivedHandler) {
                AutoComplete = false,
                MaxConcurrentCalls = _concurrentSessions
                //MessageWaitTimeout = TimeSpan.FromSeconds(30)
            };

            subscriptionClient.PrefetchCount = 250;
            subscriptionClient.RegisterMessageHandler(OnMessage, sessionOptions);

            if (_autoTryReconnect) {
                while (true) {
                    Task.Delay(10000).GetAwaiter().GetResult();
                    TryReconnect();
                }
            }
        }

        protected abstract void ProcessMessage(Message messageAsObject, string messageAsUTF8);
        protected abstract void ProcessMessagesWhenLastReceived(ConcurrentBag<string> listOfMessageAsUTF8InSession, Message lastMessage = null);

        private async Task OnMessage(Message messageToHandle, CancellationToken lockToken) {
            try {
                string groupId = messageToHandle.UserProperties["CollectionId"].ToString();

                if (!MessagesListedByGroup.ContainsKey(groupId)) {
                    MessagesListedByGroup.TryAdd(groupId, new ConcurrentBag<string>());
                }

                string dataJSON = Encoding.UTF8.GetString(messageToHandle.Body);

                MessagesListedByGroup[groupId].Add(dataJSON);

                ProcessMessage(messageToHandle, dataJSON);

                await subscriptionClient.CompleteAsync(messageToHandle.SystemProperties.LockToken);

                if (int.Parse(messageToHandle.UserProperties["Count"].ToString()) <= MessagesListedByGroup[groupId].Count()) {
                    try {
                        ProcessMessagesWhenLastReceived(MessagesListedByGroup[groupId], messageToHandle);
                    } catch (Exception ex) {
                        logger.LogError(ex.Message);
                        logger.LogDebug(ex.StackTrace);
                    } finally {
                        ConcurrentBag<string> removed = new ConcurrentBag<string>();
                        MessagesListedByGroup.TryRemove(groupId, out removed);
                    }
                }
                //await sLock.WaitAsync();
                //await session.CompleteAsync(fullList.Select(m => m.SystemProperties.LockToken)).ContinueWith((t) => sLock.Release());
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
