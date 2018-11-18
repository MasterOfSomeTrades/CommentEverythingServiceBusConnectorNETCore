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
        private ConcurrentDictionary<string, ConcurrentDictionary<string, string>> MessagesListedByGroup = new ConcurrentDictionary<string, ConcurrentDictionary<string, string>>();

        private ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _processedMessagesDictionary = new ConcurrentDictionary<string, ConcurrentDictionary<string, byte>>();
        private ConcurrentDictionary<string, short> _processedSessionsDictionary = new ConcurrentDictionary<string, short>();

        private ILoggerFactory loggerFactory = new LoggerFactory().AddConsole().AddAzureWebAppDiagnostics();
        private ILogger logger = null;
        private int _concurrentSessions;
        private int _sessionsInitializedCount = 0;
        private bool _autoTryReconnect = false;
        private int _messageLockMinutes;

        //SemaphoreSlim sLock = new SemaphoreSlim(5);

        protected SessionlessSubscriptionReceiver() {
            // --- Use parameterized constructor
        }

        public async void TryReconnect() {
            if (MessagesListedByGroup.Count == 0 && _sessionsInitializedCount >= _concurrentSessions) {
                _sessionsInitializedCount = 0;
                _processedSessionsDictionary = new ConcurrentDictionary<string, short>();
                try {
                    await subscriptionClient.CloseAsync();
                } catch (Exception ex) {
                    logger.LogWarning(ex.Message);
                }
                subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);

                var sessionOptions = new MessageHandlerOptions(ExceptionReceivedHandler) {
                    AutoComplete = false,
                    MaxConcurrentCalls = _concurrentSessions, 
                    MaxAutoRenewDuration = TimeSpan.FromMinutes(_messageLockMinutes)
                    //MessageWaitTimeout = TimeSpan.FromSeconds(30)
                };

                subscriptionClient.PrefetchCount = 250;
                subscriptionClient.RegisterMessageHandler(OnMessage, sessionOptions);
            }
        }

        /// <summary>
        /// Constructor for SessionlessSubscriptionReceiver.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="topicName"></param>
        /// <param name="subscriptionName"></param>
        /// <param name="concurrentSessions"></param>
        /// <param name="autoTryReconnect"></param>
        /// <param name="messageLockMinutes"></param>
        public SessionlessSubscriptionReceiver(string connectionString, string topicName, string subscriptionName, int concurrentSessions = 10, bool autoTryReconnect = false, int messageLockMinutes = 15) {
            ServiceBusConnectionString = connectionString;
            TopicName = topicName;
            SubscriptionName = subscriptionName;
            _concurrentSessions = concurrentSessions;
            _autoTryReconnect = autoTryReconnect;
            _messageLockMinutes = messageLockMinutes;

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
                MaxConcurrentCalls = _concurrentSessions,
                MaxAutoRenewDuration = TimeSpan.FromMinutes(_messageLockMinutes)
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
        protected abstract void ProcessMessagesWhenLastReceived(IList<string> listOfMessageAsUTF8InSession, Message lastMessage = null);

        private async Task OnMessage(Message messageToHandle, CancellationToken lockToken) {
            try {
                string groupId = messageToHandle.UserProperties["CollectionId"].ToString();

                if (!_processedSessionsDictionary.ContainsKey(groupId)) {
                    MessagesListedByGroup.TryAdd(groupId, new ConcurrentDictionary<string, string>());
                    _processedMessagesDictionary.TryAdd(groupId, new ConcurrentDictionary<string, byte>());
                    _processedSessionsDictionary.TryAdd(groupId, 0);
                }

                bool messageAdded = false;
                if (!_processedMessagesDictionary[groupId].ContainsKey(messageToHandle.MessageId)) {
                    messageAdded = _processedMessagesDictionary[groupId].TryAdd(messageToHandle.MessageId, 1);
                }

                if (messageAdded) {
                    string dataJSON = Encoding.UTF8.GetString(messageToHandle.Body);

                    MessagesListedByGroup[groupId].TryAdd(messageToHandle.MessageId, dataJSON);

                    ProcessMessage(messageToHandle, dataJSON);

                    await subscriptionClient.CompleteAsync(messageToHandle.SystemProperties.LockToken);

                    if (int.Parse(messageToHandle.UserProperties["Count"].ToString()) <= MessagesListedByGroup[groupId].Count()) {
                        if (MessagesListedByGroup[groupId].Count > int.Parse(messageToHandle.UserProperties["Count"].ToString())) {
                            throw new ApplicationException(String.Format("Duplicate message processing occurred for group ID {0}", groupId));
                        }
                        try {
                            if (_processedSessionsDictionary[groupId] == 0) {
                                _processedSessionsDictionary[groupId]++;
                                ProcessMessagesWhenLastReceived(MessagesListedByGroup[groupId].Values.ToList(), messageToHandle);
                            } else {
                                logger.LogWarning(String.Format("Duplicate batch processing (Group={0})", groupId));
                            }
                        } catch (Exception ex) {
                            logger.LogError(ex.Message);
                            logger.LogDebug(ex.StackTrace);
                        } finally {
                            ConcurrentDictionary<string, string> removed = new ConcurrentDictionary<string, string>();
                            ConcurrentDictionary<string, byte> removedDictionary = new ConcurrentDictionary<string, byte>();
                            MessagesListedByGroup.TryRemove(groupId, out removed);
                            _processedMessagesDictionary.TryRemove(groupId, out removedDictionary);
                        }
                    }
                    //await sLock.WaitAsync();
                    //await session.CompleteAsync(fullList.Select(m => m.SystemProperties.LockToken)).ContinueWith((t) => sLock.Release());
                } else {
                    logger.LogWarning(String.Format("Duplicate message processing (Group={0} Message={1})", groupId, messageToHandle.MessageId));
                }
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
