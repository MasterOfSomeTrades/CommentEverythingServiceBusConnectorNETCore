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
    public abstract class SessionlessSubscriptionReceiver : ISubscriptionReceiver {
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

        private ConcurrentDictionary<string, HashSet<string>> _messageHolder = new ConcurrentDictionary<string, HashSet<string>>();
        private ConcurrentDictionary<string, ConcurrentDictionary<string, string>> _processedMessagesHolder = new ConcurrentDictionary<string, ConcurrentDictionary<string, string>>();

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

                subscriptionClient.PrefetchCount = 0;
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

        public async void Listen() {
            subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);
            RetryPolicy policy = new RetryExponential(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), 3);
            subscriptionClient.ServiceBusConnection.RetryPolicy = policy;

            var sessionOptions = new MessageHandlerOptions(ExceptionReceivedHandler) {
                AutoComplete = false,
                MaxConcurrentCalls = _concurrentSessions,
                MaxAutoRenewDuration = TimeSpan.FromMinutes(_messageLockMinutes)
            };

            subscriptionClient.PrefetchCount = 0;
            subscriptionClient.RegisterMessageHandler(OnMessage, sessionOptions);

            if (_autoTryReconnect) {
                while (true) {
                    await Task.Delay(10000);
                    TryReconnect();
                }
            }
        }

        public abstract Task<string> ProcessMessage(Message messageAsObject, string messageAsUTF8);
        public abstract Task ProcessMessagesWhenLastReceived(IList<string> listOfOriginalMessagesAsUTF8, Message lastMessage = null, IList<string> listOfProcessedMessagesAsUTF8 = null);

        private async Task OnMessage(Message messageToHandle, CancellationToken lockToken) {
            try {
                string groupId = messageToHandle.UserProperties["CollectionId"].ToString();
                if (_messageHolder.TryAdd(groupId, new HashSet<string>())) {
                    _processedMessagesHolder.TryAdd(groupId, new ConcurrentDictionary<string, string>());
                }

                string dataJSON = Encoding.UTF8.GetString(messageToHandle.Body);
                
                int totalMessagesCount = int.Parse(messageToHandle.UserProperties["Count"].ToString());

                string updatedMessage = await ProcessMessage(messageToHandle, dataJSON);

                if (!updatedMessage.Equals("", StringComparison.InvariantCultureIgnoreCase)) {
                    _processedMessagesHolder[groupId].TryAdd(messageToHandle.MessageId, updatedMessage);
                }

                await subscriptionClient.CompleteAsync(messageToHandle.SystemProperties.LockToken);

                _messageHolder[groupId].Add(dataJSON);

                int processedMessagesCount = _messageHolder[groupId].Count;

                if (processedMessagesCount == totalMessagesCount) {
                    // --- Get original messages list
                    HashSet<string> removed = new HashSet<string>();
                    _messageHolder.TryRemove(groupId, out removed);
                    IList<string> messagesList = removed.ToList();

                    // --- Get processed messages list
                    ConcurrentDictionary<string, string> removedDictionary = new ConcurrentDictionary<string, string>();
                    _processedMessagesHolder.TryRemove(groupId, out removedDictionary);
                    IList<string> processedMessagesList = null;
                    if (removedDictionary.Count > 0) {
                        processedMessagesList = removedDictionary.Values.ToList();
                    }

                    logger.LogInformation(String.Format("====== PROCESSING GROUP OF {0} MESSAGES FOR {1} ======", totalMessagesCount.ToString(), messageToHandle.UserProperties["CollectionId"].ToString()));
                    await ProcessMessagesWhenLastReceived(messagesList, messageToHandle, processedMessagesList);
                }

                logger.LogInformation(String.Format("----- Processed message {0} of {1} for {2} -----", processedMessagesCount.ToString(), totalMessagesCount.ToString(), messageToHandle.UserProperties["CollectionId"].ToString()));
            } catch (Exception ex) {
                await subscriptionClient.AbandonAsync(messageToHandle.SystemProperties.LockToken);
                //logger.LogError(ex.Message + ex.StackTrace);
                //throw new ApplicationException(ex.Message + ex.StackTrace);
                throw ex;
            }
        }

        Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs) {
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            string exMsg = exceptionReceivedEventArgs.Exception.Message;
            string stackTrace = exceptionReceivedEventArgs.Exception.StackTrace;

            logger.LogError(exMsg);
            logger.LogDebug(stackTrace);

            return Task.CompletedTask;
        }
    }
}
