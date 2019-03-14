using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Topic {
    public abstract class ServerlessSubscriptionReceiver {
        private ILoggerFactory loggerFactory = new LoggerFactory().AddConsole().AddAzureWebAppDiagnostics();
        private ILogger logger = null;

        //private ConcurrentDictionary<string, HashSet<string>> _messageHolder = new ConcurrentDictionary<string, HashSet<string>>();

        //SemaphoreSlim sLock = new SemaphoreSlim(5);

        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() => {
            string cacheConnection = Environment.GetEnvironmentVariable("redis").ToString();
            return ConnectionMultiplexer.Connect(cacheConnection);
        });

        public static ConnectionMultiplexer Connection {
            get {
                return lazyConnection.Value;
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
        public ServerlessSubscriptionReceiver() {
            if (logger is null) {
                logger = loggerFactory.CreateLogger<ServerlessSubscriptionReceiver>();
            }
        }

        protected abstract Task<string> ProcessMessage(Message messageAsObject, string messageAsUTF8);
        protected abstract void ProcessMessagesWhenLastReceived(IList<string> listOfOriginalMessagesAsUTF8, Message lastMessage = null, IList<string> listOfProcessedMessagesAsUTF8 = null);
        static IDatabase cache = lazyConnection.Value.GetDatabase();

        public async Task OnMessage(Message messageToHandle) {
            try {
                string groupId = messageToHandle.UserProperties["CollectionId"].ToString();

                // _processedMessagesHolder.TryAdd(groupId, new ConcurrentDictionary<string, string>());
                await cache.HashSetAsync(groupId, new HashEntry[] { });

                string dataJSON = Encoding.UTF8.GetString(messageToHandle.Body);

                int totalMessagesCount = int.Parse(messageToHandle.UserProperties["Count"].ToString());

                string updatedMessage = await ProcessMessage(messageToHandle, dataJSON);
                bool isNewEntry = false;

                if (!updatedMessage.Equals("", StringComparison.InvariantCultureIgnoreCase)) {
                    //_processedMessagesHolder[groupId].TryAdd(messageToHandle.MessageId, updatedMessage);
                    if (!(await cache.HashGetAllAsync(groupId)).ToStringDictionary().ContainsKey(messageToHandle.MessageId)) {
                        isNewEntry = true;
                        await cache.HashSetAsync(groupId, new HashEntry[] { new HashEntry(messageToHandle.MessageId, updatedMessage) });
                    }
                }

                //await subscriptionClient.CompleteAsync(messageToHandle.SystemProperties.LockToken);

                //_messageHolder[groupId].Add(dataJSON);
                await cache.SetAddAsync(groupId + "_messageHolder", dataJSON);

                //int processedMessagesCount = _messageHolder[groupId].Count;
                RedisValue[] _messageHolder = await cache.SetMembersAsync(groupId + "_messageHolder");
                int processedMessagesCount = _messageHolder.Length;

                long numberProcessed = -1;

                if (processedMessagesCount == totalMessagesCount && isNewEntry) {
                    if (await cache.HashIncrementAsync("ServerlessTopicMessagesProcessed", groupId) == 1) {
                        // --- Get original messages list
                        //HashSet<string> removed = new HashSet<string>();
                        //_messageHolder.TryRemove(groupId, out removed);
                        //IList<string> messagesList = removed.ToList();
                        IList<string> messagesList = new List<string>();
                        foreach (RedisValue rv in _messageHolder) {
                            messagesList.Add(rv.ToString());
                        }
                        await cache.KeyDeleteAsync(groupId + "_messageHolder");

                        // --- Get processed messages list
                        IList<string> processedMessagesList = new List<string>();
                        //if (removedDictionary.Count > 0) {
                        HashEntry[] processedMessages = await cache.HashGetAllAsync(groupId);
                        if (processedMessages.Length > 0) {
                            //processedMessagesList = removedDictionary.Values.ToList();
                            foreach (HashEntry he in processedMessages) {
                                processedMessagesList.Add(he.Value.ToString());
                            }
                        }
                        //}

                        //_processedMessagesHolder.TryRemove(groupId, out removedDictionary);

                        logger.LogInformation(string.Format("====== PROCESSING GROUP OF {0} MESSAGES FOR {1} ======", totalMessagesCount.ToString(), messageToHandle.UserProperties["CollectionId"].ToString()));
                        ProcessMessagesWhenLastReceived(messagesList, messageToHandle, processedMessagesList);

                        await cache.KeyDeleteAsync(groupId);
                        await cache.HashDeleteAsync("ServerlessTopicMessagesProcessed", groupId);
                    }
                }

                logger.LogInformation(string.Format("----- Processed message {0} of {1} for {2} -----", processedMessagesCount.ToString(), totalMessagesCount.ToString(), messageToHandle.UserProperties["CollectionId"].ToString()));
            } catch {
                //await subscriptionClient.AbandonAsync(messageToHandle.SystemProperties.LockToken);
                throw;
                //throw new ApplicationException(ex.Message + ex.StackTrace);
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
