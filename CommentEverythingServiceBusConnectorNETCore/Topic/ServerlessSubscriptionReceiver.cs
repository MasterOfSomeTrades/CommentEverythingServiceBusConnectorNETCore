﻿using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Topic {
    public abstract class ServerlessSubscriptionReceiver : ISubscriptionReceiver {
        //private ILoggerFactory loggerFactory = new LoggerFactory().AddConsole().AddAzureWebAppDiagnostics();
        private ILogger logger = null;
        private IList<string> _eventsToReceive = new List<string>();
        private string _listenerGroupId = "NOSESSIONS";

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
            /*if (logger is null) {
                logger = loggerFactory.CreateLogger<ServerlessSubscriptionReceiver>();
            }*/
        }

        public ServerlessSubscriptionReceiver(ILogger log) {
            if (logger is null) {
                logger = log;
            }
        }

        public ServerlessSubscriptionReceiver(string[] events, string listenerGroupIdentifier) {
            _eventsToReceive = events.ToList();
            _listenerGroupId = listenerGroupIdentifier;
        }

        public ServerlessSubscriptionReceiver(string[] events, string listenerGroupIdentifier, ILogger log) {
            if (logger is null) {
                logger = log;
            }
            _eventsToReceive = events.ToList();
            _listenerGroupId = listenerGroupIdentifier;
        }

        public abstract Task<string> ProcessMessage(Message messageAsObject, string messageAsUTF8);
        public abstract Task ProcessMessagesWhenLastReceived(IList<string> listOfOriginalMessagesAsUTF8, Message lastMessage, IList<string> listOfProcessedMessagesAsUTF8);
        public abstract Task ProcessCollectionMessagesWhenAllReceived(Dictionary<string, IList<string>> dictionaryOfOriginalMessagesAsUTF8, Message lastMessage, Dictionary<string, IList<string>> dictionaryOfProcessedMessagesAsUTF8);
        static IDatabase cache = lazyConnection.Value.GetDatabase();

        public async Task OnMessage(Message messageToHandle) {
            try {
                // --- Define groupId
                if (!(logger is null)) {
                    logger.LogInformation("Processing message in OnMessage(Message messageToHandle)");
                }
                string groupId = $"{messageToHandle.UserProperties["CollectionId"].ToString()}|{messageToHandle.UserProperties["EventType"].ToString()}|{_listenerGroupId}";
                string collectionId = $"{messageToHandle.UserProperties["CollectionId"].ToString()}|{_listenerGroupId}";

                // --- If no events listed, default to only EventType
                if (_eventsToReceive.Count == 0) {
                    _eventsToReceive.Add(messageToHandle.UserProperties["EventType"].ToString());
                }
                if (!(logger is null)) {
                    logger.LogInformation($"Setting redis keys for message processing with groupId {groupId}");
                }
                // _processedMessagesHolder.TryAdd(groupId, new ConcurrentDictionary<string, string>());
                //await cache.HashSetAsync(groupId, new HashEntry[] { });
                try {
                    cache.HashSet(groupId, new HashEntry[] { });
                } catch (Exception ex) {
                    if (!(logger is null)) {
                        logger.LogWarning($"Could not set redis hash {ex.Message} {ex.StackTrace}");
                    }
                }
                //await cache.KeyExpireAsync(groupId, new TimeSpan(0, 30, 0));
                try {
                    cache.KeyExpire(groupId, new TimeSpan(0, 30, 0));
                    if (!(logger is null)) {
                        logger.LogInformation($"Redis hash expiry set to 30 minutes");
                    }
                } catch (Exception ex) {
                    if (!(logger is null)) {
                        logger.LogWarning($"Could not set redis hash expiry {ex.Message} {ex.StackTrace}");
                    }
                }

                string dataJSON = Encoding.UTF8.GetString(messageToHandle.Body);

                int totalMessagesCount = int.Parse(messageToHandle.UserProperties["Count"].ToString());
                if (!(logger is null)) {
                    logger.LogInformation("Running ProcessMessage(...)");
                }
                string updatedMessage = await ProcessMessage(messageToHandle, dataJSON);
                bool isNewEntry = false;

                if (!updatedMessage.Equals("", StringComparison.InvariantCultureIgnoreCase)) {
                    //_processedMessagesHolder[groupId].TryAdd(messageToHandle.MessageId, updatedMessage);
                    if (!(await cache.HashGetAllAsync(groupId)).ToStringDictionary().ContainsKey(messageToHandle.MessageId)) {
                        isNewEntry = true;
                        await cache.HashSetAsync(groupId, new HashEntry[] { new HashEntry(messageToHandle.MessageId, updatedMessage) });
                        await cache.KeyExpireAsync(groupId, new TimeSpan(0, 30, 0));
                    }
                }

                //await subscriptionClient.CompleteAsync(messageToHandle.SystemProperties.LockToken);

                //_messageHolder[groupId].Add(dataJSON);
                await cache.SetAddAsync($"{groupId}|messageHolder", dataJSON);
                await cache.KeyExpireAsync($"{groupId}|messageHolder", new TimeSpan(0, 30, 0));

                //int processedMessagesCount = _messageHolder[groupId].Count;
                RedisValue[] _messageHolder = await cache.SetMembersAsync($"{groupId}|messageHolder");
                int processedMessagesCount = _messageHolder.Length;

                if (processedMessagesCount == totalMessagesCount && isNewEntry) {
                    if (await cache.HashIncrementAsync("ServerlessTopicMessagesProcessed", groupId) == 1) {
                        // --- Get original messages list
                        IList<string> messagesList = new List<string>();
                        foreach (RedisValue rv in _messageHolder) {
                            messagesList.Add(rv.ToString());
                        }
                        //await cache.KeyDeleteAsync($"{groupId}|messageHolder"); // commented out - TODO: verify functionality

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

                        if (!(logger is null)) {
                            logger.LogInformation(string.Format("====== PROCESSING GROUP OF {0} MESSAGES FOR {1} ======", totalMessagesCount.ToString(), messageToHandle.UserProperties["CollectionId"].ToString()));
                        }
                        await ProcessMessagesWhenLastReceived(messagesList, messageToHandle, processedMessagesList);

                        // --- Add to Events Received for Parent Collection
                        await cache.SetAddAsync($"{collectionId}|EventsReceived", messageToHandle.UserProperties["EventType"].ToString());
                        await cache.KeyExpireAsync($"{collectionId}|EventsReceived", new TimeSpan(0, 30, 0));

                        bool AllEventsReceived = true;
                        foreach (string e in _eventsToReceive) {
                            if (!(await cache.SetContainsAsync($"{collectionId}|EventsReceived", e))) {
                                AllEventsReceived = false;
                                break;
                            }
                        }

                        // --- Delete child message holders
                        if (AllEventsReceived) {
                            Dictionary<string, IList<string>> originalMessagesDictionary = new Dictionary<string, IList<string>>();
                            Dictionary<string, IList<string>> processedMessagesDictionary = new Dictionary<string, IList<string>>();
                            foreach (string e in _eventsToReceive) {
                                await cache.SetRemoveAsync($"{collectionId}|EventsReceived", e);
                                if (await cache.SetLengthAsync($"{collectionId}|EventsReceived") == 0) {
                                    await cache.KeyDeleteAsync($"{collectionId}|EventsReceived");
                                }

                                // --- Get original messages list
                                IList<string> eventOriginalMessagesList = new List<string>();
                                //RedisValue[] eventOriginalMessagesArray = await cache.SetMembersAsync($"{messageToHandle.UserProperties["CollectionId"].ToString()}|{e}|messageHolder");
                                RedisValue[] eventOriginalMessagesArray = await cache.SetMembersAsync($"{messageToHandle.UserProperties["CollectionId"].ToString()}|{e}|{_listenerGroupId}|messageHolder");
                                foreach (RedisValue rv in eventOriginalMessagesArray) {
                                    eventOriginalMessagesList.Add(rv.ToString());
                                }
                                originalMessagesDictionary.Add(e, eventOriginalMessagesList);
                                //await cache.KeyDeleteAsync($"{messageToHandle.UserProperties["CollectionId"].ToString()}|{e}|messageHolder");
                                await cache.KeyDeleteAsync($"{messageToHandle.UserProperties["CollectionId"].ToString()}|{e}|{_listenerGroupId}|messageHolder");

                                // --- Get processed messages list
                                IList<string> eventProcessedMessagesList = new List<string>();
                                //HashEntry[] eventProcessedMessagesHash = await cache.HashGetAllAsync($"{messageToHandle.UserProperties["CollectionId"].ToString()}|{e}");
                                HashEntry[] eventProcessedMessagesHash = await cache.HashGetAllAsync($"{messageToHandle.UserProperties["CollectionId"].ToString()}|{e}|{_listenerGroupId}");
                                if (eventProcessedMessagesHash.Length > 0) {
                                    foreach (HashEntry he in eventProcessedMessagesHash) {
                                        eventProcessedMessagesList.Add(he.Value.ToString());
                                    }
                                }
                                processedMessagesDictionary.Add(e, eventProcessedMessagesList);

                                //await cache.KeyDeleteAsync($"{messageToHandle.UserProperties["CollectionId"].ToString()}|{e}");
                                await cache.KeyDeleteAsync($"{messageToHandle.UserProperties["CollectionId"].ToString()}|{e}|{_listenerGroupId}");
                                //await cache.HashDeleteAsync("ServerlessTopicMessagesProcessed", $"{messageToHandle.UserProperties["CollectionId"].ToString()}|{e}");
                                await cache.HashDeleteAsync("ServerlessTopicMessagesProcessed", $"{messageToHandle.UserProperties["CollectionId"].ToString()}|{e}|{_listenerGroupId}");
                            }

                            await ProcessCollectionMessagesWhenAllReceived(originalMessagesDictionary, messageToHandle, processedMessagesDictionary);
                        }
                    }
                }

                if (!(logger is null)) {
                    logger.LogInformation(string.Format("----- Processed message {0} of {1} for {2} -----", processedMessagesCount.ToString(), totalMessagesCount.ToString(), messageToHandle.UserProperties["CollectionId"].ToString()));
                }
            } catch (Exception ex) {
                //await subscriptionClient.AbandonAsync(messageToHandle.SystemProperties.LockToken);
                throw new ApplicationException($"Error in reading messages - {ex.Message} {ex.StackTrace}");
            }
        }

        Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs) {
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            string exMsg = exceptionReceivedEventArgs.Exception.Message;
            string stackTrace = exceptionReceivedEventArgs.Exception.StackTrace;

            if (!(logger is null)) {
                logger.LogError(exMsg);
                logger.LogDebug(stackTrace);
            }

            return Task.CompletedTask;
        }
    }
}
