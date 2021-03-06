﻿using Microsoft.Azure.ServiceBus;
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

        //private ILoggerFactory loggerFactory = new LoggerFactory().AddConsole().AddAzureWebAppDiagnostics();
        private ILogger logger = null;
        private int _concurrentSessions;
        private int _sessionsInitializedCount = 0;
        private bool _autoTryReconnect = false;

        //SemaphoreSlim sLock = new SemaphoreSlim(5);

        protected SubscriptionReceiver() {
            // --- Use parameterized constructor
        }

        public async void TryReconnect() {
            if (MessagesListedBySession.Count == 0 && _sessionsInitializedCount >= _concurrentSessions) {
                _sessionsInitializedCount = 0;
                try {
                    await subscriptionClient.CloseAsync();
                } catch (Exception ex) {
                    if (!(logger is null)) {
                        logger.LogWarning(ex.Message);
                    }
                }
                subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);

                var sessionOptions = new SessionHandlerOptions(ExceptionReceivedHandler) {
                    AutoComplete = false,
                    MaxConcurrentSessions = _concurrentSessions,
                    MaxAutoRenewDuration = TimeSpan.FromSeconds(10)
                };

                subscriptionClient.PrefetchCount = 250;
                subscriptionClient.RegisterSessionHandler(OnMessage, sessionOptions);
            }
        }

        public SubscriptionReceiver(string connectionString, string topicName, string subscriptionName, int concurrentSessions = 5, bool autoTryReconnect = false) {
            ServiceBusConnectionString = connectionString;
            TopicName = topicName;
            SubscriptionName = subscriptionName;
            _concurrentSessions = concurrentSessions;
            _autoTryReconnect = autoTryReconnect;

            /*if (logger is null) {
                logger = loggerFactory.CreateLogger<SubscriptionReceiver>();
            }*/
        }

        public void Listen() {
            subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);
            RetryPolicy policy = new RetryExponential(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(29), 10);
            subscriptionClient.ServiceBusConnection.RetryPolicy = policy;

            var sessionOptions = new SessionHandlerOptions(ExceptionReceivedHandler) {
                AutoComplete = false,
                MaxConcurrentSessions = _concurrentSessions,
                MaxAutoRenewDuration = TimeSpan.FromSeconds(20)
                //MessageWaitTimeout = TimeSpan.FromSeconds(30)
            };

            subscriptionClient.PrefetchCount = 250;
            subscriptionClient.RegisterSessionHandler(OnMessage, sessionOptions);

            if (_autoTryReconnect) {
                while (true) {
                    Task.Delay(10000).GetAwaiter().GetResult();
                    TryReconnect();
                }
            }
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
                        _sessionsInitializedCount++;
                        MessagesListedBySession.TryAdd(session.SessionId, new List<string>());
                    }

                    string dataJSON = Encoding.UTF8.GetString(msg.Body);

                    MessagesListedBySession[session.SessionId].Add(dataJSON);

                    ProcessMessage(session, msg, dataJSON);

                    if (msg.Label.Equals("last", StringComparison.InvariantCultureIgnoreCase)) {
                        try {
                            ProcessMessagesWhenLastReceived(session, MessagesListedBySession[session.SessionId], msg);
                        } catch (Exception ex) {
                            if (!(logger is null)) {
                                logger.LogError(ex.Message);
                                logger.LogDebug(ex.StackTrace);
                            }
                        } finally {
                            MessagesListedBySession.Remove(session.SessionId);
                        }
                    }
                }
                //await sLock.WaitAsync();
                //await session.CompleteAsync(fullList.Select(m => m.SystemProperties.LockToken)).ContinueWith((t) => sLock.Release());
                await session.CompleteAsync(fullList.Select(m => m.SystemProperties.LockToken));
            } catch (Exception ex) {
                if (!(logger is null)) {
                    logger.LogError(ex.Message);
                    logger.LogDebug(ex.StackTrace);
                }
                //throw new ApplicationException(ex.Message + ex.StackTrace);
            }
        }

        Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs) {
            //sLock.Release();

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
