﻿using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Topic {
    public class TopicSender {
        protected TopicSender() {
            // --- Must use parameterized constructor
        }

        /// <summary>
        /// Sets up Topic with Connection String and Topic Name. Also instantiates an ILogger.
        /// </summary>
        /// <param name="connectionString">Connection String of Topic</param>
        /// <param name="topic">Name of Topic</param>
        public TopicSender(string connectionString, string topic) {
            ServiceBusConnectionString = connectionString;
            TopicName = topic;

            /*if (logger is null) {
                logger = loggerFactory.CreateLogger<TopicSender>();
            }*/
        }
        public TopicSender(string connectionString, string topic, ILogger log) {
            ServiceBusConnectionString = connectionString;
            TopicName = topic;

            if (logger is null) {
                logger = log;
            }
        }


        string ServiceBusConnectionString;
        string TopicName;
        private ServiceBusClient queueClient;
        private ServiceBusSender queueSender;
        private List<List<ServiceBusMessage>> _messageListStructure = new List<List<ServiceBusMessage>>();
        private long _currentSizeTotal = 0;

        //private ILoggerFactory loggerFactory = new LoggerFactory().AddConsole().AddAzureWebAppDiagnostics();
        private ILogger logger = null;

        /// <summary>
        /// Send single message (only one message in session) to Topic with custom UserProperties of CollectionId, Count, and Context.
        /// Message is labelled "last."
        /// </summary>
        /// <param name="message"></param>
        /// <param name="groupId"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<bool> Send(string message, string groupId, string context = "", string eventType = "", string subContext = "") {
            bool success = false;
            try {
                success = await Send(new string[] { message }, groupId, context, DateTime.MinValue, eventType, subContext);
            } catch (Exception ex) {
                if (!(logger is null)) {
                    logger.LogError(ex.Message);
                    logger.LogDebug(ex.StackTrace);
                }
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Send scheduled messages in a session to a Topic with custom UserProperties of CollectionId, Count, and Context.
        /// Messages are labelled "first," "interim," and "last" to distinguish messages in session.
        /// If only one message exists in the session, the message is labelled "last."
        /// </summary>
        /// <param name="messages">List of messages to write to Topic</param>
        /// <param name="groupId">Session Id</param>
        /// <param name="context">Used as CorrelationId and UserProperty['Context'] to filter messages</param>
        /// <param name="scheduledTime">DateTime in UTC to write to topic</param>
        /// <returns></returns>
        public async Task<bool> Send(string message, string groupId, string context, DateTime scheduledTime, string eventType = "", string subContext = "") {
            bool success = false;
            try {
                success = await Send(new string[] { message }, groupId, context, scheduledTime, eventType, subContext);
            } catch (Exception ex) {
                if (!(logger is null)) {
                    logger.LogError(ex.Message);
                    logger.LogDebug(ex.StackTrace);
                }
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Send messages in a session to a Topic with custom UserProperties of CollectionId, Count, and Context.
        /// Messages are labelled "first," "interim," and "last" to distinguish messages in session.
        /// If only one message exists in the session, the message is labelled "last."
        /// </summary>
        /// <param name="messages">List of messages to write to Topic</param>
        /// <param name="groupId">Session Id</param>
        /// <param name="context">Used as CorrelationId and UserProperty['Context'] to filter messages</param>
        /// <returns>Success (true or false)</returns>
        public async Task<bool> Send(IList<string> messages, string groupId, string context, DateTime scheduledTime, string eventType = "", string subContext = "") {
            bool success = false;

            try {
                // --- Create ServiceBusClient
                queueClient = new ServiceBusClient(ServiceBusConnectionString);
                // Create the sender
                queueSender = queueClient.CreateSender(TopicName);

                // --- Setup
                _messageListStructure = new List<List<ServiceBusMessage>>();
                _messageListStructure.Add(new List<ServiceBusMessage>());
                _currentSizeTotal = 0;
                int messageCount = 0;

                // --- Loop through message IList
                foreach (string m in messages) {
                    messageCount = messageCount + 1;
                    ServiceBusMessage msg = new ServiceBusMessage(Encoding.UTF8.GetBytes(m)) {
                        SessionId = groupId,
                        CorrelationId = context
                    };

                    if (messageCount == messages.Count) {
                        msg.Subject = "last";
                    } else if (messageCount == 1) {
                        msg.Subject = "first";
                    } else {
                        msg.Subject = "interim";
                    }
                    msg.ApplicationProperties.Add("CollectionId", groupId);
                    msg.ApplicationProperties.Add("Count", messages.Count);
                    msg.ApplicationProperties.Add("Context", context);
                    msg.ApplicationProperties.Add("EventType", eventType);
                    msg.ApplicationProperties.Add("SubContext", subContext);
                    msg.MessageId = Guid.NewGuid().ToString("D");
                    if (scheduledTime != DateTime.MinValue) {
                        msg.ScheduledEnqueueTime = scheduledTime;
                    }
                    /*
                    if (_currentSizeTotal + msg.Size > 100000) {
                        _currentSizeTotal = 0;
                        _messageListStructure.Add(new List<ServiceBusMessage>());
                    }
                    _currentSizeTotal = _currentSizeTotal + msg.Size;
                    if (!(logger is null)) {
                        logger.LogInformation("Adding message with size " + msg.Size.ToString() + " | Total messages size " + _currentSizeTotal.ToString());
                    }
                    */
                    _messageListStructure[_messageListStructure.Count - 1].Add(msg);
                }

                List<Task> taskList = new List<Task>();
                foreach (List<ServiceBusMessage> l in _messageListStructure) {
                    if (!(logger is null)) {
                        logger.LogInformation("Adding task to send message (" + (taskList.Count + 1).ToString() + ")");
                    }
                    taskList.Add(queueSender.SendMessagesAsync(l));
                }

                // --- Send the Messages to the queue.
                await Task.WhenAll(taskList);

                success = true;
            } catch (Exception exception) {
                if (!(logger is null)) {
                    logger.LogError(exception.Message);
                    logger.LogDebug(exception.StackTrace);
                }
                success = false;
            } finally {
                // --- Close queue
                //await queueClient.CloseAsync();
            }

            return success;
        }
    }
}
