using CommentEverythingServiceBusConnectorNETCore.Monitoring.Exceptions;
using CommentEverythingServiceBusConnectorNETCore.Monitoring.Instrumentation.Listeners;
using CommentEverythingServiceBusConnectorNETCore.Topic;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Monitoring.Instrumentation.InstrumentedObjects {
    /// <summary>
    /// Listen for MonitorMessages and forward to monitoring endpoint subscription for monitoring.
    /// This listener <B><I>must</I></B> be placed on the service to be monitored.
    /// </summary>
    public class SyntheticMonitoredMessageListener {
        public SyntheticMonitoredMessageListener(string connectionString, string monitoringTopic, ILogger log) {
            _connectionString = connectionString;
            _monitoringTopic = monitoringTopic;
            if (_logger is null) {
                _logger = log;
            }
        }

        private static string _connectionString;
        private static string _monitoringTopic;
        private static ILogger _logger;
        private static SessionlessTopicSender ts = null;
        public Task OnMessage(Message theMessage) {
            Task returnTask;

            try {
                SyntheticMessageProcessor processor = new SyntheticMessageProcessor(_connectionString, _monitoringTopic, _logger);
                Task messageTask = processor.OnMessage(theMessage);
                returnTask = Task.WhenAll(new Task[] { messageTask });
            } catch (MonitoringException mex) {
                _logger.LogError($"ERROR - {mex.Message} {mex.StackTrace}");
                returnTask = Task.FromException(mex);
            } catch (Exception ex) {
                _logger.LogWarning($"Error scenario found in monitored service - {ex.Message}");

                string dataJSON = Encoding.UTF8.GetString(theMessage.Body);
                MonitorMessage errorMessage = JsonConvert.DeserializeObject<MonitorMessage>(dataJSON);
                errorMessage.AssociatedId = $"{errorMessage.AssociatedId}_DLQ";
                if (ts is null) {
                    ts = new SessionlessTopicSender(_connectionString, _monitoringTopic);
                }
                Task sendTask = ts.Send(JsonConvert.SerializeObject(errorMessage), theMessage.UserProperties["CollectionId"].ToString(), theMessage.UserProperties["Context"].ToString(), DateTime.MinValue, "SYNTHETIC_ERROR");
                returnTask = Task.WhenAll(new Task[] { sendTask });
            }

            return returnTask;
        }
    }
}
