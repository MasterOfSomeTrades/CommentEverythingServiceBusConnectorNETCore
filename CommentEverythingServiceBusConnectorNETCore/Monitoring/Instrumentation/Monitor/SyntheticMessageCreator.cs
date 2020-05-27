using CommentEverythingServiceBusConnectorLib.Topic;
using CommentEverythingServiceBusConnectorNETCore.Monitoring.Instrumentation.InstrumentedObjects;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Monitoring.Instrumentation.Monitor {
    public class SyntheticMessageCreator {
        string _servicebusConnectionString;
        string _monitoringTopicName;
        TopicSender ts;
        ILogger _logger;
        public SyntheticMessageCreator(string serviceBusConnectionString, string monitoringTopicName, ILogger log) {
            _servicebusConnectionString = serviceBusConnectionString;
            _monitoringTopicName = monitoringTopicName;
            if (ts is null) {
                ts = new TopicSender(_servicebusConnectionString, _monitoringTopicName, log);
            }
            if (_logger is null) {
                _logger = log;
            }
        }
        public Task Create(string monitoredServiceName) { // NOTE: a subscription with EventType=monitoredServiceName is expected
            Task returnTask;

            try {
                MonitorMessage msg = new MonitorMessage {
                    MonitoredService = monitoredServiceName
                };

                Task sendTask = ts.Send(JsonConvert.SerializeObject(msg), Guid.NewGuid().ToString("D"), "SYNTHETIC_MONITORING", DateTime.MinValue, msg.MonitoredService);
                returnTask = Task.WhenAll(new Task[] { sendTask });
            } catch (Exception ex) {
                _logger.LogError($"ERROR starting synthetic transaction - {ex.Message}");
                returnTask = Task.FromException(ex);
            }

            return returnTask;
        }
    }
}
