using Azure.Messaging.ServiceBus;
using CommentEverythingServiceBusConnectorNETCore.Communications;
using CommentEverythingServiceBusConnectorNETCore.Monitoring.Instrumentation.InstrumentedObjects;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Monitoring.Instrumentation.Monitor {
    public class ServiceErrorListener {
        private static ILogger _logger;

        private ServiceErrorListener() {
            // --- Must use parameterized constructor
        }
        public ServiceErrorListener(ILogger log) {
            if (_logger is null) {
                _logger = log;
            }
        }
        public Task OnMessage(ServiceBusMessage theMessage, string slackWebhookToSendAlert, string slackChannelToSendAlert) {
            Task returnTask;

            try {
                string dataJSON = Encoding.UTF8.GetString(theMessage.Body);
                MonitorMessage msg = JsonConvert.DeserializeObject<MonitorMessage>(dataJSON);

                _logger.LogWarning($"Error scenario found on {msg.MonitoredService} service");

                StringBuilder sb = new StringBuilder();
                sb.AppendLine($"*:warning: Infrastructure Problem Detected*");
                sb.AppendLine($"_{msg.MonitoredService}_");
                sb.Append($"_{msg.StartTime}_");

                SlackSender connector = new SlackSender(slackWebhookToSendAlert, slackChannelToSendAlert);
                returnTask = connector.GetJSONResponse(sb.ToString());
            } catch (Exception ex) {
                _logger.LogError($"ERROR - {ex.Message} {ex.StackTrace}");
                returnTask = Task.FromException(ex);
            }

            return returnTask;
        }
    }
}
