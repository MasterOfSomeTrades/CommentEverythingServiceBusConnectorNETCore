using System;
using System.Collections.Generic;
using System.Text;

namespace CommentEverythingServiceBusConnectorNETCore.Monitoring.Exceptions {
    class MonitoringException : ApplicationException {
        public MonitoringException(string exceptionMessage) : base(exceptionMessage) {

        }
    }
}
