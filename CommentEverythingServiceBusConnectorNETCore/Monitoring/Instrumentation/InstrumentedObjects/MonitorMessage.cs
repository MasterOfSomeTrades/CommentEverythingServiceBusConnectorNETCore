using System;
using System.Collections.Generic;
using System.Text;

namespace CommentEverythingServiceBusConnectorNETCore.Monitoring.Instrumentation.InstrumentedObjects {
    public class MonitorMessage {
        public string MonitoredService { get; set; }
        public string StartTime { get; set; } = DateTime.Now.ToString("yyyy-MMM-dd HH:mm:ss.fff");
        public string ApplicationError { get; set; } = null; // Infrastructure errors remain null
        public string AssociatedId { get; set; } = Guid.NewGuid().ToString("D"); // MessageId (or CollectionId if MessageId is not available at the time of creation)
    }
}
