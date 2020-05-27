using CommentEverythingAPIConnectionNETCore.Connectors;
using CommentEverythingAPIConnectionNETCore.DataObjects;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace CommentEverythingServiceBusConnectorNETCore.Communications {
    // TODO: Should be removed and placed in its own library
    class SlackSender : POSTConnector {
        private string _webhook = "";
        private string _channel = "";

        public override List<string> _urlFormat => new List<string>(new string[] { _webhook });

        public override List<string[]> _headers {
            get {
                List<string[]> theList = new List<string[]>();
                theList.Add(new string[] { "Content-Type", "application/json" });
                return theList;
            }
        }

        protected override string _name => "SlackConnector";

        protected override int _waitTime => 1;

        protected override int _concurrentCalls => 5;

        private SlackSender() {
            // --- Must use parameterized constructor
        }

        public SlackSender(string webhook, string channel) {
            _webhook = webhook;
            _channel = channel;
        }

        public override IData ConvertJSONToDataObject(List<string> json) {
            throw new NotImplementedException();
        }

        public override async Task<List<string>> GetJSONResponse(string requestData) {
            List<string> jsonResults = new List<string>();

            try {
                foreach (string urlStr in _urlFormat) {
                    var baseAddress = urlStr;

                    var http = (HttpWebRequest) WebRequest.Create(new Uri(baseAddress));
                    http.Accept = "application/json";
                    http.ContentType = "application/json";
                    http.Method = "POST";

                    Payload payload = new Payload();
                    payload.Text = requestData;

                    string parsedContent = JsonConvert.SerializeObject(payload);
                    ASCIIEncoding encoding = new ASCIIEncoding();
                    byte[] bytes = encoding.GetBytes(parsedContent);

                    Stream newStream = http.GetRequestStream();
                    await newStream.WriteAsync(bytes, 0, bytes.Length);
                    newStream.Close();

                    var response = await http.GetResponseAsync();

                    var stream = response.GetResponseStream();
                    var sr = new StreamReader(stream);
                    string content = await sr.ReadToEndAsync();

                    jsonResults.Add(content);
                }
            } catch (WebException wex) {
                throw new ApplicationException("ERROR updating data in " + Name + " - check URL or service availability - caused by: " + wex.Message);
            } catch (Exception ex) {
                throw new ApplicationException("ERROR updating data in " + Name + " - caused by: " + ex.Message);
            }

            return jsonResults;
        }

        protected override string FormatRequest(string requestData, string[] requestDataArray = null) {
            return $"{{\"channel\":\"#{_channel}\", \"username\":\"Amyas Au\", \"text\":\"{requestData}\"}}";
        }

        public class Payload {
            [JsonProperty("text")]
            public string Text { get; set; }
        }
    }
}
