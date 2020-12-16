// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.WebJobs.EventHubs;
using Microsoft.Azure.WebJobs.Host.Config;
using Microsoft.Azure.WebJobs.Host.TestCommon;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;
// Disambiguage between Microsoft.WindowsAzure.Storage.LogLevel
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Microsoft.Azure.WebJobs.Host.EndToEndTests
{
    public class EventHubEndToEndTests
    {
        private const string TestHubName = "webjobstesthub";
        private const string TestHubConnectionName = "AzureWebJobsTestHubConnection";
        private const string StorageConnectionName = "AzureWebJobsStorage";
        // The container name created by this extension to save snapshots of the Event Hubs stream positions
        private const string SnapshotStorageContainerName = "azure-webjobs-eventhub";
        private const int Timeout = 30000;
        private const string JsonDataField = "data";
        private static EventWaitHandle _eventWait;
        private static string _testId;
        private static List<string> _results;
        private static string _testHubConnectionString;
        private static string _storageConnectionString;
        private static DateTime _initialOffsetEnqueuedTimeUTC;
        private static DateTime? _earliestReceivedMessageEnqueuedTimeUTC = null;

        public EventHubEndToEndTests()
        {
            _results = new List<string>();
            _testId = Guid.NewGuid().ToString();
            _eventWait = new ManualResetEvent(initialState: false);

            var config = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddTestSettings()
                .Build();

            _testHubConnectionString = config.GetConnectionStringOrSetting(TestHubConnectionName);
            _storageConnectionString = config.GetConnectionStringOrSetting(StorageConnectionName);
        }

        [Fact]
        public async Task EventHub_PocoBinding()
        {
            var tuple = BuildHost<EventHubTestBindToPocoJobs>();
            using (var host = tuple.Item1)
            {
                var method = typeof(EventHubTestBindToPocoJobs).GetMethod(nameof(EventHubTestBindToPocoJobs.SendEvent_TestHub), BindingFlags.Static | BindingFlags.Public);
                await host.CallAsync(method, new { input = "{ Name: 'foo', Value: '" + _testId +"' }" });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.True(result);

                var logs = tuple.Item2.GetTestLoggerProvider().GetAllLogMessages().Select(p => p.FormattedMessage);

                Assert.Contains($"PocoValues(foo,{_testId})", logs);
            }
        }

        [Fact]
        public async Task EventHub_StringBinding()
        {
            var tuple = BuildHost<EventHubTestBindToStringJobs>();
            using (var host = tuple.Item1)
            {
                var method = typeof(EventHubTestBindToStringJobs).GetMethod(nameof(EventHubTestBindToStringJobs.SendEvent_TestHub), BindingFlags.Static | BindingFlags.Public);
                await host.CallAsync(method, new { input = _testId });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.True(result);

                var logs = tuple.Item2.GetTestLoggerProvider().GetAllLogMessages().Select(p => p.FormattedMessage);

                Assert.Contains($"Input({_testId})", logs);
            }
        }

        [Fact]
        public async Task EventHub_SingleDispatch()
        {
            Tuple<JobHost, IHost> tuple = BuildHost<EventHubTestSingleDispatchJobs>();
            using (var host = tuple.Item1)
            {
                var method = typeof(EventHubTestSingleDispatchJobs).GetMethod(nameof(EventHubTestSingleDispatchJobs.SendEvent_TestHub), BindingFlags.Static | BindingFlags.Public);
                await host.CallAsync(method, new { input = _testId });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.True(result);

                // Wait for checkpointing
                await Task.Delay(3000);

                IEnumerable<LogMessage> logMessages = tuple.Item2.GetTestLoggerProvider()
                    .GetAllLogMessages();

                Assert.Equal(logMessages.Where(x => !string.IsNullOrEmpty(x.FormattedMessage)
                    && x.FormattedMessage.Contains("Trigger Details:")
                    && x.FormattedMessage.Contains("Offset:")).Count(), 1);

                Assert.True(logMessages.Where(x => !string.IsNullOrEmpty(x.FormattedMessage)
                    && x.FormattedMessage.Contains("OpenAsync")).Count() > 0);

                Assert.True(logMessages.Where(x => !string.IsNullOrEmpty(x.FormattedMessage)
                    && x.FormattedMessage.Contains("CheckpointAsync")).Count() > 0);

                Assert.True(logMessages.Where(x => !string.IsNullOrEmpty(x.FormattedMessage)
                    && x.FormattedMessage.Contains("Sending events to EventHub")).Count() > 0);
            }
        }

        [Fact]
        public async Task EventHub_MultipleDispatch()
        {
            Tuple<JobHost, IHost> tuple = BuildHost<EventHubTestMultipleDispatchJobs>();
            using (var host = tuple.Item1)
            {
                // send some events BEFORE starting the host, to ensure
                // the events are received in batch
                var method = typeof(EventHubTestMultipleDispatchJobs).GetMethod("SendEvents_TestHub", BindingFlags.Static | BindingFlags.Public);
                int numEvents = 5;
                await host.CallAsync(method, new { numEvents = numEvents, input = _testId });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.True(result);

                // Wait for checkpointing
                await Task.Delay(3000);

                IEnumerable<LogMessage> logMessages = tuple.Item2.GetTestLoggerProvider()
                    .GetAllLogMessages();

                Assert.True(logMessages.Where(x => !string.IsNullOrEmpty(x.FormattedMessage)
                    && x.FormattedMessage.Contains("Trigger Details:")
                    && x.FormattedMessage.Contains("Offset:")).Count() > 0);

                Assert.True(logMessages.Where(x => !string.IsNullOrEmpty(x.FormattedMessage)
                    && x.FormattedMessage.Contains("OpenAsync")).Count() > 0);

                Assert.True(logMessages.Where(x => !string.IsNullOrEmpty(x.FormattedMessage)
                    && x.FormattedMessage.Contains("CheckpointAsync")).Count() > 0);

                Assert.True(logMessages.Where(x => !string.IsNullOrEmpty(x.FormattedMessage)
                    && x.FormattedMessage.Contains("Sending events to EventHub")).Count() > 0);
            }
        }

        [Fact]
        public async Task EventHub_PartitionKey()
        {
            Tuple<JobHost, IHost> tuple = BuildHost<EventHubPartitionKeyTestJobs>();
            using (var host = tuple.Item1)
            {
                var method = typeof(EventHubPartitionKeyTestJobs).GetMethod("SendEvents_TestHub", BindingFlags.Static | BindingFlags.Public);
                _eventWait = new ManualResetEvent(initialState: false);
                await host.CallAsync(method, new { input = _testId });

                bool result = _eventWait.WaitOne(Timeout);

                Assert.True(result);
            }
        }

        [Fact]
        public async Task EventHub_InitialOffsetFromEnd()
        {
            // Send a message to ensure the stream is not empty as we are trying to validate that no messages are delivered in this case
            using (var host = BuildHost<EventHubTestSendOnlyJobs>().Item1)
            {
                var method = typeof(EventHubTestSendOnlyJobs).GetMethod(nameof(EventHubTestSendOnlyJobs.SendEvent_TestHub), BindingFlags.Static | BindingFlags.Public);
                await host.CallAsync(method, new { input = _testId });
            }
            // Clear out existing checkpoints as the InitialOffset config only applies when checkpoints don't exist (first run on a host)
            await DeleteStorageCheckpoints();
            var initialOffsetOptions = new InitialOffsetOptions()
            {
                Type = "FromEnd"
            };
            using (var host = BuildHost<EventHubTestInitialOffsetFromEndJobs>(initialOffsetOptions).Item1)
            {
                // We don't expect to get signalled as there will be messages recieved with a FromEnd initial offset
                bool result = _eventWait.WaitOne(Timeout);
                Assert.False(result, "An event was received while none were expected.");
            }
        }

        [Fact]
        public async Task EventHub_InitialOffsetFromEnqueuedTime()
        {
            // Mark the time now and send a message which should be the only one that is picked up when we run the actual test host
            _initialOffsetEnqueuedTimeUTC = DateTime.UtcNow;
            using (var host = BuildHost<EventHubTestSendOnlyJobs>().Item1)
            {
                var method = typeof(EventHubTestSendOnlyJobs).GetMethod(nameof(EventHubTestSendOnlyJobs.SendEvent_TestHub), BindingFlags.Static | BindingFlags.Public);
                await host.CallAsync(method, new { input = _testId });
            }
            // Clear out existing checkpoints as the InitialOffset config only applies when checkpoints don't exist (first run on a host)
            await DeleteStorageCheckpoints();
            _earliestReceivedMessageEnqueuedTimeUTC = null;
            var initialOffsetOptions = new InitialOffsetOptions()
            {
                Type = "FromEnqueuedTime",
                EnqueuedTimeUTC = _initialOffsetEnqueuedTimeUTC.ToString("yyyy-MM-ddTHH:mm:ssZ")
            };
            using (var host = BuildHost<EventHubTestInitialOffsetFromEnqueuedTimeJobs>(initialOffsetOptions).Item1)
            {
                // Validation that we only got messages after the configured FromEnqueuedTime is done in the JobHost
                bool result = _eventWait.WaitOne(Timeout);
                Assert.True(result, $"No event was received within the timeout period of {Timeout}. " +
                    $"Expected event sent shortly after {_initialOffsetEnqueuedTimeUTC.ToString("yyyy-MM-ddTHH:mm:ssZ")} with content {_testId}");
                Assert.True(_earliestReceivedMessageEnqueuedTimeUTC > _initialOffsetEnqueuedTimeUTC, 
                    "A message was received that was enqueued before the configured Initial Offset Enqueued Time. " + 
                    $"Received message enqueued time: {_earliestReceivedMessageEnqueuedTimeUTC?.ToString("yyyy-MM-ddTHH:mm:ssZ")}" + 
                    $", initial offset enqueued time: {_initialOffsetEnqueuedTimeUTC.ToString("yyyy-MM-ddTHH:mm:ssZ")}");
            }
        }

        public async Task EventHub_AsyncCollector_EventDataEx()
        {
            Tuple<JobHost, IHost> tuple = BuildHost<EventHubPartitionKeyTestJobs>();
            using (var host = tuple.Item1)
            {
                var method = typeof(EventHubPartitionKeyTestJobs).GetMethod("SendEvents_EventDataEx_TestHub", BindingFlags.Static | BindingFlags.Public);
                _eventWait = new ManualResetEvent(initialState: false);
                await host.CallAsync(method, new { input = _testId });

                bool result = _eventWait.WaitOne(Timeout);

                Assert.True(result);
            }
        }

        [Fact]
        public async Task EventHub_AsyncCollector_Json_Body_Json()
        {
            Tuple<JobHost, IHost> tuple = BuildHost<EventHubPartitionKeyTestJobs>();
            using (var host = tuple.Item1)
            {
                var method = typeof(EventHubPartitionKeyTestJobs).GetMethod("SendEvents_Json_Body_Json_TestHub", BindingFlags.Static | BindingFlags.Public);
                _eventWait = new ManualResetEvent(initialState: false);
                await host.CallAsync(method, new { input = _testId });

                bool result = _eventWait.WaitOne(Timeout);

                Assert.True(result);
            }
        }


        [Fact]
        public async Task EventHub_AsyncCollector_Json_Body_Jarray_Bytes()
        {
            Tuple<JobHost, IHost> tuple = BuildHost<EventHubPartitionKeyTestJobs>();
            using (var host = tuple.Item1)
            {
                var method = typeof(EventHubPartitionKeyTestJobs).GetMethod("SendEvents_Json_Body_Jarray_Bytes_TestHub", BindingFlags.Static | BindingFlags.Public);
                _eventWait = new ManualResetEvent(initialState: false);
                await host.CallAsync(method, new { input = _testId });

                bool result = _eventWait.WaitOne(Timeout);

                Assert.True(result);
            }
        }


        [Fact]
        public async Task EventHub_AsyncCollector_Json_Body_Bytes()
        {
            Tuple<JobHost, IHost> tuple = BuildHost<EventHubPartitionKeyTestJobs>();
            using (var host = tuple.Item1)
            {
                var method = typeof(EventHubPartitionKeyTestJobs).GetMethod("SendEvents_Json_Body_Bytes_TestHub", BindingFlags.Static | BindingFlags.Public);
                _eventWait = new ManualResetEvent(initialState: false);
                await host.CallAsync(method, new { input = _testId });

                bool result = _eventWait.WaitOne(Timeout);

                Assert.True(result);
            }
        }

        public class EventHubTestSingleDispatchJobs
        {
            public static void SendEvent_TestHub(string input, [EventHub(TestHubName)] out EventData evt)
            {
                evt = new EventData(Encoding.UTF8.GetBytes(input));
                evt.Properties.Add("TestProp1", "value1");
                evt.Properties.Add("TestProp2", "value2");
            }

            public static void ProcessSingleEvent([EventHubTrigger(TestHubName)] string evt,
                       string partitionKey, DateTime enqueuedTimeUtc, IDictionary<string, object> properties,
                       IDictionary<string, object> systemProperties)
            {
                // filter for the ID the current test is using
                if (evt == _testId)
                {
                    Assert.True((DateTime.Now - enqueuedTimeUtc).TotalSeconds < 30);

                    Assert.Equal("value1", properties["TestProp1"]);
                    Assert.Equal("value2", properties["TestProp2"]);

                    _eventWait.Set();
                }
            }
        }

        public class EventHubTestSendOnlyJobs
        {
            public static void SendEvent_TestHub(string input, [EventHub(TestHubName)] out EventData evt)
            {
                evt = new EventData(Encoding.UTF8.GetBytes(input));
            }
        }

        public class EventHubTestInitialOffsetFromEndJobs
        {
            public static void ProcessSingleEvent([EventHubTrigger(TestHubName)] string evt,
                       string partitionKey, DateTime enqueuedTimeUtc, IDictionary<string, object> properties,
                       IDictionary<string, object> systemProperties)
            {
                _eventWait.Set();
            }
        }

        public class EventHubTestInitialOffsetFromEnqueuedTimeJobs
        {
            public static void ProcessSingleEvent([EventHubTrigger(TestHubName)] string evt,
                       string partitionKey, DateTime enqueuedTimeUtc, IDictionary<string, object> properties,
                       IDictionary<string, object> systemProperties)
            {
                if (_earliestReceivedMessageEnqueuedTimeUTC == null)
                {
                    _earliestReceivedMessageEnqueuedTimeUTC = enqueuedTimeUtc;
                    _eventWait.Set();
                }
            }
        }

        public class EventHubTestBindToPocoJobs
        {
            public static void SendEvent_TestHub(string input, [EventHub(TestHubName)] out EventData evt)
            {
                evt = new EventData(Encoding.UTF8.GetBytes(input));
            }

            public static void BindToPoco([EventHubTrigger(TestHubName)] TestPoco input, string value, string name, ILogger logger)
            {
                if (value == _testId)
                {
                    Assert.Equal(input.Value, value);
                    Assert.Equal(input.Name, name);
                    logger.LogInformation($"PocoValues({name},{value})");
                    _eventWait.Set();
                }
            }
        }

        public class EventHubTestBindToStringJobs
        {
            public static void SendEvent_TestHub(string input, [EventHub(TestHubName)] out EventData evt)
            {
                evt = new EventData(Encoding.UTF8.GetBytes(input));
            }

            public static void BindToString([EventHubTrigger(TestHubName)] string input, ILogger logger)
            {
                if (input == _testId)
                {
                    logger.LogInformation($"Input({input})");
                    _eventWait.Set();
                }
            }
        }

        public class EventHubTestMultipleDispatchJobs
        {
            public static void SendEvents_TestHub(int numEvents, string input, [EventHub(TestHubName)] out EventData[] events)
            {
                events = new EventData[numEvents];
                for (int i = 0; i < numEvents; i++)
                {
                    var evt = new EventData(Encoding.UTF8.GetBytes(input));
                    evt.Properties.Add("TestIndex", i);
                    evt.Properties.Add("TestProp1", "value1");
                    evt.Properties.Add("TestProp2", "value2");
                    events[i] = evt;
                }
            }

            public static void ProcessMultipleEvents([EventHubTrigger(TestHubName)] string[] events,
                string[] partitionKeyArray, DateTime[] enqueuedTimeUtcArray, IDictionary<string, object>[] propertiesArray,
                IDictionary<string, object>[] systemPropertiesArray)
            {
                Assert.Equal(events.Length, partitionKeyArray.Length);
                Assert.Equal(events.Length, enqueuedTimeUtcArray.Length);
                Assert.Equal(events.Length, propertiesArray.Length);
                Assert.Equal(events.Length, systemPropertiesArray.Length);

                for (int i = 0; i < events.Length; i++)
                {
                    Assert.Equal(i, propertiesArray[i]["TestIndex"]);
                }

                // filter for the ID the current test is using
                if (events[0] == _testId)
                {
                    _results.AddRange(events);
                    _eventWait.Set();
                }
            }
        }

        public class EventHubPartitionKeyTestJobs
        {
            /// <summary>
            /// Test sending partitioned output using EventHubClient directly
            /// </summary>
            public static async Task SendEvents_TestHub(
                string input,
                [EventHub(TestHubName)] EventHubClient client)
            {
                List<EventData> list = new List<EventData>();
                EventData evt = new EventData(Encoding.UTF8.GetBytes(input));

                // Send event without PK
                await client.SendAsync(evt);

                // Send event with different PKs
                for (int i = 0; i < 5; i++)
                {
                    evt = new EventData(Encoding.UTF8.GetBytes(input));
                    await client.SendAsync(evt, "test_pk" + i);
                }
            }

            /// <summary>
            /// Test sending partitioned output using EventDataEx extended properties
            /// </summary>
            public static async Task SendEvents_EventDataEx_TestHub(
                string input,
                [EventHub(TestHubName)] IAsyncCollector<EventData> collector)
            {
                byte[] bytes = Encoding.UTF8.GetBytes(input);

                // Send event without PK
                await collector.AddAsync(new EventData(bytes));

                // Send event with different PKs
                for (int i = 0; i < 5; i++)
                {
                    var evt = new EventDataEx(bytes)
                    {
                        PartitionKey = "test_pk" + i
                    };
                    await collector.AddAsync(evt);
                }
            }


            /// <summary>
            /// Test sending partitioned output using Json extended properties
            /// Test body content contains Json
            /// </summary>
            public static async Task SendEvents_Json_Body_Json_TestHub(
                string input,
                [EventHub(TestHubName)] IAsyncCollector<string> collector)
            {
                var body = new JObject();
                byte[] bytes = Encoding.UTF8.GetBytes(input);
                body[JsonDataField] = bytes;

                // Send event without PK
                await collector.AddAsync(input);

                // Send event with different PKs
                for (int i = 0; i < 5; i++)
                {
                    var evt = new JObject();
                    evt["PartitionKey"] = "test_pk" + i;
                    evt["Body"] = body;
                    await collector.AddAsync(evt.ToString());
                }
            }

            /// <summary>
            /// Test sending partitioned output using Json extended properties
            /// Test body content contains JArray bytes[]
            /// </summary>
            public static async Task SendEvents_Json_Body_Jarray_Bytes_TestHub(
                string input,
                [EventHub(TestHubName)] IAsyncCollector<string> collector)
            {
                byte[] bytes = Encoding.UTF8.GetBytes(input);
                var a = new JArray();
                for (int i=0; i < bytes.Length; i++)
                {
                    a.Add((Object)bytes[i]);
                }
                // Send event without PK
                await collector.AddAsync(input);

                // Send event with different PKs
                for (int i = 0; i < 5; i++)
                {
                    var evt = new JObject();
                    evt["PartitionKey"] = "test_pk" + i;
                    evt["Body"] = a;
                    var evt_string = evt.ToString();
                    await collector.AddAsync(evt_string);
                }
            }


            /// <summary>
            /// Test sending partitioned output using Json extended properties
            /// Test body content contains bytes[]
            /// </summary>
            public static async Task SendEvents_Json_Body_Bytes_TestHub(
                string input,
                [EventHub(TestHubName)] IAsyncCollector<string> collector)
            {
                byte[] bytes = Encoding.UTF8.GetBytes(input);
                // Send event without PK
                await collector.AddAsync(input);

                // Send event with different PKs
                for (int i = 0; i < 5; i++)
                {
                    var evt = new JObject();
                    evt["PartitionKey"] = "test_pk" + i;
                    evt["Body"] = bytes;
                    var evt_string = evt.ToString();
                    await collector.AddAsync(evt_string);
                }
            }

            public static void ProcessMultiplePartitionEvents([EventHubTrigger(TestHubName)] EventData[] events)
            {
                foreach (EventData eventData in events)
                {
                    string message = Encoding.UTF8.GetString(eventData.Body);
                    if (IsJsonObject(message))
                    {
                        JObject o = JObject.Parse(message);
                        string data = Encoding.UTF8.GetString(o.GetValue(JsonDataField).ToObject<byte[]>());

                        // filter for the ID the current test is using
                        if (data == _testId)
                        {
                            _results.Add(eventData.SystemProperties.PartitionKey);
                            _results.Sort();

                            if (_results.Count == 6 && _results[5] == "test_pk4")
                            {
                                _eventWait.Set();
                            }
                        }
                    }

                    // filter for the ID the current test is using
                    if (message == _testId)
                    {
                        _results.Add(eventData.SystemProperties.PartitionKey);
                        _results.Sort();

                        if (_results.Count == 6 && _results[5] == "test_pk4")
                        {
                            _eventWait.Set();
                        }
                    }
                }
            }
            private static bool IsJsonObject(string input)
            {
                if (string.IsNullOrEmpty(input))
                {
                    return false;
                }

                input = input.Trim();
                return (input.StartsWith("{", StringComparison.OrdinalIgnoreCase) && input.EndsWith("}", StringComparison.OrdinalIgnoreCase));
            }
        }

        private Tuple<JobHost, IHost> BuildHost<T>(InitialOffsetOptions initialOffsetOptions = null)
        {
            JobHost jobHost = null;

            Assert.True(!string.IsNullOrEmpty(_testHubConnectionString), $"Required test connection string '{TestHubConnectionName}' is missing.");

            IHost host = new HostBuilder()
                .ConfigureDefaultTestHost<T>(b =>
                {
                    b.AddEventHubs(options =>
                    {
                        options.EventProcessorOptions.EnableReceiverRuntimeMetric = true;
                        if (initialOffsetOptions != null)
                        {
                            options.InitialOffsetOptions = initialOffsetOptions;
                        }
                        // We want to validate the default options configuration logic for setting initial offset and not implemente it here
                        EventHubWebJobsBuilderExtensions.ConfigureOptions(options);
                        options.AddSender(TestHubName, _testHubConnectionString);
                        options.AddReceiver(TestHubName, _testHubConnectionString);
                    });
                })
                .ConfigureLogging(b =>
                {
                    b.SetMinimumLevel(LogLevel.Debug);
                })
                .Build();

            jobHost = host.GetJobHost();
            jobHost.StartAsync().GetAwaiter().GetResult();

            return new Tuple<JobHost, IHost>(jobHost, host);
        }

        // Deletes all checkpoints that were saved in storage so InitialOffset can be validated
        private async Task DeleteStorageCheckpoints()
        {
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(_storageConnectionString);
            CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(SnapshotStorageContainerName);
            BlobContinuationToken continuationToken = null;
            do
            {
                var response = await container.ListBlobsSegmentedAsync(string.Empty, true, BlobListingDetails.None, null, continuationToken, null, null);
                continuationToken = response.ContinuationToken;
                foreach (var blob in response.Results.OfType<CloudBlob>())
                {
                    try
                    {
                        await blob.BreakLeaseAsync(TimeSpan.Zero);
                    }
                    catch (StorageException) 
                    { 
                        // Ignore as this will be thrown if the blob has no lease on it
                    }
                    await blob.DeleteAsync();
                }
            } while (continuationToken != null);
        }

        public class TestPoco
        {
            public string Name { get; set; }
            public string Value { get; set; }
        }
    }
}