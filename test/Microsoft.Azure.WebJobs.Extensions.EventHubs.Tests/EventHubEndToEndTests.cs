// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs.Host.TestCommon;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Microsoft.Azure.WebJobs.Host.EndToEndTests
{
    public class EventHubEndToEndTests
    {
        private const string TestHubName = "webjobstesthub";
        private const int Timeout = 30000;
        private static EventWaitHandle _eventWait;
        private static string _testId;
        private static List<string> _results;
        private static bool _throwError;

        public EventHubEndToEndTests()
        {
            _results = new List<string>();
            _testId = Guid.NewGuid().ToString();
            _eventWait = new ManualResetEvent(initialState: false);
            _throwError = true;
        }

        [Fact]
        public async Task EventHub_SingleDispatch()
        {
            using (JobHost host = BuildHost<EventHubTestSingleDispatchJobs>())
            {
                var method = typeof(EventHubTestSingleDispatchJobs).GetMethod("SendEvent_TestHub", BindingFlags.Static | BindingFlags.Public);
                var id = Guid.NewGuid().ToString();
                await host.CallAsync(method, new { input = _testId });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.True(result);
            }
        }

        [Fact]
        public async Task EventHub_MultipleDispatch()
        {
            using (JobHost host = BuildHost<EventHubTestMultipleDispatchJobs>())
            {
                // send some events BEFORE starting the host, to ensure
                // the events are received in batch
                var method = typeof(EventHubTestMultipleDispatchJobs).GetMethod("SendEvents_TestHub", BindingFlags.Static | BindingFlags.Public);
                int numEvents = 5;
                await host.CallAsync(method, new { numEvents = numEvents, input = _testId });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.True(result);
            }
        }

        [Fact]
        public async Task EventHub_PartitionKey()
        {
            using (JobHost host = BuildHost<EventHubPartitionKeyTestJobs>())
            {
                var method = typeof(EventHubPartitionKeyTestJobs).GetMethod("SendEvents_TestHub", BindingFlags.Static | BindingFlags.Public);
                _eventWait = new ManualResetEvent(initialState: false);
                await host.CallAsync(method, new { input = _testId });

                bool result = _eventWait.WaitOne(Timeout);

                Assert.True(result);
            }
        }
        
        [Fact]
        public async Task EventHub_FailuresNotRetried()
        {
            using (JobHost host = BuildHost<EventHubTestFailuresNotRetriedJobs>())
            {
                var method = typeof(EventHubTestFailuresNotRetriedJobs).GetMethod("SendEvent_TestHub", BindingFlags.Static | BindingFlags.Public);
                var id = Guid.NewGuid().ToString();
                await host.CallAsync(method, new { input = _testId });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.False(result);
            }
            
            using (JobHost host = BuildHost<EventHubTestFailuresNotRetriedJobs>())
            {
                var result = _eventWait.WaitOne(Timeout);
                Assert.False(result);
            }
        }
        
        [Fact]
        public async Task EventHub_FailuresRetriedWithNewHost()
        {
            using (JobHost host = BuildHost<EventHubTestFailuresRetriedJobs>())
            {
                var method = typeof(EventHubTestFailuresRetriedJobs).GetMethod("SendEvent_TestHub", BindingFlags.Static | BindingFlags.Public);
                var id = Guid.NewGuid().ToString();
                await host.CallAsync(method, new { input = _testId });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.False(result);
            }
            
            using (BuildHost<EventHubTestFailuresRetriedJobs>())
            {
                var result = _eventWait.WaitOne(Timeout);
                Assert.True(result);
            }
        }
        
        [Fact]
        public async Task EventHub_FailuresRetriedWithNewData()
        {
            using (JobHost host = BuildHost<EventHubTestFailuresRetriedJobs>())
            {
                var method = typeof(EventHubTestFailuresRetriedJobs).GetMethod("SendEvent_TestHub", BindingFlags.Static | BindingFlags.Public);
                var id = Guid.NewGuid().ToString();
                await host.CallAsync(method, new { input = _testId });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.False(result);
                
                await host.CallAsync(method, new { input = _testId });
                
                result = _eventWait.WaitOne(Timeout);
                Assert.True(result);
            }
        }
        
        [Fact]
        public async Task EventHub_MultipleDispatchFailuresNotRetried()
        {
            using (JobHost host = BuildHost<EventHubTestMultipleDispatchFailuresNotRetriedJobs>())
            {
                var method = typeof(EventHubTestMultipleDispatchFailuresNotRetriedJobs).GetMethod("SendEvents_TestHub", BindingFlags.Static | BindingFlags.Public);
                int numEvents = 5;
                await host.CallAsync(method, new { numEvents = numEvents, input = _testId });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.False(result);
            }
            
            using (BuildHost<EventHubTestMultipleDispatchFailuresNotRetriedJobs>())
            {
                var result = _eventWait.WaitOne(Timeout);
                Assert.False(result);
            }
        }
        
        [Fact]
        public async Task EventHub_MultipleDispatchFailuresRetriedWithNewHost()
        {
            using (JobHost host = BuildHost<EventHubTestMultipleDispatchFailuresRetriedJobs>())
            {
                var method = typeof(EventHubTestMultipleDispatchFailuresRetriedJobs).GetMethod("SendEvents_TestHub", BindingFlags.Static | BindingFlags.Public);
                int numEvents = 5;
                await host.CallAsync(method, new { numEvents = numEvents, input = _testId });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.False(result);
            }
            
            using (BuildHost<EventHubTestMultipleDispatchFailuresRetriedJobs>())
            {
                var result = _eventWait.WaitOne(Timeout);
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
                    Assert.True((DateTime.UtcNow - enqueuedTimeUtc).TotalSeconds < 30);

                    Assert.Equal("value1", properties["TestProp1"]);
                    Assert.Equal("value2", properties["TestProp2"]);

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

            public static void ProcessMultiplePartitionEvents([EventHubTrigger(TestHubName)] EventData[] events)
            {
                foreach (EventData eventData in events)
                {
                    string message = Encoding.UTF8.GetString(eventData.Body);

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
        }
        
        public class EventHubTestFailuresNotRetriedJobs
        {
            private static bool _throwError = true;
            public static void SendEvent_TestHub(string input, [EventHub(TestHubName)] out EventData evt)
            {
                evt = new EventData(Encoding.UTF8.GetBytes(input));
            }

            public static void ProcessSingleEvent([EventHubTrigger(TestHubName)] string evt,
                string partitionKey, DateTime enqueuedTimeUtc, IDictionary<string, object> properties,
                IDictionary<string, object> systemProperties)
            {
                // filter for the ID the current test is using
                if (evt == _testId)
                {
                    if (_throwError)
                    {
                        _throwError = false;
                        throw new Exception();
                    }
                    
                    Assert.True(false, "It shouldn't get here, the message should be ignored after the first exception.");
                }
            }
        }
        
        public class EventHubTestFailuresRetriedJobs
        {
            public static void SendEvent_TestHub(string input, [EventHub(TestHubName)] out EventData evt)
            {
                evt = new EventData(Encoding.UTF8.GetBytes(input));
            }

            public static void ProcessSingleEvent([EventHubTrigger(TestHubName, CheckpointOnFailure = false)] string evt,
                string partitionKey, DateTime enqueuedTimeUtc, IDictionary<string, object> properties,
                IDictionary<string, object> systemProperties)
            {
                // filter for the ID the current test is using
                if (evt == _testId)
                {
                    if (_throwError)
                    {
                        _throwError = false;
                        throw new Exception();
                    }
                    
                    Assert.True((DateTime.UtcNow - enqueuedTimeUtc).TotalSeconds < 120);

                    _eventWait.Set();
                }
            }
        }
        
        public class EventHubTestMultipleDispatchFailuresNotRetriedJobs
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
                if (events[0] == _testId)
                {
                    if (_throwError)
                    {
                        _throwError = false;
                        throw new Exception();
                    }
                    
                    Assert.True(false, "It shouldn't get here, the message should be ignored after the first exception.");
                }
            }
        }
        
        public class EventHubTestMultipleDispatchFailuresRetriedJobs
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

            public static void ProcessMultipleEvents([EventHubTrigger(TestHubName, CheckpointOnFailure = false)] string[] events,
                string[] partitionKeyArray, DateTime[] enqueuedTimeUtcArray, IDictionary<string, object>[] propertiesArray,
                IDictionary<string, object>[] systemPropertiesArray)
            {
                if (_throwError)
                {
                    _throwError = false;
                    throw new Exception();
                }
                
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

        private JobHost BuildHost<T>()
        {
            JobHost jobHost = null;

            var config = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddTestSettings()
                .Build();

            const string connectionName = "AzureWebJobsTestHubConnection";
            string connection = config.GetConnectionStringOrSetting(connectionName);
            Assert.True(!string.IsNullOrEmpty(connection), $"Required test connection string '{connectionName}' is missing.");

            IHost host = new HostBuilder()
                .ConfigureDefaultTestHost<T>(b =>
                {
                    b.AddEventHubs(options =>
                    {
                        options.AddSender(TestHubName, connection);
                        options.AddReceiver(TestHubName, connection);
                    });
                })
                .Build();

            jobHost = host.GetJobHost();
            jobHost.StartAsync().GetAwaiter().GetResult();

            return jobHost;
        }
    }
}