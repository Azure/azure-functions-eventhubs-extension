// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs.EventHubs;
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

        public EventHubEndToEndTests()
        {
            _results = new List<string>();
            _testId = Guid.NewGuid().ToString();
            _eventWait = new ManualResetEvent(initialState: false);
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

        [Theory]
        [InlineData(typeof(string))]
        [InlineData(typeof(byte[]))]
        [InlineData(typeof(EventData))]
        [InlineData(typeof(DummyClass))]
        public async Task EventHub_PartitionKeySender(Type type)
        {
            using (JobHost host = BuildHost<EventHubParitionKeyTestJobs>())
            {
                var method = typeof(EventHubParitionKeyTestJobs).GetMethod("SendEvents_PartitionSender", BindingFlags.Static | BindingFlags.Public);
                _eventWait = new ManualResetEvent(initialState: false);
                switch (type)
                {
                    case Type stringType when stringType == typeof(string):
                        await host.CallAsync(method, new { input = _testId });
                        break;
                    case Type byteArrayType when byteArrayType == typeof(byte[]):
                        await host.CallAsync(method, new { input = Encoding.UTF8.GetBytes(_testId) });
                        break;
                    case Type eventDataType when eventDataType == typeof(EventData):
                        await host.CallAsync(method, new { input = new EventData(Encoding.UTF8.GetBytes(_testId)) });
                        break;
                    case Type classType when classType == typeof(DummyClass):
                        await host.CallAsync(method, new { input = new DummyClass { Value = _testId } });
                        break;
                }


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

        public class EventHubParitionKeyTestJobs
        {
            public static async Task SendEvents_PartitionSender(
                object input,
                [EventHub(TestHubName)] PartitionedSender events)
            {
                // Send event without PK
                await AddWithPartitionKeyAsync(input, "", events);

                // Send event with different PKs
                for (int i = 0; i < 5; i++)
                {
                    await AddWithPartitionKeyAsync(input, "test_pk" + i, events);
                }
            }

            private static async Task AddWithPartitionKeyAsync(object input, string parttionKey, PartitionedSender events)
            {
                switch (input.GetType())
                {
                    case Type stringType when stringType == typeof(string):
                        await events.SendAsync(input as string, parttionKey);
                        break;
                    case Type byteArrayType when byteArrayType == typeof(byte[]):
                        await events.SendAsync(input as byte[], parttionKey);
                        break;
                    case Type eventDataType when eventDataType == typeof(EventData):
                        await events.SendAsync(input as EventData, parttionKey);
                        break;
                    case Type classType when classType == typeof(DummyClass):
                        await events.SendAsync(input as DummyClass, parttionKey);
                        break;
                }
            }

            public static void ProcessMultiplePartitionEvents([EventHubTrigger(TestHubName)] EventData[] events)
            {
                foreach (EventData eventData in events)
                {
                    string message = Encoding.UTF8.GetString(eventData.Body);

                    // filter for the ID the current test is using
                    if (message.Contains(_testId))
                    {
                        _results.Add(eventData.SystemProperties.PartitionKey);
                        _results.Sort();

                        if (_results.Count == 6 && (_results[5] == "test_pk4"))
                        {
                            _eventWait.Set();
                        }
                    }
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

            string connection = config.GetConnectionStringOrSetting("AzureWebJobsTestHubConnection");
            Assert.True(!string.IsNullOrEmpty(connection), "Required test connection string is missing.");

            var host = new HostBuilder()
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

        private class DummyClass
        {
            public string Value { get; set; }
        }
    }
}