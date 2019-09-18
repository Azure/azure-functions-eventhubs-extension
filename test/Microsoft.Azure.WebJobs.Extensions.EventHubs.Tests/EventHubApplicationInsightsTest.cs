// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.ApplicationInsights.Extensibility.Implementation;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs.Host.TestCommon;
using Microsoft.Azure.WebJobs.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Xunit;

namespace Microsoft.Azure.WebJobs.Host.EndToEndTests
{
    public class EventHubApplicationInsightsTests : IDisposable
    {
        private const string TestHubName = "webjobstesthub";
        private const int Timeout = 30000;
        private static EventWaitHandle _eventWait;
        private readonly TestTelemetryChannel _channel = new TestTelemetryChannel();
        private static string _testPrefix;
        private readonly string _connection;
        private readonly string _endpoint;
        private static int _receivedMessageCount = -1;
        private readonly EventHubsConnectionStringBuilder _connectionBuilder;
        private readonly JsonSerializerSettings jsonSettingThrowOnError = new JsonSerializerSettings
        {
            MissingMemberHandling = MissingMemberHandling.Error,
            ReferenceLoopHandling = ReferenceLoopHandling.Error,
            NullValueHandling = NullValueHandling.Include,
            DefaultValueHandling = DefaultValueHandling.Include,
        };

        public EventHubApplicationInsightsTests()
        {
            _receivedMessageCount = -1;
            _eventWait = new ManualResetEvent(initialState: false);
            _testPrefix = Guid.NewGuid().ToString();

            var config = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddTestSettings()
                .Build();

            const string connectionName = "AzureWebJobsTestHubConnection";
            _connection = config.GetConnectionStringOrSetting(connectionName);
            Assert.True(!string.IsNullOrEmpty(_connection), $"Required test connection string '{connectionName}' is missing.");

            _connectionBuilder = new EventHubsConnectionStringBuilder(_connection);
            _endpoint = _connectionBuilder.Endpoint.ToString();
        }

        [Fact]
        public async Task EventHub_SingleDispatch()
        {
            using (var host = BuildHost<EventHubTestSingleDispatchJobs>())
            {
                await host.StartAsync();

                var method = typeof(EventHubTestSingleDispatchJobs).GetMethod("SendEvent_TestHub", BindingFlags.Static | BindingFlags.Public);
                await host.GetJobHost().CallAsync(method, new { input = _testPrefix });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.True(result);
            }

            List<RequestTelemetry> requests = _channel.Telemetries.OfType<RequestTelemetry>().ToList();
            List<DependencyTelemetry> dependencies = _channel.Telemetries.OfType<DependencyTelemetry>().ToList();

            // One dependency for the 'Send' from SendEvent_TestHub
            Assert.Single(dependencies);
            Assert.Single(dependencies, d => d.Name == "Send");

            var ehOutDependency = dependencies.Single(d => d.Name == "Send");

            var manualCallRequest = requests.Single(r => r.Context.Operation.Name == "SendEvent_TestHub");
            var operationId = manualCallRequest.Context.Operation.Id;

            // there could be multiple from multiple tests runs
            var ehTriggerRequest = requests.Single(r => r.Context.Operation.Name == "ProcessSingleEvent" && r.Context.Operation.Id == operationId);

            ValidateEventHubDependency(
                ehOutDependency,
                _endpoint, 
                TestHubName, 
                "Send",
                "SendEvent_TestHub", 
                operationId, 
                manualCallRequest.Id,
                LogCategories.Bindings);

            ValidateEventHubRequest(
                ehTriggerRequest,
                true,
                "ProcessSingleEvent",
                operationId,
                ehOutDependency.Id);

            Assert.False(ehTriggerRequest.Properties.ContainsKey("_MS.links"));
        }

        [Fact]
        public async Task EventHub_MultipleDispatch_BatchSend()
        {
            object receivedEventsCount = (object)-1;
            using (var host = BuildHost<EventHubTestMultipleDispatchJobs>())
            {
                await host.StartAsync();

                var method = typeof(EventHubTestMultipleDispatchJobs).GetMethod("SendEvents_TestHub", BindingFlags.Static | BindingFlags.Public);
                await host.GetJobHost().CallAsync(method, new { numEvents = 5, input = _testPrefix });

                bool result = _eventWait.WaitOne(Timeout);
                Assert.True(result);
            }

            List<RequestTelemetry> requests = _channel.Telemetries.OfType<RequestTelemetry>().ToList();
            List<DependencyTelemetry> dependencies = _channel.Telemetries.OfType<DependencyTelemetry>().ToList();

            // One dependency for the 'Send' from SendEvent_TestHub
            Assert.Single(dependencies);
            Assert.Single(dependencies, d => d.Name == "Send");

            var ehOutDependency = dependencies.Single(d => d.Name == "Send");

            var manualCallRequest = requests.Single(r => r.Context.Operation.Name == "SendEvents_TestHub");
            var manualOperationId = manualCallRequest.Context.Operation.Id;

            ValidateEventHubDependency(
                ehOutDependency,
                _endpoint,
                TestHubName,
                "Send",
                "SendEvents_TestHub",
                manualOperationId,
                manualCallRequest.Id,
                LogCategories.Bindings);

            var ehTriggerRequests = requests.Where(r => r.Context.Operation.Name == "ProcessMultipleEvents");
            List<TestLink> actualLinks = new List<TestLink>();
            foreach (var ehTriggerRequest in ehTriggerRequests)
            {
                if (ehTriggerRequest.Properties.TryGetValue("_MS.links", out var linksStr))
                {
                    ValidateEventHubRequest(
                        ehTriggerRequest,
                        true,
                        "ProcessMultipleEvents",
                        null,
                        null);

                    Assert.NotNull(ehTriggerRequest.Context.Operation.Id);
                    Assert.Null(ehTriggerRequest.Context.Operation.ParentId);

                    actualLinks.AddRange(JsonConvert.DeserializeObject<TestLink[]>(linksStr, jsonSettingThrowOnError));
                }
            }

            Assert.Equal(_receivedMessageCount, actualLinks.Count); 

            var currentTestLinks = actualLinks.Where(l => l.operation_Id == manualOperationId).ToArray();

            // there should be 5 relevant links
            // current Event Hubs SDK does not generate unique Id per message, 
            // so they all share the same Id
            Assert.Equal(_receivedMessageCount, currentTestLinks.Length); 
            Assert.Equal(_receivedMessageCount, currentTestLinks.Count(l => l.id == ehOutDependency.Id));
        }

        [Fact]
        public async Task EventHub_MultipleDispatch_IndependentMessages()
        {
            // send individual messages via EventHub client, process batch by host 
            var ehClient = EventHubClient.CreateFromConnectionString(_connection);

            var messages = new EventData[5];
            var expectedLinks = new TestLink[messages.Length];

            for (int i = 0; i < messages.Length; i++)
            {
                var operationId = ActivityTraceId.CreateRandom().ToHexString();
                var spanId = ActivitySpanId.CreateRandom().ToHexString();
                expectedLinks[i] = new TestLink
                {
                    operation_Id = operationId,
                    id = $"|{operationId}.{spanId}."
                };

                messages[i] = new EventData(Encoding.UTF8.GetBytes(_testPrefix + i))
                {
                    Properties = {["Diagnostic-Id"] = $"00-{operationId}-{spanId}-01"}
                };
            }

            await ehClient.SendAsync(messages);

            using (var host = BuildHost<EventHubTestMultipleDispatchJobs>())
            {
                await host.StartAsync();
                bool result = _eventWait.WaitOne(Timeout);
                Assert.True(result);
            }

            List<RequestTelemetry> requests = _channel.Telemetries.OfType<RequestTelemetry>().ToList();

            var ehTriggerRequests = requests.Where(r => r.Context.Operation.Name == "ProcessMultipleEvents");

            List<TestLink> actualLinks = new List<TestLink>();
            foreach (var ehTriggerRequest in ehTriggerRequests)
            {
                ValidateEventHubRequest(
                    ehTriggerRequest,
                    true,
                    "ProcessMultipleEvents",
                    null,
                    null);

                Assert.NotNull(ehTriggerRequest.Context.Operation.Id);
                Assert.Null(ehTriggerRequest.Context.Operation.ParentId);
                Assert.True(ehTriggerRequest.Properties.TryGetValue("_MS.links", out var linksStr));
                actualLinks.AddRange(JsonConvert.DeserializeObject<TestLink[]>(linksStr, jsonSettingThrowOnError));

                Assert.Equal(SamplingDecision.SampledIn, ehTriggerRequest.ProactiveSamplingDecision);
            }

            Assert.True(actualLinks.Count >= expectedLinks.Length); // we've sent 5 events
            foreach (var link in actualLinks)
            {
                Assert.Contains(expectedLinks, l => l.operation_Id == link.operation_Id 
                                                    && l.id == link.id);
            }
        }

        private void ValidateEventHubRequest(
            RequestTelemetry request,
            bool success,
            string operationName,
            string operationId,
            string parentId)
        {
            Assert.Empty(request.Source);
            Assert.Null(request.Url);

            Assert.True(request.Properties.ContainsKey(LogConstants.FunctionExecutionTimeKey));
            Assert.True(double.TryParse(request.Properties[LogConstants.FunctionExecutionTimeKey], out double functionDuration));
            Assert.True(request.Duration.TotalMilliseconds >= functionDuration);

            Assert.Equal(LogCategories.Results, request.Properties[LogConstants.CategoryNameKey]);
            Assert.Equal((success ? LogLevel.Information : LogLevel.Error).ToString(), request.Properties[LogConstants.LogLevelKey]);
            Assert.NotNull(request.Name);
            Assert.NotNull(request.Id);

            if (operationId != null)
            {
                Assert.Equal(operationId, request.Context.Operation.Id);
            }

            if (parentId != null)
            {
                Assert.Equal(parentId, request.Context.Operation.ParentId);
            }

            Assert.Equal(operationName, request.Context.Operation.Name);
            Assert.Equal(operationName, request.Name);

            Assert.True(request.Properties.ContainsKey(LogConstants.InvocationIdKey));

            // EventHub does not populate Trigger reason
            Assert.False(request.Properties.ContainsKey(LogConstants.TriggerReasonKey));

            Assert.StartsWith("webjobs:", request.Context.GetInternalContext().SdkVersion);

            Assert.Equal(success, request.Success);
            Assert.Equal("0", request.ResponseCode);

            Assert.DoesNotContain(request.Properties, p => p.Key == LogConstants.SucceededKey);
        }

        private void ValidateEventHubDependency(
            DependencyTelemetry dependency,
            string endpoint,
            string entityName,
            string name,
            string operationName,
            string operationId,
            string parentId,
            string category)
        {
            Assert.Equal($"{endpoint} | {entityName}", dependency.Target);
            Assert.Equal("Azure Event Hubs", dependency.Type);
            Assert.Equal(name, dependency.Name);
            Assert.True(dependency.Success);
            Assert.True(string.IsNullOrEmpty(dependency.Data));

            Assert.Equal(category, dependency.Properties[LogConstants.CategoryNameKey]);
            Assert.Equal(LogLevel.Information.ToString(), dependency.Properties[LogConstants.LogLevelKey]);
            Assert.NotNull(dependency.Target);
            Assert.NotNull(dependency.Name);
            Assert.NotNull(dependency.Id);

            if (operationId != null)
            {
                Assert.Equal(operationId, dependency.Context.Operation.Id);
            }

            Assert.Equal(operationName, dependency.Context.Operation.Name);

            if (parentId != null)
            {
                Assert.Equal(parentId, dependency.Context.Operation.ParentId);
            }

            Assert.True(dependency.Properties.ContainsKey(LogConstants.InvocationIdKey));
        }

        public class EventHubTestSingleSendOnlyJobs
        {
            public static void SendEvent_TestHub(string input, [EventHub(TestHubName)] out EventData evt)
            {
                evt = new EventData(Encoding.UTF8.GetBytes(input));
            }
        }

        public class EventHubTestSingleDispatchJobs
        {
            public static void SendEvent_TestHub(string input, [EventHub(TestHubName)] out EventData evt)
            {
                evt = new EventData(Encoding.UTF8.GetBytes(input));
            }

            public static void ProcessSingleEvent([EventHubTrigger(TestHubName)] string evt)
            {
                if (evt.StartsWith(_testPrefix))
                {
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
                    var evt = new EventData(Encoding.UTF8.GetBytes(input + i));
                    events[i] = evt;
                }
            }

            public static void ProcessMultipleEvents([EventHubTrigger(TestHubName)] string[] events)
            {
                if (events.Any(e => e.StartsWith(_testPrefix + "4")))
                {
                    _eventWait.Set();
                    _receivedMessageCount = events.Length;
                }
            }
        }

        private IHost BuildHost<T>()
        {
            IHost host = new HostBuilder()
                .ConfigureDefaultTestHost<T>(b =>
                {
                    b.AddEventHubs(options =>
                    {
                        options.AddSender(TestHubName, _connection);
                        options.AddReceiver(TestHubName, _connection);
                    });
                })
                .ConfigureLogging(b =>
                {
                    b.SetMinimumLevel(LogLevel.Information);
                    b.AddApplicationInsights(o => o.InstrumentationKey = "mock ikey");
                })
                .Build();

            TelemetryConfiguration telemetryConfiguration = host.Services.GetService<TelemetryConfiguration>();
            telemetryConfiguration.TelemetryChannel = _channel;

            return host;
        }

        public void Dispose()
        {
            _channel?.Dispose();
        }

        public class TestTelemetryChannel : ITelemetryChannel, ITelemetryModule
        {
            public ConcurrentBag<ITelemetry> Telemetries = new ConcurrentBag<ITelemetry>();

            public bool? DeveloperMode { get; set; }

            public string EndpointAddress { get; set; }

            public void Dispose()
            {
            }

            public void Flush()
            {
            }

            public void Send(ITelemetry item)
            {
                Telemetries.Add(item);
            }

            public void Initialize(TelemetryConfiguration configuration)
            {
            }
        }

        private class TestLink
        {
            public string operation_Id { get; set; }
            public string id { get; set; }
        }
    }
}
