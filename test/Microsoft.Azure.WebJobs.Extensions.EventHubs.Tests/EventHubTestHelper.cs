// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Host.TestCommon;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.WebJobs.EventHubs.EndToEndTests
{
    internal class EventHubTestHelper
    {
        public static Tuple<JobHost, IHost> BuildHost<T>(string testHubName, RetryPolicyOptions retry = null)
        {
            JobHost jobHost = null;
            string connection = GetConnectionString();
            IHost host = new HostBuilder()
                .ConfigureHostConfiguration(b =>
                {
                    if (retry != null)
                    {
                        string extensionPath = "AzureWebJobs:CheckpointRetryPolicy";
                        var values = new Dictionary<string, string>
                        {
                            { $"{extensionPath}:RetryCount", retry.RetryCount.ToString() },
                            { $"{extensionPath}:SleepDuration", retry.SleepDuration.ToString("c") }
                        };
                        b.AddInMemoryCollection(values);
                    }
                })
                .ConfigureDefaultTestHost<T>(b =>
                {
                    b.AddExecutionContextBinding()
                    .AddEventHubs(options =>
                    {
                        options.EventProcessorOptions.EnableReceiverRuntimeMetric = true;
                        options.EventProcessorOptions.ReceiveTimeout = TimeSpan.FromSeconds(3);
                        options.AddSender(testHubName, connection);
                        options.AddReceiver(testHubName, connection);
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

        public static async Task CheckpointOldEvents(string testHubNmae)
        {
            using (var host = BuildHost<JobCheckpointOldEvents>(testHubNmae).Item2)
            {
                await Task.Delay(3000);
            }
        }

        public static async Task SendMessage(string message)
        {
            string connection = GetConnectionString();
            EventHubClient eventHubClient = EventHubClient.CreateFromConnectionString(connection);
  
            await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
        }

        private static string GetConnectionString()
        {
            var config = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddTestSettings()
                .Build();

            const string connectionName = "AzureWebJobsTestHubConnection";
            string connection = config.GetConnectionStringOrSetting(connectionName);
            Assert.True(!string.IsNullOrEmpty(connection), $"Required test connection string '{connectionName}' is missing.");

            return connection;
        }

        public class JobCheckpointOldEvents
        {
            public void Job([EventHubTrigger("webjobstesthub")] string message, ILogger logegr)
            {
            }
        }
    }
}
