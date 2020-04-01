// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System.Text;
using Microsoft.Extensions.Hosting;
using Xunit;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Threading;
using System;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Host.TestCommon;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;

namespace Microsoft.Azure.WebJobs.EventHubs.EndToEndTests
{
    public class EventHubRetryTests
    {
        private const int Timeout = 30000;
        public static EventWaitHandle EventWait = new ManualResetEvent(false);
        private const string TestHubName = "webjobstesthub";
        private static string testId = Guid.NewGuid().ToString();

        [Fact]
        public async Task ThrowException_AllRetriesExhausted()
        {
            RetryPolicyOptions options = new RetryPolicyOptions()
            {
                RetryCount = 3,
                SleepDuration = TimeSpan.FromMilliseconds(1)
            };
            var logs = await ExecuteTest<ThrowException_AllRetriesExhaustedClass>(options);

            Assert.Equal(3, logs.Where(x => x.Contains("New linar retry attempt")).Count());
            Assert.Equal(4, logs.Where(x => x.Contains("Execute with retry")).Count());
            Assert.Single(logs.Where(x => x.Contains(", All retries have been exhausted")));
        }

        [Fact]
        public async Task ThrowException_Successed()
        {
            RetryPolicyOptions options = new RetryPolicyOptions()
            {
                RetryCount = 10,
                SleepDuration = TimeSpan.FromMilliseconds(1)
            };
            var logs = await ExecuteTest<ThrowException_SuccessedClass>(options);

            Assert.Equal(3, logs.Where(x => x.Contains("New linar retry attempt")).Count());
            Assert.Equal(4, logs.Where(x => x.Contains("Execute with retry")).Count());
        }

        [Fact]
        public async Task ThrowException_AllRetriesExhausted_Exponential()
        {
            Stopwatch watch = new Stopwatch();

            RetryPolicyOptions options = new RetryPolicyOptions()
            {
                RetryCount = 3,
                ExponentialBackoff = true
            };
            watch.Start();
            var logs = await ExecuteTest<ThrowException_AllRetriesExhaustedClass>(options);
            watch.Stop();

            Assert.True(watch.ElapsedMilliseconds > 10000);
            Assert.Equal(3, logs.Where(x => x.Contains("New linar retry attempt")).Count());
            Assert.Equal(4, logs.Where(x => x.Contains("Execute with retry")).Count());
            Assert.Single(logs.Where(x => x.Contains(", All retries have been exhausted")));
        }

        [Fact]
        public async Task ThrowException_Infinite()
        {
            Stopwatch watch = new Stopwatch();

            RetryPolicyOptions options = new RetryPolicyOptions()
            {
                SleepDuration = TimeSpan.FromSeconds(1),
                RetryCount = -1
            };
            watch.Start();
            var logs = await ExecuteTest<ThrowException_InfiniteClass>(options, false);
            watch.Stop();

            Assert.True(watch.ElapsedMilliseconds > 30000);
        }

        [Fact]
        public async Task ReturnRetry_AllRetriesExhausted()
        {
            var logs = await ExecuteTest<RetryReturn_AllRetriesExhaustedClass>(null);

            Assert.Single(logs.Where(x => x.Contains("Execute without retry")));
            Assert.Single(logs.Where(x => x.Contains("Function code returned retry settings")));
            Assert.Equal(3, logs.Where(x => x.Contains("New linar retry attempt")).Count());
            Assert.Equal(4, logs.Where(x => x.Contains("Execute with retry")).Count());
            Assert.Single(logs.Where(x => x.Contains(", All retries have been exhausted")));
        }

        [Fact]
        public async Task RetryReturn_Successed()
        {
            var logs = await ExecuteTest<RetryReturn_SuccessedClass>(null);

            Assert.Single(logs.Where(x => x.Contains("Execute without retry")));
            Assert.Single(logs.Where(x => x.Contains("Function code returned retry settings")));
            Assert.Equal(3, logs.Where(x => x.Contains("New linar retry attempt")).Count());
            Assert.Equal(4, logs.Where(x => x.Contains("Execute with retry")).Count());
        }

        [Fact]
        public async Task ReturnRetry_Infinite()
        {
            Stopwatch watch = new Stopwatch();

            RetryPolicyOptions options = new RetryPolicyOptions()
            {
                SleepDuration = TimeSpan.FromSeconds(1),
                RetryCount = 3
            };
            watch.Start();
            var logs = await ExecuteTest<ReturnRetry_InfiniteClass>(options, false);
            watch.Stop();

            var test = logs.Where(x => x.Contains("Execute with retry")).Count();
            Assert.True(watch.ElapsedMilliseconds > 30000);
        }

        [Fact]
        public async Task ReturnRetry_Wins()
        {
            RetryPolicyOptions options = new RetryPolicyOptions()
            {
                RetryCount = 10,
                SleepDuration = TimeSpan.FromMilliseconds(1)
            };
            var logs = await ExecuteTest<ReturnRetry_WinsClass>(options);

            Assert.Equal(6, logs.Where(x => x.Contains("New linar retry attempt")).Count());
            Assert.Equal(8, logs.Where(x => x.Contains("Execute with retry")).Count());
            Assert.Single(logs.Where(x => x.Contains(", All retries have been exhausted")));
        }

        private async Task<string[]> ExecuteTest<T>(RetryPolicyOptions options, bool checkForTimeout = true)
        {
            EventWait = new ManualResetEvent(false);

            await EventHubTestHelper.CheckpointOldEvents(TestHubName);

            var tuple = EventHubTestHelper.BuildHost<T>(TestHubName, options);
            using (var host = tuple.Item2)
            {
                host.GetTestLoggerProvider().ClearAllLogMessages();
                await EventHubTestHelper.SendMessage(testId);

                bool result = EventWait.WaitOne(Timeout);
                if (checkForTimeout)
                {
                    Assert.True(result);
                }

                // wait all logs tp flush
                await Task.Delay(3000);

                await host.StopAsync();
                return host.GetTestLoggerProvider().GetAllLogMessages().Where(x => x.FormattedMessage != null).Select(x => x.FormattedMessage).ToArray();
            }
        }

        public class ThrowException_AllRetriesExhaustedClass
        {
            public void Trigger([EventHubTrigger(TestHubName)] string message, ExecutionContext context)
            {
                if (context.RetryCount == 3)
                {
                    EventWait.Set();
                }
                throw new Exception("Test");
            }
        }

        public class ThrowException_InfiniteClass
        {
            public void Trigger([EventHubTrigger(TestHubName)] string message, ExecutionContext context)
            {
                throw new Exception("Test");
            }
        }

        public class ReturnRetry_InfiniteClass
        {
            public Task<RetryResult> Trigger([EventHubTrigger(TestHubName)] string message, ExecutionContext context)
            {
                if (context.RetryCount == -1)
                {
                    return Task.FromResult(new RetryResult(-1, TimeSpan.FromSeconds(1)));
                }
                throw new Exception("Test");
            }
        }

        public class RetryReturn_AllRetriesExhaustedClass
        {
            public RetryResult Trigger([EventHubTrigger(TestHubName)] string message, ExecutionContext context)
            {
                if (context.RetryCount == -1)
                {
                    return new RetryResult(3, TimeSpan.FromMilliseconds(1));
                }
                if (context.RetryCount == 3)
                {
                    EventWait.Set();
                }
                throw new Exception("Test");
            }
        }

        public class ThrowException_SuccessedClass
        {
            public void Trigger([EventHubTrigger(TestHubName)] string message, ExecutionContext context)
            {
                if (context.RetryCount == 3)
                {
                    EventWait.Set();
                    return;
                }
                throw new InvalidOperationException("Test");
            }
        }

        public class RetryReturn_SuccessedClass
        {
            public RetryResult Trigger([EventHubTrigger(TestHubName)] string message, ExecutionContext context)
            {
                if (context.RetryCount == -1)
                {
                    return new RetryResult(10, TimeSpan.FromMilliseconds(1));
                }
                if (context.RetryCount == 3)
                {
                    EventWait.Set();
                    return null;
                }
                throw new InvalidOperationException("Test");
            }
        }

        public class ReturnRetry_WinsClass
        {
            public static bool RetryReturned = false;

            public RetryResult Trigger([EventHubTrigger(TestHubName)] string message, ExecutionContext context)
            {
                if (!RetryReturned && context.RetryCount == 1)
                {
                    RetryReturned = true;
                    return new RetryResult(5, TimeSpan.FromMilliseconds(1));
                }
                if (RetryReturned && context.RetryCount == 5)
                {
                    EventWait.Set();
                }
                throw new Exception("Test");
            }
        }
    }
}
