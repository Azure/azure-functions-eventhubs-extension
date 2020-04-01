// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host;

namespace Sample
{
    public class Functions
    {
        public void EventHubTriger(
            [EventHubTrigger("eventhub-test", Connection = "AzureWebJobsEventHubReceiver")] string[] message, ILogger logger)
        {
            logger.LogInformation($"Message: {message}");
        }
    }
}
