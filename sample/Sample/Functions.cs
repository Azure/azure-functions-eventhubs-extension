using Microsoft.Azure.EventHubs;
// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace Sample
{
    public class Functions
    {
        public void EventHubTriger(
            [EventHubTrigger("eventhub-test", Connection = "AzureWebJobsEventHubReceiver")] string message, ILogger logger)
        {
            throw new Exception();
            // logger.LogInformation($"Message: {message}");
        }
    }
}
