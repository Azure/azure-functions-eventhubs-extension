// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using System.Collections.Generic;

namespace Microsoft.Azure.WebJobs.EventHubs
{
    // The core object we get when an EventHub is triggered. 
    // This gets converted to the user type (EventData, string, poco, etc) 
    internal sealed class EventHubTriggerInput
    {        
        // If != -1, then only process a single event in this batch. 
        private int _selector = -1;

        internal EventData[] Events { get; set; }

        internal PartitionContext PartitionContext { get; set; }

        public bool IsSingleDispatch
        {
            get
            {
                return _selector != -1;
            }
        }

        public static EventHubTriggerInput New(EventData eventData)
        {
            return new EventHubTriggerInput
            {
                PartitionContext = null,
                Events = new EventData[]
                {
                      eventData
                },
                _selector = 0,
            };
        }

        public EventHubTriggerInput GetSingleEventTriggerInput(int idx)
        {
            return new EventHubTriggerInput
            {
                Events = this.Events,
                PartitionContext = this.PartitionContext,
                _selector = idx
            };
        }

        public EventData GetSingleEventData()
        {
            return this.Events[this._selector];
        }

        public Dictionary<string, string> GetTriggerDetails(PartitionContext context, int retryCount)
        {
            if (Events.Length == 0)
            {
                return new Dictionary<string, string>();
            }

            string offset, enqueueTimeUtc, sequenceNumber;
            if (IsSingleDispatch)
            {
                offset = Events[0].SystemProperties?.Offset;
                enqueueTimeUtc = Events[0].SystemProperties?.EnqueuedTimeUtc.ToString("o");
                sequenceNumber = Events[0].SystemProperties?.SequenceNumber.ToString();
            }
            else
            {
                EventData first = Events[0];
                EventData last = Events[Events.Length - 1];

                offset = $"{first.SystemProperties?.Offset}-{last.SystemProperties?.Offset}";
                enqueueTimeUtc = $"{first.SystemProperties?.EnqueuedTimeUtc.ToString("o")}-{last.SystemProperties?.EnqueuedTimeUtc.ToString("o")}";
                sequenceNumber = $"{first.SystemProperties?.SequenceNumber}-{last.SystemProperties?.SequenceNumber}";
            }

            return new Dictionary<string, string>()
            {
                { "PartionId", context.PartitionId },
                { "Offset", offset },
                { "EnqueueTimeUtc", enqueueTimeUtc },
                { "SequenceNumber", sequenceNumber },
                { "Count", Events.Length.ToString()},
                { "RetryCount", retryCount.ToString() }
            };
        }
    }
}