// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using Microsoft.Azure.EventHubs;

namespace Microsoft.Azure.WebJobs.EventHubs
{
    public class EventDataEx : EventData
    {
        public EventDataEx(byte[] array) : base(array) { }

        public EventDataEx(ArraySegment<byte> arraySegment) : base(arraySegment) { }

        /// <summary>
        /// Gets or sets the partition key that should be used when the
        /// event is sent.
        /// </summary>
        public string PartitionKey { get; set; }
    }
}
