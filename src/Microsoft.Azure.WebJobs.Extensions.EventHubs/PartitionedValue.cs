// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System.Text;

namespace Microsoft.Azure.WebJobs.EventHubs
{
    public class PartitionedValue
    {
        public PartitionedValue(EventData value, string partitionKey = null)
        {
            EventData = value;
            PartitionKey = partitionKey;
        }

        public PartitionedValue(string value, string partitionKey = null)
        {
            EventData = new EventData(Encoding.UTF8.GetBytes(value));
            PartitionKey = partitionKey;
        }

        public PartitionedValue(byte[] value, string partitionKey = null)
        {
            EventData = new EventData(value);
            PartitionKey = partitionKey;
        }

        public PartitionedValue(object value, string partitionKey = null)
        {
            EventData = new EventData(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(value)));
            PartitionKey = partitionKey;
        }

        public EventData EventData { get; set; }

        public string PartitionKey { get; set; } 
    }
}
