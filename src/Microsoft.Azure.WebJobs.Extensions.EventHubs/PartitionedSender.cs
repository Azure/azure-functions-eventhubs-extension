// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.EventHubs;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.WebJobs.EventHubs
{
    /// <summary>
    /// Send events with partition keys to EventHub.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class PartitionedSender
    {
        private IConverterManager _converterManager;
        private EventHubAttribute _eventHubAttribute;
        private EventHubClient _client;

        public PartitionedSender(EventHubClient client, IConverterManager converterManager, EventHubAttribute eventHubAttribute)
        {
            _client = client;
            _converterManager = converterManager;
            _eventHubAttribute = eventHubAttribute;
        }

        /// <summary>
        /// Sends an event with partition key to EventHub.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="item"></param>
        /// <param name="partitionKey"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task SendAsync<T>(T item, string partitionKey, CancellationToken cancellationToken = default(CancellationToken))
        {
            FuncAsyncConverter convertor = _converterManager.GetConverter<EventHubAttribute>(typeof(T), typeof(EventData));
            EventData result = await convertor.Invoke(item, _eventHubAttribute, null) as EventData;

            if (string.IsNullOrEmpty(partitionKey))
            {
                await _client.SendAsync(result);
            }
            else
            {
                await _client.SendAsync(result, partitionKey);
            }
        }
    }
}
