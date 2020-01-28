// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Azure.WebJobs.Hosting;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.EventHubs
{
    public class EventHubOptions : IOptionsFormatter
    {
        // Event Hub Names are case-insensitive.
        // The same path can have multiple connection strings with different permissions (sending and receiving), 
        // so we track senders and receivers separately and infer which one to use based on the EventHub (sender) vs. EventHubTrigger (receiver) attribute. 
        // Connection strings may also encapsulate different endpoints. 

        // The client cache must be thread safe because clients are accessed/added on the function
        private readonly ConcurrentDictionary<string, EventHubClient> _clients = new ConcurrentDictionary<string, EventHubClient>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, ReceiverCreds> _receiverCreds = new Dictionary<string, ReceiverCreds>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, EventProcessorHost> _explicitlyProvidedHosts = new Dictionary<string, EventProcessorHost>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Name of the blob container that the EventHostProcessor instances uses to coordinate load balancing listening on an event hub. 
        /// Each event hub gets its own blob prefix within the container. 
        /// </summary>
        public const string LeaseContainerName = "azure-webjobs-eventhub";
        private int _batchCheckpointFrequency = 1;

        public EventHubOptions()
        {
            EventProcessorOptions = EventProcessorOptions.DefaultOptions;
            PartitionManagerOptions = new PartitionManagerOptions();
        }

        /// <summary>
        /// Gets or sets the number of batches to process before creating an EventHub cursor checkpoint. Default 1.
        /// </summary>
        public int BatchCheckpointFrequency
        {
            get => _batchCheckpointFrequency;

            set
            {
                if (value <= 0)
                {
                    throw new InvalidOperationException("Batch checkpoint frequency must be larger than 0.");
                }

                _batchCheckpointFrequency = value;
            }
        }

        public EventProcessorOptions EventProcessorOptions { get; }

        public PartitionManagerOptions PartitionManagerOptions { get; }

        /// <summary>
        /// Add an existing client for sending messages to an event hub.  Infer the eventHub name from client.path
        /// </summary>
        /// <param name="client"></param>
        public void AddEventHubClient(EventHubClient client)
        {
            if (client == null)
            {
                throw new ArgumentNullException(nameof(client));
            }

            AddEventHubClient(client.EventHubName, client);
        }

        /// <summary>
        /// Add an existing client for sending messages to an event hub.  Infer the eventHub name from client.path
        /// </summary>
        /// <param name="eventHubName">name of the event hub</param>
        /// <param name="client"></param>
        public void AddEventHubClient(string eventHubName, EventHubClient client)
        {
            if (string.IsNullOrWhiteSpace(eventHubName))
            {
                throw new ArgumentNullException(nameof(eventHubName));
            }

            _clients[eventHubName] = client ?? throw new ArgumentNullException(nameof(client));
        }

        /// <summary>
        /// Add a connection for sending messages to an event hub. Connect via the connection string. 
        /// </summary>
        /// <param name="eventHubName">name of the event hub. </param>
        /// <param name="sendConnectionString">connection string for sending messages. If this includes an EntityPath, it takes precedence over the eventHubName parameter.</param>
        public void AddSender(string eventHubName, string sendConnectionString)
        {
            if (string.IsNullOrWhiteSpace(eventHubName))
            {
                throw new ArgumentNullException(nameof(eventHubName));
            }
            if (string.IsNullOrWhiteSpace(sendConnectionString))
            {
                throw new ArgumentNullException(nameof(sendConnectionString));
            }

            EventHubsConnectionStringBuilder sb = new EventHubsConnectionStringBuilder(sendConnectionString);
            if (string.IsNullOrWhiteSpace(sb.EntityPath))
            {
                sb.EntityPath = eventHubName;
            }

            AddEventHubClient(eventHubName, EventHubClient.CreateFromConnectionString(sb.ToString()));
        }

        /// <summary>
        /// Add a connection for listening on events from an event hub. 
        /// </summary>
        /// <param name="eventHubName">Name of the event hub</param>
        /// <param name="listener">initialized listener object</param>
        /// <remarks>The EventProcessorHost type is from the ServiceBus SDK. 
        /// Allow callers to bind to EventHubConfiguration without needing to have a direct assembly reference to the ServiceBus SDK. 
        /// The compiler needs to resolve all types in all overloads, so give methods that use the ServiceBus SDK types unique non-overloaded names
        /// to avoid eager compiler resolution. 
        /// </remarks>
        public void AddEventProcessorHost(string eventHubName, EventProcessorHost listener)
        {
            if (string.IsNullOrWhiteSpace(eventHubName))
            {
                throw new ArgumentNullException(nameof(eventHubName));
            }

            _explicitlyProvidedHosts[eventHubName] = listener ?? throw new ArgumentNullException(nameof(listener));
        }

        /// <summary>
        /// Add a connection for listening on events from an event hub. Connect via the connection string and use the SDK's built-in storage account.
        /// </summary>
        /// <param name="eventHubName">name of the event hub</param>
        /// <param name="receiverConnectionString">connection string for receiving messages. This can encapsulate other service bus properties like the namespace and endpoints.</param>
        public void AddReceiver(string eventHubName, string receiverConnectionString)
        {
            if (string.IsNullOrWhiteSpace(eventHubName))
            {
                throw new ArgumentNullException("eventHubName");
            }
            if (string.IsNullOrWhiteSpace(receiverConnectionString))
            {
                throw new ArgumentNullException("receiverConnectionString");
            }

            this._receiverCreds[eventHubName] = new ReceiverCreds
            {
                EventHubConnectionString = receiverConnectionString
            };
        }

        /// <summary>
        /// Add a connection for listening on events from an event hub. Connect via the connection string and use the supplied storage account
        /// </summary>
        /// <param name="eventHubName">name of the event hub</param>
        /// <param name="receiverConnectionString">connection string for receiving messages</param>
        /// <param name="storageConnectionString">storage connection string that the EventProcessorHost client will use to coordinate multiple listener instances. </param>
        public void AddReceiver(string eventHubName, string receiverConnectionString, string storageConnectionString)
        {
            if (string.IsNullOrWhiteSpace(eventHubName))
            {
                throw new ArgumentNullException(nameof(eventHubName));
            }
            if (string.IsNullOrWhiteSpace(receiverConnectionString))
            {
                throw new ArgumentNullException(nameof(receiverConnectionString));
            }
            if (string.IsNullOrWhiteSpace(storageConnectionString))
            {
                throw new ArgumentNullException(nameof(storageConnectionString));
            }

            this._receiverCreds[eventHubName] = new ReceiverCreds
            {
                EventHubConnectionString = receiverConnectionString,
                StorageConnectionString = storageConnectionString
            };
        }

        internal EventHubClient GetEventHubClient(string eventHubName, string connection)
        {

            if (string.IsNullOrEmpty(eventHubName))
            {
                EventHubsConnectionStringBuilder builder = new EventHubsConnectionStringBuilder(connection);
                eventHubName = builder.EntityPath;
            }

            if (_clients.TryGetValue(eventHubName, out EventHubClient client))
            {
                return client;
            }
            else if (!string.IsNullOrWhiteSpace(connection))
            {
                return _clients.GetOrAdd(eventHubName, key =>
                {
                    AddSender(key, connection);
                    return _clients[key];
                });
            }

            throw new InvalidOperationException("No event hub sender named " + eventHubName);
        }

        // Lookup a listener for receiving events given the name provided in the [EventHubTrigger] attribute. 
        internal EventProcessorHost GetEventProcessorHost(IConfiguration config, string eventHubName, string consumerGroup)
        {
            if (this._receiverCreds.TryGetValue(eventHubName, out ReceiverCreds creds))
            {
                // Common case. Create a new EventProcessorHost instance to listen. 
                string eventProcessorHostName = Guid.NewGuid().ToString();

                if (string.IsNullOrWhiteSpace(consumerGroup))
                {
                    consumerGroup = PartitionReceiver.DefaultConsumerGroupName;
                }

                var storageConnectionString = creds.StorageConnectionString;
                if (string.IsNullOrWhiteSpace(storageConnectionString))
                {
                    storageConnectionString = config.GetWebJobsConnectionString(ConnectionStringNames.Storage);
                    if (string.IsNullOrWhiteSpace(storageConnectionString))
                    {
                        throw new ArgumentNullException(nameof(storageConnectionString));
                    }
                }

                // If the connection string provides a hub name, that takes precedence. 
                // Note that connection strings *can't* specify a consumerGroup, so must always be passed in. 
                string actualPath = eventHubName;
                var sb = new EventHubsConnectionStringBuilder(creds.EventHubConnectionString);
                if (sb.EntityPath != null)
                {
                    actualPath = sb.EntityPath;
                    sb.EntityPath = null; // need to remove to use with EventProcessorHost
                }

                var @namespace = GetEventHubNamespace(sb);
                var blobPrefix = GetBlobPrefix(actualPath, @namespace);

                // Use blob prefix support available in EPH starting in 2.2.6 
                var host = new EventProcessorHost(
                    hostName: eventProcessorHostName,
                    eventHubPath: actualPath,
                    consumerGroupName: consumerGroup,
                    eventHubConnectionString: sb.ToString(),
                    storageConnectionString: storageConnectionString,
                    leaseContainerName: LeaseContainerName,
                    storageBlobPrefix: blobPrefix)
                {
                    PartitionManagerOptions = PartitionManagerOptions
                };

                return host;
            }
            else
            {
                // Rare case: a power-user caller specifically provided an event processor host to use. 
                if (_explicitlyProvidedHosts.TryGetValue(eventHubName, out EventProcessorHost host))
                {
                    return host;
                }
            }
            throw new InvalidOperationException("No event hub receiver named " + eventHubName);
        }

        private static string EscapeStorageCharacter(char character)
        {
            var ordinalValue = (ushort)character;
            if (ordinalValue < 0x100)
            {
                return string.Format(CultureInfo.InvariantCulture, ":{0:X2}", ordinalValue);
            }
            else
            {
                return string.Format(CultureInfo.InvariantCulture, "::{0:X4}", ordinalValue);
            }
        }

        // Escape a blob path.
        // For diagnostics, we want human-readble strings that resemble the input.
        // Inputs are most commonly alphanumeric with a fex extra chars (dash, underscore, dot).
        // Escape character is a ':', which is also escaped.
        // Blob names are case sensitive; whereas input is case insensitive, so normalize to lower.
        private static string EscapeBlobPath(string path)
        {
            var sb = new StringBuilder(path.Length);
            foreach (char c in path)
            {
                if (c >= 'a' && c <= 'z')
                {
                    sb.Append(c);
                }
                else if (c == '-' || c == '_' || c == '.')
                {
                    // Potentially common carahcters. 
                    sb.Append(c);
                }
                else if (c >= 'A' && c <= 'Z')
                {
                    sb.Append((char)(c - 'A' + 'a')); // ToLower
                }
                else if (c >= '0' && c <= '9')
                {
                    sb.Append(c);
                }
                else
                {
                    sb.Append(EscapeStorageCharacter(c));
                }
            }

            return sb.ToString();
        }

        internal static string GetEventHubNamespace(EventHubsConnectionStringBuilder connectionString) =>
            // EventHubs only have 1 endpoint.
            connectionString.Endpoint.Host;

        /// <summary>
        /// Get the blob prefix used with EventProcessorHost for a given event hub.  
        /// </summary>
        /// <param name="eventHubName">the event hub path</param>
        /// <param name="serviceBusNamespace">the event hub's service bus namespace.</param>
        /// <returns>a blob prefix path that can be passed to EventProcessorHost.</returns>
        /// <remarks>
        /// An event hub is defined by it's path and namespace. The namespace is extracted from the connection string. 
        /// This must be an injective one-to-one function because:
        /// 1. multiple machines listening on the same event hub must use the same blob prefix. This means it must be deterministic. 
        /// 2. different event hubs must not resolve to the same path. 
        /// </remarks>
        public static string GetBlobPrefix(string eventHubName, string serviceBusNamespace)
        {
            if (string.IsNullOrWhiteSpace(eventHubName))
            {
                throw new ArgumentNullException(nameof(eventHubName));
            }
            if (string.IsNullOrWhiteSpace(serviceBusNamespace))
            {
                throw new ArgumentNullException(nameof(serviceBusNamespace));
            }

            return EscapeBlobPath(serviceBusNamespace) + "/" + EscapeBlobPath(eventHubName) + "/";
        }

        public string Format()
        {
            JObject eventProcessorOptions = null;
            if (EventProcessorOptions != null)
            {
                eventProcessorOptions = new JObject
                {
                    { nameof(EventProcessorOptions.EnableReceiverRuntimeMetric), EventProcessorOptions.EnableReceiverRuntimeMetric },
                    { nameof(EventProcessorOptions.InvokeProcessorAfterReceiveTimeout), EventProcessorOptions.InvokeProcessorAfterReceiveTimeout },
                    { nameof(EventProcessorOptions.MaxBatchSize), EventProcessorOptions.MaxBatchSize },
                    { nameof(EventProcessorOptions.PrefetchCount), EventProcessorOptions.PrefetchCount },
                    { nameof(EventProcessorOptions.ReceiveTimeout), EventProcessorOptions.ReceiveTimeout }
                };
            }

            JObject partitionManagerOptions = null;
            if (PartitionManagerOptions != null)
            {
                partitionManagerOptions = new JObject
                {
                    { nameof(PartitionManagerOptions.LeaseDuration), PartitionManagerOptions.LeaseDuration },
                    { nameof(PartitionManagerOptions.RenewInterval), PartitionManagerOptions.RenewInterval },
                };
            }

            JObject options = new JObject
            {
                { nameof(BatchCheckpointFrequency), BatchCheckpointFrequency },
                { nameof(EventProcessorOptions), eventProcessorOptions },
                { nameof(PartitionManagerOptions), partitionManagerOptions }
            };

            return options.ToString(Formatting.Indented);
        }

        // Hold credentials for a given eventHub name. 
        // Multiple consumer groups (and multiple listeners) on the same hub can share the same credentials. 
        private class ReceiverCreds
        {
            // Required.
            public string EventHubConnectionString { get; set; }

            // Optional. If not found, use the stroage from JobHostConfiguration
            public string StorageConnectionString { get; set; }
        }
    }
}
