// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Azure.WebJobs.EventHubs.Listeners;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Listeners;
using Microsoft.Azure.WebJobs.Host.Scale;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using Polly.Wrap;

namespace Microsoft.Azure.WebJobs.EventHubs
{
    internal sealed class EventHubListener : IListener, IEventProcessorFactory, IScaleMonitorProvider
    {
        private static readonly Dictionary<string, object> EmptyScope = new Dictionary<string, object>();
        private readonly string _functionId;
        private readonly string _eventHubName;
        private readonly string _consumerGroup;
        private readonly string _connectionString;
        private readonly string _storageConnectionString;
        private readonly ITriggeredFunctionExecutor _executor;
        private readonly EventProcessorHost _eventProcessorHost;
        private readonly bool _singleDispatch;
        private readonly EventHubOptions _options;
        private readonly RetryPolicyOptions _checkpointRetryPolicyOptions;
        private readonly ILogger _logger;
        private bool _started;
        private CancellationTokenSource _cts = new CancellationTokenSource();
        private Lazy<EventHubsScaleMonitor> _scaleMonitor;
        private bool _disposed = false;

        public EventHubListener(
            string functionId,
            string eventHubName,
            string consumerGroup,
            string connectionString,
            string storageConnectionString,
            ITriggeredFunctionExecutor executor,
            EventProcessorHost eventProcessorHost,
            bool singleDispatch,
            EventHubOptions options,
            RetryPolicyOptions checkpointRetryPolicyOptions,
            ILogger logger,
            CloudBlobContainer blobContainer = null)
        {
            _functionId = functionId;
            _eventHubName = eventHubName;
            _consumerGroup = consumerGroup;
            _connectionString = connectionString;
            _storageConnectionString = storageConnectionString;
            _executor = executor;
            _eventProcessorHost = eventProcessorHost;
            _singleDispatch = singleDispatch;
            _options = options;
            _checkpointRetryPolicyOptions = checkpointRetryPolicyOptions;
            _logger = logger;
            _scaleMonitor = new Lazy<EventHubsScaleMonitor>(() => new EventHubsScaleMonitor(_functionId, _eventHubName, _consumerGroup, _connectionString, _storageConnectionString, _logger, blobContainer));
        }

        void IListener.Cancel()
        {
            StopAsync(CancellationToken.None).Wait();
        }


        void IDisposable.Dispose()
        {
            Dispose(true);
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (_cts.IsCancellationRequested)
            {
                _cts.Dispose();
                _cts = new CancellationTokenSource();
            }

            await _eventProcessorHost.RegisterEventProcessorFactoryAsync(this, _options.EventProcessorOptions);
            _started = true;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            if (_started)
            {
                // signal cancellation for any in progress executions 
                _cts.Cancel();

                await _eventProcessorHost.UnregisterEventProcessorAsync();
            }
            _started = false;
        }

        IEventProcessor IEventProcessorFactory.CreateEventProcessor(PartitionContext context)
        {
            return new EventProcessor(_options, _checkpointRetryPolicyOptions, _executor, _logger, _singleDispatch, _cts.Token);
        }

        public IScaleMonitor GetMonitor()
        {
            return _scaleMonitor.Value;
        }

        /// <summary>
        /// Wrapper for un-mockable checkpoint APIs to aid in unit testing
        /// </summary>
        internal interface ICheckpointer
        {
            Task CheckpointAsync(PartitionContext context);
        }

        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _cts.Dispose();
                }

                _disposed = true;
            }
        }

        // We get a new instance each time Start() is called. 
        // We'll get a listener per partition - so they can potentialy run in parallel even on a single machine.
        internal class EventProcessor : IEventProcessor, ICheckpointer
        {
            private readonly ITriggeredFunctionExecutor _executor;
            private readonly RetryPolicyOptions _checkpointRetryPolicyOptions;
            private readonly bool _singleDispatch;
            private readonly ILogger _logger;
            private readonly CancellationToken _cancellationToken;
            private readonly ICheckpointer _checkpointer;
            private readonly int _batchCheckpointFrequency;
            private int _batchCounter = 0;

            public EventProcessor(EventHubOptions options, RetryPolicyOptions checkpointRetryPolicyOptions,
                ITriggeredFunctionExecutor executor, ILogger logger, bool singleDispatch, CancellationToken cancellationToken, ICheckpointer checkpointer = null)
            {
                _checkpointer = checkpointer ?? this;
                _executor = executor;
                _singleDispatch = singleDispatch;
                _batchCheckpointFrequency = options.BatchCheckpointFrequency;
                _checkpointRetryPolicyOptions = checkpointRetryPolicyOptions;
                _logger = logger;
                _cancellationToken = cancellationToken;
            }

            public Task CloseAsync(PartitionContext context, CloseReason reason)
            {
                _logger.LogDebug(GetOperationDetails(context, $"CloseAsync, {reason.ToString()}"));
                return Task.CompletedTask;
            }

            public Task OpenAsync(PartitionContext context)
            {
                _logger.LogDebug(GetOperationDetails(context, "OpenAsync"));
                return Task.CompletedTask;
            }

            public Task ProcessErrorAsync(PartitionContext context, Exception error)
            {
                string errorDetails = $"Partition Id: '{context.PartitionId}', Owner: '{context.Owner}', EventHubPath: '{context.EventHubPath}'";

                if (error is ReceiverDisconnectedException ||
                    error is LeaseLostException)
                {
                    // For EventProcessorHost these exceptions can happen as part
                    // of normal partition balancing across instances, so we want to
                    // trace them, but not treat them as errors.
                    _logger.LogInformation($"An Event Hub exception of type '{error.GetType().Name}' was thrown from {errorDetails}. This exception type is typically a result of Event Hub processor rebalancing and can be safely ignored.");
                }
                else
                {
                    _logger.LogError(error, $"Error processing event from {errorDetails}");
                }

                return Task.CompletedTask;
            }

            public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
            {
                RetryManager retryManager = new RetryManager(_checkpointRetryPolicyOptions, _logger);

                await retryManager.ExecuteWithRetriesAsync(ExecuteAsync, new object[] { context, messages }, _cancellationToken);

                if (_cancellationToken.IsCancellationRequested)
                {
                    _logger.LogDebug("All executions were completed but checkpointing was not performed as a cancellation was requested");
                    return;
                }

                await CheckpointAsync(context, messages);
            }

            private async Task<IEnumerable<FunctionResult>> ExecuteAsync(int retryCount, object[] args)
            {
                PartitionContext context = (PartitionContext)args[0];
                IEnumerable<EventData> messages = (IEnumerable<EventData>)args[1];

                var triggerInput = new EventHubTriggerInput
                {
                    Events = messages.ToArray(),
                    PartitionContext = context
                };

                TriggeredFunctionData input = null;
                List<Task<FunctionResult>> invocationTasks = new List<Task<FunctionResult>>();
                if (_singleDispatch)
                {
                    // Single dispatch
                    int eventCount = triggerInput.Events.Length;
                    for (int i = 0; i < eventCount; i++)
                    {
                        if (_cancellationToken.IsCancellationRequested)
                        {
                            break;
                        }

                        EventHubTriggerInput eventHubTriggerInput = triggerInput.GetSingleEventTriggerInput(i);
                        input = new TriggeredFunctionData
                        {
                            TriggerValue = eventHubTriggerInput,
                            TriggerDetails = eventHubTriggerInput.GetTriggerDetails(context, retryCount),
                        };

                        Task<FunctionResult> task = TryExecuteWithLoggingAsync(input, triggerInput.Events[i]);
                        invocationTasks.Add(task);
                    }
                }
                else
                {
                    // Batch dispatch
                    input = new TriggeredFunctionData
                    {
                        TriggerValue = triggerInput,
                        TriggerDetails = triggerInput.GetTriggerDetails(context, retryCount)
                    };

                    using (_logger.BeginScope(GetLinksScope(triggerInput.Events)))
                    {
                        invocationTasks.Add(_executor.TryExecuteAsync(input, _cancellationToken));
                    }
                }

                // Drain the whole batch before taking more work
                if (invocationTasks.Count > 0)
                {
                    await Task.WhenAll(invocationTasks);
                }

                return invocationTasks.Select(x => x.Result);
            }

            private async Task CheckpointAsync(PartitionContext context, IEnumerable<EventData> messages)
            {
                // Dispose all messages to help with memory pressure. If this is missed, the finalizer thread will still get them.
                bool hasEvents = false;
                foreach (var message in messages)
                {
                    hasEvents = true;
                    message.Dispose();
                }

                // Checkpoint if we processed any events.
                // Don't checkpoint if no events. This can reset the sequence counter to 0.
                // Note: we intentionally checkpoint the batch regardless of function 
                // success/failure. EventHub doesn't support any sort "poison event" model,
                // so that is the responsibility of the user's function currently. E.g.
                // the function should have try/catch handling around all event processing
                // code, and capture/log/persist failed events, since they won't be retried.
                if (hasEvents)
                {
                    await CheckpointAsync(context);
                }
            }

            private async Task<FunctionResult> TryExecuteWithLoggingAsync(TriggeredFunctionData input, EventData message)
            {
                using (_logger.BeginScope(GetLinksScope(message)))
                {
                    return await _executor.TryExecuteAsync(input, _cancellationToken);
                }
            }

            private async Task CheckpointAsync(PartitionContext context)
            {
                bool checkpointed = false;
                if (_batchCheckpointFrequency == 1)
                {
                    await _checkpointer.CheckpointAsync(context);
                    checkpointed = true;
                }
                else
                {
                    // only checkpoint every N batches
                    if (++_batchCounter >= _batchCheckpointFrequency)
                    {
                        _batchCounter = 0;
                        await _checkpointer.CheckpointAsync(context);
                        checkpointed = true;
                    }
                }
                if (checkpointed)
                {
                    _logger.LogDebug(GetOperationDetails(context, "CheckpointAsync"));
                }
            }

            async Task ICheckpointer.CheckpointAsync(PartitionContext context)
            {
                await context.CheckpointAsync();
            }

            private Dictionary<string, object> GetLinksScope(EventData message)
            {
                if (TryGetLinkedActivity(message, out var link))
                {
                    return new Dictionary<string, object> {["Links"] = new[] {link}};
                }

                return EmptyScope;
            }

            private Dictionary<string, object> GetLinksScope(EventData[] messages)
            {
                List<Activity> links = null;

                foreach (var message in messages)
                {
                    if (TryGetLinkedActivity(message, out var link))
                    {
                        if (links == null)
                        {
                            links = new List<Activity>(messages.Length);
                        }

                        links.Add(link);
                    }
                }

                if (links != null)
                {
                    return new Dictionary<string, object> {["Links"] = links};
                }

                return EmptyScope;
            }

            private bool TryGetLinkedActivity(EventData message, out Activity link)
            {
                link = null;

                if (((message.SystemProperties != null && message.SystemProperties.TryGetValue("Diagnostic-Id", out var diagnosticIdObj)) || message.Properties.TryGetValue("Diagnostic-Id", out diagnosticIdObj)) 
                    && diagnosticIdObj is string diagnosticIdString)
                {
                    link = new Activity("Microsoft.Azure.EventHubs.Process");
                    link.SetParentId(diagnosticIdString);
                    return true;
                }

                return false;
            }

            private string GetOperationDetails(PartitionContext context, string operation)
            {
                StringWriter sw = new StringWriter();
                using (JsonTextWriter writer = new JsonTextWriter(sw) { Formatting = Formatting.None })
                {
                    writer.WriteStartObject();
                    WritePropertyIfNotNull(writer, "operation", operation);
                    writer.WritePropertyName("partitionContext");
                    writer.WriteStartObject();
                    WritePropertyIfNotNull(writer, "partitionId", context.PartitionId);
                    WritePropertyIfNotNull(writer, "owner", context.Owner);
                    WritePropertyIfNotNull(writer, "eventHubPath", context.EventHubPath);
                    writer.WriteEndObject();

                    // Log partition lease
                    if (context.Lease != null)
                    {
                        writer.WritePropertyName("lease");
                        writer.WriteStartObject();
                        WritePropertyIfNotNull(writer, "offset", context.Lease.Offset);
                        WritePropertyIfNotNull(writer, "sequenceNumber", context.Lease.SequenceNumber.ToString());
                        writer.WriteEndObject();
                    }

                    // Log RuntimeInformation if EnableReceiverRuntimeMetric is enabled
                    if (context.RuntimeInformation != null)
                    {
                        writer.WritePropertyName("runtimeInformation");
                        writer.WriteStartObject();
                        WritePropertyIfNotNull(writer, "lastEnqueuedOffset", context.RuntimeInformation.LastEnqueuedOffset);
                        WritePropertyIfNotNull(writer, "lastSequenceNumber", context.RuntimeInformation.LastSequenceNumber.ToString());
                        WritePropertyIfNotNull(writer, "lastEnqueuedTimeUtc", context.RuntimeInformation.LastEnqueuedTimeUtc.ToString("o"));
                        writer.WriteEndObject();
                    }
                    writer.WriteEndObject();
                }
                return sw.ToString();
            }

            private static void WritePropertyIfNotNull(JsonTextWriter writer, string propertyName, string propertyValue)
            {
                if (propertyValue != null)
                {
                    writer.WritePropertyName(propertyName);
                    writer.WriteValue(propertyValue);
                }
            }
        }
    }
}