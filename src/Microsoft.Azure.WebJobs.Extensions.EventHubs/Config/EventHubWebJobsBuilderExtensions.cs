// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.EventHubs;
using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.Extensions.Hosting
{
    public static class EventHubWebJobsBuilderExtensions
    {
        public static IWebJobsBuilder AddEventHubs(this IWebJobsBuilder builder)
        {
            if (builder == null)
            {
                throw new ArgumentNullException(nameof(builder));
            }

            builder.AddEventHubs(ConfigureOptions);

            return builder;
        }

        public static IWebJobsBuilder AddEventHubs(this IWebJobsBuilder builder, Action<EventHubOptions> configure)
        {
            if (builder == null)
            {
                throw new ArgumentNullException(nameof(builder));
            }

            if (configure == null)
            {
                throw new ArgumentNullException(nameof(configure));
            }

            builder.AddExtension<EventHubExtensionConfigProvider>()
                .BindOptions<EventHubOptions>();

            builder.Services.Configure<EventHubOptions>(options =>
            {
                configure(options);
            });

            return builder;
        }

        internal static void ConfigureOptions(EventHubOptions options)
        {
            string offsetType = options?.InitialOffsetOptions?.Type?.ToLower() ?? String.Empty;
            if (!offsetType.Equals(String.Empty))
            {
                switch (offsetType)
                {
                    case "fromstart":
                        options.EventProcessorOptions.InitialOffsetProvider = (s) => { return EventPosition.FromStart(); };
                        break;
                    case "fromend":
                        options.EventProcessorOptions.InitialOffsetProvider = (s) => { return EventPosition.FromEnd(); };
                        break;
                    case "fromenqueuedtime":
                        try
                        {
                            DateTime enqueuedTimeUTC = DateTime.Parse(options.InitialOffsetOptions.EnqueuedTimeUTC).ToUniversalTime();
                            options.EventProcessorOptions.InitialOffsetProvider = (s) => { return EventPosition.FromEnqueuedTime(enqueuedTimeUTC); };
                        }
                        catch (System.FormatException fe)
                        {
                            string message = $"{nameof(EventHubOptions)}:{nameof(InitialOffsetOptions)}:{nameof(InitialOffsetOptions.EnqueuedTimeUTC)} is configured with an invalid format. " +
                                "Please use a format supported by DateTime.Parse().  e.g. 'yyyy-MM-ddTHH:mm:ssZ'";
                            throw new InvalidOperationException(message, fe);
                        }
                        break;
                    default:
                        throw new InvalidOperationException("An unsupported value was supplied for initialOffsetOptions.type");
                }
                // If not specified, EventProcessor's default offset will apply
            }
        }
    }
}
