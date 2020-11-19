# Event Hubs Extension for Azure Functions guide to running integration tests locally
Integration tests are implemented in the `EventHubsEndToEndTests` and `EventHubApplicationInsightsTest` classes and require special configuration to execute locally in Visual Studio or via dotnet test.  

All configuration is done via a json file called `appsettings.tests` which on windows should be located in the `%USERPROFILE%\.azurefunctions` folder (e.g. `C:\Users\user123\.azurefunctions`)

**Note:** *The specifics of the configuration will change when the validation code is modified so check the code for the latest configuration if the tests do not pass as this readme file may not have been updated with each code change.*

Create the appropriate Azure resources if needed as explained below and create or update the `appsettings.tests` file in the location specified above by copying the configuration below and replacing all the `PLACEHOLDER` values

appsettings.tests contents
```
{
    "ConnectionStrings": {
        "AzureWebJobsTestHubConnection": "PLACEHOLDER"
    },
    "AzureWebJobsStorage": "PLACEHOLDER"
}
```
## Create Azure resources and configure test environment
1. Create a storage account and configure its connection string into `AzureWebJobsStorage`.  This will be used by the webjobs hosts created by the tests as well as the Event Hubs extension for checkpointing.
2. Create anEvent Hubs namespace namespaces and within it, create an Event Hubs resource named `webjobstesthub`.
3. Navigate to the Event Hubs resource you created called `webjobstesthub` and create a "Shared access policy" with any name and "Manage, Send, Listen" claims.
4. Navigate to the policy above and copy either the primary or secondary connection string and set the value of the `AzureWebJobsTestHubConnection` element of the appsettings.tests file to the connection string you copied.
5. If you will keep this setup around for more than a day, set the "Message Retention" on the Event Hubs resource to the minimum value of 1 day to make tests run faster.