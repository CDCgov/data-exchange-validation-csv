package gov.cdc.dex.csv.debatcher

import com.microsoft.azure.functions.ExecutionContext
import com.microsoft.azure.functions.HttpMethod
import com.microsoft.azure.functions.HttpRequestMessage
import com.microsoft.azure.functions.HttpResponseMessage
import com.microsoft.azure.functions.HttpStatus
import com.microsoft.azure.functions.annotation.AuthorizationLevel
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.HttpTrigger

import java.util.Optional

/**
 * Azure Functions with HTTP Trigger.
 */
class Function {
   
    @FunctionName("DexCsvDebatcher")
    fun eventHubProcessor(
			@EventHubTrigger(
                name = "msg", 
                eventHubName = "dex-csv-event-hub",
                connection = "EventHubConnectionString") 
        message: String,
			@BindingName("SystemPropertiesArray")
		eventHubMD: List<EventHubMetadata>,
		context: ExecutionContext) {
		
        context.logger.info("Debatcher function triggered with message "+message)

    }
}
