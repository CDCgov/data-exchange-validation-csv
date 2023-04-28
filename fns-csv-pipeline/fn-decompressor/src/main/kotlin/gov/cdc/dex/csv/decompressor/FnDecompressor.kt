package gov.cdc.dex.csv.decompressor

import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.ExecutionContext


/**
 * Azure Functions with event trigger.
 */
class FnDecompressor {
   
    @FunctionName("DexCsvDecompressor")
    fun eventHubProcessor(
			@EventHubTrigger(
                name = "msg", 
                eventHubName = "%EventHubName_Ingest%",
                connection = "EventHubConnectionListen_Ingest") 
        message: String,
		context: ExecutionContext) {
		
        context.logger.info("Decompressor function triggered with message "+message)

    }
}
