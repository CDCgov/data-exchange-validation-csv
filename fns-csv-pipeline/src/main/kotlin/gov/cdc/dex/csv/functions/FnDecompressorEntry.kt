package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.ExecutionContext
import gov.cdc.dex.csv.services.AzureBlobServiceImpl

/**
 * Azure Functions with event trigger.
 */
class FnDecompressorEntry {
    //TODO look into dependency injection frameworks
    private val ingestBlobConnection = System.getenv("BlobConnection")
    private val blobService = AzureBlobServiceImpl(ingestBlobConnection);
    private val functionMethod = FnDecompressor(blobService);


    @FunctionName("DexCsvDecompressor")
    fun eventHubProcessor(
			@EventHubTrigger(
                name = "msg", 
                eventHubName = "%EventHubName_Ingest%",
                connection = "EventHubConnectionListen_Ingest") 
        message: String,
        context: ExecutionContext
    ) {
        functionMethod.process(message, context);
    }
}
