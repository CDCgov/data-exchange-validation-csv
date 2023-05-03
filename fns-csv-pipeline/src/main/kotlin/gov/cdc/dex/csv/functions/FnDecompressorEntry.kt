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
    private val blobConnection = System.getenv("BlobConnection")
    private val ingestBlobContainer = System.getenv("BlobContainer_Ingest")
    private val processedBlobContainer = System.getenv("BlobContainer_Processed")
    private val errorBlobContainer = System.getenv("BlobContainer_Error")

    private val blobService = AzureBlobServiceImpl(blobConnection, ingestBlobContainer, processedBlobContainer, errorBlobContainer);
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
