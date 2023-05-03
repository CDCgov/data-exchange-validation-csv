package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.ExecutionContext
import gov.cdc.dex.csv.services.AzureBlobServiceImpl
import gov.cdc.dex.csv.services.AzureEventServiceImpl
import gov.cdc.dex.csv.dtos.ConnectionNames
import gov.cdc.dex.csv.dtos.EventHubNames
import gov.cdc.dex.csv.dtos.BlobStorageNames

/**
 * Azure Functions with event trigger.
 */
class FnDecompressorEntry {
    private val blobConnection = System.getenv("BlobConnection")
    private val ingestBlobContainer = System.getenv("BlobContainer_Ingest")
    private val processedBlobContainer = System.getenv("BlobContainer_Processed")
    private val errorBlobContainer = System.getenv("BlobContainer_Error")
    
    private val eventNamespaceConnection = System.getenv("EventHubConnection")
    private val ingestEventHub = System.getenv("EventHubName_Ingest")
    private val decompressOkEventHub = System.getenv("EventHubName_DecompressOk")
    private val decompressFailEventHub = System.getenv("EventHubName_DecompressFail")

    private val blobService = AzureBlobServiceImpl(blobConnection);
    private val eventService = AzureEventServiceImpl(eventNamespaceConnection)

    private val blobNames = BlobStorageNames(ingestBlobContainer, processedBlobContainer, errorBlobContainer)
    private val eventHubNames = EventHubNames(ingestEventHub, decompressOkEventHub, decompressFailEventHub);
    private val connectionNames = ConnectionNames(eventHubNames, blobNames);

    private val functionMethod = FnDecompressor(blobService,eventService,connectionNames);


    @FunctionName("DexCsvDecompressor")
    fun eventHubProcessor(
			@EventHubTrigger(
                name = "msg", 
                eventHubName = "%EventHubName_Ingest%",
                connection = "EventHubConnection") 
        message: String,
        context: ExecutionContext
    ) {

        functionMethod.process(message, context);
    }
}
