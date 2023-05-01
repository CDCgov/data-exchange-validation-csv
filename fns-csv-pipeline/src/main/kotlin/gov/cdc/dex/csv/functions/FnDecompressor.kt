package gov.cdc.dex.csv.functions

import com.google.gson.Gson
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.ExecutionContext

import gov.cdc.dex.csv.services.BlobService
import gov.cdc.dex.csv.dtos.AzureBlobCreateEventMessage

/**
 * Azure Functions with event trigger.
 */
class FnDecompressor(blobService:BlobService) {
    private val BLOB_CREATED = "Microsoft.Storage.BlobCreated"
   
    fun process(message: String, context: ExecutionContext) {
        context.logger.info("Decompressor function triggered with message "+message)
        
        if(message.isEmpty()){
            throw IllegalArgumentException("Empty message from Azure!");
        }
		
        val eventArrArr = Gson().fromJson(message, Array<Array<AzureBlobCreateEventMessage>>::class.java)

        for(eventArr in eventArrArr){
            for(event in eventArr){
                if ( event.eventType == BLOB_CREATED) {
                    context.logger.info("Received BLOB_CREATED event: --> $event")
                    //TODO read from blob
                    //TODO if not compressed, create OK event with that file (preserving metadata)
                    //TODO if compressed, decompress
                    //TODO if empty, create error event
                    //TODO for each file, create OK event with that file (preserving metadata)
                }
            }
        }
        //TODO
    }
}
