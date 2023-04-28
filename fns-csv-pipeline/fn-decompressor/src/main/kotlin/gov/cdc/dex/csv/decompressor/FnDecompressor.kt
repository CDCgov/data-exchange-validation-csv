package gov.cdc.dex.csv.decompressor

import com.google.gson.Gson
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.ExecutionContext

/**
 * Azure Functions with event trigger.
 */
class FnDecompressor {
    companion object {
        const val BLOB_CREATED = "Microsoft.Storage.BlobCreated"
    }
   
    @FunctionName("DexCsvDecompressor")
    fun eventHubProcessor(
			@EventHubTrigger(
                name = "msg", 
                eventHubName = "%EventHubName_Ingest%",
                connection = "EventHubConnectionListen_Ingest") 
        message: String,
        context: ExecutionContext
    ) {
        context.logger.info("Decompressor function triggered with message "+message)
        
        if(message.isEmpty()){
            throw IllegalArgumentException("Empty message from Azure!");
        }
		
        val eventArrArr = Gson().fromJson(message, Array<Array<AzBlobCreateEventMessage>>::class.java)

        for(eventArr in eventArrArr){
            for(event in eventArr){
                if ( event.eventType == BLOB_CREATED) {
                    context.logger.info("Received BLOB_CREATED event: --> $event")
                    //TODO
                }
            }
        }
        //TODO
    }
}
