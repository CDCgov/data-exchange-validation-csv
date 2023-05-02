package gov.cdc.dex.csv.functions

import com.google.gson.Gson
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.ExecutionContext

import java.util.zip.ZipInputStream

import gov.cdc.dex.csv.services.BlobService
import gov.cdc.dex.csv.dtos.AzureBlobCreateEventMessage

/**
 * Azure Functions with event trigger.
 */
class FnDecompressor(blobService:BlobService) {
    private val BLOB_CREATED = "Microsoft.Storage.BlobCreated"
    private val blobService = blobService;
   
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

                    val id = event.id;
                    val type = event.evHubData?.contentType
                    val url = event.evHubData?.url;

                    if(id==null){
                        throw IllegalArgumentException("Azure message missing id!");
                    }
                    if(type==null){
                        throw IllegalArgumentException("Azure message missing blob content-type!");
                    }
                    if(url==null){
                        throw IllegalArgumentException("Azure message missing blob URL!");
                    }

                    val path:String = getPathFromUrl(url);
                    blobService.copyBlobToWorking(path,id);

                    //TODO copy file into working

                    if(isTypeCompressed(type)){
                        //TODO if compressed, decompress
                        //TODO if empty, create error event
                        //TODO for each file, create OK event with that file (preserving metadata)

                    }else{
                        
                        //TODO if not compressed, create OK event with that file (preserving metadata)
                    }
                    // val blobClient = blobService.getBlobClient(path)

                }
            }
        }
        //TODO
    }

    private fun getPathFromUrl(url:String):String{
        //TODO
        return "";
    }

    private fun isTypeCompressed(type:String):Boolean{
        //TODO
        return false;
    }
}
