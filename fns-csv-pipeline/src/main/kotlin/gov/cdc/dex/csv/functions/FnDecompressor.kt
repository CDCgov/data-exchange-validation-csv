package gov.cdc.dex.csv.functions

import com.google.gson.Gson
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.ExecutionContext

import gov.cdc.dex.csv.services.BlobService
import gov.cdc.dex.csv.dtos.AzureBlobCreateEventMessage
import gov.cdc.dex.csv.utils.*

import java.io.IOException
import java.io.File

/**
 * Azure Functions with event trigger.
 */
class FnDecompressor(blobService:BlobService) {
    private val BLOB_CREATED = "Microsoft.Storage.BlobCreated"
    private val ZIP_TYPE = "application/zip"
    private val blobService = blobService;
   
    fun process(message: String, context: ExecutionContext) {
        context.logger.info("Decompressor function triggered with message $message")
        
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
                    if(!blobService.doesIngestBlobExist(path)){
                        throw IllegalArgumentException("File missing in Azure! $url");
                    }
                    
                    //copy whether unzipping or not, to preserve the zipped file
                    val processPath = blobService.copyIngestBlobToProcess(path,id);

                    if(type == ZIP_TYPE){
                        var localDir = File("temp/id");
                        localDir.mkdirs();
                        try{
                            //in the utils package
                            decompressFileStream(blobService.getProcessBlobInputStream(processPath),localDir);
                            //TODO if empty, create error event
                        }catch(e:IOException){
                            createFailEventAndMoveFile(event, processPath, e);
                        }
                        //TODO for each file, 
                          //TODO upload to storage
                          //TODO create OK event with that file (preserving metadata)
                    }else{
                        createOkEvent(event, processPath)
                    }
                }
            }
        }
    }

    private fun getPathFromUrl(url:String):String{
        //assume url is formatted "http://domain/container/path/morePath/morePath"
        val urlArray = url.split("/");
        if(urlArray.size < 5){
            throw IllegalArgumentException("Azure message had bad URL for the file! $url");
        }
        return urlArray.subList(4, urlArray.size).joinToString(separator="/") 
    }

    private fun createOkEvent(parentEvent:AzureBlobCreateEventMessage, processPath:String){

    }

    private fun createFailEventAndMoveFile(parentEvent:AzureBlobCreateEventMessage, processPath:String, exception: Exception){
        //TODO also move the file from process to error
    }
}
