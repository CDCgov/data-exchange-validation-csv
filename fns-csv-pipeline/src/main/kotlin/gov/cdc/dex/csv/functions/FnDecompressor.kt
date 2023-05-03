package gov.cdc.dex.csv.functions

import com.google.gson.Gson
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.ExecutionContext

import gov.cdc.dex.csv.services.BlobService
import gov.cdc.dex.csv.dtos.AzureBlobCreateEventMessage

import java.io.IOException
import java.io.File
import java.io.InputStream
import java.io.BufferedOutputStream
import java.io.OutputStream
import java.util.logging.Level
import java.util.zip.ZipInputStream

/**
 * Azure Functions with event trigger.
 */
class FnDecompressor(blobService:BlobService) {
    private val BLOB_CREATED = "Microsoft.Storage.BlobCreated"
    private val ZIP_TYPE = "application/zip"
    private val blobService = blobService;
    private val BUFFER_SIZE = 4096
   
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
                        var writtenPaths:List<String> = try{
                            var downloadStream = blobService.getProcessBlobInputStream(processPath);
                            decompressFileStream(downloadStream, processPath);
                        }catch(e:IOException){
                            context.logger.log(Level.SEVERE, "Error unzipping: $processPath", e);
                            createFailEventAndMoveFile(event, processPath, "Error unzipping: $processPath : $e.localizedMessage");
                            continue;
                        }

                        if(writtenPaths.isEmpty()){
                            createFailEventAndMoveFile(event, processPath, "Zipped file was empty: $processPath");
                        }else{
                            for(writtenPath in writtenPaths){
                                createOkEvent(event,writtenPath)
                            }
                        }
                    }else{
                        createOkEvent(event, processPath)
                    }
                }
            }
        }
    }

    private fun createOkEvent(parentEvent:AzureBlobCreateEventMessage, processPath:String){

    }

    private fun createFailEventAndMoveFile(parentEvent:AzureBlobCreateEventMessage, processPath:String, errorMessage: String){
        //TODO also move the file from process to error
println("\n\n $errorMessage \n\n")
    }

    private fun getPathFromUrl(url:String):String{
        //assume url is formatted "http://domain/container/path/morePath/morePath"
        val urlArray = url.split("/");
        if(urlArray.size < 5){
            throw IllegalArgumentException("Azure message had bad URL for the file! $url");
        }
        return urlArray.subList(4, urlArray.size).joinToString(separator="/") 
    }

    private fun decompressFileStream(stream:InputStream, processPath:String):List<String>{
        val outputPath = processPath.replace(".zip", "/");
        var writtenPaths : MutableList<String> = mutableListOf();
        ZipInputStream(stream).use{ zis ->
            var zipEntry = zis.nextEntry;
            
            while (zipEntry != null) {
                if (!zipEntry.isDirectory()) {
                    // write file content
                    var pathToWrite = outputPath+zipEntry.name
                    var uploadStream = blobService.getProcessBlobOutputStream(pathToWrite)
                    BufferedOutputStream(uploadStream).use{bos ->
                        val bytesIn = ByteArray(BUFFER_SIZE)
                        var read: Int
                        while (zis.read(bytesIn).also { read = it } != -1) {
                            bos.write(bytesIn, 0, read)
                        }
                    }
                    writtenPaths.add(pathToWrite)
                }
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
        }
        return writtenPaths
    }
}
