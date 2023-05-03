package gov.cdc.dex.csv.functions

import com.google.gson.Gson
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.ExecutionContext

import gov.cdc.dex.csv.services.BlobService
import gov.cdc.dex.csv.services.EventService
import gov.cdc.dex.csv.dtos.AzureBlobCreateEventMessage
import gov.cdc.dex.csv.dtos.ConnectionNames

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
class FnDecompressor(blobService:BlobService, eventService:EventService, connectionNames:ConnectionNames) {
    private val blobService = blobService;
    private val eventService = eventService;
    private val connectionNames = connectionNames;

    private val BLOB_CREATED = "Microsoft.Storage.BlobCreated"
    private val ZIP_TYPE = "application/zip"
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

                    val (ingestFolder,ingestFileName) = getPathFromUrl(url);
                    val ingestFilePath = ingestFolder+"/"+ingestFileName
                    if(!blobService.doesBlobExist(connectionNames.blobStorage.ingest, ingestFilePath)){
                        throw IllegalArgumentException("File missing in Azure! $url");
                    }
                    
                    //copy whether unzipping or not, to preserve the zipped file
                    val processFolder = ingestFolder+"/"+id
                    val processFilePath = processFolder+"/"+ingestFileName
                    blobService.moveBlob(connectionNames.blobStorage.ingest, ingestFilePath, connectionNames.blobStorage.processed, processFilePath)

                    if(type == ZIP_TYPE){
                        var writtenPaths:List<String> = try{
                            var downloadStream = blobService.getBlobDownloadStream(connectionNames.blobStorage.processed, processFilePath)
                            decompressFileStream(downloadStream, processFilePath);
                        }catch(e:IOException){
                            context.logger.log(Level.SEVERE, "Error unzipping: $processFilePath", e);
                            createFailEventAndMoveFile(event, processFilePath, "Error unzipping: $processFilePath : $e.localizedMessage");
                            continue;
                        }

                        if(writtenPaths.isEmpty()){
                            createFailEventAndMoveFile(event, processFilePath, "Zipped file was empty: $processFilePath");
                        }else{
                            for(writtenPath in writtenPaths){
                                createOkEvent(event,writtenPath)
                            }
                        }
                    }else{
                        createOkEvent(event, processFilePath)
                    }
                }
            }
        }
    }

    private fun createOkEvent(parentEvent:AzureBlobCreateEventMessage, processFilePath:String){

        //eventService.sendOne(connectionNames.eventHubs.decompressOk, jsonMessage)
    }

    private fun createFailEventAndMoveFile(parentEvent:AzureBlobCreateEventMessage, processFilePath:String, errorMessage: String){
        blobService.moveBlob(connectionNames.blobStorage.processed, processFilePath, connectionNames.blobStorage.error, processFilePath)

println("\n\n $errorMessage \n\n")
        //eventService.sendOne(connectionNames.eventHubs.decompressFail, jsonMessage)
    }

    private fun getPathFromUrl(url:String):Pair<String,String>{
        //assume url is formatted "http://domain/container/path/morePath/morePath"
        val urlArray = url.split("/");
        if(urlArray.size < 5){
            throw IllegalArgumentException("Azure message had bad URL for the file! $url");
        }
        var path = urlArray.subList(4, urlArray.size-1).joinToString(separator="/") 
        var file = urlArray[urlArray.size-1];
        return Pair(path,file);
    }

    private fun decompressFileStream(stream:InputStream, processPath:String):List<String>{
        val outputPath = processPath.replace(".zip","/");
        var writtenPaths : MutableList<String> = mutableListOf();
        ZipInputStream(stream).use{ zis ->
            var zipEntry = zis.nextEntry;
            
            while (zipEntry != null) {
                if (!zipEntry.isDirectory()) {
                    // write file content
                    var pathToWrite = outputPath+zipEntry.name
                    var uploadStream = blobService.getBlobUploadStream(connectionNames.blobStorage.processed, pathToWrite)
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
