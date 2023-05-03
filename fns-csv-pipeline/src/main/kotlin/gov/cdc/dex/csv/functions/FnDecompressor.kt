package gov.cdc.dex.csv.functions

import com.google.gson.Gson
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.ExecutionContext

import gov.cdc.dex.csv.services.BlobService
import gov.cdc.dex.csv.services.EventService
import gov.cdc.dex.csv.dtos.AzureBlobCreateEventMessage
import gov.cdc.dex.csv.dtos.ConnectionNames
import gov.cdc.dex.csv.dtos.DecompressOkEventMessage
import gov.cdc.dex.csv.dtos.DecompressFailEventMessage

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
    private val ZIP_TYPES = listOf("application/zip","application/x-zip-compressed")
    private val BUFFER_SIZE = 4096
   
    fun process(message: String, context: ExecutionContext) {
        context.logger.info("Decompressor function triggered with message $message")
        
        if(message.isEmpty()){
            throw IllegalArgumentException("Empty message from Azure!");
        }
		
        val eventArr = Gson().fromJson(message, Array<AzureBlobCreateEventMessage>::class.java)

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
                val processUrl = blobService.moveBlob(connectionNames.blobStorage.ingest, ingestFilePath, connectionNames.blobStorage.processed, processFilePath)

                if(ZIP_TYPES.contains(type)){
                    var writtenPaths:List<String> = try{
                        var downloadStream = blobService.getBlobDownloadStream(connectionNames.blobStorage.processed, processFilePath)
                        decompressFileStream(downloadStream, processFolder);
                    }catch(e:IOException){
                        context.logger.log(Level.SEVERE, "Error unzipping: $processFilePath", e);
                        createFailEventAndMoveFile(event, processFilePath, "Error unzipping: $processFilePath : $e.localizedMessage", context);
                        continue;
                    }

                    if(writtenPaths.isEmpty()){
                        createFailEventAndMoveFile(event, processFilePath, "Zipped file was empty: $processFilePath", context);
                    }else{
                        createOkEvents(event,writtenPaths, context)
                    }
                }else{
                    createOkEvents(event, listOf(processUrl), context)
                }
            }
        }
    }

    private fun createOkEvents(parentEvent:AzureBlobCreateEventMessage, processFilePaths:List<String>, context: ExecutionContext){
        context.logger.info("Decompress OK : "+processFilePaths);

        val gson=Gson()
        var messages = processFilePaths.map{path -> DecompressOkEventMessage(parentEvent, path)}.map{pojo -> gson.toJson(pojo)}

        eventService.sendBatch(connectionNames.eventHubs.decompressOk, messages)
    }

    private fun createFailEventAndMoveFile(parentEvent:AzureBlobCreateEventMessage, processFilePath:String, errorMessage: String, context: ExecutionContext){
        context.logger.warning("Decompress Fail : $processFilePath : $errorMessage");

        blobService.moveBlob(connectionNames.blobStorage.processed, processFilePath, connectionNames.blobStorage.error, processFilePath)

        var pojo = DecompressFailEventMessage(parentEvent, processFilePath,errorMessage)
        var message=Gson().toJson(pojo)
        eventService.sendOne(connectionNames.eventHubs.decompressFail, message)
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
        val outputPath = processPath+"/";
        var writtenPaths : MutableList<String> = mutableListOf();
        //use is equivalent of try-with-resources, will close the stream at the end
        stream.use{ str ->
            decompressFileStreamRecursive(str, outputPath,writtenPaths)
        }
        return writtenPaths
    }
    

    private fun decompressFileStreamRecursive(stream:InputStream, outputPath:String,writtenPaths : MutableList<String>){
        //don't close stream here because of recursion
        var zis = ZipInputStream(stream)
        var zipEntry = zis.nextEntry;
        
        while (zipEntry != null) {
            if (!zipEntry.isDirectory()) {
                if(zipEntry.name.endsWith(".zip")){
                    //if a nested zip, recurse
                    var localPath = zipEntry.name.replace(".zip","/")
                    decompressFileStreamRecursive(zis,outputPath+localPath,writtenPaths)
                }else{
                    // write file content
                    var pathToWrite = outputPath+zipEntry.name
                    var (uploadStream,blobUrl) = blobService.getBlobUploadStream(connectionNames.blobStorage.processed, pathToWrite)
                    BufferedOutputStream(uploadStream).use{bos ->
                        val bytesIn = ByteArray(BUFFER_SIZE)
                        var read: Int
                        while (zis.read(bytesIn).also { read = it } != -1) {
                            bos.write(bytesIn, 0, read)
                        }
                    }
                    writtenPaths.add(blobUrl)
                }
            }
            zipEntry = zis.getNextEntry();
        }
    }
}
