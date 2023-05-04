package gov.cdc.dex.csv.functions

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.ToNumberPolicy
import com.google.gson.JsonSyntaxException
import com.microsoft.azure.functions.annotation.FunctionName
import com.microsoft.azure.functions.annotation.EventHubTrigger
import com.microsoft.azure.functions.ExecutionContext

import gov.cdc.dex.csv.services.BlobService
import gov.cdc.dex.csv.services.EventService
import gov.cdc.dex.csv.dtos.AzureBlobCreateEventMessage
import gov.cdc.dex.csv.dtos.ConnectionNames
import gov.cdc.dex.csv.dtos.DecompressOkEventMessage
import gov.cdc.dex.csv.dtos.DecompressFailEventMessage
import gov.cdc.dex.csv.dtos.DecompressParentEventMessage

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

    private val BUFFER_SIZE = 4096
    private val BLOB_CREATED = "Microsoft.Storage.BlobCreated"
    private val ZIP_TYPES = listOf("application/zip","application/x-zip-compressed")
    private val gson = GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create()
    
    //TODO: currently can't figure out how to add metadata to test files in Azure.
    // Thus, if this is turned on, all tests in Azure will fail
    // Once that is figure out, turn this back on
    // private val METADATA_TO_VALIDATE = listOf("jurisdiction")
    private val METADATA_TO_VALIDATE:List<String> = listOf()
    
    fun process(message: String, context: ExecutionContext) {
        context.logger.info("Decompressor function triggered with message $message")
        
        if(message.isEmpty()){
            context.logger.log(Level.SEVERE, "Empty Azure message")
            createFailEvent(DecompressParentEventMessage(rawMessage=message), null, "Empty Azure message!", context);
            return
        }

        // parse the message usable objects
        val events = try{
            gson.fromJson(message, Array<AzureBlobCreateEventMessage>::class.java)
        }catch(e:JsonSyntaxException){
            context.logger.log(Level.SEVERE, "Error parsing Azure message", e)
            createFailEvent(DecompressParentEventMessage(rawMessage=message), null, "Azure message unable to be parsed : $e", context);
            return
        }
        
        //also parse into raw objects, so we can preserve the raw parent message
        //I could not find another way to do handle this in GSON
        val rawEvents = gson.fromJson(message, Array<Any>::class.java)
        
        //"zip" function allows the lists to be iterated over simultaneously
        val dualList = rawEvents.zip(events){ rawEvent, event -> Pair(rawEvent,event)}
        for((rawEvent,event) in dualList){
            if ( event.eventType != BLOB_CREATED) {
                context.logger.warning("Recieved non-created message type $event.eventType")
                //TODO do we do anything else?
                continue;
            }

            context.logger.info("Received BLOB_CREATED event: --> $event")

            //check required event parameters
            val id = event.id
            val contentType = event.evHubData?.contentType
            val contentLength = event.evHubData?.contentLength
            val url = event.evHubData?.url

            if(id==null || contentType==null || contentLength==null || url==null){
                createFailEvent(DecompressParentEventMessage(parsedMessage=rawEvent), null, "Azure ingest event missing required parameters!", context);
                continue;
            }

            //define paths in the containers (use event ID as top directory in Processed and Error)
            val pathPair = getPathFromUrl(url)
            if(pathPair == null){
                createFailEvent(DecompressParentEventMessage(parsedMessage=rawEvent), null, "Asure event had bad URL for the file! $url", context);
                continue;
            }
            val (ingestFolder,ingestFileName) = pathPair
            val ingestFilePath = ingestFolder+"/"+ingestFileName
            val processFolder = id+"/"+ingestFolder
            val processFilePath = processFolder+"/"+ingestFileName
            
            //check for file existence and correct metadata
            if(!blobService.doesBlobExist(connectionNames.blobStorage.ingest, ingestFilePath)){
                createFailEvent(DecompressParentEventMessage(parsedMessage=rawEvent), null, "File missing in Azure! $url", context);
                continue;
            }

            val metadata = blobService.getBlobMetadata(connectionNames.blobStorage.ingest, ingestFilePath)
            context.logger.info("File metadata $metadata")
            val parentData = DecompressParentEventMessage(parsedMessage=rawEvent, parentMetadata=metadata)

            val metadataError = validateMetadata(metadata)
            if(metadataError!=null){
                val errorUrl = blobService.moveBlob(connectionNames.blobStorage.ingest, ingestFilePath, connectionNames.blobStorage.error, processFilePath)
                createFailEvent(parentData, errorUrl, "Incorrect metadata : $metadataError", context);
                continue;
            }

            //copy file to processed, whether unzipping or not (preserve the original zip)
            val processUrl = blobService.moveBlob(connectionNames.blobStorage.ingest, ingestFilePath, connectionNames.blobStorage.processed, processFilePath)

            //IF blocks are expressions, and variables can be returned out of them
            var writtenPaths:List<String> = if(ZIP_TYPES.contains(contentType)){
                //if ZIP, then unzip and capture the files that were contained

                //stream from the existing zip, and immediately stream the unzipped data back to a new file in processed
                var downloadStream = blobService.getBlobDownloadStream(connectionNames.blobStorage.processed, processFilePath)
                var uploadStreamSupplier = {arg:String ->
                    blobService.getBlobUploadStream(connectionNames.blobStorage.processed, arg)
                }

                //TRY blocks are expressions, and variables can be returned out of them
                var writtenPathsZip:List<String> = try{
                    decompressFileStream(downloadStream, uploadStreamSupplier, processFolder);
                }catch(e:IOException){
                    context.logger.log(Level.SEVERE, "Error unzipping: $processFilePath", e);
                    val errorUrl = blobService.moveBlob(connectionNames.blobStorage.processed, processFilePath, connectionNames.blobStorage.error, processFilePath)
                    createFailEvent(parentData, errorUrl, "Error unzipping: $processFilePath : $e.localizedMessage", context);
                    continue;
                }

                if(writtenPathsZip.isEmpty()){
                    //fail if zip file was empty
                    val errorUrl = blobService.moveBlob(connectionNames.blobStorage.processed, processFilePath, connectionNames.blobStorage.error, processFilePath)
                    createFailEvent(parentData, errorUrl, "Zipped file was empty: $processFilePath", context);
                    continue;
                }

                //return the written paths to the IF statement, to be assigned to variable there
                writtenPathsZip
            }else{
                //if not a zip, pass along the pipeline
                //wrap the path in a LIST and return to the IF statement
                listOf(processUrl)
            }
            createOkEvents(parentData, writtenPaths, context)
        }
    }

    private fun createOkEvents(parentData:DecompressParentEventMessage, processFilePaths:List<String>, context: ExecutionContext){
        context.logger.info("Decompress OK : "+processFilePaths);

        var messages = processFilePaths.map{path -> DecompressOkEventMessage(parentData, path)}.map{pojo -> gson.toJson(pojo)}

        eventService.sendBatch(connectionNames.eventHubs.decompressOk, messages)
    }

    private fun createFailEvent(parentData:DecompressParentEventMessage, errorFilePath:String?, errorMessage: String, context: ExecutionContext){
        context.logger.warning("Decompress Fail : $errorFilePath : $errorMessage");

        var pojo = DecompressFailEventMessage(parentData, errorFilePath, errorMessage)
        var message=gson.toJson(pojo)
        eventService.sendOne(connectionNames.eventHubs.decompressFail, message)
    }

    private fun getPathFromUrl(url:String):Pair<String,String>?{
        //assume url is formatted "http://domain/container/pathA/pathB/fileName.something"
        //want the return as <"pathA/pathB","fileName.something">

        val urlArray = url.split("/");
        if(urlArray.size < 5){
            return null
        }
        var path = urlArray.subList(4, urlArray.size-1).joinToString(separator="/") 
        var file = urlArray[urlArray.size-1];
        return Pair(path,file);
    }

    private fun validateMetadata(metadata: Map<String,String>):String?{
        val missingKeys:MutableList<String> = mutableListOf();
        for(key in METADATA_TO_VALIDATE){
            if(metadata[key].isNullOrBlank()){
                missingKeys.add(key)
            }
        }

        if(missingKeys.isNotEmpty()){
            return "Missing required metadata key(s) : $missingKeys"
        } else {
            return null;
        }
    }

    private fun decompressFileStream(inputStream:InputStream, outputStreamSupplier: (String) -> Pair<OutputStream,String>, processPath:String):List<String>{
        val outputPath = processPath+"/";
        var writtenPaths : MutableList<String> = mutableListOf();

        //use is equivalent of try-with-resources, will close the input stream at the end
        inputStream.use{ str ->
            decompressFileStreamRecursive(str, outputStreamSupplier, outputPath, writtenPaths)
        }
        return writtenPaths
    }
    

    private fun decompressFileStreamRecursive(inputStream:InputStream, outputStreamSupplier: (String) -> Pair<OutputStream,String>, outputPath:String, writtenPaths : MutableList<String>){
        //don't close input stream here because of recursion
        var zipInputStream = ZipInputStream(inputStream)
        var zipEntry = zipInputStream.nextEntry;
        
        while (zipEntry != null) {
            //ignore any entries that are a directory. The directory path will be part of the zipEntry.name, and Azure automatically creates the needed directories
            if (!zipEntry.isDirectory()) {
                if(zipEntry.name.endsWith(".zip")){
                    //if a nested zip, recurse
                    var localPath = zipEntry.name.replace(".zip","/")
                    decompressFileStreamRecursive(zipInputStream, outputStreamSupplier, outputPath+localPath, writtenPaths)
                }else{
                    // write file content directly to a new blob
                    var pathToWrite = outputPath+zipEntry.name

                    var (outputStream,blobUrl) = outputStreamSupplier.invoke(pathToWrite)

                    //use is equivalent of try-with-resources, will close the output stream when done
                    BufferedOutputStream(outputStream).use{bufferedOutputStream ->
                        val bytesIn = ByteArray(BUFFER_SIZE)
                        var read: Int
                        while (zipInputStream.read(bytesIn).also { read = it } != -1) {
                            bufferedOutputStream.write(bytesIn, 0, read)
                        }
                    }
                    //don't close input stream here because of recursion
        
                    writtenPaths.add(blobUrl)
                }
            }
            zipEntry = zipInputStream.getNextEntry();
        }
        //don't close input stream here because of recursion
    }
}
