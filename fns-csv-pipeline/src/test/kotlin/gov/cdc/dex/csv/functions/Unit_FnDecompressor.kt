package gov.cdc.dex.csv.functions

import com.microsoft.azure.functions.ExecutionContext
import com.google.gson.Gson

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.logging.Logger;

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock;

import gov.cdc.dex.csv.services.BlobService
import gov.cdc.dex.csv.services.EventService
import gov.cdc.dex.csv.dtos.ConnectionNames
import gov.cdc.dex.csv.dtos.EventHubNames
import gov.cdc.dex.csv.dtos.BlobStorageNames
import gov.cdc.dex.csv.dtos.DecompressorContext

internal class Unit_FnDecompressor {
    // in the companion object so it gets initialized once
    companion object{
        private val outputParentDir = File("target/test-output/"+LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSS")))

        private val ingestDir = File(outputParentDir,"ingest");
        private val processDir = File(outputParentDir,"process");
        private val errorDir = File(outputParentDir,"error");

        private val blobNames = BlobStorageNames(ingestDir.name, processDir.name, errorDir.name)
        private val eventHubNames = EventHubNames("ingest", "ok", "fail");
        private val connectionNames = ConnectionNames(eventHubNames, blobNames);
    }

    //not in the companion object so it gets reinitialized every test
    private val eventMap = mutableMapOf<String,MutableList<String>>()

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //happy path
    

    @Test
    internal fun happyPath_singleCsv(){
        println("\n\n--START happyPath_singleCsv")
        copyToIngest("test-upload.csv")
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-upload.csv", contentType="text/csv",id="happy_single")
        var message = buildTestMessage(map);
        
        getDecompressor().process(message,mockContext());

        Assertions.assertTrue(ingestDir.list().isEmpty(), "ingest file not empty!")
        val okFile = File(processDir,"happy_single/test-upload.csv");
        Assertions.assertTrue(okFile.exists(), "file not put in error!")
        Assertions.assertFalse(File(errorDir,"happy_single").exists(), "incorrect error dir created!")

        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
        var okMessageList = eventMap["ok"]
        Assertions.assertFalse(okMessageList.isNullOrEmpty(), "missing ok message")
        Assertions.assertEquals(1, okMessageList?.size, "too many ok messages")
        
        val okObject = Gson().fromJson(okMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNull((okObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "incorrectly present raw message")
        Assertions.assertNotNull((okObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "no parsed message")
        Assertions.assertNotNull((okObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "no parent metadata")
        Assertions.assertEquals(okFile.absolutePath, okObject["processedPath"])
    }

    @Test
    internal fun happyPath_singleCsvInFolder(){
        println("\n\n--START happyPath_singleCsvInFolder")
        copyToIngest("ingest-folder/test-upload.csv")
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/ingest-folder/test-upload.csv", contentType="text/csv",id="happy_single_folder")
        var message = buildTestMessage(map);
        
        getDecompressor().process(message,mockContext());

        val ingestInnerFolder = File(ingestDir,"ingest-folder");
        Assertions.assertTrue(ingestInnerFolder.list().isEmpty(), "ingest file not empty!")
        ingestInnerFolder.delete()
        Assertions.assertTrue(ingestDir.list().isEmpty(), "ingest file not empty!")
        val okFile = File(processDir,"happy_single_folder/ingest-folder/test-upload.csv");
        Assertions.assertTrue(okFile.exists(), "file not put in error!")
        Assertions.assertFalse(File(errorDir,"happy_single_folder").exists(), "incorrect error dir created!")

        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
        var okMessageList = eventMap["ok"]
        Assertions.assertFalse(okMessageList.isNullOrEmpty(), "missing ok message")
        Assertions.assertEquals(1, okMessageList?.size, "too many ok messages")
        
        val okObject = Gson().fromJson(okMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNull((okObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "incorrectly present raw message")
        Assertions.assertNotNull((okObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "no parsed message")
        Assertions.assertNotNull((okObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "no parent metadata")
        Assertions.assertEquals(okFile.absolutePath, okObject["processedPath"])
    }

    @Test
    internal fun happyPath_zip(){
        println("\n\n--START happyPath_zip")
        copyToIngest("test-upload-zip.zip")
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-upload-zip.zip", contentType="application/zip",id="happy_zip")
        var message = buildTestMessage(map);
        
        getDecompressor().process(message,mockContext());

        Assertions.assertTrue(ingestDir.list().isEmpty(), "ingest file not empty!")
        Assertions.assertFalse(File(errorDir,"happy_zip").exists(), "incorrect error dir created!")
        Assertions.assertTrue(File(processDir,"happy_zip/test-upload-zip.zip").exists(), "file not in processed!")

        val okFileList = listOf(
            File(processDir,"happy_zip/test-upload-zip/test-upload-1.csv"),
            File(processDir,"happy_zip/test-upload-zip/test-upload-2.csv"),
            File(processDir,"happy_zip/test-upload-zip/test-upload-3/test-upload.csv"),
            File(processDir,"happy_zip/test-upload-zip/test-upload-inner/test-upload-4.csv")
        )

        var okFileStringList:MutableList<String> = mutableListOf()
        for(okFile in okFileList){
            Assertions.assertTrue(okFile.exists(), "file not in processed! $okFile")
            okFileStringList.add(okFile.absolutePath)
        }


        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
        var okMessageList = eventMap["ok"]
        Assertions.assertFalse(okMessageList.isNullOrEmpty(), "missing ok message")
        Assertions.assertEquals(4, okMessageList?.size, "incorrect number of ok messages")

        for(okMessage in okMessageList!!){
            val okObject = Gson().fromJson(okMessage, com.google.gson.internal.LinkedTreeMap::class.java)
            Assertions.assertNull((okObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "incorrectly present raw message")
            Assertions.assertNotNull((okObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "no parsed message")
            Assertions.assertNotNull((okObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "no parent metadata")

            Assertions.assertTrue(okFileStringList.remove(okObject["processedPath"]), "bad processed URL ")
        }
        
        Assertions.assertTrue(okFileStringList.isEmpty(), "some URLs not in messages $okFileStringList")
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //bad Azure message
    //theoretically not possible, but put in just in case Azure changes the format of their messages

    @Test
    internal fun negative_message_empty(){
        println("\n\n--START negative_message_empty")
        getDecompressor().process("",mockContext())
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")

        val failObject = Gson().fromJson(failMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "no raw message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "incorrectly present parsed message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "incorrectly present parent metadata")
        Assertions.assertNull(failObject["errorPath"], "incorrectly present error path")
        Assertions.assertEquals("Empty Azure message!",failObject["failReason"])
    }

    @Test
    internal fun negative_message_badFormat(){
        println("\n\n--START negative_message_badFormat")
        getDecompressor().process("][",mockContext());
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")

        val failObject = Gson().fromJson(failMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "no raw message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "incorrectly present parsed message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "incorrectly present parent metadata")
        Assertions.assertNull(failObject["errorPath"], "incorrectly present error path")
        Assertions.assertEquals("Azure message unable to be parsed : com.google.gson.JsonSyntaxException: com.google.gson.stream.MalformedJsonException: Unexpected value at line 1 column 2 path $",failObject["failReason"])
    }

    @Test
    internal fun negative_message_missingEventType(){
        println("\n\n--START negative_message_missingEventType")
        var map = defaultMap(id="no_event_type");
        map.remove("eventType");
        var message = buildTestMessage(map);

        getDecompressor().process(message,mockContext());
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        Assertions.assertTrue(eventMap["fail"].isNullOrEmpty(), "incorrect fail message")
    }

    @Test
    internal fun negative_message_missingId(){
        println("\n\n--START negative_message_missingId")
        var map = defaultMap();
        map.remove("id");
        var message = buildTestMessage(map);

        getDecompressor().process(message,mockContext());
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")

        val failObject = Gson().fromJson(failMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "incorrectly present raw message")
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "no parsed message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "incorrectly present parent metadata")
        Assertions.assertNull(failObject["errorPath"], "incorrectly present error path")
        Assertions.assertEquals("Azure ingest event missing required parameters!",failObject["failReason"])
    }

    @Test
    internal fun negative_message_missingData(){
        println("\n\n--START negative_message_missingData")
        var map = defaultMap();
        map.remove("data");
        var message = buildTestMessage(map);

        getDecompressor().process(message,mockContext());
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")

        val failObject = Gson().fromJson(failMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "incorrectly present raw message")
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "no parsed message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "incorrectly present parent metadata")
        Assertions.assertNull(failObject["errorPath"], "incorrectly present error path")
        Assertions.assertEquals("Azure ingest event missing required parameters!",failObject["failReason"])
    }

    @Test
    internal fun negative_message_missingContentType(){
        println("\n\n--START negative_message_missingContentType")
        var map = defaultMap();
        var dataMap = map["data"] as MutableMap<*,*>;
        dataMap.remove("contentType");
        var message = buildTestMessage(map);

        getDecompressor().process(message,mockContext());
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")

        val failObject = Gson().fromJson(failMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "incorrectly present raw message")
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "no parsed message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "incorrectly present parent metadata")
        Assertions.assertNull(failObject["errorPath"], "incorrectly present error path")
        Assertions.assertEquals("Azure ingest event missing required parameters!",failObject["failReason"])
    }

    @Test
    internal fun negative_message_missingUrl(){
        println("\n\n--START negative_message_missingUrl")
        var map = defaultMap();
        var dataMap = map["data"] as MutableMap<*,*>;
        dataMap.remove("url");
        var message = buildTestMessage(map);

        getDecompressor().process(message,mockContext());
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")

        val failObject = Gson().fromJson(failMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "incorrectly present raw message")
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "no parsed message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "incorrectly present parent metadata")
        Assertions.assertNull(failObject["errorPath"], "incorrectly present error path")
        Assertions.assertEquals("Azure ingest event missing required parameters!",failObject["failReason"])
    }

    @Test
    internal fun negative_message_missingContentLength(){
        println("\n\n--START negative_message_missingContentLength")
        var map = defaultMap();
        var dataMap = map["data"] as MutableMap<*,*>;
        dataMap.remove("contentLength");
        var message = buildTestMessage(map);

        getDecompressor().process(message,mockContext());
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")

        val failObject = Gson().fromJson(failMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "incorrectly present raw message")
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "no parsed message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "incorrectly present parent metadata")
        Assertions.assertNull(failObject["errorPath"], "incorrectly present error path")
        Assertions.assertEquals("Azure ingest event missing required parameters!",failObject["failReason"])
    }

    @Test
    internal fun negative_message_badContentLength(){
        println("\n\n--START negative_message_badContentLength")
        var map = defaultMap(contentLength="DUMMY_CONTENT_LENGTH");
        var message = buildTestMessage(map);

        getDecompressor().process(message,mockContext());
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")

        val failObject = Gson().fromJson(failMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "no raw message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "incorrectly present parsed message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "incorrectly present parent metadata")
        Assertions.assertNull(failObject["errorPath"], "incorrectly present error path")
        Assertions.assertEquals("Azure message unable to be parsed : com.google.gson.JsonSyntaxException: java.lang.NumberFormatException: For input string: \"DUMMY_CONTENT_LENGTH\"",failObject["failReason"])
    }

    @Test
    internal fun negative_message_badUrl(){
        println("\n\n--START negative_message_badUrl")
        var map = defaultMap();
        var message = buildTestMessage(map);

        getDecompressor().process(message,mockContext());
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")

        val failObject = Gson().fromJson(failMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "incorrectly present raw message")
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "no parsed message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "incorrectly present parent metadata")
        Assertions.assertNull(failObject["errorPath"], "incorrectly present error path")
        Assertions.assertEquals("Asure event had bad URL for the file! DUMMY_URL",failObject["failReason"])
    }

    @Test
    internal fun negative_file_missingFile(){
        println("\n\n--START negative_file_missingFile")
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-not-there")
        var message = buildTestMessage(map);

        getDecompressor().process(message,mockContext());
        
        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")

        val failObject = Gson().fromJson(failMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "incorrectly present raw message")
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "no parsed message")
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "incorrectly present parent metadata")
        Assertions.assertNull(failObject["errorPath"], "incorrectly present error path")
        Assertions.assertEquals("File missing in Azure! DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-not-there",failObject["failReason"])
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //bad file

    @Test
    internal fun negative_file_unableToZip(){
        println("\n\n--START negative_file_unableToZip")
        copyToIngest("test-upload.csv")
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-upload.csv", contentType="application/zip",id="bad_zip")
        var message = buildTestMessage(map);
        
        getDecompressor().process(message,mockContext());

        Assertions.assertTrue(ingestDir.list().isEmpty(), "ingest file not empty!")
        Assertions.assertFalse(File(processDir,"bad_zip/test-upload.csv").exists(), "file remained in processed!")
        val errorFile = File(errorDir,"bad_zip/test-upload.csv");
        Assertions.assertTrue(errorFile.exists(), "file not put in error!")

        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")

        val failObject = Gson().fromJson(failMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "incorrectly present raw message")
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "no parsed message")
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "no parent metadata")
        Assertions.assertEquals(errorFile.absolutePath, failObject["errorPath"])
        Assertions.assertEquals("Zipped file was empty: bad_zip//test-upload.csv",failObject["failReason"])
    }

    @Test
    internal fun negative_file_emptyZip(){
        println("\n\n--START negative_file_emptyZip")
        copyToIngest("test-empty.zip")
        var map = defaultMap(url="DUMMY_HTTP://DUMMY_LOCAL/DUMMY_CONTAINER/test-empty.zip", contentType="application/zip",id="empty_zip")
        var message = buildTestMessage(map);
        
        getDecompressor().process(message,mockContext());

        Assertions.assertTrue(ingestDir.list().isEmpty(), "ingest file not empty!")
        Assertions.assertFalse(File(processDir,"empty_zip/test-empty.zip").exists(), "file remained in processed!")
        val errorFile = File(errorDir,"empty_zip/test-empty.zip");
        Assertions.assertTrue(errorFile.exists(), "file not put in error!")

        Assertions.assertTrue(eventMap["ok"].isNullOrEmpty(), "incorrect ok message")
        var failMessageList = eventMap["fail"]
        Assertions.assertFalse(failMessageList.isNullOrEmpty(), "missing fail message")
        Assertions.assertEquals(1, failMessageList?.size, "too many fail messages")

        val failObject = Gson().fromJson(failMessageList!!.get(0), com.google.gson.internal.LinkedTreeMap::class.java)
        Assertions.assertNull((failObject["parent"] as? Map<Any,Any>)?.get("rawMessage"), "incorrectly present raw message")
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("parsedMessage"), "no parsed message")
        Assertions.assertNotNull((failObject["parent"] as? Map<Any,Any>)?.get("parentMetadata"), "no parent metadata")
        Assertions.assertEquals(errorFile.absolutePath, failObject["errorPath"])
        Assertions.assertEquals("Zipped file was empty: empty_zip//test-empty.zip",failObject["failReason"])
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //helper functions


    
    private fun getDecompressor():FnDecompressor{
        val mockBlobService = Mockito.mock(BlobService::class.java)
        val mockEventService = Mockito.mock(EventService::class.java)

        initiateMocks(mockBlobService,mockEventService)
        
        return FnDecompressor(DecompressorContext(mockBlobService,mockEventService,connectionNames,""));
    }

    private fun initiateMocks(mockBlobService:BlobService,mockEventService:EventService){
        Mockito.`when`(mockBlobService.doesBlobExist(Mockito.anyString(),Mockito.anyString())).thenAnswer(this::doesTestFileExist)
        Mockito.`when`(mockBlobService.getBlobMetadata(Mockito.anyString(),Mockito.anyString())).thenAnswer(this::getMetadata)
        Mockito.`when`(mockBlobService.moveBlob(Mockito.anyString(),Mockito.anyString(),Mockito.anyString(),Mockito.anyString())).thenAnswer(this::moveTestFile)
        Mockito.`when`(mockBlobService.getBlobDownloadStream(Mockito.anyString(),Mockito.anyString())).thenAnswer(this::openInputStream)
        Mockito.`when`(mockBlobService.getBlobUploadStream(Mockito.anyString(),Mockito.anyString())).thenAnswer(this::openOutputStream)

        Mockito.`when`(mockEventService.sendOne(Mockito.anyString(),Mockito.anyString())).thenAnswer(this::addEvent)
        Mockito.`when`(mockEventService.sendBatch(Mockito.anyString(),Mockito.anyList())).thenAnswer(this::addEvents)
    }


    private fun copyToIngest(fileName:String){
        var localFileIn = File("src/test/resources/testfiles",fileName);
        var localFileOut = File(ingestDir,fileName);
        
        localFileOut.parentFile.mkdirs()
        localFileIn.copyTo(localFileOut);
    }

    private fun doesTestFileExist(i: InvocationOnMock):Boolean{
        val container:String = i.getArgument(0);
        val relativePath:String = i.getArgument(1);

        var localFile = File(File(outputParentDir, container),relativePath);
        return localFile.exists();
    }
    
    private fun getMetadata(i: InvocationOnMock):Map<String,String>{
        val container:String = i.getArgument(0);
        val relativePath:String = i.getArgument(1);

        var localFile = File(File(outputParentDir, container),relativePath);

        var meta = mapOf("exists" to ""+localFile.exists(), "filepath" to localFile.absolutePath)
        return meta;
    }
    
    private fun moveTestFile(i: InvocationOnMock):String{
        val containerIn:String = i.getArgument(0);
        val relativePathIn:String = i.getArgument(1);
        val containerOut:String = i.getArgument(2);
        val relativePathOut:String = i.getArgument(3);

        var localFileIn = File(File(outputParentDir, containerIn),relativePathIn);
        var localFileOut = File(File(outputParentDir, containerOut),relativePathOut);

        localFileOut.parentFile.mkdirs()

        localFileIn.copyTo(localFileOut);

        localFileIn.delete()

        return localFileOut.absolutePath;
    }
    
    private fun openInputStream(i: InvocationOnMock):InputStream{
        val container:String = i.getArgument(0);
        val relativePath:String = i.getArgument(1);

        var localFile = File(File(outputParentDir, container),relativePath);
        return FileInputStream(localFile);
    }
    
    private fun openOutputStream(i: InvocationOnMock):Pair<OutputStream,String>{
        val container:String = i.getArgument(0);
        val relativePath:String = i.getArgument(1);

        var localFile = File(File(outputParentDir, container),relativePath);
        localFile.parentFile.mkdirs();
        return Pair(FileOutputStream(localFile),localFile.absolutePath);
    }
    
    private fun addEvent(i: InvocationOnMock){
        val hubName:String = i.getArgument(0);
        val message:String = i.getArgument(1);

        commonAddEvent(hubName, listOf(message))
    }
    
    private fun addEvents(i: InvocationOnMock){
        val hubName:String = i.getArgument(0);
        val messages:List<String> = i.getArgument(1);
        
        commonAddEvent(hubName, messages)
    }

    private fun commonAddEvent(hubName:String, messages:List<String>){
        for(message in messages){
            println("[event][$hubName] $message")
        }

        var list = eventMap.getOrDefault(hubName, mutableListOf());
        eventMap.put(hubName, list)
        list.addAll(messages);
    }

    
    fun defaultMap(id:String = "DUMMY_ID",url:String = "DUMMY_URL", contentType:String = "DUMMY_CONTENT_TYPE", contentLength:Any = 0): MutableMap<*,*>{
        val mapData = mutableMapOf("url" to url, "contentType" to contentType, "contentLength" to contentLength, "extraField" to "DUMMY_EXTRA_FIELD");
        return mutableMapOf("eventType" to "Microsoft.Storage.BlobCreated", "id" to id, "data" to mapData, "extraField" to "DUMMY_EXTRA_FIELD");
    }

    fun buildTestMessage(mapObj:Map<*,*>): String { 
        val message = recursiveBuildMessageFromMap(mapObj);
        return "[$message]"
    }

    private fun recursiveBuildMessageFromMap(map:Map<*,*>):String{
        return map.map { (k,v)-> 
            var value = if(v is Map<*,*>){recursiveBuildMessageFromMap(v)}else if(v is String){"\"$v\""}else{"$v"};
            var combined="\"$k\":$value";
            combined
        }
        .joinToString(separator=",",prefix="{",postfix="}") 
    }

    fun mockContext(): ExecutionContext{
        val mockContext : ExecutionContext = Mockito.mock(ExecutionContext::class.java);
        val logger : Logger = Mockito.mock(Logger::class.java);

        Mockito.`when`(mockContext.logger).thenReturn(logger)
        Mockito.`when`(logger.info(Mockito.anyString())).thenAnswer(::loggerInvocation)
        Mockito.`when`(logger.warning(Mockito.anyString())).thenAnswer(::loggerInvocation)
        return mockContext
    }

    private fun loggerInvocation(i: InvocationOnMock){
        val toLog:String = i.getArgument(0);
        println("[log] $toLog");
    }
}